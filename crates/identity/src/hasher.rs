//! Argon2id hashing and verification (section 7) off the executor threads.
//!
//! Every hash and verification takes one permit of a semaphore of
//! `hash_concurrency` permits and runs on `spawn_blocking`, so memory is
//! bounded by `permits × m`. Waiting longer than `hash_queue_timeout`
//! fails transient `overloaded` (503 with `Retry-After`). Unknown accounts
//! verify against a dummy PHC string computed at startup with the
//! configured parameters, so every sign-in path costs one verification.
use crate::{config::PasswordSettings, metrics::Metrics, password::Password};
use argon2::{
    Algorithm, Argon2, Params, Version,
    password_hash::{PasswordHasher as _, PasswordVerifier as _, phc::PasswordHash},
};
use merezha_domain::Failure;
use merezha_platform::Random;
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;

/// Bytes of salt per hash.
const SALT_BYTES: usize = 16;
/// Bytes of output per hash.
const OUTPUT_BYTES: usize = 32;

/// The Argon2id parameters of a configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Cost {
    pub memory_kib: u32,
    pub iterations: u32,
    pub parallelism: u32,
}
impl Cost {
    pub fn of(settings: &PasswordSettings) -> Self {
        Self {
            memory_kib: settings.memory_kib,
            iterations: settings.iterations,
            parallelism: settings.parallelism,
        }
    }
    fn params(&self) -> Result<Params, Failure> {
        Params::new(
            self.memory_kib,
            self.iterations,
            self.parallelism,
            Some(OUTPUT_BYTES),
        )
        .map_err(|_| Failure::permanent("invalid_password_params"))
    }
}

/// Hash with `cost` on the calling thread: the benchmark and startup use it.
pub fn hash_blocking(cost: Cost, password: &[u8], salt: &[u8]) -> Result<String, Failure> {
    let argon = Argon2::new(Algorithm::Argon2id, Version::V0x13, cost.params()?);
    argon
        .hash_password_with_salt(password, salt)
        .map(|hash| hash.to_string())
        .map_err(|_| Failure::permanent("password_hash_failed"))
}

/// Verify on the calling thread. A malformed PHC string never matches.
fn verify_blocking(password: &[u8], phc: &str) -> bool {
    let Ok(parsed) = PasswordHash::new(phc) else {
        return false;
    };
    // The parameters come from the PHC string itself.
    Argon2::default().verify_password(password, &parsed).is_ok()
}

/// Whether `phc` was made with other parameters than `cost`.
pub fn needs_rehash(phc: &str, cost: Cost) -> bool {
    let Ok(parsed) = PasswordHash::new(phc) else {
        return true;
    };
    let Ok(params) = Params::try_from(&parsed) else {
        return true;
    };
    parsed.algorithm.as_str() != "argon2id"
        || parsed.version != Some(0x13)
        || params.m_cost() != cost.memory_kib
        || params.t_cost() != cost.iterations
        || params.p_cost() != cost.parallelism
}

/// Counters the tests read: every sign-in path must cost exactly one
/// verification.
#[derive(Debug, Default)]
pub struct HashCounts {
    pub hashes: AtomicU64,
    pub verifications: AtomicU64,
}

/// The hasher a running identity block uses.
pub struct Hasher {
    cost: Cost,
    permits: Arc<Semaphore>,
    /// Admission of failure-charged checks: a request holds one permit from
    /// its rate-limit peek to its charge, so at most `hash_concurrency`
    /// attempts pass a nearly empty bucket at once (section 7).
    gate: Arc<Semaphore>,
    queue_timeout: Duration,
    dummy: String,
    counts: Arc<HashCounts>,
    metrics: Metrics,
}
impl std::fmt::Debug for Hasher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Hasher")
            .field("cost", &self.cost)
            .field("permits", &self.permits.available_permits())
            .finish_non_exhaustive()
    }
}
impl Hasher {
    /// Build the hasher and its dummy PHC string. Blocking (one hash): call
    /// it from `spawn_blocking`.
    pub fn new(
        settings: &PasswordSettings,
        random: &dyn Random,
        metrics: Metrics,
    ) -> Result<Self, Failure> {
        let cost = Cost::of(settings);
        let permits = match settings.hash_concurrency {
            0 => std::thread::available_parallelism().map_or(1, |n| n.get()),
            n => n,
        };
        let mut salt = [0u8; SALT_BYTES];
        random.fill(&mut salt)?;
        let mut filler = [0u8; 32];
        random.fill(&mut filler)?;
        let dummy = hash_blocking(cost, &filler, &salt)?;
        Ok(Self {
            cost,
            permits: Arc::new(Semaphore::new(permits)),
            gate: Arc::new(Semaphore::new(permits)),
            queue_timeout: settings.hash_queue_timeout,
            dummy,
            counts: Arc::new(HashCounts::default()),
            metrics,
        })
    }
    pub fn cost(&self) -> Cost {
        self.cost
    }
    pub fn counts(&self) -> &Arc<HashCounts> {
        &self.counts
    }
    /// Wait for a permit within the queue timeout.
    async fn permit(&self) -> Result<tokio::sync::OwnedSemaphorePermit, Failure> {
        let started = Instant::now();
        let permit = tokio::time::timeout(self.queue_timeout, self.permits.clone().acquire_owned())
            .await
            .map_err(|_| Failure::transient("overloaded"))?
            .map_err(|_| Failure::transient("overloaded"))?;
        self.metrics.hash_queue_wait(started.elapsed());
        Ok(permit)
    }
    /// Admission for one peek-verify-charge sequence, within the queue
    /// timeout (`overloaded` otherwise). Hold it until the failure is
    /// charged.
    pub async fn admit(&self) -> Result<tokio::sync::OwnedSemaphorePermit, Failure> {
        tokio::time::timeout(self.queue_timeout, self.gate.clone().acquire_owned())
            .await
            .map_err(|_| Failure::transient("overloaded"))?
            .map_err(|_| Failure::transient("overloaded"))
    }
    /// A new PHC string for `password` with a fresh salt.
    pub async fn hash(&self, password: &Password, random: &dyn Random) -> Result<String, Failure> {
        let mut salt = [0u8; SALT_BYTES];
        random.fill(&mut salt)?;
        let permit = self.permit().await?;
        let cost = self.cost;
        let bytes = zeroize::Zeroizing::new(password.expose().as_bytes().to_vec());
        self.counts.hashes.fetch_add(1, Ordering::SeqCst);
        let started = Instant::now();
        let result = merezha_telemetry::spawn_blocking_in_span(move || {
            let _permit = permit;
            hash_blocking(cost, &bytes, &salt)
        })
        .await
        .map_err(|_| Failure::transient("internal"))?;
        self.metrics.hash_duration(started.elapsed());
        result
    }
    /// Verify `password` against `phc`, or against the dummy string when the
    /// account has no password, so the cost is the same either way.
    pub async fn verify(&self, password: &Password, phc: Option<&str>) -> Result<bool, Failure> {
        let permit = self.permit().await?;
        let (target, real) = match phc {
            Some(phc) => (phc.to_owned(), true),
            None => (self.dummy.clone(), false),
        };
        let bytes = zeroize::Zeroizing::new(password.expose().as_bytes().to_vec());
        self.counts.verifications.fetch_add(1, Ordering::SeqCst);
        let started = Instant::now();
        let matched = merezha_telemetry::spawn_blocking_in_span(move || {
            let _permit = permit;
            verify_blocking(&bytes, &target)
        })
        .await
        .map_err(|_| Failure::transient("internal"))?;
        self.metrics.hash_duration(started.elapsed());
        Ok(matched && real)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cheap() -> PasswordSettings {
        PasswordSettings {
            memory_kib: PasswordSettings::MIN_MEMORY_KIB,
            iterations: 2,
            hash_concurrency: 1,
            hash_queue_timeout: Duration::from_millis(50),
            ..PasswordSettings::default()
        }
    }

    #[tokio::test]
    async fn hashes_verify_and_unknown_accounts_cost_one_verification() {
        let hasher =
            Hasher::new(&cheap(), &merezha_platform::OsRandom, Metrics::disabled()).unwrap();
        let password = Password::new("correct horse battery").unwrap();
        let phc = hasher
            .hash(&password, &merezha_platform::OsRandom)
            .await
            .unwrap();
        assert!(phc.starts_with("$argon2id$v=19$m=19456,t=2,p=1$"), "{phc}");
        assert!(!phc.contains("correct"));
        assert!(hasher.verify(&password, Some(&phc)).await.unwrap());
        let wrong = Password::new("incorrect horse").unwrap();
        assert!(!hasher.verify(&wrong, Some(&phc)).await.unwrap());
        // The dummy never matches, even for its own filler.
        assert!(!hasher.verify(&password, None).await.unwrap());
        assert!(!hasher.verify(&password, Some("not a phc")).await.unwrap());
        assert_eq!(hasher.counts().verifications.load(Ordering::SeqCst), 4);
        assert_eq!(hasher.counts().hashes.load(Ordering::SeqCst), 1);
        assert!(!needs_rehash(&phc, hasher.cost()));
        let stronger = Cost {
            iterations: 3,
            ..hasher.cost()
        };
        assert!(needs_rehash(&phc, stronger));
        assert!(needs_rehash("garbage", stronger));
    }

    #[tokio::test]
    async fn a_saturated_queue_is_overloaded_within_the_timeout() {
        let hasher = Arc::new(
            Hasher::new(&cheap(), &merezha_platform::OsRandom, Metrics::disabled()).unwrap(),
        );
        let held = hasher.permits.clone().acquire_owned().await.unwrap();
        let started = Instant::now();
        let password = Password::new("correct horse battery").unwrap();
        let failure = hasher.verify(&password, None).await.unwrap_err();
        assert_eq!(failure.code, "overloaded");
        assert!(started.elapsed() < Duration::from_secs(1));
        drop(held);
        assert!(!hasher.verify(&password, None).await.unwrap());
    }
}
