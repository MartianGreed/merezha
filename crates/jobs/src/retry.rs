//! The retry policy (AP §B.6): what a handler outcome makes of a job, and
//! the equal-jitter exponential backoff before its next attempt.
use merezha_domain::{Classification, Failure};
use merezha_platform::{Random, config::JobDefaults, ports::JobFailure};
use std::time::Duration;

/// How one attempt ended, before the policy decides.
#[derive(Debug)]
pub(crate) enum Outcome {
    Succeeded,
    Failed(JobFailure),
    /// The handler task panicked (`JoinError::is_panic`): poison.
    Panicked,
    /// The stored payload is not JSON `serde_json` reads back.
    Undecodable,
    /// The handler ran past the queue's `timeout_ms`.
    TimedOut,
}

/// The next state of a job, written by the fenced settle statement.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Settlement {
    Succeeded,
    /// Back to `available` after `delay`.
    Retry {
        delay: Duration,
        code: String,
        class: &'static str,
    },
    Dead {
        code: String,
        class: &'static str,
    },
}
impl Settlement {
    /// The `state` column value.
    pub(crate) fn state(&self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Retry { .. } => "available",
            Self::Dead { .. } => "dead",
        }
    }
}

pub(crate) fn class_name(classification: Classification) -> &'static str {
    match classification {
        Classification::Permanent => "permanent",
        Classification::Transient => "transient",
        Classification::Conflict => "conflict",
    }
}

/// The B.6 table. `attempt` is the attempt that just ran (1-based, already
/// counted at lease time).
pub(crate) fn decide(
    outcome: Outcome,
    attempt: u32,
    max_attempts: u32,
    settings: &JobDefaults,
    random: &dyn Random,
) -> Settlement {
    let (failure, retry_after) = match outcome {
        Outcome::Succeeded => return Settlement::Succeeded,
        Outcome::Panicked => {
            return Settlement::Dead {
                code: "handler_panic".into(),
                class: "permanent",
            };
        }
        Outcome::Undecodable => {
            return Settlement::Dead {
                code: "invalid_job_payload".into(),
                class: "permanent",
            };
        }
        Outcome::TimedOut => (Failure::transient("timeout"), None),
        Outcome::Failed(failure) => (
            Failure::new(&failure.code, failure.classification),
            failure.retry_after,
        ),
    };
    let class = class_name(failure.classification);
    if failure.classification == Classification::Permanent || attempt >= max_attempts {
        return Settlement::Dead {
            code: failure.code,
            class,
        };
    }
    Settlement::Retry {
        delay: backoff(attempt, settings, retry_after, random),
        code: failure.code,
        class,
    }
}

/// Equal jitter: half the capped exponential delay plus a uniform share of
/// the other half, so it never collapses to zero; at least the handler's
/// `retry_after`, itself capped at `backoff_max`. A failing random source
/// takes the upper bound rather than a predictable spread.
pub(crate) fn backoff(
    attempt: u32,
    settings: &JobDefaults,
    retry_after: Option<Duration>,
    random: &dyn Random,
) -> Duration {
    let max = settings.backoff_max;
    // At most 2^63 times a base of at most a day: finite in f64.
    let doublings = attempt.saturating_sub(1).min(63) as i32;
    let base = Duration::from_secs_f64(
        (settings.backoff_base.as_secs_f64() * 2f64.powi(doublings)).min(max.as_secs_f64()),
    );
    let half = base / 2;
    let mut bytes = [0u8; 8];
    let share = match random.fill(&mut bytes) {
        Ok(()) => u64::from_le_bytes(bytes) as f64 / u64::MAX as f64,
        Err(_) => 1.0,
    };
    let delay = half + (base - half).mul_f64(share);
    match retry_after {
        Some(after) => delay.max(after.min(max)),
        None => delay,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixed(u64);
    impl Random for Fixed {
        fn fill(&self, bytes: &mut [u8]) -> Result<(), Failure> {
            bytes.copy_from_slice(&self.0.to_le_bytes()[..bytes.len()]);
            Ok(())
        }
    }
    /// Within a microsecond: the delay is computed in f64 seconds.
    fn near(a: Duration, b: Duration) -> bool {
        a.abs_diff(b) <= Duration::from_micros(1)
    }
    fn settings(base_ms: u64, max_ms: u64) -> JobDefaults {
        JobDefaults {
            backoff_base: Duration::from_millis(base_ms),
            backoff_max: Duration::from_millis(max_ms),
            ..JobDefaults::default()
        }
    }

    #[test]
    fn equal_jitter_stays_between_half_and_the_full_capped_delay() {
        let policy = settings(1_000, 3_600_000);
        for attempt in 1..=12u32 {
            let full = Duration::from_millis(1_000 * 2u64.pow(attempt - 1)).min(policy.backoff_max);
            assert!(near(backoff(attempt, &policy, None, &Fixed(0)), full / 2));
            assert!(near(
                backoff(attempt, &policy, None, &Fixed(u64::MAX)),
                full
            ));
        }
        // Far past the cap, including shifts that would overflow.
        for attempt in [13, 40, 64, 100, u32::MAX] {
            let delay = backoff(attempt, &policy, None, &merezha_platform::OsRandom);
            assert!(delay >= policy.backoff_max / 2 - Duration::from_micros(1));
            assert!(delay <= policy.backoff_max + Duration::from_micros(1));
        }
        // `retry_after` raises the floor, capped at backoff_max.
        let hint = Some(Duration::from_secs(30));
        assert_eq!(
            backoff(1, &policy, hint, &Fixed(0)),
            Duration::from_secs(30)
        );
        let capped = settings(1_000, 10_000);
        assert_eq!(
            backoff(1, &capped, Some(Duration::from_secs(600)), &Fixed(0)),
            Duration::from_secs(10)
        );
    }

    #[test]
    fn the_retry_table_follows_the_classification() {
        let policy = settings(20, 1_000);
        let random = Fixed(0);
        let decide = |outcome, attempt| decide(outcome, attempt, 3, &policy, &random);
        assert_eq!(decide(Outcome::Succeeded, 3), Settlement::Succeeded);
        assert_eq!(
            decide(Outcome::Failed(JobFailure::permanent("bad_address")), 1),
            Settlement::Dead {
                code: "bad_address".into(),
                class: "permanent"
            }
        );
        match decide(Outcome::Failed(JobFailure::transient("busy", None)), 2) {
            Settlement::Retry { delay, code, class } => {
                assert!(near(delay, Duration::from_millis(20)), "{delay:?}");
                assert_eq!((code.as_str(), class), ("busy", "transient"));
            }
            other => panic!("{other:?}"),
        }
        let conflict = JobFailure::from(Failure::conflict("database_conflict"));
        assert_eq!(decide(Outcome::Failed(conflict), 3).state(), "dead");
        assert_eq!(
            decide(Outcome::Panicked, 1),
            Settlement::Dead {
                code: "handler_panic".into(),
                class: "permanent"
            }
        );
        assert_eq!(
            decide(Outcome::Undecodable, 1),
            Settlement::Dead {
                code: "invalid_job_payload".into(),
                class: "permanent"
            }
        );
        match decide(Outcome::TimedOut, 1) {
            Settlement::Retry { code, class, .. } => {
                assert_eq!((code.as_str(), class), ("timeout", "transient"))
            }
            other => panic!("{other:?}"),
        }
    }
}
