//! `merezha identity password-benchmark`: time Argon2id with a set of
//! parameters on this machine, so operators can tune
//! `[modules.identity.password]` (section 7: aim for roughly 60 to 120 ms
//! per hash on production cores).
use crate::hasher::{Cost, hash_blocking};
use merezha_domain::Failure;
use std::time::{Duration, Instant};

/// Most samples one run takes.
pub const MAX_SAMPLES: u32 = 100;

/// The timings of [`run`].
#[derive(Clone, Debug, PartialEq)]
pub struct Benchmark {
    pub cost: Cost,
    pub samples: u32,
    pub min: Duration,
    pub median: Duration,
    pub max: Duration,
}

/// Hash a fixed password `samples` times with `cost` on the calling thread.
pub fn run(cost: Cost, samples: u32) -> Result<Benchmark, Failure> {
    if samples == 0 || samples > MAX_SAMPLES {
        return Err(Failure::permanent("invalid_arguments"));
    }
    let salt = *b"merezha-benchmrk";
    let mut timings = Vec::with_capacity(samples as usize);
    for _ in 0..samples {
        let started = Instant::now();
        hash_blocking(cost, b"benchmark password", &salt)?;
        timings.push(started.elapsed());
    }
    timings.sort();
    Ok(Benchmark {
        cost,
        samples,
        min: timings[0],
        median: timings[timings.len() / 2],
        max: timings[timings.len() - 1],
    })
}

impl Benchmark {
    /// The JSON report the command prints.
    pub fn json(&self) -> serde_json::Value {
        let ms = |d: Duration| (d.as_secs_f64() * 1000.0 * 10.0).round() / 10.0;
        serde_json::json!({
            "status": "ok",
            "algorithm": "argon2id",
            "argon2_memory_kib": self.cost.memory_kib,
            "argon2_iterations": self.cost.iterations,
            "argon2_parallelism": self.cost.parallelism,
            "samples": self.samples,
            "min_ms": ms(self.min),
            "median_ms": ms(self.median),
            "max_ms": ms(self.max),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_benchmark_reports_ordered_timings() {
        let cost = Cost {
            memory_kib: 19_456,
            iterations: 2,
            parallelism: 1,
        };
        let report = run(cost, 3).unwrap();
        assert!(report.min <= report.median && report.median <= report.max);
        assert_eq!(report.json()["argon2_memory_kib"], 19_456);
        assert_eq!(run(cost, 0).unwrap_err().code, "invalid_arguments");
    }
}
