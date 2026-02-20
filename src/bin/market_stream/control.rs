use std::time::{Duration, Instant};

use crate::cli::Config;

pub(crate) fn should_stop(started_at: Instant, iteration: u64, config: &Config) -> bool {
    if let Some(max_iterations) = config.iterations {
        if iteration >= max_iterations {
            return true;
        }
    }

    if let Some(duration_secs) = config.duration_secs {
        if started_at.elapsed() >= Duration::from_secs(duration_secs) {
            return true;
        }
    }

    false
}
