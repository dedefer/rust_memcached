use serde::Deserialize;
use config::{Environment, Config, ConfigError};
use duration_string::DurationString;

use config;

#[derive(Deserialize)]
pub struct Settings {
    pub memory_limit: u64,
    pub gc_interval: DurationString,
    pub addr: String,
}

impl Settings {
    pub fn new() -> Result<Settings, ConfigError> {
        let mut cfg = Config::new();
        cfg.merge(
            Environment::with_prefix("memcached")
        )?
        .set_default("memory_limit", 1 << 20)?
        .set_default("gc_interval", "100ms")?
        .set_default("addr", "0.0.0.0:8080")?;

        cfg.try_into()
    }
}
