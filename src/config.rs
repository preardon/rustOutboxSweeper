use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub database_url: Option<String>,
    #[serde(default = "default_sweep_interval")]
    pub sweep_interval_ms: u64,
    pub aws_region: String,
    pub batch_size: i32,
}

fn default_sweep_interval() -> u64 {
    5000 // Default to 5 seconds
}

impl Config {
    pub fn load() -> Result<Self, envy::Error> {
        dotenvy::dotenv().ok();

        let config = envy::from_env::<Config>()?;

        // Manually check that DATABASE_URL was loaded for the main app
        if config.database_url.is_none() {
            return Err(envy::Error::MissingValue("DATABASE_URL"));
        }

        Ok(config)
    }

    #[cfg(test)] // Only compile this function when running tests
    pub fn load_test() -> Result<Self, envy::Error> {
        dotenvy::from_filename_override(".env.test").ok();

        envy::from_env::<Config>()
    }

    /// Returns the database URL.
    ///
    /// # Panics
    /// Panics if the database_url is not set. This should only be
    /// called after `load()` which validates it.
    pub fn database_url(&self) -> &str {
        self.database_url
            .as_deref()
            .expect("DATABASE_URL is not set")
    }
}
