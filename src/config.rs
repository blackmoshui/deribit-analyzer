use anyhow::{Context, Result};

#[derive(Debug, Clone)]
pub struct Config {
    pub client_id: String,
    pub client_secret: String,
    pub ws_url: String,
    pub alert_threshold: f64,
    pub heartbeat_interval: u64,
    pub db_path: String,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        dotenvy::dotenv().ok();

        let env = std::env::var("DERIBIT_ENV").unwrap_or_else(|_| "test".to_string());
        let ws_url = match env.as_str() {
            "prod" => "wss://www.deribit.com/ws/api/v2".to_string(),
            _ => "wss://test.deribit.com/ws/api/v2".to_string(),
        };

        Ok(Config {
            client_id: std::env::var("DERIBIT_CLIENT_ID").context("DERIBIT_CLIENT_ID not set")?,
            client_secret: std::env::var("DERIBIT_CLIENT_SECRET")
                .context("DERIBIT_CLIENT_SECRET not set")?,
            ws_url,
            alert_threshold: std::env::var("ALERT_THRESHOLD")
                .unwrap_or_else(|_| "0.005".to_string())
                .parse()
                .context("Invalid ALERT_THRESHOLD")?,
            heartbeat_interval: 30,
            db_path: std::env::var("DB_PATH").unwrap_or_else(|_| "deribit.db".to_string()),
        })
    }
}
