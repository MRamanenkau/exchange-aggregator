use dotenvy::dotenv;
use std::env;

pub struct Config {
    pub base_url: String,
    pub kline_endpoint: String,
    pub start_time: i64,
    pub pairs: Vec<String>,
    pub timeframes: Vec<String>,

    pub db_host: String,
    pub db_port: String,
    pub db_username: String,
}

impl Config {
    pub fn get() -> Self {
        dotenv().ok();

        Self {
            base_url: env::var("BASE_URL")
                .expect("BASE_URL must be set"),
            kline_endpoint: env::var("KLINE_ENDPOINT")
                .expect("KLINE_ENDPOINT must be set"),
            start_time: env::var("START_TIME")
                .expect("START_TIME must be set")
                .parse::<i64>()
                .expect("START_TIME must be a valid i64"),
            pairs: env::var("PAIRS")
                .expect("PAIRS must be set")
                .split(',')
                .map(|s| s.trim().to_string())
                .collect(),
            timeframes: env::var("TIMEFRAMES")
                .expect("PAIRS must be set")
                .split(',')
                .map(|t| t.trim().to_string())
                .collect(),
            db_host: env::var("DB_HOST").expect("DB_HOST must be set"),
            db_port: env::var("DB_PORT").expect("DB_PORT must be set"),
            db_username: env::var("DB_USERNAME").expect("DB_USERNAME must be set"),
        }
    }
}
