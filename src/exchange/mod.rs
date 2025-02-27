mod parser;

use chrono::{Duration, Utc};
use serde::Deserialize;
use tokio::sync::mpsc;
use rusqlite::{params, Connection};
use std::sync::Arc;
use std::error::Error;
use crate::rest_client::RestClient;

const PAIRS: [&str; 5] = ["BTC_USDT", "TRX_USDT", "ETH_USDT", "DOGE_USDT", "BCH_USDT"];
const INTERVALS: [&str; 4] = ["MINUTE_5", "MINUTE_15", "HOUR_1", "DAY_1"];
const LIMIT: i64 = 500;
const PARALLEL_REQUESTS: usize = 10;

// Raw candle data from Poloniex API
#[derive(Debug, Deserialize)]
struct KlineRaw(
    String, String, String, String, // low, high, open, close
    String, String, String, String, // amount, quantity, buyTakerAmount, buyTakerQuantity
    i64, i64, String, String,       // tradeCount, ts, weightedAverage, interval
    i64, i64                        // startTime, closeTime
);

pub struct Exchange<P: KlineParser> {
    rest_url: String,
    rest_client: Box<dyn RestClient>,
    db: Arc<Connection>,
    parser: P,
}

impl<P: KlineParser> Exchange<P> {
    pub async fn collect_klines(
        &self,
        pairs: Vec<&str>,
        intervals: Vec<&str>,
        start_date: i64
    ) -> Result<(), Box<dyn Error>> {
        for pair in &pairs {
            for interval in &intervals {
                let endpoints = self.build_endpoints(pair, interval, start_date);
                self.process_endpoints(pair, interval, endpoints).await?;
            }
        }
        Ok(())
    }

    fn build_endpoints(&self, pair: &str, interval: &str, start_date: i64) -> Vec<String> {
        let now = Utc::now().timestamp_millis();
        let interval_ms = match interval {
            "MINUTE_5" => 5 * 60 * 1000,
            "MINUTE_15" => 15 * 60 * 1000,
            "HOUR_1" => 60 * 60 * 1000,
            "DAY_1" => 24 * 60 * 60 * 1000,
            _ => unreachable!(),
        };

        let mut endpoints = Vec::new();
        let mut current_start = start_date;

        while current_start < now {
            let end_time = current_start + (interval_ms * LIMIT) - 1;
            let url = format!(
                "{}/{}/candles?interval={}&startTime={}&endTime={}&limit={}",
                self.rest_url, pair, interval, current_start, end_time.min(now), LIMIT
            );
            endpoints.push(url);
            current_start = end_time + 1;
        }

        endpoints
    }

    async fn process_endpoints(&self, pair: &str, interval: &str, endpoints: Vec<String>) -> Result<(), Box<dyn Error>> {
        let (tx, mut rx) = mpsc::channel(PARALLEL_REQUESTS);

        tokio::spawn({
            let client = self.rest_client.clone();
            let tx = tx.clone();
            async move {
                for endpoint in endpoints {
                    let client = client.clone();
                    let tx = tx.clone();
                    tokio::spawn(async move {
                        if let Ok(resp) = client.get(&endpoint).send().await {
                            if let Ok(data) = resp.json::<Vec<Vec<String>>>().await {
                                let _ = tx.send(data).await; // Send each chunk as it’s ready
                            }
                        }
                    });
                }
            }
        });

        // Process each chunk as it arrives
        while let Some(raw_data) = rx.recv().await {
            let klines = self.parser.parse(pair, interval, vec![raw_data])?;
            self.store(klines).await?;
        }

        Ok(())
    }

    fn parse(&self, pair: &str, interval: &str, raw_data: Vec<Vec<CandleRaw>>) -> Result<Vec<Kline>, Box<dyn Error>> {
        let time_frame = match interval {
            "MINUTE_5" => "5m".to_string(),
            "MINUTE_15" => "15m".to_string(),
            "HOUR_1" => "1h".to_string(),
            "DAY_1" => "1d".to_string(),
            _ => unreachable!(),
        };

        let mut klines = Vec::new();

        for batch in raw_data {
            for candle in batch {
                let total_base = candle.5.parse::<f64>()?;
                let total_quote = candle.4.parse::<f64>()?;
                let buy_base = candle.7.parse::<f64>()?;
                let buy_quote = candle.6.parse::<f64>()?;

                let kline = Kline {
                    pair: pair.to_string(),
                    time_frame: time_frame.clone(),
                    l: candle.0.parse()?,
                    h: candle.1.parse()?,
                    o: candle.2.parse()?,
                    c: candle.3.parse()?,
                    utc_begin: candle.12,
                    volume_bs: VBS {
                        buy_base,
                        sell_base: total_base - buy_base,
                        buy_quote,
                        sell_quote: total_quote - buy_quote,
                    },
                };
                klines.push(kline);
            }
        }

        Ok(klines)
    }

    async fn store(&self, klines: Vec<Kline>) -> Result<(), Box<dyn Error>> {
        for kline in klines {
            self.db.save(&kline).await?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum ExchangeBuilderError {
    MissingRestUrl,
    MissingRestClient,
    MissingDB,
}

impl std::fmt::Display for ExchangeBuilderError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::MissingRestUrl => write!(f, "Missing REST URL"),
            Self::MissingRestClient => write!(f, "Missing REST client"),
            Self::MissingDB => write!(f, "Missing database"),
        }
    }
}

impl Error for ExchangeBuilderError {}

pub struct ExchangeBuilder<P: KlineParser> {
    rest_url: Option<String>,
    rest_client: Option<reqwest::Client>,
    db: Option<Arc<Connection>>,
    parser: Option<P>,
}

impl<P: KlineParser> ExchangeBuilder<P> {
    pub fn new() -> Self {
        Self {
            rest_url: None,
            rest_client: None,
            db: None,
            parser: None,
        }
    }

    pub fn set_rest_url(mut self, rest_url: &str) -> Self {
        self.rest_url = Some(rest_url.to_string());
        self
    }

    pub fn set_rest_client(mut self, client: reqwest::Client) -> Self {
        self.rest_client = Some(client);
        self
    }

    pub fn set_db(mut self, db: Connection) -> Self {
        self.db = Some(Arc::new(db));
        self
    }

    pub fn set_parser(mut self, parser: P) -> Self {
        self.parser = Some(parser);
        self
    }

    pub async fn build(self) -> Result<Exchange<P>, Box<dyn Error>> {
        let rest_url = self.rest_url.ok_or(ExchangeBuilderError::MissingRestUrl)?;
        let rest_client = self.rest_client.ok_or(ExchangeBuilderError::MissingRestClient)?;
        let db = self.db.ok_or(ExchangeBuilderError::MissingDB)?;
        let parser = self.parser.ok_or_else(|| Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Missing parser")))?;
        Ok(Exchange { rest_url, rest_client, db, parser })
    }
}