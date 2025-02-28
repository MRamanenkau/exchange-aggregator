pub(crate) mod parser;

pub(crate) use chrono::Utc;
use tokio::sync::mpsc;
use std::sync::Arc;
use std::error::Error;
use std::fmt::format;
use crate::rest_client::{ReqwestClient, RestClient};
use crate::db::Database;
use crate::exchange::parser::KlineParser;

const LIMIT: i64 = 500;
const PARALLEL_REQUESTS: usize = 10;

pub struct Exchange<'a, P: KlineParser> {
rest_url: String,
    rest_client: Arc<Box<dyn RestClient>>,
    db: Arc<&'a Database>,
    parser: P,
}

impl<'a, P: KlineParser> Exchange<'a, P> {
pub async fn collect_klines(
    &self,
    pairs: Vec<String>,
    intervals: Vec<String>,
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
        let rest_client = Arc::clone(&self.rest_client);

        tokio::spawn(async move {
            for endpoint in endpoints {
                let tx = tx.clone();
                let rest_client = Arc::clone(&rest_client);
                tokio::spawn(async move {
                    if let Ok(resp) = rest_client.get(&endpoint).await {
                        if let Ok(data) = serde_json::from_str::<Vec<Vec<String>>>(&resp) {
                            let _ = tx.send(data).await;
                        }
                    }
                });
            }
        });

        while let Some(raw_data) = rx.recv().await {
            let klines = self.parser.parse(pair, interval, raw_data)?;
            for kline in klines {
                self.db.save(&kline).await?;
            }
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

pub struct ExchangeBuilder<'a, P: KlineParser> {
base_url: Option<String>,
    rest_client: Option<ReqwestClient>,
    db: Option<Arc<&'a Database>>,
    parser: Option<P>,
}

impl<'a, P: KlineParser> ExchangeBuilder<'a, P> {
pub fn new() -> Self {
    Self {
        base_url: None,
        rest_client: None,
        db: None,
        parser: None,
    }
}

    pub fn set_base_url(mut self, base_url: String) -> Self {
        self.base_url = Some(base_url);
        self
    }

    pub fn set_rest_client(mut self, client: ReqwestClient) -> Self {
        self.rest_client = Some(client);
        self
    }

    pub fn set_db(mut self, db: Arc<&'a Database>) -> Self {
        self.db = Some(db);
        self
    }

    pub fn set_parser(mut self, parser: P) -> Self {
        self.parser = Some(parser);
        self
    }

    pub async fn build(self) -> Result<Exchange<'a, P>, Box<dyn Error>> {
        let rest_url = self.base_url.ok_or(ExchangeBuilderError::MissingRestUrl)?;
        let rest_client = self.rest_client.ok_or(ExchangeBuilderError::MissingRestClient)?;
        let db = self.db.ok_or(ExchangeBuilderError::MissingDB)?;
        let parser = self.parser.ok_or_else(|| Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Missing parser")))?;
        Ok(Exchange {
            rest_url,
            rest_client: Arc::new(Box::new(rest_client) as Box<dyn RestClient>),
            db,
            parser,
        })
    }
}