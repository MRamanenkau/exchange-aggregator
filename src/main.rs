mod db;
mod rest_client;
mod exchange;

use tarantool::{Client, ClientConfig, Tuple};
use serde::{Serialize, Deserialize};
use std::env;
use thiserror::Error;
use tokio::sync::OnceCell;
use crate::exchange::ExchangeBuilder;
use crate::rest_client::{ReqwestClient, RestClient};

const PAIRS: [&str; 5] = ["BTC_USDT", "TRX_USDT", "ETH_USDT", "DOGE_USDT", "BCH_USDT"];
const INTERVALS: [&str; 4] = ["MINUTE_5", "MINUTE_15", "HOUR_1", "DAY_1"];

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env::set_var("TARANTOOL_HOST", "localhost");
    env::set_var("TARANTOOL_PORT", "3301");
    env::set_var("TARANTOOL_USERNAME", "guest");

    let db = db::Database::get().await?;

    println!("Discovered spaces: {:?}", db.list_spaces());

    let exchange = ExchangeBuilder::new()
        .set_rest_url("https://api.poloniex.com/markets")
        .set_rest_client(ReqwestClient::new())
        .set_parser(PoloniexKlineParser)
        .set_db(db)
        .build()
        .await?;

    let start_date = 1704067200000; // Jan 1, 2025, 00:00:00 UTC

    exchange.collect_klines(pairs, intervals, start_date).await?;
    Ok(())
}