mod db;
mod rest_client;
mod exchange;
mod config;

use std::env;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::OnceCell;
use crate::config::Config;
use crate::db::{Database, DbConfigBuilder};
use crate::exchange::ExchangeBuilder;
use crate::exchange::parser::PoloniexKlineParser;
use crate::rest_client::{ReqwestClient, RestClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::get();

    let db = Database::get(
        DbConfigBuilder::new()
            .host(config.db_host)
            .port(config.db_port)
            .username(config.db_username)
    ).await?;

    let exchange = ExchangeBuilder::new()
        .set_base_url(config.base_url)
        .set_rest_client(ReqwestClient::new())
        .set_parser(PoloniexKlineParser)
        .set_db(Arc::new(db))
        .build()
        .await?;

    exchange.collect_klines(
        config.pairs,
        config.timeframes,
        config.start_time,
    ).await?;

    Ok(())
}