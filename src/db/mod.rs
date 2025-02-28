use rusty_tarantool::tarantool::{Client, ClientConfig};
use rmpv::{Value, Integer, Utf8String};
use std::collections::HashMap;
use thiserror::Error;
use tokio::sync::{OnceCell, Mutex};
use crate::exchange::parser::Kline;

static INSTANCE: OnceCell<Database> = OnceCell::const_new();

#[derive(Error, Debug)]
pub enum DbError {
    #[error("Configuration error: {0}")]
    Config(String),
    #[error("Connection error: {0}")]
    Connection(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("Space discovery failed: {0}")]
    SpaceDiscovery(String),
    #[error("Unknown space: {0}")]
    UnknownSpace(String),
}

#[derive(Clone, Default)]
pub struct DbConfigBuilder {
    host: Option<String>,
    port: Option<String>,
    username: Option<String>,
    password: Option<String>,
}

impl DbConfigBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = Some(host.into());
        self
    }

    pub fn port(mut self, port: impl Into<String>) -> Self {
        self.port = Some(port.into());
        self
    }

    pub fn username(mut self, username: impl Into<String>) -> Self {
        self.username = Some(username.into());
        self
    }

    // Explicitly optional password method, can be omitted
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    fn build(self) -> Result<DbConfig, DbError> {
        let port = self.port.unwrap_or_else(|| "3301".to_string())
            .parse::<u16>()
            .map_err(|e| DbError::Config(format!("Invalid port: {}", e)))?;
        Ok(DbConfig {
            host: self.host.unwrap_or_else(|| "localhost".to_string()),
            port,
            username: self.username.unwrap_or_else(|| "guest".to_string()),
            password: self.password,
        })
    }
}

#[derive(Clone)]
struct DbConfig {
    host: String,
    port: u16,
    username: String,
    password: Option<String>, // Optional field
}

pub struct Database {
    client: Mutex<Client>,
    space_ids: HashMap<String, u32>,
}

impl Database {
    fn from_config(config: DbConfig) -> Result<ClientConfig, DbError> {
        // Pass password as-is (None means no password provided)
        Ok(ClientConfig::new(
            format!("{}:{}", config.host, config.port),
            config.username,
            config.password.unwrap_or_default(), // None becomes "" for Tarantool
        ))
    }

    async fn discover_spaces(client: &Client) -> Result<HashMap<String, u32>, DbError> {
        let result = client
            .call_fn("get_spaces", &())
            .await
            .map_err(|e| DbError::SpaceDiscovery(format!("Failed to call get_spaces: {}", e)))?;
        let spaces = result.decode::<Vec<(u32, String)>>()?;
        let mut space_ids = HashMap::new();
        for (id, name) in spaces {
            space_ids.insert(name, id);
        }
        Ok(space_ids)
    }

    async fn new(config: DbConfig) -> Result<Self, DbError> {
        let client_config = Self::from_config(config)?;
        let client = Client::new(client_config);
        let space_ids = Self::discover_spaces(&client).await?;
        Ok(Database {
            client: Mutex::new(client),
            space_ids,
        })
    }

    pub async fn get(builder: DbConfigBuilder) -> Result<&'static Self, DbError> {
        let config = builder.build()?;
        INSTANCE
            .get_or_try_init(|| async { Self::new(config).await })
            .await
    }

    pub async fn save(&self, data: &Kline) -> Result<(), DbError> {
        let space_name = format!("klines_{}", data.pair.to_lowercase());
        let space_id = self.space_ids.get(&space_name)
            .ok_or_else(|| DbError::UnknownSpace(space_name.clone()))?;
        let values = vec![
            Value::String(Utf8String::from(data.pair.clone())),
            Value::String(Utf8String::from(data.time_frame.clone())),
            Value::F64(data.o),
            Value::F64(data.h),
            Value::F64(data.l),
            Value::F64(data.c),
            Value::Integer(Integer::from(data.utc_begin)),
            Value::F64(data.volume_bs.buy_base),
            Value::F64(data.volume_bs.sell_base),
            Value::F64(data.volume_bs.buy_quote),
            Value::F64(data.volume_bs.sell_quote),
        ];
        let client = self.client.lock().await;
        client
            .insert(*space_id as i32, &values)
            .await
            .map(|_| ())
            .map_err(Into::into)
    }
}