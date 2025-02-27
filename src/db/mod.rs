use tarantool::{Client, ClientConfig, Tuple, Value};
use serde::{Serialize, Deserialize};
use std::env;
use std::collections::HashMap;
use thiserror::Error;
use tokio::sync::OnceCell;

// Database module
#[derive(Error, Debug)]
pub enum DbError {
    #[error("Configuration error: {0}")]
    Config(String),
    #[error("Connection error: {0}")]
    Connection(#[from] tarantool::error::Error),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("Space discovery failed: {0}")]
    SpaceDiscovery(String),
    #[error("Unknown space: {0}")]
    UnknownSpace(String),
}

#[derive(Clone)]
struct DbConfig {
    host: String,
    port: u16,
    username: String,
    password: Option<String>,
}

pub struct Database {
    client: Client,
    space_ids: HashMap<String, u32>,
}

impl Database {
    static INSTANCE: OnceCell<Database> = OnceCell::const_new();

    fn load_config() -> Result<DbConfig, DbError> {
        let host = env::var("TARANTOOL_HOST").unwrap_or_else(|_| "localhost".to_string());
        let port = env::var("TARANTOOL_PORT")
            .unwrap_or_else(|_| "3301".to_string())
            .parse::<u16>()
            .map_err(|e| DbError::Config(format!("Invalid port: {}", e)))?;
        let username = env::var("TARANTOOL_USERNAME").unwrap_or_else(|_| "guest".to_string());
        let password = env::var("TARANTOOL_PASSWORD").ok();
        Ok(DbConfig { host, port, username, password })
    }

    async fn discover_spaces(client: &Client) -> Result<HashMap<String, u32>, DbError> {
        let result = client.eval("return box.space._space:select{}", ())
            .await
            .map_err(|e| DbError::SpaceDiscovery(format!("Failed to query _space: {}", e)))?;
        let mut space_ids = HashMap::new();
        let spaces = result.decode::<Vec<Vec<Value>>>()?;
        for space in spaces {
            if let (Some(Value::Integer(id)), Some(Value::String(name))) = (space.get(0), space.get(2)) {
                if !name.starts_with('_') {
                    space_ids.insert(name.clone(), *id as u32);
                }
            }
        }
        Ok(space_ids)
    }

    async fn new() -> Result<Self, DbError> {
        let config = Self::load_config()?;
        let client_config = ClientConfig::new(
            format!("{}:{}", config.host, config.port),
            &config.username,
            config.password,
        );
        let client = Client::new(client_config).await.map_err(DbError::Connection)?;
        let space_ids = Self::discover_spaces(&client).await?;
        Ok(Database { client, space_ids })
    }

    pub async fn get() -> Result<&'static Self, DbError> {
        Self::INSTANCE.get_or_try_init(|| async { Self::new().await }).await
    }

    pub async fn save(&self, data: &Kline) -> Result<(), DbError> {
        let space_name = format!("klines_{}", data.pair.to_lowercase());
        let space_id = self.space_ids.get(&space_name)
            .ok_or_else(|| DbError::UnknownSpace(space_name))?;
        let tuple: Tuple = serde_json::to_value(data)?.into();
        self.client.insert(*space_id, &tuple).await.map(|_| ()).map_err(Into::into)
    }
}