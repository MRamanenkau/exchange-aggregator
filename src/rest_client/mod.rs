use async_trait::async_trait;
use reqwest::{Client, Response, StatusCode};
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum HttpClientError {
    #[error("Request failed: {0}")]
    RequestError(#[from] reqwest::Error),
    #[error("Unexpected response status: {0}")]
    UnexpectedStatus(StatusCode),
    #[error("Failed to read response body")]
    ReadBodyError,
}

#[async_trait]
pub trait RestClient: Send + Sync {
async fn get(&self, url: &str) -> Result<String, HttpClientError>;
}

#[derive(Clone)]
pub struct ReqwestClient {
    client: Arc<Client>,
}

impl ReqwestClient {
    pub fn new() -> Self {
        ReqwestClient {
            client: Arc::new(Client::new()),
        }
    }
}

#[async_trait]
impl RestClient for ReqwestClient {
    async fn get(&self, url: &str) -> Result<String, HttpClientError> {
        let response = self.client.get(url).send().await
            .map_err(HttpClientError::RequestError)?;
        if !response.status().is_success() {
            return Err(HttpClientError::UnexpectedStatus(response.status()));
        }
        response.text().await.map_err(|_| HttpClientError::ReadBodyError)
    }
}