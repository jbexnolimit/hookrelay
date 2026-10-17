use anyhow::{Context, Result};
use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub kafka_brokers: String,
    pub kafka_topic: String,
    pub kafka_group_id: String,
    pub django_api_base_url: String,
    pub http_timeout_seconds: u64,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let http_timeout_seconds = env::var("HTTP_TIMEOUT_SECONDS")
            .unwrap_or_else(|_| "10".to_string())
            .parse::<u64>()
            .context("HTTP_TIMEOUT_SECONDS must be a positive integer")?;

        if http_timeout_seconds == 0 {
            anyhow::bail!("HTTP_TIMEOUT_SECONDS must be greater than zero");
        }

        Ok(Self {
            kafka_brokers: env_or("KAFKA_BROKERS", "127.0.0.1:9092"),
            kafka_topic: env_or("KAFKA_TOPIC", "hookrelay.delivery-tasks"),
            kafka_group_id: env_or("KAFKA_GROUP_ID", "hookrelay-rust-worker"),
            django_api_base_url: env_or("DJANGO_API_BASE_URL", "http://127.0.0.1:8001"),
            http_timeout_seconds,
        })
    }
}

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}
