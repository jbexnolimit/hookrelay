mod config;
mod models;
mod worker;

use anyhow::{Context, Result};
use config::Config;
use models::DeliveryTask;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;

use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = Config::from_env()?;

    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(config.http_timeout_seconds))
        .build()
        .context("Failed to create HTTP client")?;

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &config.kafka_brokers)
        .set("group.id", &config.kafka_group_id)
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "earliest")
        .create()
        .context("Failed to create Kafka consumer")?;

    consumer
        .subscribe(&[config.kafka_topic.as_str()])
        .context("Failed to subscribe to Kafka topic")?;

    info!(
        brokers = %config.kafka_brokers,
        topic = %config.kafka_topic,
        group = %config.kafka_group_id,
        "HookRelay Rust worker started; waiting for tasks"
    );

    loop {
        let message = match consumer.recv().await {
            Ok(message) => message,
            Err(error) => {
                error!(error = %error, "Kafka receive error");
                sleep(Duration::from_secs(2)).await;
                continue;
            }
        };

        let payload = match message.payload() {
            Some(payload) => payload,
            None => {
                warn!(
                    offset = message.offset(),
                    "Received an empty Kafka message; skipping it"
                );

                // Prevent an empty message from blocking this partition.
                consumer.commit_message(&message, CommitMode::Sync)?;
                continue;
            }
        };

        let task = match serde_json::from_slice::<DeliveryTask>(payload) {
            Ok(task) => task,
            Err(error) => {
                error!(
                    error = %error,
                    offset = message.offset(),
                    "Invalid delivery task JSON; skipping message"
                );

                // A dead-letter queue can replace this behavior later.
                consumer.commit_message(&message, CommitMode::Sync)?;
                continue;
            }
        };

        info!(
            delivery_id = task.delivery_id,
            event_id = task.event_id,
            event_type = %task.event_type,
            "Received delivery task"
        );

        // Do not commit the Kafka offset until Django accepts the result.
        worker::process_delivery(&http_client, &config, &task).await?;

        consumer
            .commit_message(&message, CommitMode::Sync)
            .context("Failed to commit processed Kafka message")?;

        info!(delivery_id = task.delivery_id, "Kafka task committed");
    }
}
