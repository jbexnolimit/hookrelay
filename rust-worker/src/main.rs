mod config;
mod models;
mod worker;

use anyhow::{Context, Result};
use config::Config;
use models::DeliveryTask;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;

use serde::Serialize;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Serialize)]
struct DlqEnvelope {
    source_topic: String,
    source_partition: i32,
    source_offset: i64,
    rejection_reason: String,
    rejected_at_unix_ms: u64,
    original_payload: Option<Vec<u8>>,
}

fn build_dlq_envelope(
    source_topic: &str,
    source_partition: i32,
    source_offset: i64,
    rejection_reason: &str,
    original_payload: Option<&[u8]>,
) -> DlqEnvelope {
    let rejected_at_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    DlqEnvelope {
        source_topic: source_topic.to_string(),
        source_partition,
        source_offset,
        rejection_reason: rejection_reason.to_string(),
        rejected_at_unix_ms,
        original_payload: original_payload.map(<[u8]>::to_vec),
    }
}

async fn publish_rejected_message(
    producer: &FutureProducer,
    dlq_topic: &str,
    message: &impl Message,
    rejection_reason: &str,
    original_payload: Option<&[u8]>,
) -> Result<()> {
    let envelope = build_dlq_envelope(
        message.topic(),
        message.partition(),
        message.offset(),
        rejection_reason,
        original_payload,
    );

    let key = format!(
        "{}-{}-{}",
        envelope.source_topic, envelope.source_partition, envelope.source_offset
    );
    let payload =
        serde_json::to_vec(&envelope).context("Failed to serialize dead-letter envelope")?;

    let publish_result = producer
        .send(
            FutureRecord::to(dlq_topic).key(&key).payload(&payload),
            Timeout::After(Duration::from_secs(5)),
        )
        .await;

    if let Err((error, _)) = publish_result {
        error!(
            source_topic = %envelope.source_topic,
            source_partition = envelope.source_partition,
            source_offset = envelope.source_offset,
            dlq_topic = %dlq_topic,
            error = %error,
            "DLQ publication failed; source message will not be committed"
        );

        return Err(anyhow::anyhow!(error))
            .context("Failed to publish rejected message to dead-letter topic");
    }

    warn!(
        source_topic = %envelope.source_topic,
        partition = envelope.source_partition,
        offset = envelope.source_offset,
        dlq_topic = %dlq_topic,
        "Rejected message published to dead-letter queue"
    );

    Ok(())
}

async fn wait_for_shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut terminate =
            signal(SignalKind::terminate()).context("Failed to install SIGTERM handler")?;

        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                result.context("Failed to listen for Ctrl+C")
            }
            _ = terminate.recv() => Ok(()),
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .context("Failed to listen for Ctrl+C")
    }
}

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

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &config.kafka_brokers)
        .set("acks", "all")
        .set("request.timeout.ms", "10000")
        .set("delivery.timeout.ms", "30000")
        .create()
        .context("Failed to create Kafka DLQ producer")?;

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

    let shutdown_signal = wait_for_shutdown_signal();
    tokio::pin!(shutdown_signal);

    loop {
        let message = tokio::select! {
            result = &mut shutdown_signal => {
                result?;
                info!("Shutdown signal received; stopping worker");
                break;
            }
            result = consumer.recv() => match result {
                Ok(message) => message,
                Err(error) => {
                    error!(error = %error, "Kafka receive error");
                    sleep(Duration::from_secs(2)).await;
                    continue;
                }
            }
        };

        let payload = match message.payload() {
            Some(payload) => payload,
            None => {
                publish_rejected_message(
                    &producer,
                    &config.kafka_dlq_topic,
                    &message,
                    "Kafka message has no payload",
                    None,
                )
                .await?;

                consumer
                    .commit_message(&message, CommitMode::Sync)
                    .context("Failed to commit dead-lettered empty Kafka message")?;
                continue;
            }
        };

        let task = match serde_json::from_slice::<DeliveryTask>(payload) {
            Ok(task) => task,
            Err(error) => {
                error!(
                    error = %error,
                    offset = message.offset(),
                    "Invalid delivery task JSON; sending message to DLQ"
                );

                publish_rejected_message(
                    &producer,
                    &config.kafka_dlq_topic,
                    &message,
                    &format!("Invalid delivery task JSON: {error}"),
                    Some(payload),
                )
                .await?;

                consumer
                    .commit_message(&message, CommitMode::Sync)
                    .context("Failed to commit dead-lettered invalid Kafka message")?;
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
        // If shutdown wins, cancel processing and leave the message uncommitted
        // so Kafka can redeliver it after the worker restarts.
        let processing_completed = tokio::select! {
            result = worker::process_delivery(&http_client, &config, &task) => {
                result?;
                true
            }
            result = &mut shutdown_signal => {
                result?;
                info!(
                    delivery_id = task.delivery_id,
                    "Shutdown signal received during delivery; leaving Kafka offset uncommitted"
                );
                false
            }
        };

        if !processing_completed {
            break;
        }

        consumer
            .commit_message(&message, CommitMode::Sync)
            .context("Failed to commit processed Kafka message")?;

        info!(delivery_id = task.delivery_id, "Kafka task committed");
    }

    Ok(())
}

#[cfg(test)]
mod dlq_tests {
    use super::*;

    #[test]
    fn envelope_preserves_metadata_and_original_bytes() {
        let original = [0_u8, 255, 10, 128];

        let envelope = build_dlq_envelope(
            "hookrelay.delivery-tasks",
            2,
            123,
            "Invalid JSON",
            Some(&original),
        );

        let json = serde_json::to_value(&envelope).unwrap();

        assert_eq!(json["source_topic"], "hookrelay.delivery-tasks");
        assert_eq!(json["source_partition"], 2);
        assert_eq!(json["source_offset"], 123);
        assert_eq!(json["rejection_reason"], "Invalid JSON");
        assert!(json["rejected_at_unix_ms"].as_u64().is_some());
        assert_eq!(
            json["original_payload"],
            serde_json::json!([0, 255, 10, 128])
        );
    }

    #[test]
    fn envelope_records_missing_payload_as_null() {
        let envelope = build_dlq_envelope(
            "hookrelay.delivery-tasks",
            0,
            9,
            "Kafka message has no payload",
            None,
        );

        let json = serde_json::to_value(&envelope).unwrap();

        assert_eq!(json["source_partition"], 0);
        assert_eq!(json["source_offset"], 9);
        assert_eq!(json["rejection_reason"], "Kafka message has no payload");
        assert!(json["original_payload"].is_null());
    }
}
