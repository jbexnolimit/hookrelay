use crate::config::Config;
use crate::models::{DeliveryResult, DeliveryTask};

use anyhow::Result;
use reqwest::Client;
use serde_json::json;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::{info, warn};

const MAX_RESPONSE_BODY_CHARS: usize = 2_000;
const CALLBACK_RETRY_SECONDS: u64 = 5;

pub async fn process_delivery(client: &Client, config: &Config, task: &DeliveryTask) -> Result<()> {
    let started = Instant::now();

    // Send the business event to the destination webhook.
    let event_body = json!({
        "event_id": task.event_id,
        "event_type": &task.event_type,
        "payload": &task.payload,
    });

    let result = match client
        .post(&task.webhook_url)
        .json(&event_body)
        .send()
        .await
    {
        Ok(response) => {
            let status = response.status();

            let body = response
                .text()
                .await
                .unwrap_or_else(|error| format!("Could not read receiver response: {error}"));

            let success = status.is_success();

            DeliveryResult {
                success,
                http_status_code: Some(status.as_u16()),
                response_time_ms: started.elapsed().as_millis() as u64,
                response_body: body.chars().take(MAX_RESPONSE_BODY_CHARS).collect(),
                error_message: if success {
                    String::new()
                } else {
                    format!("Webhook returned HTTP {status}")
                },
            }
        }
        Err(error) => DeliveryResult {
            success: false,
            http_status_code: None,
            response_time_ms: started.elapsed().as_millis() as u64,
            response_body: String::new(),
            error_message: error.to_string(),
        },
    };

    // Report the result to Django.
    // This endpoint will be added to the Django API in the next integration step.
    let callback_url = format!(
        "{}/api/deliveries/{}/result/",
        config.django_api_base_url.trim_end_matches('/'),
        task.delivery_id,
    );

    loop {
        match client.post(&callback_url).json(&result).send().await {
            Ok(response) if response.status().is_success() => {
                info!(
                    delivery_id = task.delivery_id,
                    event_id = task.event_id,
                    success = result.success,
                    http_status = ?result.http_status_code,
                    "Delivery result recorded by Django"
                );

                return Ok(());
            }
            Ok(response) => {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();

                warn!(
                    delivery_id = task.delivery_id,
                    status = %status,
                    response = %body,
                    "Django rejected the delivery result; will retry"
                );
            }
            Err(error) => {
                warn!(
                    delivery_id = task.delivery_id,
                    error = %error,
                    "Could not report delivery result to Django; will retry"
                );
            }
        }

        sleep(Duration::from_secs(CALLBACK_RETRY_SECONDS)).await;
    }
}
