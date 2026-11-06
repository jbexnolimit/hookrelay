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

fn delivery_idempotency_key(delivery_id: i64) -> String {
    format!("hookrelay-delivery-{delivery_id}")
}

pub async fn process_delivery(client: &Client, config: &Config, task: &DeliveryTask) -> Result<()> {
    let started = Instant::now();

    // Send the business event to the destination webhook.
    let event_body = json!({
        "event_id": task.event_id,
        "event_type": &task.event_type,
        "payload": &task.payload,
    });

    let attempt_id = uuid::Uuid::new_v4();

    // Keep the key stable across Kafka replays of this logical delivery.
    let idempotency_key = delivery_idempotency_key(task.delivery_id);

    let result = match client
        .post(&task.webhook_url)
        .header("Idempotency-Key", &idempotency_key)
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
                attempt_id,
                claim_token: task.claim_token,
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
            attempt_id,
            claim_token: task.claim_token,
            success: false,
            http_status_code: None,
            response_time_ms: started.elapsed().as_millis() as u64,
            response_body: String::new(),
            error_message: error.to_string(),
        },
    };

    // Report the result to the existing Django delivery-result endpoint.
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

#[cfg(test)]
mod tests {
    use super::{delivery_idempotency_key, process_delivery};
    use crate::config::Config;
    use crate::models::DeliveryTask;
    use reqwest::Client;
    use serde_json::json;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    async fn read_http_request(stream: &mut TcpStream) -> String {
        let mut data = Vec::new();
        let mut buffer = [0_u8; 1024];

        loop {
            let count = stream.read(&mut buffer).await.unwrap();
            if count == 0 {
                break;
            }

            data.extend_from_slice(&buffer[..count]);

            if let Some(header_end) = data.windows(4).position(|w| w == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&data[..header_end]);
                let content_length = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().ok())
                            .flatten()
                    })
                    .unwrap_or(0);

                if data.len() >= header_end + 4 + content_length {
                    break;
                }
            }
        }

        String::from_utf8_lossy(&data).into_owned()
    }

    async fn respond_ok(stream: &mut TcpStream) {
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}")
            .await
            .unwrap();
    }

    #[test]
    fn idempotency_key_is_stable_for_the_same_delivery() {
        assert_eq!(delivery_idempotency_key(42), "hookrelay-delivery-42");
        assert_eq!(delivery_idempotency_key(42), delivery_idempotency_key(42));
    }

    #[test]
    fn different_deliveries_have_different_idempotency_keys() {
        assert_ne!(delivery_idempotency_key(42), delivery_idempotency_key(43));
    }

    #[tokio::test]
    async fn webhook_request_sends_stable_idempotency_key() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let base_url = format!("http://{address}");

        // One request goes to the webhook; the second is the Django callback.
        let server = tokio::spawn(async move {
            let mut requests = Vec::new();

            for _ in 0..2 {
                let (mut stream, _) = listener.accept().await.unwrap();
                requests.push(read_http_request(&mut stream).await);
                respond_ok(&mut stream).await;
            }

            requests
        });

        let client = Client::builder()
            .timeout(Duration::from_secs(3))
            .build()
            .unwrap();

        let config = Config {
            kafka_brokers: String::new(),
            kafka_topic: String::new(),
            kafka_group_id: String::new(),
            django_api_base_url: base_url.clone(),
            http_timeout_seconds: 3,
        };

        let task = DeliveryTask {
            delivery_id: 42,
            claim_token: uuid::Uuid::new_v4(),
            event_id: 7,
            event_type: "order.created".to_string(),
            payload: json!({"order_id": "ORD-1001"}),
            webhook_url: format!("{base_url}/receiver"),
        };

        process_delivery(&client, &config, &task)
            .await
            .expect("delivery and result callback should succeed");

        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 2);

        let webhook_request = requests[0].to_ascii_lowercase();
        assert!(
            webhook_request.contains("idempotency-key: hookrelay-delivery-42"),
            "webhook request did not contain the expected idempotency header:\n{}",
            requests[0]
        );
    }
}
