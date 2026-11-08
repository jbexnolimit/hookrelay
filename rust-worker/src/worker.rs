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

enum HeartbeatStatus {
    Active,
    Inactive,
    Unavailable,
}

/// Verify the current Django claim before doing work or reporting a result.
/// An unavailable or malformed response never authorizes delivery progress.
async fn refresh_heartbeat(
    client: &Client,
    config: &Config,
    task: &DeliveryTask,
) -> HeartbeatStatus {
    let heartbeat_url = format!(
        "{}/api/deliveries/{}/heartbeat/",
        config.django_api_base_url.trim_end_matches('/'),
        task.delivery_id,
    );

    match client
        .post(&heartbeat_url)
        .json(&json!({ "claim_token": task.claim_token }))
        .send()
        .await
    {
        Ok(response) if response.status().is_success() => {
            match response.json::<serde_json::Value>().await {
                Ok(body) => match body.get("active").and_then(|value| value.as_bool()) {
                    Some(true) => HeartbeatStatus::Active,
                    Some(false) => {
                        info!(
                            delivery_id = task.delivery_id,
                            "Delivery claim is no longer active; stopping worker"
                        );
                        HeartbeatStatus::Inactive
                    }
                    None => {
                        warn!(
                            delivery_id = task.delivery_id,
                            "Heartbeat response omitted the active flag; retrying"
                        );
                        HeartbeatStatus::Unavailable
                    }
                },
                Err(error) => {
                    warn!(
                        delivery_id = task.delivery_id,
                        error = %error,
                        "Could not parse heartbeat response; retrying"
                    );
                    HeartbeatStatus::Unavailable
                }
            }
        }
        Ok(response) => {
            warn!(
                delivery_id = task.delivery_id,
                status = %response.status(),
                "Heartbeat request was rejected; retrying"
            );
            HeartbeatStatus::Unavailable
        }
        Err(error) => {
            warn!(
                delivery_id = task.delivery_id,
                error = %error,
                "Could not refresh worker heartbeat; retrying"
            );
            HeartbeatStatus::Unavailable
        }
    }
}

async fn wait_for_active_claim(client: &Client, config: &Config, task: &DeliveryTask) -> bool {
    loop {
        match refresh_heartbeat(client, config, task).await {
            HeartbeatStatus::Active => return true,
            HeartbeatStatus::Inactive => return false,
            HeartbeatStatus::Unavailable => {
                sleep(Duration::from_secs(CALLBACK_RETRY_SECONDS)).await;
            }
        }
    }
}

pub async fn process_delivery(client: &Client, config: &Config, task: &DeliveryTask) -> Result<()> {
    if !wait_for_active_claim(client, config, task).await {
        return Ok(());
    }

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
        if !wait_for_active_claim(client, config, task).await {
            return Ok(());
        }

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
        let body = r#"{"active":true}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        stream.write_all(response.as_bytes()).await.unwrap();
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

        // The four requests are: heartbeat, webhook, heartbeat, and Django callback.
        let server = tokio::spawn(async move {
            let mut requests = Vec::new();

            for _ in 0..4 {
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
            kafka_dlq_topic: String::new(),
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
        assert_eq!(requests.len(), 4);
        assert!(requests[0].contains("/api/deliveries/42/heartbeat/"));
        assert!(requests[1].contains("POST /receiver "));
        assert!(requests[2].contains("/api/deliveries/42/heartbeat/"));
        assert!(requests[3].contains("/api/deliveries/42/result/"));

        let webhook_request = requests[1].to_ascii_lowercase();
        assert!(
            webhook_request.contains("idempotency-key: hookrelay-delivery-42"),
            "webhook request did not contain the expected idempotency header:\n{}",
            requests[1]
        );
    }

    #[tokio::test]
    async fn temporary_heartbeat_failure_waits_for_recovery_before_webhook() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let base_url = format!("http://{address}");

        let server = tokio::spawn(async move {
            let mut requests = Vec::new();

            // First heartbeat fails temporarily.
            let (mut stream, _) = listener.accept().await.unwrap();
            requests.push(read_http_request(&mut stream).await);
            stream
                .write_all(
                    b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await
                .unwrap();
            drop(stream);

            // Recovery heartbeat, webhook, pre-callback heartbeat, result callback.
            for _ in 0..4 {
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
            kafka_dlq_topic: String::new(),
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
            .expect("delivery should resume after heartbeat recovery");

        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 5);
        assert!(requests[0].contains("POST /api/deliveries/42/heartbeat/"));
        assert!(requests[1].contains("POST /api/deliveries/42/heartbeat/"));
        assert!(
            requests[2].contains("POST /receiver "),
            "webhook should be called only after heartbeat recovery"
        );
        assert!(requests[3].contains("POST /api/deliveries/42/heartbeat/"));
        assert!(requests[4].contains("POST /api/deliveries/42/result/"));
    }

    #[tokio::test]
    async fn callback_retry_reuses_attempt_id_without_repeating_webhook() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let base_url = format!("http://{address}");

        let server = tokio::spawn(async move {
            let mut requests = Vec::new();

            // Expected order:
            // heartbeat, webhook, heartbeat, failed callback,
            // heartbeat, successful callback.
            for index in 0..6 {
                let (mut stream, _) = listener.accept().await.unwrap();
                requests.push(read_http_request(&mut stream).await);

                if index == 3 {
                    stream
                        .write_all(
                            b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                        )
                        .await
                        .unwrap();
                } else {
                    respond_ok(&mut stream).await;
                }
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
            kafka_dlq_topic: String::new(),
            kafka_group_id: String::new(),
            django_api_base_url: base_url.clone(),
            http_timeout_seconds: 3,
        };

        let claim_token = uuid::Uuid::new_v4();
        let task = DeliveryTask {
            delivery_id: 42,
            claim_token,
            event_id: 7,
            event_type: "order.created".to_string(),
            payload: json!({"order_id": "ORD-1001"}),
            webhook_url: format!("{base_url}/receiver"),
        };

        process_delivery(&client, &config, &task)
            .await
            .expect("delivery should succeed after callback recovery");

        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 6);

        assert!(requests[0].contains("/api/deliveries/42/heartbeat/"));
        assert!(requests[1].contains("POST /receiver "));
        assert!(requests[2].contains("/api/deliveries/42/heartbeat/"));
        assert!(requests[3].contains("/api/deliveries/42/result/"));
        assert!(requests[4].contains("/api/deliveries/42/heartbeat/"));
        assert!(requests[5].contains("/api/deliveries/42/result/"));

        let callback_body = |request: &str| -> serde_json::Value {
            let (_, body) = request
                .split_once("\r\n\r\n")
                .expect("HTTP request should contain a body separator");
            serde_json::from_str(body).expect("callback body should be valid JSON")
        };

        let first_result = callback_body(&requests[3]);
        let second_result = callback_body(&requests[5]);

        assert_eq!(
            first_result["attempt_id"], second_result["attempt_id"],
            "callback retries must reuse the same attempt ID"
        );
        assert_eq!(
            first_result["claim_token"], second_result["claim_token"],
            "callback retries must reuse the same claim token"
        );
        assert_eq!(
            first_result["success"], true,
            "the successful webhook response should be reported"
        );
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.contains("POST /receiver "))
                .count(),
            1,
            "callback retries must not repeat the webhook request"
        );
    }

    #[tokio::test]
    async fn inactive_claim_stops_before_calling_webhook() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let base_url = format!("http://{address}");

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut stream).await;
            let body = r#"{"active":false}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            request
        });

        let client = Client::builder()
            .timeout(Duration::from_secs(3))
            .build()
            .unwrap();

        let config = Config {
            kafka_brokers: String::new(),
            kafka_topic: String::new(),
            kafka_dlq_topic: String::new(),
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
            .expect("inactive claim should stop processing without an error");

        let request = server.await.unwrap();
        assert!(request.contains("POST /api/deliveries/42/heartbeat/"));
        assert!(
            !request.contains("/receiver"),
            "inactive claim should stop processing before calling the webhook"
        );
    }
}
