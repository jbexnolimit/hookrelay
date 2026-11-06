use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Deserialize)]
pub struct DeliveryTask {
    pub delivery_id: i64,
    pub claim_token: uuid::Uuid,
    pub event_id: i64,
    pub event_type: String,
    pub payload: Value,
    pub webhook_url: String,
}

#[derive(Debug, Serialize)]
pub struct DeliveryResult {
    pub attempt_id: uuid::Uuid,
    pub claim_token: uuid::Uuid,
    pub success: bool,
    pub http_status_code: Option<u16>,
    pub response_time_ms: u64,
    pub response_body: String,
    pub error_message: String,
}
