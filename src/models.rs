use chrono::{Utc, DateTime};
use sqlx::FromRow;

#[allow(dead_code)]
#[derive(Debug, FromRow)]
pub struct OutboxMessage {
    pub id: i64,
    pub message_id: String,
    pub message_type: String,
    pub channel_address: String,
    pub dispatched: Option<DateTime<Utc>>,
    pub timestamp: DateTime<Utc>,
    pub body: String,
    pub trace_parent: Option<String>,
}
