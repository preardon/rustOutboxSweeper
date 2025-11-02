use crate::models::OutboxMessage;
use sqlx::{query_as, PgPool, Row};


/// Fetches a list of distinct topics (queue URLs) that have pending messages.
pub async fn get_distinct_pending_topics(pool: &PgPool) -> Result<Vec<String>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT message_type
        FROM core.outbox
        WHERE dispatched is null
        "#,
    )
        .fetch_all(pool)
        .await?;

    // Extract the 'topic' string from each row
    let topics = rows.into_iter().map(|row| row.get("message_type")).collect();
    Ok(topics)
}

/// Fetches a batch of pending messages from the outbox table
/// and locks them for update.
///
/// This function must be called inside a transaction.
pub async fn get_pending_messages(
    db_pool: &PgPool,
    topic: &str,
) -> Result<Vec<OutboxMessage>, sqlx::Error> {
    let messages = query_as::<_, OutboxMessage>(
        r#"
        SELECT id, message_id, message_type, channel_address, dispatched, timestamp, body, trace_parent
        FROM core.outbox
        WHERE dispatched is null
            And message_type = $1
        ORDER BY timestamp
        LIMIT 10
        FOR UPDATE SKIP LOCKED
        "#,
    )
        .bind(topic)
        .fetch_all(db_pool) // Run the query within the transaction
        .await?;

    Ok(messages)
}


/// Marks a specific message as 'sent' in the database.
///
/// This function must be called inside the same transaction
/// that fetched the message.
pub async fn mark_messages_as_sent(
    db_pool: &PgPool,
    message_ids: Vec<i64>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        UPDATE core.outbox
        SET dispatched = NOW()
        WHERE id = Any($1)
        "#,
    )
        .bind(message_ids)
        .execute(db_pool) // Run the query within the transaction
        .await?;

    Ok(())
}
