use crate::{messaging, outbox};
use aws_sdk_sqs::Client as SqsClient;
use aws_sdk_sns::Client as SnsClient;
use sqlx::PgPool;
use tracing::{error, info, instrument, Span};

// Helper function to mark messages as sent and log the result
async fn mark_and_log_sent(db_pool: &sqlx::PgPool, topic: &str, messages: &[crate::models::OutboxMessage], messages_found: usize) {
    let message_ids: Vec<i64> = messages.iter().map(|m| m.id).collect();
    match outbox::mark_messages_as_sent(db_pool, message_ids).await {
        Ok(_) => {
            info!(%topic, messages_sent = messages_found, "Successfully sent and marked messages.");
        }
        Err(e) => {
            error!(%topic, "Error marking messages: {}. These messages WILL be re-sent.", e);
        }
    }
}

#[instrument(skip_all, fields(topics_needing_dispatch=0))]
pub async fn sweep_outbox_and_send(
    db_pool: &PgPool,
    sqs_client: &SqsClient,
    sns_client: &SnsClient,
    batch_size: &i32,
) -> Result<(), sqlx::Error> {
    info!("Checking outbox for pending messages...");

    let topics = outbox::get_distinct_pending_topics(db_pool).await?;

    let topics_needing_dispatch = topics.len();
    if topics_needing_dispatch == 0{
        info!("No un-dispatches messages found.");
        return Ok(());
    }
    Span::current().record("topics_needing_dispatch", topics_needing_dispatch);

    for topic in topics {
        sweep_channel(db_pool, sqs_client, sns_client, batch_size, &topic).await?;
    }

    info!("Outbox sweep complete for all topics.");

    Ok(())
}

#[instrument(skip_all, fields(messages_found=0))]
pub async fn sweep_channel(
    db_pool: &PgPool,
    sqs_client: &SqsClient,
    sns_client: &SnsClient,
    batch_size: &i32,
    channel_name: &str,
) -> Result<(), sqlx::Error>
{
    let messages = outbox::get_pending_messages(db_pool, &channel_name, &batch_size).await?;

        let messages_found = messages.len();
        if messages_found == 0 {
            info!("No pending messages found.");
            return Ok(());
        }
        info!(messages_found, "Found messages to send.");
        Span::current().record("messages_found", messages_found);

        let channel_address = messages[0].channel_address.clone();

        if let Some((channel_type, address)) = channel_address.split_once("::") {
            info!(channel_type, "Channel Selected");
            match messaging::send_messages_to_sns(sns_client, address.to_string(), &messages).await{
                Ok(_) =>{
                    mark_and_log_sent(db_pool, &channel_name, &messages, messages_found).await;
                }
                Err(error) =>{
                    error!(%channel_name, "Failed to send messages to {}: {:#?}.", channel_type, error);
                }
            }
        }
        else {
            match messaging::send_messages_to_sqs(sqs_client, channel_address, &messages).await {
                Ok(_) => {
                    mark_and_log_sent(db_pool, &channel_name, &messages, messages_found).await;

                }
                Err(e) => {
                    error!(%channel_name, "Failed to send messages to SQS: {:#?}.", e);
                }
            }
        }
        info!("Outbox sweep complete for channel {}. Sent {} messages.", channel_name, messages_found);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::clients::setup_aws_clients;
    use sqlx::{Executor, PgPool};
    use uuid::{ Uuid};
    use crate::models::OutboxMessage; // Import this

    // Helper function to insert a test message
    async fn insert_test_message(pool: &PgPool, channel_address: &str) -> String {
        let message_id = Uuid::new_v4().to_string();
        let body = r#"{ "foo"": "bar" }"#;
        sqlx::query(
            r#"
                INSERT INTO core.outbox (message_id, message_type, channel_address, timestamp, body)
            VALUES ($1, 'test.topic', $2, NOW(), $3)
            "#,
        )
            .bind(&message_id)
            .bind(channel_address)
            .bind(body)
            .execute(pool)
            .await
            .expect("Failed to insert test message");

        message_id
    }

    // Helper function to get a message
    async fn get_message(pool: &PgPool, message_id: String) -> Option<OutboxMessage> {
        // Use query_as to get the full struct
        sqlx::query_as::<_, OutboxMessage>(
            "SELECT id, message_id, message_type, channel_address, timestamp, body, dispatched, trace_parent FROM core.outbox WHERE message_id = $1"
        )
            .bind(message_id)
            .fetch_one(pool)
            .await
            .ok()
    }

    /// This is a full integration test.
    /// It requires:
    /// 1. A running Postgres database (configure with DATABASE_URL env var).
    ///    - Run `cargo install sqlx-cli`
    ///    - Run `sqlx database create`
    ///    - Run `sqlx migrate add init` (this creates a migrations folder)
    ///    - Copy your `schema.sql` content into the new .sql file in `migrations/`
    ///    - Run `sqlx migrate run`
    /// 2. AWS credentials and SQS_QUEUE_URL env var set to a REAL test queue.
    #[sqlx::test(migrations = false)] // We'll manually create the table
    async fn test_sweep_sends_message_and_updates_db(pool: PgPool) {
        let cases = vec![
            "https://localhost.localstack.cloud:4566/000000000000/test-queue",
            "SNS::arn:aws:sns:eu-west-1:000000000000:test-topic"
        ];

        // Setup Database
        let schema_sql = include_str!("../schema.sql");
        pool.execute(schema_sql).await.expect("Failed to create schema");

        for case in cases {
            // --- ARRANGE ---

            let config = Config::load_test().expect("Failed to load config for test");
            let (sqs_client, sns_client) = setup_aws_clients(&config).await;
            let channel_address = case;

            let message_id = insert_test_message(&pool, channel_address).await;

            let msg = get_message(&pool, message_id.clone()).await;
            assert!(msg.is_some(), "Test message was not inserted");

            // --- ACT ---
            let result = sweep_outbox_and_send(&pool, &sqs_client, &sns_client, &10).await;

            // --- ASSERT ---
            assert!(result.is_ok(), "Sweeper returned an error: {:?}", result.err());

            let remaining_messages = outbox::get_pending_messages(&pool, "test.topic", &10).await.unwrap();

            assert_eq!(remaining_messages.len(), 0, "Message was not marked as 'sent'");

            let msg_after_dispatch = get_message(&pool, message_id.clone()).await;
            assert_ne!(msg_after_dispatch.unwrap().dispatched, None, "Message was marked as sent");
        }
    }

    #[sqlx::test(migrations = false)]
    async fn test_sweep_with_no_messages(pool: PgPool) {
        // --- ARRANGE ---

        // Manually run our schema for this test.
        let schema_sql = include_str!("../schema.sql");
        pool.execute(schema_sql).await.expect("Failed to create schema");

        // 1. Get SQS config.
        let config = Config::load_test().expect("Failed to load config for test");
        let (sqs_client, sns_client) = setup_aws_clients(&config).await;

        // 2. Ensure no messages are in the DB
        let initial_messages = outbox::get_pending_messages(&pool, "test.topic", &10).await.unwrap();
        assert_eq!(initial_messages.len(), 0, "Database was not empty at start");

        // --- ACT ---
        let result = sweep_outbox_and_send(&pool, &sqs_client, &sns_client, &10).await;

        // --- ASSERT ---

        // 1. Assert the sweeper ran successfully
        assert!(result.is_ok(), "Sweeper returned an error: {:?}", result.err());

        // 2. Assert the database is still empty
        let final_messages = outbox::get_pending_messages(&pool, "test.topic", &10).await.unwrap();
        assert_eq!(final_messages.len(), 0, "Messages appeared after empty sweep");
    }

    #[sqlx::test(migrations = false)]
    async fn test_sweep_rolls_back_on_sqs_failure(pool: PgPool) {
        // --- ARRANGE ---

        // // Manually run our schema for this test.
        let schema_sql = include_str!("../schema.sql");
        pool.execute(schema_sql).await.expect("Failed to create schema");

        // 1. Get SQS config (we need a valid client, but a bad queue URL)
        let config = Config::load_test().expect("Failed to load config for test");
        let (sqs_client, sns_client) = setup_aws_clients(&config).await;
        let invalid_queue_url = "https_sqs_fake_url_that_does_not_exist";

        // 2. Insert a test message
        let message_id = insert_test_message(&pool, invalid_queue_url).await;

        // 3. Check its initial status
        let initial_messages = outbox::get_pending_messages(&pool, "test.topic", &10).await.unwrap();
        assert_eq!(initial_messages.len(), 1, "Test message was not inserted");
        assert_eq!(initial_messages[0].message_id, message_id);


        // --- ACT ---
        // Run the sweeper with the INVALID queue URL
        let _result = sweep_outbox_and_send(&pool, &sqs_client, &sns_client, &10).await;

        // --- ASSERT ---
        let final_messages = outbox::get_pending_messages(&pool, "test.topic", &10).await.unwrap();

        assert_eq!(final_messages.len(), 1, "Message was not rolled back");
        let final_message = final_messages.first().unwrap();
        assert_eq!(final_message.message_id, message_id, "Wrong message found after rollback");
            assert_eq!(final_message.dispatched, None, "Message not marked as dispatched");
    }
}
