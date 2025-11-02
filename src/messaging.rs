use aws_sdk_sqs::error::SdkError;
use aws_sdk_sqs::Client as SqsClient;
use aws_sdk_sqs::operation::send_message_batch::SendMessageBatchError;
use aws_sdk_sqs::types::SendMessageBatchRequestEntry;
use aws_sdk_sns::Client as SnsClient;
use aws_sdk_sns::operation::publish_batch::PublishBatchError;
use aws_sdk_sns::types::PublishBatchRequestEntry;
use tracing::instrument;
use crate::models::OutboxMessage;

#[instrument(skip(sqs_client, messages))]
pub async fn send_messages_to_sqs(
    sqs_client: &SqsClient,
    channel_address: String,
    messages: &Vec<OutboxMessage>,
) -> Result<(), SdkError<SendMessageBatchError>> {
    let message_batch: Vec<SendMessageBatchRequestEntry> = messages. iter().enumerate().map(|(_index, msg)| {
        SendMessageBatchRequestEntry::builder()
            .id(msg.message_id.clone())
            .message_body(msg.body.clone())
            .build()
            .expect(format!("failed to build message batch entry for message with id {}", msg.message_id).as_str())
    }).collect();

    sqs_client
        .send_message_batch()
        .queue_url(channel_address)
        .set_entries(Some(message_batch))
        .send()
        .await?;

    Ok(())
}

#[instrument(skip(sns_client, messages))]
pub async fn send_messages_to_sns(
    sns_client: &SnsClient,
    channel_address: String,
    messages: &Vec<OutboxMessage>,
) -> Result<(), SdkError<PublishBatchError>> {
    let message_batch: Vec<PublishBatchRequestEntry> = messages. iter().enumerate().map(|(_index, msg)| {
        PublishBatchRequestEntry::builder()
            .id(msg.message_id.clone())
            .message(msg.body.clone())
            .build()
            .expect(format!("failed to build message batch entry for message with id {}", msg.message_id).as_str())
    }).collect();

    sns_client
        .publish_batch()
        .topic_arn(channel_address)
        .set_publish_batch_request_entries(Some(message_batch))
        .send()
        .await?;

    Ok(())
}

