use aws_config::{BehaviorVersion, Region};
use crate::config::Config;
use aws_sdk_sqs::Client as SqsClient;
use aws_sdk_sns::Client as SnsClient;
use sqlx::{postgres::PgPoolOptions, PgPool};

/// Creates and returns a new database connection pool.
pub async fn setup_db_pool(config: &Config) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(5)
        .connect(&config.database_url())
        .await
}

/// Creates and returns a new AWS SQS client.
pub async fn setup_aws_clients(config: &Config) -> (SqsClient, SnsClient) {
    let aws_config = aws_config::defaults(BehaviorVersion::v2025_08_07()).region(Region::new(config.aws_region.clone())).load().await;
    (SqsClient::new(&aws_config), SnsClient::new(&aws_config))
}
