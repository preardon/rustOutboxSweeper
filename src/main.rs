mod clients;
mod config;
mod models;
mod sweeper;
mod outbox;
mod messaging;

use crate::clients::{setup_db_pool, setup_aws_clients};
use crate::config::Config;
use crate::sweeper::sweep_outbox_and_send;

use std::time::Duration;
use tokio::time;
use tracing::{error, info, Level};
use tracing_subscriber::EnvFilter;

/// The main function sets up our application's state and runs the timer.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Setup logging
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(Level::INFO.into()))
        .init();

    // --- Configuration ---
    info!("Loading configuration...");
    let config = Config::load()?;
    info!("Configuration loaded.");

    // --- End Configuration ---

    // 1. Connect to the Database
    info!("Connecting to database...");
    let db_pool = setup_db_pool(&config).await?;
    info!("Database connection established.");

    // 2. Setup the AWS SQS Client
    info!("Setting up AWS clients...");
    let (sqs_client, sns_client) = setup_aws_clients(&config).await;
    info!("AWS SQS client established.");

    // 3. This is your "Timer Function"
    info!(interval_ms = config.sweep_interval_ms, "Starting outbox sweeper timer...");
    let mut interval = time::interval(Duration::from_millis(config.sweep_interval_ms));

    // Run the sweeper in an infinite loop
    loop {
        interval.tick().await; // Wait for the next tick

        // We clone the clients for the async task.
        let db_pool_clone = db_pool.clone();
        let sqs_client_clone = sqs_client.clone();
        let sns_client_clone = sns_client.clone();

        tokio::spawn(async move {
            if let Err(e) =
                // The core logic is now called from its own module
                sweep_outbox_and_send(&db_pool_clone, &sqs_client_clone, &sns_client_clone).await
            {
                error!("Error during outbox sweep: {}", e);
            }
        });
    }
}

