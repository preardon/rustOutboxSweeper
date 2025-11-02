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
use actix_web::{App, HttpResponse, HttpServer, Responder, get};
use tokio::time;
use tracing::{error, info, Level};
use tracing_subscriber::EnvFilter;

#[get("/health")]
async fn health_check() -> impl Responder {
    // Just return a 200 OK response
    HttpResponse::Ok().body("OK")
}

// Graceful shutdown signal future
    async fn shutdown_signal() {
        use tokio::signal;
        let ctrl_c = signal::ctrl_c();
        #[cfg(unix)]
        let mut term_signal = signal::unix::signal(signal::unix::SignalKind::terminate()).expect("Failed to install SIGTERM handler");
        #[cfg(unix)]
        let terminate = term_signal.recv();
        #[cfg(not(unix))]
        let terminate = std::future::pending::<()>();
        tokio::select! {
            _ = ctrl_c => {},
            _ = terminate => {},
        }
        info!("Shutdown signal received. Exiting sweeper loop.");
    }

async fn run_sweeper_logic() {
    // Setup logging
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(Level::INFO.into()))
        .init();

    // --- Configuration ---
    info!("Loading configuration...");
    let config = Config::load().expect("Failed to load configuration");
    info!("Configuration loaded.");

    // --- End Configuration ---

    // 1. Connect to the Database
    info!("Connecting to database...");
    let db_pool = setup_db_pool(&config).await.expect("failed to create database connection.");
    info!("Database connection established.");

    // 2. Setup the AWS SQS Client
    info!("Setting up AWS clients...");
    let (sqs_client, sns_client) = setup_aws_clients(&config).await;
    info!("AWS SQS client established.");

    // 3. This is your "Timer Function"
    info!(interval_ms = config.sweep_interval_ms, "Starting outbox sweeper timer...");
    let mut interval = time::interval(Duration::from_millis(config.sweep_interval_ms));

    loop {
        tokio::select! {
            _ = interval.tick() => {
                // We clone the clients for the async task.
                let db_pool_clone = db_pool.clone();
                let sqs_client_clone = sqs_client.clone();
                let sns_client_clone = sns_client.clone();
                let batch_size = config.batch_size.clone();

                tokio::spawn(async move {
                    if let Err(e) =
                        // The core logic is now called from its own module
                        sweep_outbox_and_send(&db_pool_clone, &sqs_client_clone, &sns_client_clone, &batch_size).await
                    {
                        error!("Error during outbox sweep: {}", e);
                    }
                });
            },
            _ = shutdown_signal() => {
                break;
            }
        }
    }
    info!("Sweeper shutting down.");
}

/// The main function sets up our application's state and runs the timer.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let sweeper_handle = tokio::spawn(async {
        run_sweeper_logic().await;
    });

    // Spawn the health check server
    let health_server = HttpServer::new(|| {
        App::new().service(health_check)
    })
    .bind(("0.0.0.0", 8080))? // Binds to all interfaces on port 8080
    .run();

    println!("Health check server running on http://0.0.0.0:8080");

    // Keep both tasks running
    // This will error out if either the server or your sweeper task fails
    let _ = tokio::try_join!(
        async { health_server.await },
        async { sweeper_handle.await.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)) }
    )?;

    Ok(())
}

