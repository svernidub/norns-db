mod error;
mod handlers;
mod models;
mod telemetry;

use axum::{Router, routing::get};
use engine::{Database, NornsConfig};
use std::{path::PathBuf, sync::Arc};
use tracing::{error, info};

#[tokio::main]
async fn main() {
    let _telemetry = match telemetry::init_telemetry() {
        Ok(handle) => handle,
        Err(error) => {
            error!(%error, "failed to initialize telemetry");
            std::process::exit(1);
        }
    };

    let cfg = match NornsConfig::load_from_env() {
        Ok(cfg) => cfg,
        Err(error) => {
            error!(%error, "failed to load configuration from environment");
            std::process::exit(1);
        }
    };

    let data_dir = cfg.data_directory_path();
    let config = cfg.database_config();

    let db = match load_or_create_db(&data_dir, config) {
        Ok(db) => Arc::new(db),
        Err(error) => {
            error!(
                %error,
                path = %data_dir.display(),
                "failed to load or create database"
            );
            std::process::exit(1);
        }
    };

    let bind = std::env::var("NORNS_BIND").unwrap_or_else(|_| "0.0.0.0:3000".to_string());
    let listener = match tokio::net::TcpListener::bind(&bind).await {
        Ok(l) => l,
        Err(error) => {
            error!(%error, bind, "failed to bind listener");
            std::process::exit(1);
        }
    };

    info!(bind, "listening for incoming connections");

    let shutdown = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            error!(%error, "failed to listen for ctrl+c");
            return;
        }
        info!("shutting down on ctrl+c");
    };

    tokio::select! {
        result = axum::serve(listener, router(db.clone()))
            .with_graceful_shutdown(shutdown) => {
            if let Err(error) = result {
                error!(%error , "server error");
                std::process::exit(1);
            }
        }
        _ = async {
            tokio::signal::ctrl_c().await.ok();
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        } => {
            info!("force shutdown after timeout");
        }
    }

    drop(db);
    info!("database flushed");
}

fn load_or_create_db(
    data_dir: &PathBuf,
    config: engine::DatabaseConfig,
) -> Result<Database, dbcore::error::NornsDbError> {
    if data_dir.join("schema").exists() {
        info!("Loading existing database from {}", data_dir.display());

        Database::load(data_dir, config)
    } else {
        info!("Creating new database at {}", data_dir.display());

        Database::new(data_dir, config)
    }
}

fn router(db: Arc<Database>) -> Router {
    Router::new()
        .route(
            "/{table_name}",
            get(handlers::list_rows)
                .post(handlers::create_table)
                .delete(handlers::drop_table),
        )
        .route(
            "/{table_name}/{key}",
            get(handlers::get_row)
                .post(handlers::insert)
                .delete(handlers::delete_row),
        )
        .with_state(db)
}
