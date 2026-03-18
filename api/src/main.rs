mod error;
mod handlers;
mod models;

use axum::{Router, routing::get};
use engine::{Database, NornsConfig};
use std::{path::PathBuf, sync::Arc};

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

fn load_or_create_db(
    data_dir: &PathBuf,
    config: engine::DatabaseConfig,
) -> Result<Database, dbcore::error::NornsDbError> {
    if data_dir.join("schema").exists() {
        eprintln!("Loading existing database from {}", data_dir.display());
        Database::load(data_dir, config)
    } else {
        eprintln!("Creating new database at {}", data_dir.display());
        Database::new(data_dir, config)
    }
}

#[tokio::main]
async fn main() {
    let cfg = match NornsConfig::load_from_env() {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(1);
        }
    };

    let data_dir = cfg.data_directory_path();
    let config = cfg.database_config();

    let db = match load_or_create_db(&data_dir, config) {
        Ok(db) => Arc::new(db),
        Err(e) => {
            eprintln!(
                "Failed to load/create database at {}: {e}",
                data_dir.display()
            );
            std::process::exit(1);
        }
    };

    let bind = std::env::var("NORNS_BIND").unwrap_or_else(|_| "0.0.0.0:3000".to_string());
    let listener = match tokio::net::TcpListener::bind(&bind).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("Failed to bind to {bind}: {e}");
            std::process::exit(1);
        }
    };

    eprintln!("Listening on {bind}");

    let shutdown = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            eprintln!("Failed to listen for ctrl+c: {e}");
            return;
        }
        eprintln!("Shutting down...");
    };

    tokio::select! {
        result = axum::serve(listener, router(db.clone()))
            .with_graceful_shutdown(shutdown) => {
            if let Err(e) = result {
                eprintln!("Server error: {e}");
                std::process::exit(1);
            }
        }
        _ = async {
            tokio::signal::ctrl_c().await.ok();
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        } => {
            eprintln!("Force shutdown after timeout");
        }
    }

    drop(db);
    eprintln!("Database flushed, goodbye.");
}
