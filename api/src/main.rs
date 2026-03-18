mod error;
mod handlers;
mod models;

use axum::{Router, routing::get};
use engine::{Database, DatabaseConfig};
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

fn load_or_create_db(data_dir: &PathBuf, config: DatabaseConfig) -> Database {
    if data_dir.join("schema").exists() {
        eprintln!("Loading existing database from {}", data_dir.display());
        Database::load(data_dir, config).expect("failed to load database")
    } else {
        eprintln!("Creating new database at {}", data_dir.display());
        Database::new(data_dir, config).expect("failed to create database")
    }
}

#[tokio::main]
async fn main() {
    let data_dir: PathBuf = std::env::var("NORNS_DATA_DIR")
        .unwrap_or_else(|_| "data".to_string())
        .into();

    let config = DatabaseConfig {
        memtable_size: 10_000,
        level_0_size: 15,
        ss_table_block_size: 4096,
    };

    let db = Arc::new(load_or_create_db(&data_dir, config));

    let bind = std::env::var("NORNS_BIND").unwrap_or_else(|_| "0.0.0.0:3000".to_string());
    let listener = tokio::net::TcpListener::bind(&bind)
        .await
        .expect("failed to bind");

    eprintln!("Listening on {bind}");

    let shutdown = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for ctrl+c");
        eprintln!("Shutting down...");
    };

    tokio::select! {
        result = axum::serve(listener, router(db.clone()))
            .with_graceful_shutdown(shutdown) => {
            result.expect("server error");
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
