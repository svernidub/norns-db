use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use reqwest::Client;
use tokio::time::sleep;
use tracing::{error, info, instrument};
use tracing_subscriber::EnvFilter;

const LOGS_TABLE: &str = "logs";
const TOTAL_WRITES: u64 = 1_000_000;
const NUM_WRITERS: usize = 8;
const NUM_READERS: usize = 4;

const LIST_INTERVAL_SECS: u64 = 1;
const DELETE_INTERVAL_MILLIS: u64 = 500;

const LOG_MESSAGES: [&str; 10] = [
    "user logged in",
    "user logged out",
    "order created",
    "order cancelled",
    "payment succeeded",
    "payment failed",
    "cache miss",
    "cache hit",
    "background job started",
    "background job finished",
];

const LOG_LEVELS: [&str; 3] = ["INFO", "WARN", "ERROR"];

fn now_millis() -> i64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0));

    now.as_millis() as i64
}

fn message_for_id(id: u64) -> &'static str {
    let idx = (id as usize) % LOG_MESSAGES.len();
    LOG_MESSAGES[idx]
}

fn level_for_id(id: u64) -> &'static str {
    let idx = (id as usize) % LOG_LEVELS.len();
    LOG_LEVELS[idx]
}

fn sample_key_id(counter: u64, max_id: u64) -> Option<u64> {
    if max_id == 0 {
        return None;
    }

    let m = max_id;
    let a = 1664525_u64;
    let c = 1013904223_u64;

    let candidate = (a.wrapping_mul(counter).wrapping_add(c)) % m;
    Some(candidate)
}

async fn create_logs_table(client: &Client, base_url: &str) -> Result<(), reqwest::Error> {
    let url = format!("{base_url}/{LOGS_TABLE}");

    let body = serde_json::json!({
        "primary_key": {
            "name": "id",
            "type": "big_integer",
        },
        "columns": [
            { "name": "level", "type": "varchar" },
            { "name": "message", "type": "varchar" },
            { "name": "ts", "type": "timestamp" },
        ],
    });

    let response = client.post(url).json(&body).send().await?;

    if response.status().is_success() {
        info!("logs table created via API");
    } else {
        info!(
            status = %response.status(),
            "create logs table returned non-success status (assuming it may already exist)"
        );
    }

    Ok(())
}

async fn drop_logs_table(client: &Client, base_url: &str) -> Result<(), reqwest::Error> {
    let url = format!("{base_url}/{LOGS_TABLE}");

    let response = client.delete(url).send().await?;

    if response.status().is_success() {
        info!("logs table dropped via API");
    } else {
        error!(status = %response.status(), "failed to drop logs table via API");
    }

    Ok(())
}

async fn insert_log(client: &Client, base_url: &str, id: u64) -> Result<(), reqwest::Error> {
    let url = format!("{base_url}/{LOGS_TABLE}/{id}");

    let level = level_for_id(id);
    let message = message_for_id(id);
    let ts = now_millis();

    let body = serde_json::json!({
        "level": level,
        "message": message,
        "ts": ts,
    });

    let response = client.post(url).json(&body).send().await?;

    if !response.status().is_success() {
        error!(id, status = %response.status(), "insert_log returned non-success status");
    }

    Ok(())
}

async fn get_log(client: &Client, base_url: &str, id: u64) -> Result<(), reqwest::Error> {
    let url = format!("{base_url}/{LOGS_TABLE}/{id}");

    let response = client.get(url).send().await?;

    if !response.status().is_success() && response.status().as_u16() != 404 {
        error!(id, status = %response.status(), "get_log returned non-success status");
    }

    Ok(())
}

async fn list_logs(client: &Client, base_url: &str) -> Result<(), reqwest::Error> {
    let url = format!("{base_url}/{LOGS_TABLE}");

    let response = client.get(url).send().await?;

    if !response.status().is_success() {
        error!(status = %response.status(), "list_logs returned non-success status");
        return Ok(());
    }

    let body = response.json::<serde_json::Value>().await?;

    let count = body.as_array().map(|a| a.len()).unwrap_or(0);

    info!(count, "list_logs returned rows");

    Ok(())
}

async fn delete_log(client: &Client, base_url: &str, id: u64) -> Result<(), reqwest::Error> {
    let url = format!("{base_url}/{LOGS_TABLE}/{id}");

    let response = client.delete(url).send().await?;

    if response.status().is_success() {
        info!(id, "delete_log succeeded");
    } else if response.status().as_u16() == 404 {
        info!(id, "delete_log got 404 (already gone)");
    } else {
        error!(id, status = %response.status(), "delete_log returned non-success status");
    }

    Ok(())
}

#[instrument(skip(client, writes_started, writes_completed))]
async fn writer_task(
    writer_id: usize,
    client: Arc<Client>,
    base_url: String,
    writes_started: Arc<AtomicU64>,
    writes_completed: Arc<AtomicU64>,
) {
    loop {
        let id = writes_started.fetch_add(1, Ordering::Relaxed);

        if id >= TOTAL_WRITES {
            break;
        }

        if let Err(error) = insert_log(&client, &base_url, id).await {
            error!(%error, writer_id, id, "writer failed to insert log");
            continue;
        }

        writes_completed.fetch_add(1, Ordering::Relaxed);
    }

    info!(writer_id, "writer finished");
}

#[instrument(skip(client, writes_completed, shutdown))]
async fn reader_task(
    reader_id: usize,
    client: Arc<Client>,
    base_url: String,
    writes_completed: Arc<AtomicU64>,
    shutdown: Arc<AtomicBool>,
) {
    let mut local_counter = 0_u64;
    let mut found_or_ok = 0_u64;
    let mut not_found = 0_u64;

    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        let completed = writes_completed.load(Ordering::Relaxed);

        if completed == 0 {
            tokio::task::yield_now().await;
            continue;
        }

        if let Some(id) = sample_key_id(local_counter, completed) {
            if let Err(error) = get_log(&client, &base_url, id).await {
                error!(%error, reader_id, id, "reader failed to get log");
            } else {
                found_or_ok += 1;
            }
        } else {
            not_found += 1;
        }

        local_counter = local_counter.wrapping_add(1);

        if local_counter.is_multiple_of(10_000) {
            info!(
                reader_id,
                found_or_ok, not_found, completed, "reader progress"
            );
        }

        tokio::task::yield_now().await;
    }

    info!(reader_id, found_or_ok, not_found, "reader finished");
}

#[instrument(skip(client, writes_completed, shutdown))]
async fn lister_task(
    client: Arc<Client>,
    base_url: String,
    writes_completed: Arc<AtomicU64>,
    shutdown: Arc<AtomicBool>,
) {
    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        let completed = writes_completed.load(Ordering::Relaxed);

        if completed > 0
            && let Err(error) = list_logs(&client, &base_url).await
        {
            error!(%error, "lister failed to list logs");
        }

        sleep(Duration::from_secs(LIST_INTERVAL_SECS)).await;
    }

    info!("lister finished");
}

#[instrument(skip(client, writes_completed, shutdown))]
async fn deleter_task(
    client: Arc<Client>,
    base_url: String,
    writes_completed: Arc<AtomicU64>,
    shutdown: Arc<AtomicBool>,
) {
    let mut local_counter = 0_u64;

    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        let completed = writes_completed.load(Ordering::Relaxed);

        if completed > 0
            && let Some(id) = sample_key_id(local_counter, completed)
            && let Err(error) = delete_log(&client, &base_url, id).await
        {
            error!(%error, id, "deleter failed to delete log");
        }

        local_counter = local_counter.wrapping_add(1);

        sleep(Duration::from_millis(DELETE_INTERVAL_MILLIS)).await;
    }

    info!("deleter finished");
}

#[instrument]
async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let base_url =
        std::env::var("PLAYGROUND_API_URL").unwrap_or_else(|_| "http://127.0.0.1:3000".to_string());

    info!(%base_url, "starting HTTP playground");

    let client = Arc::new(Client::builder().build()?);

    create_logs_table(&client, &base_url).await?;

    let writes_started = Arc::new(AtomicU64::new(0));
    let writes_completed = Arc::new(AtomicU64::new(0));
    let shutdown = Arc::new(AtomicBool::new(false));

    let mut writer_handles = Vec::with_capacity(NUM_WRITERS);
    for writer_id in 0..NUM_WRITERS {
        let client = Arc::clone(&client);
        let base_url = base_url.clone();
        let writes_started = Arc::clone(&writes_started);
        let writes_completed = Arc::clone(&writes_completed);

        let handle = tokio::spawn(writer_task(
            writer_id,
            client,
            base_url,
            writes_started,
            writes_completed,
        ));

        writer_handles.push(handle);
    }

    let mut reader_handles = Vec::with_capacity(NUM_READERS);
    for reader_id in 0..NUM_READERS {
        let client = Arc::clone(&client);
        let base_url = base_url.clone();
        let writes_completed = Arc::clone(&writes_completed);
        let shutdown = Arc::clone(&shutdown);

        let handle = tokio::spawn(reader_task(
            reader_id,
            client,
            base_url,
            writes_completed,
            shutdown,
        ));

        reader_handles.push(handle);
    }

    let lister_client = Arc::clone(&client);
    let lister_base_url = base_url.clone();
    let lister_writes_completed = Arc::clone(&writes_completed);
    let lister_shutdown = Arc::clone(&shutdown);
    let lister_handle = tokio::spawn(lister_task(
        lister_client,
        lister_base_url,
        lister_writes_completed,
        lister_shutdown,
    ));

    let deleter_client = Arc::clone(&client);
    let deleter_base_url = base_url.clone();
    let deleter_writes_completed = Arc::clone(&writes_completed);
    let deleter_shutdown = Arc::clone(&shutdown);
    let deleter_handle = tokio::spawn(deleter_task(
        deleter_client,
        deleter_base_url,
        deleter_writes_completed,
        deleter_shutdown,
    ));

    for handle in writer_handles {
        if let Err(error) = handle.await {
            error!(%error, "writer task join error");
        }
    }

    shutdown.store(true, Ordering::Relaxed);

    for handle in reader_handles {
        if let Err(error) = handle.await {
            error!(%error, "reader task join error");
        }
    }

    if let Err(error) = lister_handle.await {
        error!(%error, "lister task join error");
    }

    if let Err(error) = deleter_handle.await {
        error!(%error, "deleter task join error");
    }

    let started = writes_started.load(Ordering::Relaxed);
    let completed = writes_completed.load(Ordering::Relaxed);

    info!(
        started,
        completed,
        target = TOTAL_WRITES,
        "workload finished"
    );

    if let Err(error) = drop_logs_table(&client, &base_url).await {
        error!(%error, "failed to drop logs table at the end of playground");
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .json()
        .init();

    if let Err(error) = run().await {
        error!(%error, "playground run failed");
        return Err(error);
    }

    Ok(())
}
