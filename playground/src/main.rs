use engine::{
    Database, DatabaseConfig, TableSchema,
    column::{Column, ColumnType},
    primary_key::{PrimaryKey, PrimaryKeyType},
    row::Row,
};
use opentelemetry::trace::TracerProvider;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tracing::{error, info, instrument};
use tracing_subscriber::{Layer, layer::SubscriberExt, util::SubscriberInitExt};

const NUM_WRITERS: usize = 10;
const ENTRIES_PER_WRITER: usize = 15000;
const NUM_READERS: usize = 4;
const READER_PROBES: usize = 2000;

/// Demonstrates the Database API with concurrent reads and writes.
///
/// The write path (handled internally by Database → Table → Journal + LsmTree):
///   1. Validate key/row against the table schema
///   2. Append to WAL (fsync) — durability guarantee
///   3. Insert into the LSM tree memtable — fast, in-memory
///
/// The read path hits (in order):
///   1. Active memtable (in-memory BTreeMap)
///   2. Frozen memtable (if a flush is in progress)
///   3. Level-0 SSTables (most recent first)
///   4. Level-1 SSTables (compacted)
#[instrument(skip(db, writes_done))]
async fn writer_task(writer_id: usize, db: Arc<Database>, writes_done: Arc<AtomicUsize>) {
    for i in 0..ENTRIES_PER_WRITER {
        let key = PrimaryKey::Varchar(format!("key_{writer_id}_{i}"));
        let row = Row(vec![Column::Varchar(format!("value_{writer_id}_{i}"))]);

        db.insert("entries", key, row).await.expect("insert failed");

        writes_done.fetch_add(1, Ordering::Relaxed);
    }
    info!(writer_id, entries = ENTRIES_PER_WRITER, "writer done");
}

#[instrument(skip(db, writes_done))]
async fn reader_task(reader_id: usize, db: Arc<Database>, writes_done: Arc<AtomicUsize>) {
    while writes_done.load(Ordering::Relaxed) == 0 {
        tokio::task::yield_now().await;
    }

    let mut found = 0;
    let mut not_found = 0;

    for probe in 0..READER_PROBES {
        let writer_id = probe % NUM_WRITERS;
        let key_id = probe % ENTRIES_PER_WRITER;
        let key = PrimaryKey::Varchar(format!("key_{writer_id}_{key_id}"));

        match db.get("entries", &key).await {
            Ok(Some(_)) => found += 1,
            Ok(None) => not_found += 1,
            Err(e) => error!(reader_id, error = %e, "reader error"),
        }
    }

    let total_writes = writes_done.load(Ordering::Relaxed);
    info!(
        reader_id,
        found,
        not_found,
        writes_at_finish = total_writes,
        total_writes = NUM_WRITERS * ENTRIES_PER_WRITER,
        "reader done"
    );
}

#[instrument]
async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let tmp_dir = std::env::temp_dir().join(format!("norns-db-example-{}", std::process::id()));

    info!(dir = %tmp_dir.display(), "starting norns-db playground");

    let config = DatabaseConfig {
        memtable_size: 10000,
        level_0_size: 15,
        ss_table_block_size: 4096,
    };

    let db = Arc::new(Database::new(&tmp_dir, config)?);

    let schema = TableSchema {
        primary_key_name: "key".to_string(),
        primary_key_type: PrimaryKeyType::Varchar,
        columns: vec![("value".to_string(), ColumnType::Varchar)],
    };
    db.create_table("entries", schema).await?;

    info!(
        writers = NUM_WRITERS,
        entries_per_writer = ENTRIES_PER_WRITER,
        readers = NUM_READERS,
        reader_probes = READER_PROBES,
        "starting concurrent workload"
    );

    let writes_done = Arc::new(AtomicUsize::new(0));

    // -- Spawn concurrent writers --
    let mut writer_handles = vec![];
    for writer_id in 0..NUM_WRITERS {
        let db = db.clone();
        let writes_done = writes_done.clone();
        let handle = tokio::spawn(writer_task(writer_id, db, writes_done));
        writer_handles.push(handle);
    }

    // -- Spawn concurrent readers --
    let mut reader_handles = vec![];
    for reader_id in 0..NUM_READERS {
        let db = db.clone();
        let writes_done = writes_done.clone();
        let handle = tokio::spawn(reader_task(reader_id, db, writes_done));
        reader_handles.push(handle);
    }

    // -- Wait for everything --
    for h in writer_handles {
        h.await?;
    }
    for h in reader_handles {
        h.await?;
    }

    // -- Verify all data is present --
    info!("all tasks finished, verifying data integrity");

    let mut verified = 0;
    for writer_id in 0..NUM_WRITERS {
        for i in 0..ENTRIES_PER_WRITER {
            let key = PrimaryKey::Varchar(format!("key_{writer_id}_{i}"));
            let expected = format!("value_{writer_id}_{i}");

            let row = db
                .get("entries", &key)
                .await?
                .unwrap_or_else(|| panic!("missing key: key_{writer_id}_{i}"));

            match &row.0[0] {
                Column::Varchar(v) => assert_eq!(v, &expected, "mismatch for key_{writer_id}_{i}"),
                other => panic!("unexpected column type: {other:?}"),
            }
            verified += 1;
        }
    }
    info!(verified, "all entries verified OK");

    // -- Demonstrate delete --
    let del_key = PrimaryKey::Varchar("key_0_0".to_string());
    info!("deleting key_0_0");
    db.delete("entries", del_key.clone()).await?;
    assert!(db.get("entries", &del_key).await?.is_none());
    info!("key_0_0 deleted and confirmed absent");

    // -- Demonstrate drop table --
    info!("dropping table 'entries'");
    db.drop_table("entries").await?;

    let names = db.table_names().await;
    assert!(names.is_empty());
    info!("table dropped, database is empty");

    // -- Cleanup --
    let db = Arc::into_inner(db).expect("outstanding Arc references to Database");
    db.destroy().await?;
    info!("database destroyed");

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

    let tracing_enabled = std::env::var("NORNS_TRACING").is_ok();

    // -- Traces (conditional on NORNS_TRACING) --
    let tracer_provider = if tracing_enabled {
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .build()?;
        Some(
            opentelemetry_sdk::trace::SdkTracerProvider::builder()
                .with_batch_exporter(exporter)
                .build(),
        )
    } else {
        None
    };

    // -- Metrics (always on) --
    let otlp_metric_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .build()?;

    let (meter_provider, _recorder) =
        metrics_exporter_opentelemetry::Recorder::builder("norns-db-playground")
            .with_meter_provider(|b| {
                let reader =
                    opentelemetry_sdk::metrics::PeriodicReader::builder(otlp_metric_exporter)
                        .with_interval(std::time::Duration::from_secs(5))
                        .build();
                b.with_reader(reader)
            })
            .install()?;

    opentelemetry::global::set_meter_provider(meter_provider.clone());

    // -- Logs (always on) --
    let otlp_log_exporter = opentelemetry_otlp::LogExporter::builder()
        .with_http()
        .build()?;

    let logger_provider = opentelemetry_sdk::logs::SdkLoggerProvider::builder()
        .with_batch_exporter(otlp_log_exporter)
        .build();

    // Option<Layer> is itself a Layer (None = no-op)
    let otel_trace_layer = tracer_provider
        .as_ref()
        .map(|tp| tracing_opentelemetry::layer().with_tracer(tp.tracer("norns-db-playground")));

    // Filter out OpenTelemetry SDK internal logs from the bridge to prevent
    // infinite recursion: bridge → logger → SDK log → bridge → ...
    let otel_log_layer =
        opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge::new(&logger_provider)
            .with_filter(
                tracing_subscriber::filter::Targets::new()
                    .with_default(tracing::Level::TRACE)
                    .with_target(
                        "opentelemetry",
                        tracing_subscriber::filter::LevelFilter::OFF,
                    )
                    .with_target(
                        "opentelemetry_sdk",
                        tracing_subscriber::filter::LevelFilter::OFF,
                    ),
            );

    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    let fmt_layer = tracing_subscriber::fmt::layer().json();

    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .with(otel_trace_layer)
        .with(otel_log_layer)
        .init();

    run().await?;

    // Shutdown OpenTelemetry providers, flushing any pending data.
    // Logger provider last so it can still capture shutdown logs from the others.
    meter_provider.shutdown()?;
    if let Some(tp) = tracer_provider {
        tp.shutdown()?;
    }
    logger_provider.shutdown()?;

    Ok(())
}
