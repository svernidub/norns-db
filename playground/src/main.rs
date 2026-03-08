use journal::{Journal, JournalRecord, JournalRecordKind};
use opentelemetry::trace::TracerProvider;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use storage::lsm_tree::LsmTree;
use tracing::{error, info, instrument};
use tracing_subscriber::{Layer, layer::SubscriberExt, util::SubscriberInitExt};

const NUM_WRITERS: usize = 10;
const ENTRIES_PER_WRITER: usize = 15000;
const NUM_READERS: usize = 4;
const READER_PROBES: usize = 2000;

/// Demonstrates WAL + LSM tree working together with concurrent reads and writes.
///
/// The write path:
///   1. Encode the operation as a JournalRecord
///   2. Append to WAL (fsync) — this is the durability guarantee
///   3. Insert into the LSM tree memtable — fast, in-memory
///
/// On crash recovery (not shown here), you'd replay the WAL to rebuild the memtable
/// for any entries that weren't flushed to SSTables yet.
///
/// The read path hits (in order):
///   1. Active memtable (in-memory BTreeMap)
///   2. Frozen memtable (if a flush is in progress)
///   3. Level-0 SSTables (most recent first)
///   4. Level-1 SSTables (compacted)
#[instrument(skip(lsm, journal, writes_done))]
async fn writer_task(
    writer_id: usize,
    lsm: Arc<LsmTree<String, String>>,
    journal: Arc<Journal<String, String>>,
    writes_done: Arc<AtomicUsize>,
) {
    for i in 0..ENTRIES_PER_WRITER {
        let key = format!("key_{writer_id}_{i}");
        let value = format!("value_{writer_id}_{i}");

        let record = JournalRecord::new(JournalRecordKind::Upsert {
            key: key.clone(),
            value: value.clone(),
        });
        journal.append(&record).await.expect("WAL append failed");

        lsm.insert(key, value).expect("LSM insert failed");

        writes_done.fetch_add(1, Ordering::Relaxed);
    }
    info!(writer_id, entries = ENTRIES_PER_WRITER, "writer done");
}

#[instrument(skip(lsm, writes_done))]
fn reader_task(reader_id: usize, lsm: Arc<LsmTree<String, String>>, writes_done: Arc<AtomicUsize>) {
    let mut found = 0;
    let mut not_found = 0;

    while writes_done.load(Ordering::Relaxed) == 0 {
        std::thread::yield_now();
    }

    for probe in 0..READER_PROBES {
        let writer_id = probe % NUM_WRITERS;
        let key_id = probe % ENTRIES_PER_WRITER;
        let key = format!("key_{writer_id}_{key_id}");

        match lsm.get(&key) {
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
    std::fs::create_dir_all(&tmp_dir)?;

    let data_dir = tmp_dir.join("lsm_data");
    let wal_path = tmp_dir.join("wal.log");

    info!(dir = %tmp_dir.display(), "starting norns-db playground");

    // -- Initialize the LSM tree and WAL --
    let lsm = Arc::new(LsmTree::<String, String>::new(
        data_dir.to_string_lossy().to_string(),
        10000, // memtable_size: flush to SSTable every 100 entries
        15,    // level_0_size: compact after 4 L0 SSTables
        4096,  // ss_table_block_size
    )?);

    let journal: Arc<Journal<String, String>> = Arc::new(Journal::new(&wal_path)?);

    info!(
        writers = NUM_WRITERS,
        entries_per_writer = ENTRIES_PER_WRITER,
        readers = NUM_READERS,
        reader_probes = READER_PROBES,
        "starting concurrent workload"
    );

    let writes_done = Arc::new(AtomicUsize::new(0));

    // -- Spawn concurrent writers --
    // Each writer owns a range of keys and follows the WAL-first protocol.
    let mut writer_handles = vec![];
    for writer_id in 0..NUM_WRITERS {
        let lsm = lsm.clone();
        let journal = journal.clone();
        let writes_done = writes_done.clone();

        let handle = tokio::spawn(writer_task(writer_id, lsm, journal, writes_done));
        writer_handles.push(handle);
    }

    // -- Spawn concurrent readers --
    // Readers probe random keys while writers are still active.
    // Some probes will miss (key not yet written) — that's expected.
    let mut reader_handles = vec![];
    for reader_id in 0..NUM_READERS {
        let lsm = lsm.clone();
        let writes_done = writes_done.clone();

        let handle = tokio::task::spawn_blocking(move || {
            reader_task(reader_id, lsm, writes_done);
        });
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
            let key = format!("key_{writer_id}_{i}");
            let expected = format!("value_{writer_id}_{i}");
            let actual = lsm
                .get(&key)?
                .unwrap_or_else(|| panic!("missing key: {key}"));
            assert_eq!(actual, expected, "mismatch for {key}");
            verified += 1;
        }
    }
    info!(verified, "all entries verified OK");

    // -- Demonstrate delete through WAL --
    info!("deleting key_0_0 via WAL");
    let del_record = JournalRecord::new(JournalRecordKind::Delete::<String, String> {
        key: "key_0_0".to_string(),
    });
    journal.append(&del_record).await?;
    lsm.delete("key_0_0".to_string())?;
    assert!(lsm.get(&"key_0_0".to_string())?.is_none());
    info!("key_0_0 deleted and confirmed absent");

    // -- Final flush --
    info!("flushing remaining memtable to SSTables");
    lsm.flush()?;

    info!(wal_path = %wal_path.display(), "done");

    // Cleanup
    std::fs::remove_dir_all(&tmp_dir)?;

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
