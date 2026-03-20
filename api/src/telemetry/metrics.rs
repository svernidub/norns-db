use opentelemetry::metrics::{Meter, MeterProvider};
use opentelemetry_sdk::metrics::{Aggregation, Instrument, SdkMeterProvider, Stream};
use std::{
    sync::{Arc, RwLock},
    time::Duration,
};
use sysinfo::{Pid, System};

const DURATION_US_BOUNDARIES: [f64; 16] = [
    50.0,        // 0.05 ms
    100.0,       // 0.1 ms
    250.0,       // 0.25 ms
    500.0,       // 0.5 ms
    1_000.0,     // 1 ms
    2_500.0,     // 2.5 ms
    5_000.0,     // 5 ms
    10_000.0,    // 10 ms
    25_000.0,    // 25 ms
    50_000.0,    // 50 ms
    100_000.0,   // 100 ms
    250_000.0,   // 250 ms
    500_000.0,   // 500 ms
    1_000_000.0, // 1s
    2_000_000.0, // 2s
    5_000_000.0, // 5s
];

const DURATION_MS_BOUNDARIES: [f64; 12] = [
    1.0,     // 1 ms
    2.0,     // 2 ms
    5.0,     // 5 ms
    10.0,    // 10 ms
    25.0,    // 25 ms
    50.0,    // 50 ms
    100.0,   // 100 ms
    250.0,   // 250 ms
    500.0,   // 500 ms
    1_000.0, // 1 s
    2_500.0, // 2.5 s
    5_000.0, // 5 s
];

const METRICS_INTERVAL: Duration = Duration::from_secs(5);

pub fn init_metrics() -> Result<SdkMeterProvider, Box<dyn std::error::Error>> {
    let otlp_metric_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .build()?;

    let (meter_provider, _recorder) =
        metrics_exporter_opentelemetry::Recorder::builder("norns-db-api")
            .with_meter_provider(|b| {
                let reader =
                    opentelemetry_sdk::metrics::PeriodicReader::builder(otlp_metric_exporter)
                        .with_interval(METRICS_INTERVAL)
                        .build();

                b.with_reader(reader)
                    .with_view(duration_us_view)
                    .with_view(duration_ms_view)
            })
            .install()?;

    let meter = meter_provider.meter("norns-db");
    register_process_gauges(&meter);

    opentelemetry::global::set_meter_provider(meter_provider.clone());

    Ok(meter_provider)
}

fn duration_us_view(i: &Instrument) -> Option<Stream> {
    if i.name().ends_with("_duration_us") {
        Stream::builder()
            .with_aggregation(Aggregation::ExplicitBucketHistogram {
                boundaries: DURATION_US_BOUNDARIES.to_vec(),
                record_min_max: true,
            })
            .build()
            .ok()
    } else {
        None
    }
}

fn duration_ms_view(i: &Instrument) -> Option<Stream> {
    if i.name().ends_with("_duration_ms") {
        Stream::builder()
            .with_aggregation(Aggregation::ExplicitBucketHistogram {
                boundaries: DURATION_MS_BOUNDARIES.to_vec(),
                record_min_max: true,
            })
            .build()
            .ok()
    } else {
        None
    }
}

fn register_process_gauges(meter: &Meter) {
    let sys = Arc::new(RwLock::new(System::new_all()));
    let pid = Pid::from(std::process::id() as usize);

    spawn_process_refresh(sys.clone(), pid);

    register_rss_gauge(meter, sys.clone(), pid);
    register_virtual_mem_gauge(meter, sys.clone(), pid);
    register_cpu_gauge(meter, sys.clone(), pid);
    register_disk_read_gauge(meter, sys.clone(), pid);
    register_disk_written_gauge(meter, sys, pid);
}

fn spawn_process_refresh(sys: Arc<RwLock<System>>, pid: Pid) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(METRICS_INTERVAL).await;

            let mut lock = sys.write().unwrap_or_else(|m| m.into_inner());
            lock.refresh_process(pid);
        }
    });
}

fn register_rss_gauge(meter: &Meter, sys: Arc<RwLock<System>>, pid: Pid) {
    let _gauge = meter
        .f64_observable_gauge("norns_api_process_memory_rss_bytes")
        .with_description("Process resident set size in bytes")
        .with_unit("bytes")
        .with_callback(move |observer| {
            let sys = sys.read().unwrap_or_else(|m| m.into_inner());
            if let Some(proc) = sys.process(pid) {
                observer.observe(proc.memory() as f64, &[]);
            }
        })
        .build();
}

fn register_virtual_mem_gauge(meter: &Meter, sys: Arc<RwLock<System>>, pid: Pid) {
    let _gauge = meter
        .f64_observable_gauge("norns_api_process_memory_virtual_bytes")
        .with_description("Process virtual memory usage in bytes")
        .with_unit("bytes")
        .with_callback(move |observer| {
            let sys = sys.read().unwrap_or_else(|m| m.into_inner());
            if let Some(proc) = sys.process(pid) {
                observer.observe(proc.virtual_memory() as f64, &[]);
            }
        })
        .build();
}

fn register_cpu_gauge(meter: &Meter, sys: Arc<RwLock<System>>, pid: Pid) {
    let _gauge = meter
        .f64_observable_gauge("norns_api_process_cpu_usage_percent")
        .with_description("Process CPU usage percentage")
        .with_unit("percent")
        .with_callback(move |observer| {
            let sys = sys.read().unwrap_or_else(|m| m.into_inner());
            if let Some(proc) = sys.process(pid) {
                observer.observe(proc.cpu_usage() as f64, &[]);
            }
        })
        .build();
}

fn register_disk_read_gauge(meter: &Meter, sys: Arc<RwLock<System>>, pid: Pid) {
    let _gauge = meter
        .f64_observable_gauge("norns_api_process_disk_read_bytes")
        .with_description("Total bytes read by the process (cumulative)")
        .with_unit("bytes")
        .with_callback(move |observer| {
            let sys = sys.read().unwrap_or_else(|m| m.into_inner());
            if let Some(proc) = sys.process(pid) {
                observer.observe(proc.disk_usage().read_bytes as f64, &[]);
            }
        })
        .build();
}

fn register_disk_written_gauge(meter: &Meter, sys: Arc<RwLock<System>>, pid: Pid) {
    let _gauge = meter
        .f64_observable_gauge("norns_api_process_disk_written_bytes")
        .with_description("Total bytes written by the process (cumulative)")
        .with_unit("bytes")
        .with_callback(move |observer| {
            let sys = sys.read().unwrap_or_else(|m| m.into_inner());
            if let Some(proc) = sys.process(pid) {
                observer.observe(proc.disk_usage().written_bytes as f64, &[]);
            }
        })
        .build();
}
