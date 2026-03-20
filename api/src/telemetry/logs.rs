use opentelemetry::trace::TracerProvider;
use opentelemetry_sdk::{logs::SdkLoggerProvider, trace::SdkTracerProvider};
use std::error::Error;
use tracing_subscriber::{Layer, layer::SubscriberExt, util::SubscriberInitExt};

pub fn init_logger_provider() -> Result<SdkLoggerProvider, Box<dyn Error>> {
    let exporter = opentelemetry_otlp::LogExporter::builder()
        .with_http()
        .build()?;

    Ok(SdkLoggerProvider::builder()
        .with_batch_exporter(exporter)
        .build())
}

pub fn init_tracing_subscriber(
    tracer_provider: &Option<SdkTracerProvider>,
    logger_provider: &SdkLoggerProvider,
) {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    let fmt_layer = tracing_subscriber::fmt::layer().json();

    let otel_trace_layer = tracer_provider
        .as_ref()
        .map(|tp| tracing_opentelemetry::layer().with_tracer(tp.tracer("norns-db-api")));

    let log_layer =
        opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge::new(logger_provider)
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

    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .with(otel_trace_layer)
        .with(log_layer)
        .init();
}
