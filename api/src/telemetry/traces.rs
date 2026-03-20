use opentelemetry_sdk::trace::SdkTracerProvider;
use std::error::Error;

pub fn init_tracer_provider() -> Result<Option<SdkTracerProvider>, Box<dyn Error>> {
    if std::env::var("NORNS_TRACING").is_err() {
        return Ok(None);
    }

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .build()?;

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .build();

    Ok(Some(provider))
}
