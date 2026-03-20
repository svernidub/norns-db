mod logs;
mod metrics;
mod traces;

use opentelemetry_sdk::propagation::TraceContextPropagator;
use std::error::Error;

pub struct TelemetryHandle {
    meter_provider: opentelemetry_sdk::metrics::SdkMeterProvider,
    tracer_provider: Option<opentelemetry_sdk::trace::SdkTracerProvider>,
    logger_provider: opentelemetry_sdk::logs::SdkLoggerProvider,
}

impl Drop for TelemetryHandle {
    fn drop(&mut self) {
        let _ = self.meter_provider.shutdown();

        if let Some(tp) = &self.tracer_provider {
            let _ = tp.shutdown();
        }

        let _ = self.logger_provider.shutdown();
    }
}

pub fn init_telemetry() -> Result<TelemetryHandle, Box<dyn Error>> {
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

    let tracer_provider = traces::init_tracer_provider()?;
    let meter_provider = metrics::init_metrics()?;
    let logger_provider = logs::init_logger_provider()?;

    logs::init_tracing_subscriber(&tracer_provider, &logger_provider);

    Ok(TelemetryHandle {
        meter_provider,
        tracer_provider,
        logger_provider,
    })
}
