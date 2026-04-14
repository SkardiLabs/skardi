use anyhow::Result;
use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::trace::TracerProvider;
use opentelemetry_sdk::{Resource, runtime};

/// Guards OTel providers — shuts them down cleanly when dropped at the end of main.
pub struct TelemetryGuard {
    tracer_provider: TracerProvider,
    meter_provider: SdkMeterProvider,
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        if let Err(e) = self.tracer_provider.shutdown() {
            eprintln!("Error shutting down tracer provider: {e}");
        }
        if let Err(e) = self.meter_provider.shutdown() {
            eprintln!("Error shutting down meter provider: {e}");
        }
    }
}

/// Initialise OpenTelemetry traces and metrics, exporting via OTLP gRPC.
///
/// Returns `None` when `otlp_endpoint` is `None` — OTLP export is disabled
/// and no providers are registered.
///
/// Call this before setting up the tracing subscriber so the returned tracer
/// can be passed into `tracing_opentelemetry::layer()`.
///
/// The returned `TelemetryGuard` must be kept alive until the process exits.
pub fn init(
    otlp_endpoint: Option<&str>,
) -> Result<Option<(TelemetryGuard, opentelemetry_sdk::trace::Tracer)>> {
    let Some(otlp_endpoint) = otlp_endpoint else {
        return Ok(None);
    };

    let resource = Resource::new(vec![KeyValue::new(
        opentelemetry_semantic_conventions::resource::SERVICE_NAME,
        "skardi-server",
    )]);

    // --- Traces ---------------------------------------------------------------
    let span_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(otlp_endpoint)
        .build()?;

    let tracer_provider = TracerProvider::builder()
        .with_batch_exporter(span_exporter, runtime::Tokio)
        .with_resource(resource.clone())
        .build();

    global::set_tracer_provider(tracer_provider.clone());
    let tracer = tracer_provider.tracer("skardi-server");

    // --- Metrics --------------------------------------------------------------
    let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_endpoint(otlp_endpoint)
        .build()?;

    let reader = PeriodicReader::builder(metric_exporter, runtime::Tokio).build();

    let meter_provider = SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(resource)
        .build();

    global::set_meter_provider(meter_provider.clone());

    Ok(Some((
        TelemetryGuard {
            tracer_provider,
            meter_provider,
        },
        tracer,
    )))
}
