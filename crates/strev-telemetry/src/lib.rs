//! Tracing and metrics middleware for strev.
//!
//! [`Telemetry`] wraps a handler to emit a `tracing` span per message and `metrics`
//! facade measurements: a handler-duration histogram and acked/nacked/errored counters.
//! It is backend-agnostic; wire up whatever `tracing` subscriber and `metrics` exporter
//! your application already uses.
use std::time::{Duration, Instant};

use async_trait::async_trait;
use strev::{ConsumerLag, Handler, HandlerError, HandlerResult, Message, Middleware, Topic};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

const GAUGE_LAG: &str = "strev_consumer_lag";

/// Periodically record `source.lag(&topic)` as the `strev_consumer_lag` gauge (labelled by
/// topic) until `shutdown` fires. Spawn it alongside a subscriber on a backend that
/// implements [`ConsumerLag`] (e.g. Postgres or Redis) to feed autoscaling and alerting.
pub async fn report_consumer_lag<L: ConsumerLag + ?Sized>(
    source: &L,
    topic: Topic,
    interval: Duration,
    shutdown: CancellationToken,
) {
    loop {
        if let Ok(lag) = source.lag(&topic).await {
            metrics::gauge!(GAUGE_LAG, "topic" => topic.as_str().to_string()).set(lag as f64);
        }

        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = tokio::time::sleep(interval) => {}
        }
    }
}

const COUNTER_TOTAL: &str = "strev_messages_total";
const COUNTER_ACKED: &str = "strev_messages_acked";
const COUNTER_NACKED: &str = "strev_messages_nacked";
const COUNTER_ERRORED: &str = "strev_messages_errored";
const HISTOGRAM_DURATION: &str = "strev_handler_duration_seconds";

#[derive(Default)]
pub struct Telemetry;

impl Telemetry {
    pub fn new() -> Self {
        Self
    }
}

impl Middleware for Telemetry {
    fn wrap(&self, next: Box<dyn Handler>) -> Box<dyn Handler> {
        Box::new(TelemetryHandler { next })
    }
}

struct TelemetryHandler {
    next: Box<dyn Handler>,
}

#[async_trait]
impl Handler for TelemetryHandler {
    async fn handle(&self, msg: Message) -> Result<HandlerResult, HandlerError> {
        let message_id = *msg.uuid();
        let span = tracing::info_span!("strev.handle", message_id = %message_id);
        let start = Instant::now();

        let result = self.next.handle(msg).instrument(span).await;

        metrics::histogram!(HISTOGRAM_DURATION).record(start.elapsed().as_secs_f64());
        metrics::counter!(COUNTER_TOTAL).increment(1);
        match &result {
            Ok(handler_result) if handler_result.outcome().is_acked() => {
                metrics::counter!(COUNTER_ACKED).increment(1);
            }
            Ok(_) => {
                metrics::counter!(COUNTER_NACKED).increment(1);
            }
            Err(_) => {
                metrics::counter!(COUNTER_ERRORED).increment(1);
            }
        }

        result
    }
}
