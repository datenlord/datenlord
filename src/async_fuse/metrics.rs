use hyper::header::CONTENT_TYPE;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};
use lazy_static::lazy_static;
use prometheus::{opts, register_counter, Counter, Encoder, TextEncoder};
use tracing::debug;

lazy_static! {
    /// Datenlord cache hit metrics
    pub static ref CACHE_HITS: Counter = register_counter!(opts!(
        "datenlord_cache_hits",
        "Approximate number of Cache hits since last server start"
    ))
    .unwrap();
    /// Datenlord cache miss metrics
    pub static ref CACHE_MISSES: Counter = register_counter!(opts!(
        "datenlord_cache_misses",
        "Approximate number of Cache misses since last server start"
    ))
    .unwrap();
}

/// Serve prometheus requests, return metrics response
///
/// # Errors
/// Returns [`hyper::Error`]
#[allow(clippy::unused_async)] // Hyper requires an async function
pub async fn serve_req(_req: Request<Body>) -> Result<Response<Body>, hyper::Error> {
    let encoder = TextEncoder::new();

    let metric_families = prometheus::gather();
    let mut buffer = vec![];
    encoder
        .encode(&metric_families, &mut buffer)
        .unwrap_or_else(|_| panic!("Fail to encode metrics"));

    let response = Response::builder()
        .status(200)
        .header(CONTENT_TYPE, encoder.format_type())
        .body(Body::from(buffer))
        .unwrap_or_else(|_| panic!("Fail to build prometheus response"));
    Ok(response)
}

/// Start a server to process prometheus request
pub fn start_metrics_server() {
    tokio::task::spawn(async move {
        let addr = ([0, 0, 0, 0], 9897).into();
        let serve_future = Server::bind(&addr).serve(make_service_fn(|_| async {
            Ok::<_, hyper::Error>(service_fn(serve_req))
        }));
        if let Err(err) = serve_future.await {
            debug!("Metric server error: {}", err);
        }
    });
}
