//! The metrics server.

use hyper::header::CONTENT_TYPE;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};
use prometheus::{Encoder, TextEncoder};
use tracing::{error, info};

use super::DATENLORD_REGISTRY;

/// Serve prometheus requests, return metrics response
#[allow(clippy::unused_async)] // Hyper requires an async function
async fn serve_req(_req: Request<Body>) -> Result<Response<Body>, hyper::Error> {
    let encoder = TextEncoder::new();

    let metric_families = DATENLORD_REGISTRY.gather();
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
#[inline]
pub fn start_metrics_server() {
    tokio::task::spawn(async move {
        let addr = ([0, 0, 0, 0], 9897).into();
        let serve_future = Server::bind(&addr).serve(make_service_fn(|_| async {
            Ok::<_, hyper::Error>(service_fn(serve_req))
        }));

        info!("Metrics server is listening on: {addr}");

        if let Err(err) = serve_future.await {
            error!("Metric server error: {}", err);
        }
    });
}
