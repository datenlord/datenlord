use tracing::level_filters::LevelFilter as Level;
use tracing_subscriber::filter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::prelude::*;

/// Initialize the logger with the default settings.
/// The log file is located at `./datenlord.log`.
#[allow(clippy::let_underscore_must_use)]
#[inline]
pub fn init_logger() {
    let filter = filter::Targets::new()
        .with_target("hyper", Level::WARN)
        .with_target("h2", Level::WARN)
        .with_target("tower", Level::WARN)
        .with_target("datenlord::async_fuse::fuse", Level::INFO)
        .with_target("", Level::DEBUG);

    let file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open("./datenlord.log")
        .unwrap_or_else(|err| panic!("Failed to open log file ,err {err}"));

    let layer = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .event_format(tracing_subscriber::fmt::format().pretty())
        .with_writer(std::sync::Mutex::new(file))
        .with_filter(filter);

    let subscriber = tracing_subscriber::Registry::default().with(layer);

    if cfg!(test) {
        let _: Result<(), tracing::subscriber::SetGlobalDefaultError> =
            tracing::subscriber::set_global_default(subscriber);
    } else {
        tracing::subscriber::set_global_default(subscriber)
            .unwrap_or_else(|error| panic!("Could not set logger ,err {error}"));
    }
}
