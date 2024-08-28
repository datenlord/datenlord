use tracing::level_filters::LevelFilter as Level;
use tracing_subscriber::filter;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::prelude::*;

/// Represents the role of the logger.
#[derive(Debug)]
#[allow(clippy::upper_case_acronyms)] // consider making the acronym lowercase, except the initial letter: `Sdk`
pub enum LogRole {
    /// Same as `NodeRole::Node`.
    Node,
    /// Same as `NodeRole::Controller`.
    Controller,
    /// Same as `NodeRole::SchedulerExtender`.
    SchedulerExtender,
    /// Same as `NodeRole::AsyncFuse`.
    AsyncFuse,
    /// For testing purpose.
    #[cfg(test)]
    Test,
    /// For bind mounter which is a helper command to bind mount for non-root
    /// user.
    #[allow(dead_code)] // /bin/bind_mounter.rs is still using this.
    BindMounter,
    /// Same as `NodeRole::SDK`.
    #[allow(dead_code)] // /sdk is still using this.
    SDK,
}

impl From<crate::config::NodeRole> for LogRole {
    #[inline]
    fn from(role: crate::config::NodeRole) -> Self {
        match role {
            crate::config::NodeRole::Node => LogRole::Node,
            crate::config::NodeRole::Controller => LogRole::Controller,
            crate::config::NodeRole::SchedulerExtender => LogRole::SchedulerExtender,
            crate::config::NodeRole::AsyncFuse | crate::config::NodeRole::SDK => LogRole::AsyncFuse,
        }
    }
}

impl LogRole {
    /// Returns the string representation of the log role.
    #[must_use]
    #[inline]
    pub fn as_str(&self) -> &'static str {
        match *self {
            LogRole::Node => "node",
            LogRole::Controller => "controller",
            LogRole::SchedulerExtender => "scheduler_extender",
            LogRole::AsyncFuse => "async_fuse",
            #[cfg(test)]
            LogRole::Test => "test",
            LogRole::BindMounter => "bind_mounter",
            LogRole::SDK => "sdk",
        }
    }
}
/// Initialize the logger with the default settings.
/// The log file is located at `./datenlord.log`.
#[allow(clippy::let_underscore_must_use)]
#[allow(clippy::needless_pass_by_value)] // Just pass a temporary value is fine.
#[inline]
pub fn init_logger(role: LogRole, level: Level) {
    let filter = filter::Targets::new()
        .with_target("hyper", Level::WARN)
        .with_target("h2", Level::WARN)
        .with_target("tower", Level::WARN)
        .with_target("datenlord::async_fuse::fuse", Level::ERROR)
        .with_target("datenlord::metrics", Level::INFO)
        .with_target("datenlordsdk", Level::DEBUG)
        .with_target("", level);

    let log_path = format!("./datenlord_{}.log", role.as_str());
    let file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(log_path)
        .unwrap_or_else(|err| panic!("Failed to open log file ,err {err}"));

    let layer = tracing_subscriber::fmt::layer()
        .compact()
        .with_file(false)
        .with_target(false)
        .with_ansi(false)
        .with_writer(std::sync::Mutex::new(file))
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
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
