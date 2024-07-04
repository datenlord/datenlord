use std::sync::Once;

use tracing::level_filters::LevelFilter;
use tracing_subscriber::{
    filter, fmt::layer, layer::SubscriberExt, util::SubscriberInitExt, Layer,
};

/// This module contains the RPC server and client for the cache service.

/// The client module contains the client implementation for the cache service.
pub mod client;

/// The common module contains the shared structures and functions for the cache service.
pub mod common;

/// The error module contains the error types for the cache service.
pub mod error;

/// The message module contains the data structures shared between the client and server.
pub mod message;

/// The server module contains the server implementation for the cache service.
pub mod server;

/// The workerpool module contains the worker pool implementation for the cache service.
pub mod workerpool;

/// The packet module contains the packet encoding and decoding functions for the cache service.
pub mod packet;

/// The utils module contains the utility functions for the cache service.
#[macro_use]
pub mod utils;
