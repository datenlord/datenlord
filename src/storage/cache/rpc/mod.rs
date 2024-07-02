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


#[cfg(test)]
#[allow(clippy::unwrap_used)]
#[allow(clippy::indexing_slicing)]
mod tests {
    use bytes::BytesMut;
    use error::RpcError;
    use tokio::net::TcpListener;
    use tracing::debug;
    use tracing::level_filters::LevelFilter;
    use tracing_subscriber::fmt::layer;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::{filter, Layer};

    use crate::connect_timeout;
    use crate::storage::cache::rpc::packet::PacketStatus;

    use super::*;
    use std::time::Duration;

    #[derive(Debug, Clone)]
    pub struct TestFilePacket {
        pub seq: u64,
        pub op: u8,
        pub status: PacketStatus,
        pub buffer: BytesMut,
    }

    impl Packet for TestFilePacket {
        fn seq(&self) -> u64 {
            self.seq
        }

        fn set_seq(&mut self, seq: u64) {
            self.seq = seq;
        }

        fn op(&self) -> u8 {
            self.op
        }

        fn set_op(&mut self, op: u8) {
            self.op = op;
        }

        fn set_req_data(&mut self, data: &[u8]) -> Result<(), RpcError<String>> {
            // Try to set the request data
            debug!("Setting request data");

            Ok(())
        }

        fn get_req_data(&self) -> Result<Vec<u8>, RpcError<String>> {
            // Try to get the request data
            debug!("Getting request data");

            // Return a 4MB vec
            Ok(vec![0u8; 0])
        }

        fn set_resp_data(&mut self, _data: &[u8]) -> Result<(), RpcError<String>> {
            // Try to set the response data
            debug!("Setting response data");

            Ok(())
        }

        fn get_resp_data(&self) -> Result<Vec<u8>, RpcError<String>> {
            // Try to get the response data

            // Return a 4MB vec
            Ok(vec![0u8; 0])
        }

        fn status(&self) -> PacketStatus {
            self.status
        }

        fn set_status(&mut self, status: PacketStatus) {
            self.status = status;
        }
    }

    fn setup() {
        // Set the tracing log level to debug
        let filter =
            filter::Targets::new().with_target("datenlord::storage::cache", LevelFilter::DEBUG);
        tracing_subscriber::registry()
            .with(layer().with_filter(filter))
            .init();
    }

    #[tokio::test]
    async fn test_send_and_recv_packet() {

    }
}