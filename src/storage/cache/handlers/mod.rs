/// This module contains all the message for the server
pub mod message;

/// This module contains all the file handler for the server
pub mod file_handler;

/// This max packet size is used for control init buffer in client
pub const MAX_PACKET_SIZE: u64 =  8 * 1024 * 1024;