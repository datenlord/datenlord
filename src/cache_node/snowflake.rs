//! Snowflake ID generator

use std::time::{SystemTime, UNIX_EPOCH};

/// The epoch of the Twitter snowflake algorithm
const TWEPOCH: i64 = 1288834974657;
/// The number of bits for the worker ID
const WORKER_ID_BITS: u64 = 5;
/// The number of bits for the datacenter ID
const DATACENTER_ID_BITS: u64 = 5;
/// The number of bits for the sequence
const SEQUENCE_BITS: u64 = 12;
/// The maximum worker ID
const MAX_WORKER_ID: i64 = -1 ^ (-1 << WORKER_ID_BITS);
/// The maximum datacenter ID
const MAX_DATACENTER_ID: i64 = -1 ^ (-1 << DATACENTER_ID_BITS);
/// The worker ID shift
const WORKER_ID_SHIFT: u64 = SEQUENCE_BITS;
/// The datacenter ID shift
const DATACENTER_ID_SHIFT: u64 = SEQUENCE_BITS + WORKER_ID_BITS;
/// The timestamp left shift
const TIMESTAMP_LEFT_SHIFT: u64 = SEQUENCE_BITS + WORKER_ID_BITS + DATACENTER_ID_BITS;
/// The sequence mask
const SEQUENCE_MASK: i64 = -1 ^ (-1 << SEQUENCE_BITS);

/// A snowflake ID generator
#[derive(Debug)]
pub struct Snowflake {
    datacenter_id: i64,
    worker_id: i64,
    sequence: i64,
    last_timestamp: i64,
}

impl Snowflake {
    /// Create a new Snowflake ID generator
    pub fn new(datacenter_id: i64, worker_id: i64) -> Snowflake {
        if worker_id > MAX_WORKER_ID || worker_id < 0 {
            panic!(
                "worker Id can't be greater than {} or less than 0",
                MAX_WORKER_ID
            );
        }
        if datacenter_id > MAX_DATACENTER_ID || datacenter_id < 0 {
            panic!(
                "datacenter Id can't be greater than {} or less than 0",
                MAX_DATACENTER_ID
            );
        }
        Snowflake {
            datacenter_id,
            worker_id,
            sequence: 0,
            last_timestamp: -1,
        }
    }

    /// Generate the next ID
    pub fn next_id(&mut self) -> i64 {
        let mut timestamp = self.time_gen();

        if timestamp < self.last_timestamp {
            panic!(
                "Clock moved backwards. Refusing to generate id for {} milliseconds",
                self.last_timestamp - timestamp
            );
        }

        if self.last_timestamp == timestamp {
            self.sequence = (self.sequence + 1) & SEQUENCE_MASK;
            if self.sequence == 0 {
                timestamp = self.til_next_millis(self.last_timestamp);
            }
        } else {
            self.sequence = 0;
        }

        self.last_timestamp = timestamp;
        ((timestamp - TWEPOCH) << TIMESTAMP_LEFT_SHIFT)
            | (self.datacenter_id << DATACENTER_ID_SHIFT)
            | (self.worker_id << WORKER_ID_SHIFT)
            | self.sequence
    }

    fn til_next_millis(&self, last_timestamp: i64) -> i64 {
        let mut timestamp = self.time_gen();
        while timestamp <= last_timestamp {
            timestamp = self.time_gen();
            // Sleep for 10 millisecond
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        timestamp
    }

    #[allow(clippy::unused_self)]
    fn time_gen(&self) -> i64 {
        match SystemTime::now()
            .duration_since(UNIX_EPOCH) {
            Ok(n) => n.as_millis() as i64,
            Err(_) => {
                panic!("SystemTime before UNIX EPOCH!");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unique_id_generation() {
        let mut generator = Snowflake::new(0, 0);
        let id1 = generator.next_id();
        let id2 = generator.next_id();
        assert_ne!(id1, id2, "Generated IDs should be unique");
    }

    #[test]
    fn test_sequence_rollover() {
        let mut generator = Snowflake::new(0, 0);
        generator.last_timestamp = generator.time_gen();
        for _ in 0..=SEQUENCE_MASK {
            generator.next_id();
        }
        assert_eq!(
            generator.sequence, 0,
            "Sequence should roll over to 0 after reaching the maximum value"
        );
    }
}
