//! The memory backend implementation

use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use dashmap::DashMap;
use itertools::Itertools;

use super::{Backend, StorageResult};
/// A memory backend for testing purposes.
#[derive(Clone)]
pub struct MemoryBackend {
    /// The inner map of memory backend.
    map: Arc<DashMap<String, Vec<u8>>>,
    /// The mock latency in ms
    latency: Duration,
}

impl Debug for MemoryBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let keys = self.map.iter().map(|k| k.key().clone()).collect_vec();
        f.debug_struct("MemoryBackend")
            .field("map", &keys)
            .field("latency", &self.latency)
            .finish()
    }
}

#[async_trait]
impl Backend for MemoryBackend {
    #[inline]
    async fn read(&self, path: &str, buf: &mut [u8]) -> StorageResult<usize> {
        // mock latency
        tokio::time::sleep(self.latency).await;

        let Some(data) = self.map.get(path) else {
            return Ok(0);
        };
        let len = data.len().min(buf.len());
        let buf = buf
            .get_mut(..len)
            .unwrap_or_else(|| unreachable!("The length is checked to be enough."));
        let data = data
            .get(..len)
            .unwrap_or_else(|| unreachable!("The length is checked to be enough."));
        buf.copy_from_slice(data);
        Ok(len)
    }

    #[inline]
    async fn write(&self, path: &str, buf: &[u8]) -> StorageResult<()> {
        // mock latency
        tokio::time::sleep(self.latency).await;
        self.map.insert(path.to_owned(), buf.to_vec());
        Ok(())
    }

    #[inline]
    async fn remove(&self, path: &str) -> StorageResult<()> {
        // mock latency
        tokio::time::sleep(self.latency).await;
        self.map.remove(path);
        Ok(())
    }

    #[inline]
    async fn remove_all(&self, prefix: &str) -> StorageResult<()> {
        tokio::time::sleep(self.latency).await;
        self.map.retain(|k, _| !k.starts_with(prefix));
        Ok(())
    }
}

impl MemoryBackend {
    /// Creates a new `MemoryBackend` instance with the given latency.
    #[inline]
    #[must_use]
    pub fn new(latency: Duration) -> Self {
        MemoryBackend {
            map: Arc::new(DashMap::new()),
            latency,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use tokio::time::Instant;

    use super::super::test_backend;
    use super::*;

    #[tokio::test]
    async fn test_latency() {
        let backend = MemoryBackend::new(Duration::from_millis(100));

        let instant = Instant::now();
        test_backend(backend).await;
        let latency = instant.elapsed().as_millis();
        assert!(latency >= 400, "latency = {latency} ms");
    }

    #[tokio::test]
    async fn test_remove_all() {
        let backend = MemoryBackend::new(Duration::from_millis(0));
        let mut buf = vec![0; 16];
        backend.write("a/1", &buf).await.unwrap();
        backend.write("a/2", &buf).await.unwrap();

        backend.remove_all("a/").await.unwrap();

        let size = backend.read("a/1", &mut buf).await.unwrap();
        assert_eq!(size, 0);
        let size = backend.read("a/2", &mut buf).await.unwrap();
        assert_eq!(size, 0);
    }
}
