use std::fmt::Debug;

use anyhow::Error;

use crate::{node::Node, ring::HashRing};

/// Meta data client trait.
///
/// This trait is used to interact with meta data service.
/// Probably it's a etcd server
pub trait MetaClient {
    /// Create a new node meta data
    fn create_cache_node(&self, prefix: &str, node: Node) -> Result<(), Error>;

    /// Create a hashring to meta data
    fn create_hash_ring(&self, prefix: &str, ring: HashRing) -> Result<(), Error>;

    /// Update the node meta data
    fn update_cache_node(&self, prefix: &str, node: Node) -> Result<(), Error>;

    /// Update the hashring meta data
    fn update_hash_ring(&self, prefix: &str, ring: HashRing) -> Result<(), Error>;

    /// Delete the node meta data
    fn delete_cache_node(&self, prefix: &str) -> Result<(), Error>;

    /// Delete the hashring meta data
    fn delete_hash_ring(&self, prefix: &str) -> Result<(), Error>;

    /// Get the node meta data
    fn get_cache_node(&self, prefix: &str) -> Result<Node, Error>;

    /// Get the hashring meta data
    fn get_hash_ring(&self, prefix: &str) -> Result<HashRing, Error>;
}

/// Create a new meta data client
pub fn new_meta_client<C: MetaClient>(endpoints: Vec<String>) -> C {
    let _ = endpoints;
    unimplemented!()
}

/// ETCD client
///
/// This struct is used to interact with etcd server.
#[allow(dead_code)]
pub struct ETCDClient {
    /// client
    etcd_rs_client: etcd_client::Client,
    /// Etcd end points address
    endpoints: Vec<String>,
}

impl Debug for ETCDClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ETCDClient {{ endpoints: {:?} }}", self.endpoints)
    }
}

impl ETCDClient {
    /// Create a new etcd client
    pub async fn new(endpoints: Vec<String>) -> Result<Self, Error> {
        let endpoints = endpoints.clone();
        let etcd_rs_client = etcd_client::Client::connect(endpoints.clone(), None)
            .await
            .map_err(|e| Error::msg(format!("Failed to connect etcd server: {:?}", e)))?;

        Ok(Self {
            etcd_rs_client: etcd_rs_client,
            endpoints: endpoints,
        })
    }
}

impl MetaClient for ETCDClient {
    fn create_cache_node(&self, prefix: &str, node: Node) -> Result<(), Error> {
        let _ = prefix;
        let _ = node;
        unimplemented!()
    }

    fn create_hash_ring(&self, prefix: &str, ring: HashRing) -> Result<(), Error> {
        let _ = prefix;
        let _ = ring;
        unimplemented!()
    }

    fn update_cache_node(&self, prefix: &str, node: Node) -> Result<(), Error> {
        let _ = prefix;
        let _ = node;
        unimplemented!()
    }

    fn update_hash_ring(&self, prefix: &str, ring: HashRing) -> Result<(), Error> {
        let _ = prefix;
        let _ = ring;
        unimplemented!()
    }

    fn delete_cache_node(&self, prefix: &str) -> Result<(), Error> {
        let _ = prefix;
        unimplemented!()
    }

    fn delete_hash_ring(&self, prefix: &str) -> Result<(), Error> {
        let _ = prefix;
        unimplemented!()
    }

    fn get_cache_node(&self, prefix: &str) -> Result<Node, Error> {
        let _ = prefix;
        unimplemented!()
    }

    fn get_hash_ring(&self, prefix: &str) -> Result<HashRing, Error> {
        let _ = prefix;
        unimplemented!()
    }
}
