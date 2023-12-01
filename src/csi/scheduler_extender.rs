//! `DatenLord` K8S Scheduler Extender

use std::collections::{HashMap, HashSet};
use std::io::Error;
use std::net::SocketAddr;
use std::sync::Arc;

use clippy_utilities::OverflowArithmetic;
use k8s_openapi::api::core::v1::{Node, Pod};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta;
use serde::{Deserialize, Serialize};
use tiny_http::{Method, Request, Response, Server, StatusCode};
use tracing::{error, info};

use super::meta_data::MetaData;
use crate::common::error::DatenLordResult;

/// Node List
#[derive(Clone, Debug, Serialize, Deserialize)]
struct NodeList {
    /// Standard list metadata
    metadata: ListMeta,
    /// List of nodes
    items: Vec<Node>,
}

/// Extender Arguments
#[allow(non_snake_case)]
#[derive(Clone, Debug, Serialize, Deserialize)]
struct ExtenderArgs {
    /// Pod being scheduled
    pub Pod: Pod,
    /// List of candidate nodes where the pod can be scheduled; to be populated
    /// only if Extender.NodeCacheCapable == false
    pub Nodes: Option<NodeList>,
    /// List of candidate node names where the pod can be scheduled; to be
    /// populated only if Extender.NodeCacheCapable == true
    pub NodeNames: Option<Vec<String>>,
}

/// Extender Filter Result
#[allow(non_snake_case)]
#[derive(Clone, Debug, Serialize, Deserialize)]
struct ExtenderFilterResult {
    /// Filtered set of nodes where the pod can be scheduled; to be populated
    /// only if Extender.NodeCacheCapable == false
    pub Nodes: Option<NodeList>,
    /// Filtered set of nodes where the pod can be scheduled; to be populated
    /// only if Extender.NodeCacheCapable == true
    pub NodeNames: Option<Vec<String>>,
    /// Filtered out nodes where the pod can't be scheduled and the failure
    /// messages
    pub FailedNodes: HashMap<String, String>,
    /// Error message indicating failure
    pub Error: String,
}

/// Host Priority
#[allow(non_snake_case)]
#[derive(Clone, Debug, Serialize, Deserialize)]
struct HostPriority {
    /// Name of the host
    pub Host: String,
    /// Score associated with the host
    pub Score: i64,
}

/// Scheduler Extender
pub struct SchedulerExtender {
    /// Meta Data
    meta_data: Arc<MetaData>,
    /// Address of scheduler extender
    address: SocketAddr,
    /// Http server
    server: Server,
}

/// Send error reply if result is error or get wrapped data
macro_rules! try_or_return_err {
    ($request:expr, $result:expr, $error:expr) => {
        match $result {
            Ok(r) => r,
            Err(err) => {
                error!("error is: {}", err);
                return Ok(
                    $request.respond(Response::from_string(serde_json::to_string(
                        &ExtenderFilterResult {
                            Nodes: None,
                            NodeNames: None,
                            FailedNodes: HashMap::new(),
                            Error: $error,
                        },
                    )?))?,
                );
            }
        }
    };
}
impl SchedulerExtender {
    /// Create `SchedulerExtender`
    pub fn new(meta_data: Arc<MetaData>, address: SocketAddr) -> Self {
        let server = Server::http(address)
            .unwrap_or_else(|e| panic!("failed to create server at {address}, error is: {e}"));
        Self {
            meta_data,
            address,
            server,
        }
    }

    /// Start scheduler extender server
    pub async fn start(&self) {
        info!("listening on http://{}", self.address);
        for req in self.server.incoming_requests() {
            if let Err(e) = self.scheduler(req).await {
                error!("Failed to send response, error is: {}", e);
            }
        }
    }

    /// Filter candidate nodes
    #[allow(clippy::redundant_closure_for_method_calls)]
    async fn filter(&self, args: ExtenderArgs) -> ExtenderFilterResult {
        let pod = args.Pod.clone();
        info!("Pod name is {:?}", pod.metadata.name);

        if let Some(volumes) = pod.spec.and_then(|pod_spec| pod_spec.volumes) {
            let all_nodes_res = self.meta_data.get_nodes().await;
            match all_nodes_res {
                Ok(all_nodes) => {
                    let mut nodes_map = HashMap::new();
                    for node in all_nodes {
                        nodes_map.insert(node, 1_usize);
                    }
                    for vol in &volumes {
                        let vol_res = self.meta_data.get_volume_by_name(&vol.name).await;
                        if let Ok(vol) = vol_res {
                            vol.accessible_nodes.iter().for_each(|node| {
                                if let Some(count) = nodes_map.get_mut(node) {
                                    (*count).overflow_add(1);
                                }
                            });
                        }
                    }
                    let accessible_nodes: HashSet<_> = nodes_map
                        .iter()
                        .filter_map(|(k, v)| (v == &volumes.len()).then_some(k))
                        .collect();

                    if let Some(ref node_list) = args.Nodes {
                        let candidate_nodes: Vec<_> = node_list
                            .items
                            .iter()
                            .filter_map(|n| {
                                n.metadata.name.as_ref().and_then(|name| {
                                    accessible_nodes.contains(&name).then(|| n.clone())
                                })
                            })
                            .collect();
                        ExtenderFilterResult {
                            Nodes: Some(NodeList {
                                metadata: node_list.metadata.clone(),
                                items: candidate_nodes,
                            }),
                            NodeNames: None,
                            FailedNodes: HashMap::new(),
                            Error: String::new(),
                        }
                    } else if let Some(ref nodes) = args.NodeNames {
                        let candidate_nodes: Vec<_> = nodes
                            .iter()
                            .filter(|&n| accessible_nodes.contains(n))
                            .map(|n| n.clone())
                            .collect();
                        ExtenderFilterResult {
                            Nodes: None,
                            NodeNames: Some(candidate_nodes),
                            FailedNodes: HashMap::new(),
                            Error: String::new(),
                        }
                    } else {
                        ExtenderFilterResult {
                            Nodes: args.Nodes,
                            NodeNames: args.NodeNames,
                            FailedNodes: HashMap::new(),
                            Error: String::new(),
                        }
                    }
                }
                Err(e) => ExtenderFilterResult {
                    Nodes: args.Nodes.map(|node_list| NodeList {
                        metadata: node_list.metadata,
                        items: vec![],
                    }),
                    NodeNames: args.NodeNames.and(Some(vec![])),
                    FailedNodes: HashMap::new(),
                    Error: format!(
                        "failed to get all nodes from etcd, failed to schedule, the error is {e}"
                    ),
                },
            }
        } else {
            ExtenderFilterResult {
                Nodes: args.Nodes,
                NodeNames: args.NodeNames,
                FailedNodes: HashMap::new(),
                Error: String::new(),
            }
        }
    }

    /// Prioritize candidate nodes
    const fn prioritize(_args: &ExtenderArgs) -> Vec<HostPriority> {
        // TODO: add prioritize logic
        vec![]
    }

    /// Reply empty 400 response
    fn empty_400(request: Request) -> DatenLordResult<()> {
        Ok(request.respond(Response::empty(StatusCode(400)))?)
    }

    /// Scheduler pod
    async fn scheduler(&self, mut request: Request) -> DatenLordResult<()> {
        info!("{:?}", request);
        match *request.method() {
            Method::Post => match request.url() {
                "/filter" | "/prioritize" => {
                    let body = request.as_reader();
                    let args: ExtenderArgs = try_or_return_err!(
                        request,
                        serde_json::from_reader(body),
                        "failed to parse request".to_owned()
                    );

                    let response = if request.url() == "/filter" {
                        info!("Receive filter");
                        let result = self.filter(args).await;
                        try_or_return_err!(
                            request,
                            serde_json::to_string(&result),
                            "failed to serialize response".to_owned()
                        )
                    } else {
                        info!("Receive prioritize");
                        let result = Self::prioritize(&args);
                        try_or_return_err!(
                            request,
                            serde_json::to_string(&result),
                            "failed to serialize response".to_owned()
                        )
                    };
                    Ok(request.respond(Response::from_string(response))?)
                }
                _ => Self::empty_400(request),
            },
            Method::Get => request
                .respond(Response::from_string("hello"))
                .map_err(Error::into),
            Method::Head
            | Method::Put
            | Method::Delete
            | Method::Connect
            | Method::Options
            | Method::Trace
            | Method::Patch
            | Method::NonStandard(..) => Self::empty_400(request),
        }
    }
}
