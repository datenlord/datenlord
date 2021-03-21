//! `DatenLord` K8S Scheduler Extender

use k8s_openapi::api::core::v1::Node;
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta;

use super::meta_data::MetaData;
use common::error::DatenLordResult;
use log::{error, info};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use tiny_http::{Method, Request, Response, Server, StatusCode};

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
    /// Filtered out nodes where the pod can't be scheduled and the failure messages
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
pub struct SchdulerExtender {
    /// Meta Data
    meta_data: Arc<MetaData>,
    /// Address of scheduler extender
    address: SocketAddr,
    /// Http server
    server: Server,
}

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
impl SchdulerExtender {
    /// Create `SchedulerExtender`
    pub fn new(meta_data: Arc<MetaData>, address: SocketAddr) -> Self {
        let server = Server::http(address)
            .unwrap_or_else(|e| panic!("failed to create server at {}, error is: {}", address, e));
        Self {
            meta_data,
            address,
            server,
        }
    }

    /// Start scheduler extender server
    pub fn start(&self) {
        info!("listening on http://{}", self.address);
        for req in self.server.incoming_requests() {
            if let Err(e) = self.scheduler(req) {
                error!("Faild to send response, error is: {}", e);
            }
        }
    }

    /// Filter candidate nodes
    fn filter(&self, args: ExtenderArgs) -> ExtenderFilterResult {
        let pod = args.Pod.clone();
        info!("Pod name is {:?}", pod.metadata.name);

        if let Some(volumes) = pod.spec.and_then(|pod_spec| pod_spec.volumes) {
            let mut node_set = HashSet::new();
            volumes.iter().for_each(|vol| {
                let vol_res =
                    smol::block_on(async { self.meta_data.get_volume_by_name(&vol.name).await });
                if let Ok(vol) = vol_res {
                    node_set.insert(vol.node_id);
                }
            });
            if node_set.len() > 1 {
                return ExtenderFilterResult {
                    Nodes: args.Nodes.map(|node_list| NodeList {
                        metadata: node_list.metadata,
                        items: vec![],
                    }),
                    NodeNames: args.NodeNames.and(Some(vec![])),
                    FailedNodes: HashMap::new(),
                    Error: "pod contains volumes that exist on different nodes, failed to schedule"
                        .to_string(),
                };
            } else {
                for node in &node_set {
                    if let Some(ref node_list) = args.Nodes {
                        let node_opt = node_list
                            .items
                            .iter()
                            .find(|&n| n.metadata.name.as_ref().map_or(false, |name| name == node));
                        if let Some(node) = node_opt {
                            return ExtenderFilterResult {
                                Nodes: Some(NodeList {
                                    metadata: node_list.metadata.to_owned(),
                                    items: vec![node.to_owned()],
                                }),
                                NodeNames: None,
                                FailedNodes: HashMap::new(),
                                Error: "".to_string(),
                            };
                        }
                    } else if let Some(ref nodes) = args.NodeNames {
                        let node_opt = nodes.iter().find(|&n| node == n);
                        if let Some(node) = node_opt {
                            return ExtenderFilterResult {
                                Nodes: None,
                                NodeNames: Some(vec![node.to_owned()]),
                                FailedNodes: HashMap::new(),
                                Error: "".to_string(),
                            };
                        }
                    } else {
                    }
                }
            }
        }

        ExtenderFilterResult {
            Nodes: args.Nodes,
            NodeNames: args.NodeNames,
            FailedNodes: HashMap::new(),
            Error: "".to_string(),
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
    fn scheduler(&self, mut request: Request) -> DatenLordResult<()> {
        info!("{:?}", request);
        match *request.method() {
            Method::Post => match request.url() {
                "/filter" | "/prioritize" => {
                    let body = request.as_reader();
                    let args: ExtenderArgs = try_or_return_err!(
                        request,
                        serde_json::from_reader(body),
                        "failed to parse request".to_string()
                    );

                    let response = if request.url() == "/filter" {
                        info!("Receive filter");
                        let result = self.filter(args);
                        try_or_return_err!(
                            request,
                            serde_json::to_string(&result),
                            "failed to serialize response".to_string()
                        )
                    } else {
                        info!("Receive prioritize");
                        let result = Self::prioritize(&args);
                        try_or_return_err!(
                            request,
                            serde_json::to_string(&result),
                            "failed to serialize response".to_string()
                        )
                    };
                    Ok(request.respond(Response::from_string(response))?)
                }
                _ => Self::empty_400(request),
            },
            Method::Get => request
                .respond(Response::from_string("hello"))
                .map_err(|e| e.into()),
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
