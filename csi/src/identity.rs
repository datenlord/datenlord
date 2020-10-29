//! The implementation for CSI identity service

use grpcio::{RpcContext, UnarySink};
use log::debug;

use super::csi::{
    GetPluginCapabilitiesRequest, GetPluginCapabilitiesResponse, GetPluginInfoRequest,
    GetPluginInfoResponse, PluginCapability, PluginCapability_Service_Type, ProbeRequest,
    ProbeResponse,
};
use super::csi_grpc::Identity;
use super::util;

/// for `IdentityService` implmentation
#[derive(Clone)]
pub struct IdentityImpl {
    /// The name of the CSI plugin
    plugin_name: String,
    /// The version of the CSI plugin
    version: String,
}

impl IdentityImpl {
    /// Create `IdentityImpl`
    pub fn new(plugin_name: String, version: String) -> Self {
        assert!(!plugin_name.is_empty(), "driver name cannot be empty");
        assert!(!version.is_empty(), "version cannot be empty");
        Self {
            plugin_name,
            version,
        }
    }
}

impl Identity for IdentityImpl {
    fn get_plugin_info(
        &mut self,
        ctx: RpcContext,
        req: GetPluginInfoRequest,
        sink: UnarySink<GetPluginInfoResponse>,
    ) {
        debug!("get_plugin_info request: {:?}", req);

        let mut r = GetPluginInfoResponse::new();
        r.set_name(self.plugin_name.clone());
        r.set_vendor_version(self.version.clone());
        util::success(&ctx, sink, r)
    }

    fn get_plugin_capabilities(
        &mut self,
        ctx: RpcContext,
        req: GetPluginCapabilitiesRequest,
        sink: UnarySink<GetPluginCapabilitiesResponse>,
    ) {
        debug!("get_plugin_capabilities request: {:?}", req);

        let mut p1 = PluginCapability::new();
        p1.mut_service()
            .set_field_type(PluginCapability_Service_Type::CONTROLLER_SERVICE);
        let mut p2 = PluginCapability::new();
        p2.mut_service()
            .set_field_type(PluginCapability_Service_Type::VOLUME_ACCESSIBILITY_CONSTRAINTS);
        let mut r = GetPluginCapabilitiesResponse::new();
        r.set_capabilities(::protobuf::RepeatedField::from_vec(vec![p1, p2]));
        util::success(&ctx, sink, r)
    }

    fn probe(&mut self, ctx: RpcContext, req: ProbeRequest, sink: UnarySink<ProbeResponse>) {
        debug!("probe request: {:?}", req);

        let mut r = ProbeResponse::new();
        r.mut_ready().set_value(true);
        util::success(&ctx, sink, r)
    }
}
