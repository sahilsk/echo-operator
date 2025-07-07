use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(CustomResource, Debug, Serialize, Deserialize, Default, Clone, JsonSchema)]
#[kube(group = "sonukr.rs", version = "v1", kind = "Echo", namespaced)] 
// https://docs.rs/kube/1.1.0/kube/derive.CustomResource.html#example-with-all-properties
pub struct EchoSpec {
    pub message: String,
    pub count: usize,
}
