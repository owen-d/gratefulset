use k8s_openapi::api::apps::v1::{StatefulSetSpec, StatefulSetStatus};
use kube_derive::CustomResource;
use serde::{Deserialize, Serialize};

#[derive(CustomResource, Clone, Debug, Deserialize, Serialize)]
#[kube(
    group = "owen-d.dev",
    version = "v1",
    kind = "GratefulSet",
    status = "GratefulSetStatus",
    shortname = "gs",
    scale = r#"{"specReplicasPath":".spec.replicas", "statusReplicasPath":".status.replicas"}"#,
    namespaced
)]

pub struct GratefulSetSpec {
    name: String,
    info: String,
    statefulset_spec: StatefulSetSpec,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
pub struct GratefulSetStatus {
    replicas: i32,
    cur: StatefulSetStatus,
    prev: Vec<StatefulSetStatus>,
}
