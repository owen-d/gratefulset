use k8s_openapi::api::apps::v1::StatefulSetSpec;
use kube_derive::CustomResource;
use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};

#[derive(CustomResource, Clone, Debug, Deserialize, Serialize)]
#[kube(
    group = "pikach.us",
    version = "v1",
    kind = "GratefulSet",
    status = "GratefulSetStatus",
    shortname = "gs",
    scale = r#"{"specReplicasPath":".spec.replicas", "statusReplicasPath":".status.replicas"}"#,
    namespaced
)]
pub struct GratefulSetSpec {
    name: String,
    statefulset_spec: StatefulSetSpec,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
pub struct GratefulSetStatus {
    /// currentReplicas is the number of Pods created by the StatefulSet controller from the StatefulSet version indicated by currentRevision.
    pub current_replicas: Option<i32>,

    /// readyReplicas is the number of Pods created by the StatefulSet controller that have a Ready Condition.
    pub ready_replicas: Option<i32>,

    /// replicas is the number of Pods created by the StatefulSet controller.
    pub replicas: i32,

    /// updatedReplicas is the number of Pods created by the StatefulSet controller from the StatefulSet version indicated by updateRevision.
    pub updated_replicas: Option<i32>,
}

#[derive(CustomResource, Clone, Debug, Deserialize, Serialize)]
#[kube(
    group = "pikach.us",
    version = "v1",
    kind = "GratefulSetPool",
    status = "GratefulSetPoolStatus",
    shortname = "gsp",
    scale = r#"{"specReplicasPath":".spec.replicas", "statusReplicasPath":".status.replicas"}"#,
    namespaced
)]
pub struct GratefulSetPoolSpec {
    pub name: String,
    pub statefulset_spec: StatefulSetSpec,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
pub struct GratefulSetPoolStatus {
    /// currentReplicas is the number of Pods created by the StatefulSet controller from the StatefulSet version indicated by currentRevision.
    pub current_replicas: Option<i32>,

    /// readyReplicas is the number of Pods created by the StatefulSet controller that have a Ready Condition.
    pub ready_replicas: Option<i32>,

    /// replicas is the number of Pods created by the StatefulSet controller.
    pub replicas: i32,

    /// updatedReplicas is the number of Pods created by the StatefulSet controller from the StatefulSet version indicated by updateRevision.
    pub updated_replicas: Option<i32>,
}

impl GratefulSetPoolStatus {
    // stabilized indicates whether the underlying statefulset is ready and up to date.
    fn stabilized(&self) -> bool {
        [
            self.current_replicas,
            self.ready_replicas,
            self.updated_replicas,
        ]
        .iter()
        .all(|x| match *x {
            Some(x) => x == self.replicas,
            _ => false,
        })
        // self.replicas == self.current_replicas == self.ready_replicas == self.updated_replicas
    }
}

pub struct ImmutableSts<'a>(pub &'a StatefulSetSpec);

impl<'a> Hash for ImmutableSts<'a> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.pod_management_policy.hash(state);
        self.0.revision_history_limit.hash(state);
        if let Some(xs) = &self.0.selector.match_expressions {
            for x in xs {
                x.key.hash(state);
                x.operator.hash(state);
                x.values.hash(state);
            }
        }
        self.0.selector.match_labels.hash(state);
        self.0.service_name.hash(state);
        if let Some(xs) = &self.0.volume_claim_templates {
            for x in xs {
                x.metadata.annotations.hash(state);
                x.metadata.cluster_name.hash(state);
                x.metadata.deletion_grace_period_seconds.hash(state);
                x.metadata.finalizers.hash(state);
                x.metadata.generate_name.hash(state);
                x.metadata.labels.hash(state);
                x.metadata.name.hash(state);
                x.metadata.namespace.hash(state);

                if let Some(spec) = &x.spec {
                    spec.access_modes.hash(state);
                    if let Some(r) = &spec.resources {
                        if let Some(l) = &r.limits {
                            for (k, v) in l {
                                k.hash(state);
                                v.0.as_str().hash(state);
                            }
                        }
                        if let Some(reqs) = &r.limits {
                            for (k, v) in reqs {
                                k.hash(state);
                                v.0.as_str().hash(state);
                            }
                        }
                    }

                    if let Some(ls) = &spec.selector {
                        if let Some(xs) = &ls.match_expressions {
                            for x in xs {
                                x.key.hash(state);
                                x.operator.hash(state);
                                x.values.hash(state);
                            }
                        }
                        ls.match_labels.hash(state);
                    }

                    spec.storage_class_name.hash(state);
                    spec.volume_mode.hash(state);
                    spec.volume_name.hash(state);
                }
            }
        }
    }
}

impl<'a> ImmutableSts<'a> {
    // discard all but 16 bits. We'll hex them into 8 characters.
    pub fn checksum(&self) -> u16 {
        let mut s = std::collections::hash_map::DefaultHasher::new();
        self.hash(&mut s);
        s.finish() as u16
    }
}
