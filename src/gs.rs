use k8s_openapi::api::apps::v1::{StatefulSetSpec, StatefulSetStatus};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
use k8s_openapi::{Metadata, Resource};
use kube::api::Meta;
use kube_derive::CustomResource;
use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};

#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, PartialEq, Default)]
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
    pub name: String,
    pub statefulset_spec: StatefulSetSpec,
}

impl GratefulSetSpec {
    pub fn pool(&self) -> GratefulSetPool {
        let name = format!(
            "{}-{:x}",
            self.name,
            ImmutableSts(&self.statefulset_spec).checksum()
        );
        let mut want = GratefulSetPool::new(
            &name,
            GratefulSetPoolSpec {
                statefulset_spec: self.statefulset_spec.clone(),
                ..Default::default()
            },
        );

        // Set owner reference and label pointing to gratefulset
        let mut want_md: &mut ObjectMeta = want.metadata_mut();
        want_md.owner_references = Some(vec![OwnerReference {
            kind: GratefulSet::KIND.to_string(),
            ..Default::default()
        }]);
        let mut labels = std::collections::BTreeMap::new();
        labels.insert(String::from("owner.pikach.us"), String::from(name));
        want_md.labels = Some(labels);

        want
    }
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

#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, PartialEq, Default)]
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

impl GratefulSetPoolSpec {
    pub fn without_replicas(&self) -> StatefulSetSpec {
        let mut x = self.statefulset_spec.clone();
        x.replicas = None;
        x
    }
}

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
pub struct GratefulSetPoolStatus {
    pub sts_status: StatefulSetStatus,
}

impl GratefulSetPoolStatus {
    // stabilized indicates whether the underlying statefulset is ready and up to date.
    fn stabilized(&self) -> bool {
        [
            self.sts_status.current_replicas,
            self.sts_status.ready_replicas,
            self.sts_status.updated_replicas,
        ]
        .iter()
        .all(|x| match *x {
            Some(x) => x == self.sts_status.replicas,
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
