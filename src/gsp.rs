use crate::errors::*;
use crate::manager::Data;
use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::apps::v1::{StatefulSetSpec, StatefulSetStatus};
use kube::api::ListParams;
use kube::api::Meta;
use kube::Api;
use kube_derive::CustomResource;
use kube_runtime::controller::Context;
use kube_runtime::controller::ReconcilerAction;
use log::debug;
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::hash::{Hash, Hasher};
use std::time::Duration;

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
    pub sts_spec: StatefulSetSpec,
}

impl GratefulSetPoolSpec {
    pub fn without_replicas(&self) -> StatefulSetSpec {
        let mut x = self.sts_spec.clone();
        x.replicas = None;
        x
    }

    pub fn delta_replicas(&mut self, n: i32) {
        self.sts_spec.replicas = self.sts_spec.replicas.map(|x| max(0, x + n));
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

async fn reconcile(gs: GratefulSetPool, ctx: Context<Data>) -> Result<ReconcilerAction> {
    let client = ctx.get_ref().client.clone();
    let name = Meta::name(&gs);
    let ns = Meta::namespace(&gs).expect("gs is namespaced");
    debug!("Reconcile GratefulSetPool {}: {:?}", name, gs);

    let sts: Api<StatefulSet> = Api::namespaced(client.clone(), &ns);
    let lp = ListParams {
        label_selector: Some(format!("owner.pikach.us={}", name)),
        ..ListParams::default()
    };

    Ok(ReconcilerAction {
        // try again in 5min
        requeue_after: Some(Duration::from_secs(300)),
    })
}
