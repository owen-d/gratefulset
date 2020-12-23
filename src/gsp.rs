use crate::errors::*;
use crate::manager::Data;
use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::apps::v1::{StatefulSetSpec, StatefulSetStatus};
use k8s_openapi::api::core::v1::ConfigMap;
use k8s_openapi::api::core::v1::ConfigMapVolumeSource;
use k8s_openapi::api::core::v1::Container;
use k8s_openapi::api::core::v1::PodSpec;
use k8s_openapi::api::core::v1::Volume;
use k8s_openapi::api::core::v1::VolumeMount;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use k8s_openapi::{Metadata, Resource};
use kube::api::Meta;
use kube::api::{ListParams, PatchParams};
use kube::Api;
use kube_derive::CustomResource;
use kube_runtime::controller::Context;
use kube_runtime::controller::ReconcilerAction;
use log::debug;
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::iter::FromIterator;
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

pub fn without_replicas(spec: &StatefulSetSpec) -> StatefulSetSpec {
    let mut x = spec.clone();
    x.replicas = None;
    x
}

impl GratefulSetPoolSpec {
    pub fn delta_replicas(&mut self, n: i32) {
        self.sts_spec.replicas = self.sts_spec.replicas.map(|x| max(0, x + n));
    }

    // Adds initcontainer + configmap references
    pub fn with_lock(&self) -> StatefulSetSpec {
        let mut x = self.sts_spec.clone();
        x.template.spec = x.template.spec.map(|p| {
            let mut inits = p.init_containers.unwrap_or_default();
            inits.push(Container {
                name: String::from("gsp-unlocker"),
                image: Some(String::from("alpine:3.7")),
                command: Some(vec![
                    String::from("/bin/sh"),
                ]),
                args: Some(vec![
                    String::from("-c"),
                    String::from(r#"echo ${HOSTNAME} | sed -r 's/.*-([0-9])+$/\1/' | xargs -n 1 -I {} -- [ -e {} ] && echo successfully acquired lock at $$(date -u) || (echo failure && exit 1)"#),
                ]),
                working_dir: Some(self.lock_dir()),
                volume_mounts: Some(vec![VolumeMount {
                    mount_path: self.lock_dir(),
                    name: self.lock_volume(),
                    ..Default::default()
                }]),
                ..Default::default()
            });

            let mut vols = p.volumes.unwrap_or_default();
            vols.push(Volume {
                config_map: Some(ConfigMapVolumeSource {
                    name: Some(self.lock_volume()),
                    ..Default::default()
                }),
                ..Default::default()
            });

            PodSpec {
            init_containers: Some(inits),
            volumes: Some(vols),
            ..p
        }});
        x
    }

    fn configmap_name(&self) -> String {
        format!("{}-lock", self.name)
    }

    fn lock_dir(&self) -> String {
        String::from("/locks")
    }

    fn lock_volume(&self) -> String {
        format!("{}-locks", "pikach.us")
    }

    // returns the desired configmap with locks for each desired replica.
    pub fn configmap(&self, ns: String) -> ConfigMap {
        let data = (0..self.sts_spec.replicas.unwrap_or(1))
            .into_iter()
            .map(|x| (x.to_string(), x.to_string()));

        ConfigMap {
            data: Some(BTreeMap::from_iter(data)),
            metadata: ObjectMeta {
                name: Some(self.configmap_name()),
                namespace: Some(ns),
                owner_references: Some(vec![OwnerReference {
                    kind: GratefulSetPool::KIND.to_string(),
                    ..Default::default()
                }]),
                labels: Some(BTreeMap::from_iter(
                    vec![(String::from("owner.pikach.us"), self.name.clone())].into_iter(),
                )),
                ..Default::default()
            },
            ..Default::default()
        }
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

    // TODO: replace with 404 matching when we know which error type this is.
    let found = sts.get(&Meta::name(&gs)).await?;

    // If the specs are equal, this is a noop.
    if gs.spec.with_lock() == found.spec.clone().unwrap_or_default() {
        return Ok(ReconcilerAction {
            requeue_after: None,
        });
    }

    // The spec has changed. Update it & start the underlying rollout (sans replicas).
    let desired_sans_replicas = without_replicas(&gs.spec.with_lock());
    if without_replicas(&found.spec.unwrap_or_default()) != desired_sans_replicas {
        let patch = serde_yaml::to_vec(&serde_json::json!({ "spec": desired_sans_replicas }))?;
        return sts
            .patch(
                &Meta::name(&gs),
                &PatchParams::apply("gratefulset-mgr"),
                patch,
            )
            .await
            .map_err(|e| Error::with_chain(e, "something went wrong"))
            .map(|_| ReconcilerAction {
                requeue_after: None,
            });
    }

    // From this point on, we know the desired spec (sans replicas) exists on the underlying sts.

    Ok(ReconcilerAction {
        // try again in 5min
        requeue_after: Some(Duration::from_secs(300)),
    })
}
