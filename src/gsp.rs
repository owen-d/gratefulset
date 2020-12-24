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
use std::cmp::min;
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
    pub scale_down_records: BTreeMap<i32, String>,
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

async fn reconcile(gsp: GratefulSetPool, ctx: Context<Data>) -> Result<ReconcilerAction> {
    let client = ctx.get_ref().client.clone();
    let name = Meta::name(&gsp);
    let ns = Meta::namespace(&gsp).expect("gs is namespaced");
    debug!("Reconcile GratefulSetPool {}: {:?}", name, gsp);

    let sts: Api<StatefulSet> = Api::namespaced(client.clone(), &ns);

    // TODO: replace with 404 matching when we know which error type this is.
    let found = sts.get(&Meta::name(&gsp)).await?;

    // If the specs are equal, this is a noop.
    if gsp.spec.with_lock() == found.spec.clone().unwrap_or_default() {
        return Ok(ReconcilerAction {
            requeue_after: None,
        });
    }

    // The spec has changed. Update it & start the underlying rollout (sans replicas).
    let desired_sans_replicas = without_replicas(&gsp.spec.with_lock());
    if without_replicas(&found.clone().spec.unwrap_or_default()) != desired_sans_replicas {
        let prev_replicas = found.clone().spec.clone().and_then(|x| x.replicas);
        let patch = serde_yaml::to_vec(&serde_json::json!({ "spec": StatefulSetSpec {
            replicas: prev_replicas,
            ..desired_sans_replicas.clone()
        } }))?;
        return sts
            .patch(
                &Meta::name(&gsp),
                &PatchParams::apply("gratefulsetpool-mgr"),
                patch,
            )
            .await
            .map_err(|e| Error::with_chain(e, "something went wrong"))
            .map(|_| ReconcilerAction {
                requeue_after: None,
            });
    }

    // From this point on, we know the desired spec (sans replicas) exists on the underlying sts
    // and therefore the only difference is the number of specified replicas.
    // Thus we have two options: scaling down and scaling up.
    // Scaling up is expected to be the simple path, but may change in the future. For now, we'll update the underlying configmap to include the lockfile for our new replica and increase the sts replicas to the desired number. It's foreseeable that in the future we may include custom scale-up strategies.
    // Scaling down is a different story. We do the following in order:
    // 1) Update underlying locks configmap to (cur-1) replicas
    // 2) Run ScaleDown implementation (http call, etc)
    // 3) Wait for new replica count to settle.

    let desired_replicas: i32 = gsp.spec.sts_spec.replicas.unwrap_or(1);
    let found_spec_replicas: i32 = found.spec.as_ref().and_then(|x| x.replicas).unwrap_or(1);
    let found_current_replicas: i32 = found
        .status
        .as_ref()
        .and_then(|x| x.current_replicas)
        .unwrap_or(0);
    let found_ready: i32 = min(
        found
            .status
            .as_ref()
            .and_then(|x| x.current_replicas)
            .unwrap_or(0),
        found
            .status
            .as_ref()
            .and_then(|x| x.ready_replicas)
            .unwrap_or(0),
    );

    // Bail early if we're still waiting for a spec difference to finish rolling out
    if found_spec_replicas != found_current_replicas {
        return Ok(ReconcilerAction {
            requeue_after: None,
        });
    }

    let configmaps: Api<ConfigMap> = Api::namespaced(client.clone(), &ns);

    // Scale up
    if desired_replicas > found_spec_replicas {
        // ensure locks configmap is updated
        let locks = gsp.spec.configmap(String::from(&ns));
        let should_update = configmaps
            .get(&Meta::name(&locks))
            .await
            .and_then(|x| x.data)
            .map(|x| x.get(desired_replicas.to_string()).is_some())
            .unwrap_or(false);

        let cm_patch = serde_yaml::to_vec(&serde_json::json!(locks))?;
        configmaps
            .patch(
                &Meta::name(&locks),
                &PatchParams::apply("gratefulsetpool-mgr"),
                cm_patch,
            )
            .await
            .map_err(|e| Error::with_chain(e, "something went wrong"))?;

        let patch = serde_yaml::to_vec(&serde_json::json!({ "spec": desired_sans_replicas }))?;
        return sts
            .patch(
                &Meta::name(&gsp),
                &PatchParams::apply("gratefulsetpool-mgr"),
                patch,
            )
            .await
            .map_err(|e| Error::with_chain(e, "something went wrong"))
            .map(|_| ReconcilerAction {
                requeue_after: None,
            });
    }

    // Scale down
    // TODO: scale down hooks

    // ensure lock is removed for the nth replica

    // if nth replica is no longer ready scale down sts
    // and remove any `>n` fields from the status scale down map.

    // check if status has [n -> hash(config)] in the status indicating the scaledown has been run.
    // If not, run scaledown and add this field.

    Ok(ReconcilerAction {
        // try again in 5min
        requeue_after: Some(Duration::from_secs(300)),
    })
}
