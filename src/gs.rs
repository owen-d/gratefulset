use crate::manager::{error_policy, Data};
use crate::{errors::*, gsp::*};
use futures::{future::BoxFuture, FutureExt, StreamExt};
use k8s_openapi::api::apps::v1::StatefulSetSpec;
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1beta1::CustomResourceDefinition;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
use k8s_openapi::{Metadata, Resource};
use kube::api::DeleteParams;
use kube::api::ListParams;
use kube::api::Meta;
use kube::api::PatchParams;
use kube::Api;
use kube::Client;
use kube_derive::CustomResource;
use kube_runtime::controller::Context;
use kube_runtime::controller::Controller;
use kube_runtime::controller::ReconcilerAction;
use log::debug;
use log::info;
use serde::{Deserialize, Serialize};
use std::time::Duration;

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
    pub sts_spec: StatefulSetSpec,
}

impl GratefulSetSpec {
    pub fn pool(&self) -> GratefulSetPool {
        let name = format!(
            "{}-{:x}",
            self.name,
            ImmutableSts(&self.sts_spec).checksum()
        );
        let mut want = GratefulSetPool::new(
            &name,
            GratefulSetPoolSpec {
                sts_spec: self.sts_spec.clone(),
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

async fn reconcile(gs: GratefulSet, ctx: Context<Data>) -> Result<ReconcilerAction> {
    let client = ctx.get_ref().client.clone();
    let name = Meta::name(&gs);
    let ns = Meta::namespace(&gs).expect("gs is namespaced");
    debug!("Reconcile Foo {}: {:?}", name, gs);

    let pools: Api<GratefulSetPool> = Api::namespaced(client.clone(), &ns);
    let lp = ListParams {
        label_selector: Some(format!("owner.pikach.us={}", name)),
        ..ListParams::default()
    };

    // Fetch all pools belonging to this GratefulSet and
    // separate into ([old_pool], desired_pool)
    let mut want = gs.spec.pool();
    // Default a potentially new pool to 0 replicas (scaling is handled independently).
    want.spec.sts_spec.replicas = Some(0);
    let desired_hash = ImmutableSts(&want.spec.sts_spec).checksum();
    // If the desired pool does not exist, we'll want to create it starting at 0 replicas.

    let (old_pools, cur_pool): (Vec<GratefulSetPool>, GratefulSetPool) =
        pools.list(&lp).await?.into_iter().fold(
            (vec![], want.clone()),
            move |(mut old_pools, mut cur_pool), p| {
                let hash = ImmutableSts(&p.spec.sts_spec).checksum();
                if desired_hash != hash {
                    old_pools.push(p);
                } else {
                    cur_pool = p;
                }
                (old_pools, cur_pool)
            },
        );

    // If only the desired pool exists & it has the correct config & replicas,
    // ensure any old pools are deleted then bail.
    if cur_pool.spec.sts_spec == gs.spec.sts_spec {
        for p in old_pools {
            pools
                .delete(&Meta::name(&p), &DeleteParams::default())
                .await?;
        }
        return Ok(ReconcilerAction {
            requeue_after: None,
        });
    }

    // If the desired pool exists but has a different spec (sans replicas), update it and return early. We'll need to wait for the underlying sts to roll to the new spec before continuing.
    if cur_pool.spec.sts_spec.replicas > Some(0)
        && want.spec.without_replicas() == cur_pool.spec.without_replicas()
    {
        // Don't update the replicas; those will be scaled independently once thew new spec settles.
        let mut diff = cur_pool.clone();
        diff.spec = want.spec.clone();
        diff.spec.sts_spec.replicas = cur_pool.spec.sts_spec.replicas;

        let serialized = serde_json::to_string(&diff)?;
        let patch = serde_yaml::to_vec(&serialized)?;

        return pools
            .patch(
                &Meta::name(&cur_pool),
                &PatchParams::apply("gratefulset-mgr"),
                patch,
            )
            .await
            .map_err(|e| Error::with_chain(e, "something went wrong"))
            .map(|_| ReconcilerAction {
                requeue_after: None,
            });
    }

    // If we've gotten this far, we're assured that the current pool has the correct spec, but
    // there may be older pools still around and the current pool may not have the correct replica count.

    let total_ready = old_pools.iter().fold(
        cur_pool
            .clone()
            .status
            .and_then(|s| s.sts_status.ready_replicas)
            .unwrap_or(0),
        |total_ready, x| {
            total_ready
                + x.status
                    .as_ref()
                    .and_then(|s| s.sts_status.ready_replicas)
                    .unwrap_or(0)
        },
    );
    let total_desired = gs.spec.sts_spec.replicas.unwrap_or(1);

    // Now we have to handle a few cases for scaling:
    // Order of operations should be (ScaleDown -> ScaleUp)

    // If replicas across all pools >= desired replicas,
    // remove one from the most out of date pool (ScaleDown). This
    // mimics the statefulset rollout semantics where
    // one is removed before adding a new revision replica.
    if total_ready >= total_desired {
        // remove one from the oldest possible pool
        let delta_pool = old_pools
            .iter()
            .fold(None, |acc, x| {
                acc.or_else(|| {
                    let reps = x.spec.sts_spec.replicas.unwrap_or(1);
                    if reps > 0 {
                        let mut updated = x.clone();
                        updated.spec.sts_spec.replicas = Some(reps - 1);
                    }
                    return None;
                })
            })
            // default to using the most recent pool if the previous pools don't exist
            // or have replicas set to 0.
            .unwrap_or_else(|| {
                let mut x = cur_pool.clone();
                x.spec.delta_replicas(-1);
                x
            });

        let serialized = serde_json::to_string(&delta_pool)?;
        let patch = serde_yaml::to_vec(&serialized)?;
        pools
            .patch(
                &Meta::name(&delta_pool),
                &PatchParams::apply("gratefulset-mgr"),
                patch,
            )
            .await
            .map_err(|e| Error::with_chain(e, "something went wrong"))
            .map(|_| ReconcilerAction {
                requeue_after: None,
            })?;
    } else {
        // If replicas across all pools < desired replicas,
        // add one to desired pool (ScaleUp).

        let mut diff = cur_pool.clone();
        diff.spec.delta_replicas(1);
        let serialized = serde_json::to_string(&diff)?;
        let patch = serde_yaml::to_vec(&serialized)?;
        pools
            .patch(
                &Meta::name(&diff),
                &PatchParams::apply("gratefulset-mgr"),
                patch,
            )
            .await
            .map_err(|e| Error::with_chain(e, "something went wrong"))
            .map(|_| ReconcilerAction {
                requeue_after: None,
            })?;
    }

    Ok(ReconcilerAction {
        // try again in 5min
        requeue_after: Some(Duration::from_secs(300)),
    })
}

pub struct Manager {}

/// Example Manager that owns a Controller for Foo
impl Manager {
    /// Lifecycle initialization interface for app
    ///
    /// This returns a `Manager` that drives a `Controller` + a future to be awaited
    /// It is up to `main` to wait for the controller stream.
    pub async fn new(client: Client) -> (Self, BoxFuture<'static, ()>) {
        let context = Context::new(Data {
            client: client.clone(),
        });
        let crds: Api<CustomResourceDefinition> = Api::all(client.clone());
        crds.get("gratefulset.pikach.us")
            .await
            .expect("install gratefulset crd first");

        crds.get("gratefulsetpool.pikach.us")
            .await
            .expect("install gratefulsetpool crd first");

        let gs = Api::<GratefulSet>::all(client.clone());
        let pools = Api::<GratefulSetPool>::all(client.clone());

        let drainer = Controller::new(gs, ListParams::default())
            .owns(pools, ListParams::default())
            .run(reconcile, error_policy, context)
            .for_each(|o| {
                info!("Reconciled {:?}", o);
                futures::future::ready(())
            })
            .boxed();
        // what we do with the controller stream from .run() ^^ does not matter
        // but we do need to consume it, hence general printing + return future

        (Self {}, drainer)
    }
}
