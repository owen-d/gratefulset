use crate::{errors::*, gs::*};
use futures::{future::BoxFuture, FutureExt, StreamExt};
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1beta1::CustomResourceDefinition;
use kube::{
    api::{Api, ListParams, Meta, PatchParams},
    client::Client,
};
use kube_runtime::controller::{Context, Controller, ReconcilerAction};
use log::{debug, error, info, trace, warn};
use std::time::Duration;

// Context for our reconciler
#[derive(Clone)]
struct Data {
    /// kubernetes client
    client: Client,
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
    pools
        .list(&lp)
        .await?
        .iter()
        .filter_map(|p| p.status.as_ref())
        .for_each(|s| {
            let status: &GratefulSetPoolStatus = s;
        });

    // gs -> pool(immutable-config) -> sts-hash(immutable-config)
    // Order of operations should be (ScaleDown -> UpdateConfig -> ScaleUp)

    // If only the desired pool exists & it has the correct config & replicas, noop.

    // Ensure the desired pool exists with the correct config. If this results in a create (updated pvc definitions, etc), set the desired replicas to 0 and we'll take care of scaling one at a time.

    // Now we have to handle a few cases:
    // 1) Only the desired pool exists (change can be applied to underlying pool and managed there)
    // 2) There are older pool revisions around. First ensure the desired pool has the correct config applied (sans replicas). Once it settles (all relevant pods have been rolled within the sts), we can handle scaling as per usual.

    // If replicas across all pools < desired replicas,
    // add one to desired pool (ScaleUp).

    // If replicas across all pools >= desired replicas,
    // remove one from the most out of date pool (ScaleDown). This
    // mimics the statefulset rollout semantics where
    // one is removed before adding a new revision replica.

    // pod|replica change -> update pool definition
    // What if has an old pool lying around from previous definition?
    // Should take down old pool first
    // If not?
    // Update the new pool spec and let it handle rollout.

    Ok(ReconcilerAction {
        // try again in 5min
        requeue_after: Some(Duration::from_secs(300)),
    })
}

fn error_policy(error: &Error, _ctx: Context<Data>) -> ReconcilerAction {
    warn!("reconcile failed: {}", error);
    ReconcilerAction {
        requeue_after: Some(Duration::from_secs(60)),
    }
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
