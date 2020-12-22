use crate::errors::*;
use kube::client::Client;
use kube_runtime::controller::{Context, ReconcilerAction};
use log::{warn};
use std::time::Duration;

// Context for our reconciler
#[derive(Clone)]
pub struct Data {
    /// kubernetes client
    pub client: Client,
}

pub fn error_policy(error: &Error, _ctx: Context<Data>) -> ReconcilerAction {
    warn!("reconcile failed: {}", error);
    ReconcilerAction {
        requeue_after: Some(Duration::from_secs(60)),
    }
}
