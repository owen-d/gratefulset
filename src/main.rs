use gratefulset::{errors::*, gs::*};
use kube::api::{Api, ListParams, Meta, PostParams, WatchEvent};
use kube::Client;
// `block_on` blocks the current thread until the provided future has run to
// completion. Other executors provide more complex behavior, like scheduling
// multiple futures onto the same thread.
use futures::executor::block_on;
use futures::{StreamExt, TryStreamExt};

fn main() {
    let x = block_on(libmain());
    println!("got {:#?}", x)
}

async fn libmain() -> Result<()> {
    // Read the environment to find config for kube client.
    // Note that this tries an in-cluster configuration first,
    // then falls back on a kubeconfig file.
    let client = Client::try_default().await?;

    // Get a strongly typed handle to the Kubernetes API for interacting
    // with gratefulsets in the "default" namespace.
    let gs: Api<GratefulSet> = Api::namespaced(client, "default");

    // Start a watch call for gratefulsets matching our name
    let lp = ListParams::default()
        .fields(&format!("metadata.name={}", "my-gratefulset"))
        .timeout(10);
    let mut stream = gs.watch(&lp, "0").await?.boxed();

    // Observe the gratefulsets phase for 10 seconds
    while let Some(status) = stream.try_next().await? {
        match status {
            WatchEvent::Added(o) => println!("Added {}", Meta::name(&o)),
            WatchEvent::Modified(o) => {
                let s = o.status.as_ref().expect("status exists on gratefulset");
                println!("Modified: {}: {:#?}", Meta::name(&o), s);
            }
            WatchEvent::Deleted(o) => println!("Deleted {}", Meta::name(&o)),
            WatchEvent::Error(e) => println!("Error {}", e),
            _ => {}
        }
    }

    Ok(())
}
