#[macro_use]
extern crate error_chain;

pub mod gs;
pub mod manager;
pub mod errors {
    // Create the Error, ErrorKind, ResultExt, and Result types
    error_chain! {
        foreign_links {
            Io(std::io::Error);
            HttpRequest(reqwest::Error);
            Kube(kube::Error);
        }
    }
}

/*
Need to navigate changes that aren't natively supported by statefulsets, including:

- Volume claim changes (access modes, size, storage class, etc)
  - Therefore should be hashed: pvc-ingester-abc123-{0,1,2,etc}

- custom ScaleUp/ScaleDown traits
  - Allow hitting shutdown endpoints, flushing WALs, etc.
  - Could use init containers to block startup unless a "lease" is held,
which helps avoid race conditions during deletions
    - If we don't want pod to have k8s API access, can seed the lease in a configmap that has all available leases.

- Going to need two new CRDs:
  - 1) gratefulsets (manages (2))
  - 2) gratefulsetpool (like replicasets, hashed by immutable fields)
    - immutable fields are: all_fields - [replicas, template, updatestrategy].

Examples:
  - 1) A scale down could look like:
    - 1) edit configmap to revoke highest numbered lease.
    - 2) ping shutdown endpoint.
    - 3) Wait for underlying statefulset to hit (x-1/x) ready pods. The pod will not restart b/c the initcontainer will fail it due to expired lease.
    - 4) Scale down underlying sts to x-1. Fin.

  - 2) Roll one pod spec to another (pvc change)
    - 1) scale one sts down and another up step-wise as per example (1).

  - 3) Roll one pod spec to another (no immutable changes)
    - 1) pass the change to underlying sts.
*/
