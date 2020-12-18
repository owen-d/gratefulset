## What

Aims to enable more declarative, easier stateful applications on top of k8s.

## How

- Exposes application-aware scaling strategies
- Allows changing fields that are immutable in sts definitions (pvcs, etc). This is handled by transitioning across multiple sts pools under the hood.
- Uses statefulsets under the hood and provides the same sts guarantees, notably:
  - Mutable sts changes (think replicas, pod spec, etc) are handled by the sts themselves.
  - Rotating a sts spec functions in the same fasion (removes one old replica before adding a new one)

## Expectations

- The managed application should handle regular sts changes without help. This means it could otherwise tolerate a change to the pod spec within a vanilla sts, for example.

## Pluggable application specific behavior

This library exposes pluggable traits, `ScaleUp` and `ScaleDown` which be implemented at an application level.

### ScaleDown

Under the hood, we add a `locks` configmap to each managed statefulset. This is paired with an init container that reads said configmap and fails unless it finds itself in the `locks` file.

A ScaleDown event from `n` -> `n-1` works as following:
1) Update the locks configmap in the target statefulset removing the lock entry for `n`.
2) Run scale down implementation via trait. This expects the container to exit. 
3) Once the sts settles at `n-1` ready replicas (the `nth` replica will no longer be able to start via the init container failing to acquire its lock), change the sts desired replicas to `n-1`.

#### Example: Loki

1) Lock `n` is removed from configmap
2) `HTTP` ScaleDown implementation hits the `/ingester/shutdown` endpoint which flushes all buffered data to storage then shuts down. Normally, the ingester would use it's write ahead log to recover this in memory data on the next startup, but in the event of a scale down, there won't be a next startup so we preemptively flush it to storage.
3) Pod tries to restart, but never becomes ready due to init container lock acquisition failure.
4) sts adjusts replicas to `n-1`, removing this pod.
5) Fin.

## Known issues

The rollout logic isn't always the most inefficient. For example, changing the pod spec _and_ decreasing the replicas at the same time (from `n` -> `n-m`) will first roll out `n` new pods with the updated spec, then scale down `m` of them.
