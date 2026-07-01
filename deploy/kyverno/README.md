# Kyverno Sidecar Injection

This bundle applies two admission policies to Pods created in namespaces labeled:

```yaml
s3-snapshot-proxy.insiderscore.com/inject: "enabled"
```

Each opted-in namespace must also contain a `s3-snapshot-proxy-config`
ConfigMap. The sidecar policy reads `IMAGE` from that ConfigMap at admission
time and passes the remaining keys to the sidecar container.

The proxy is injected as a native Kubernetes sidecar init container with
`restartPolicy: Always`. The policy inserts it as the first init container, so
Kubernetes starts it before any pre-existing init container or regular app
container. The sidecar's startup probe must pass before Kubernetes advances to
the next init container.

The environment policy separately patches regular app containers and
pre-existing init containers with:

```yaml
AWS_ENDPOINT_URL_S3=http://127.0.0.1:9000
```

Pods can opt out with:

```yaml
metadata:
  annotations:
    s3-snapshot-proxy.insiderscore.com/inject: disabled
```

## Namespace Config

Minimum namespace-local config. The example uses shell-style placeholders;
render them with Flux post-build substitution, `envsubst`, Helm, Kustomize
replacements, or another deployment-time templating mechanism.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: s3-snapshot-proxy-config
  namespace: ${S3_SNAPSHOT_PROXY_NAMESPACE}
data:
  IMAGE: ${S3_SNAPSHOT_PROXY_IMAGE}
  AWS_REGION: ${S3_SNAPSHOT_PROXY_AWS_REGION}
  ORIGIN_S3_URL: ${S3_SNAPSHOT_PROXY_ORIGIN_S3_URL}
  OVERLAY_S3_URL: ${S3_SNAPSHOT_PROXY_OVERLAY_S3_URL}
  OVERLAY_BUCKET: ${S3_SNAPSHOT_PROXY_OVERLAY_BUCKET}
```

`MAX_CONTROL_BODY_BYTES` is optional.

## Notes

The policies intentionally mutate Pods on admission rather than higher-level
controllers. Source manifests remain readable, while created Pods are forced
through the local S3 endpoint in opted-in namespaces.

The sidecar must be first because init containers are ordered. The environment
policy patches pre-existing init containers with JSON Patch by `elementIndex`;
do not use strategic merge for that list, since it can reorder init containers
and defeat the startup guarantee.

The namespace label is the coarse opt-in. The Pod annotation is the escape
hatch for jobs or debugging Pods that need direct S3 behavior.

## Mountpoint For S3

Mountpoint CSI Pods are created by the CSI controller in the Mountpoint
namespace, not in the workload namespace. The Mountpoint policy injects the
proxy sidecar into selected Mountpoint Pods and leaves ordinary Mountpoint Pods
alone.

To opt in, label the Mountpoint namespace:

```yaml
s3-snapshot-proxy.insiderscore.com/mountpoint-inject: "enabled"
```

The policy follows the Mountpoint Pod annotation
`s3.csi.aws.com/volume-name` to the PersistentVolume, then follows the PV's
`claimRef` to the bound PVC. That PVC controls whether the Mountpoint Pod is
eligible for injection:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  annotations:
    s3-snapshot-proxy.insiderscore.com/mountpoint-inject: "enabled"
```

The workload namespace containing that PVC must contain
`s3-snapshot-proxy-config`. The policy reads this ConfigMap at admission time
and injects literal environment values into the Mountpoint Pod, even though the
Mountpoint Pod itself runs in the Mountpoint namespace.

Use a runtime proxy image, not a conformance/test target image. On mixed-node
clusters, publish it as a multi-platform image covering every node architecture
that may run Mountpoint Pods.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: s3-snapshot-proxy-config
  namespace: workload-namespace
data:
  IMAGE: example.invalid/s3-snapshot-proxy:runtime
  AWS_REGION: us-east-1
  ORIGIN_S3_URL: https://s3.us-east-1.amazonaws.com
  OVERLAY_S3_URL: https://s3.us-east-1.amazonaws.com
  OVERLAY_BUCKET: example-overlay-bucket
  MOUNTPOINT_CREDENTIAL_DISCOVERY_TIMEOUT_SECONDS: "30"
```

The selected PV must use pod-level credentials and point Mountpoint at the
local sidecar endpoint, for example:

```yaml
mountOptions:
  - region us-east-1
  - endpoint-url http://127.0.0.1:9000
  - force-path-style
csi:
  driver: s3.csi.aws.com
  volumeAttributes:
    authenticationSource: pod
    bucketName: example-origin-bucket
```

The sidecar derives the workload web identity token path from the Mountpoint
Pod UID and volume id under `/comm/credentials`, then reads
`MountpointS3PodAttachment` to discover the workload role ARN. The included
RBAC grants read access to that cluster-scoped CR for the default Mountpoint
service account in `mount-s3`.

Kyverno also needs admission-time read access to PersistentVolumes,
PersistentVolumeClaims, and ConfigMaps so it can follow the Mountpoint Pod to
the workload PVC and workload namespace config. The included lookup RBAC binds
those reads to the default `kyverno/kyverno-admission-controller` service
account used by the Kyverno Helm chart.

The proxy startup and readiness probes intentionally hit `/health`, not
`/readyz`, because `/comm/credentials` is populated during mount setup after the
Mountpoint Pod has started.

Mountpoint Pods are node-affined to the workload node. Keep sidecar resource
requests conservative; avoid adding a CPU request unless the target nodes have
enough allocatable slack for every selected Mountpoint mount.
