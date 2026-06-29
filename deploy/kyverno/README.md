# Kyverno Sidecar Injection

This bundle applies two admission policies to Pods created in namespaces labeled:

```yaml
s3-snapshot-proxy.inject: "enabled"
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
    s3-snapshot-proxy.inject: disabled
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
