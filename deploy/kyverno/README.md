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

Minimum namespace-local config:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: s3-snapshot-proxy-config
  namespace: apps
data:
  IMAGE: 675770171148.dkr.ecr.us-east-1.amazonaws.com/s3-snapshot-proxy-test:sidecar-smoke
  AWS_REGION: us-east-1
  ORIGIN_S3_URL: https://s3.us-east-1.amazonaws.com
  OVERLAY_S3_URL: https://s3.us-east-1.amazonaws.com
  OVERLAY_BUCKET: staging-iscore-s3-snapshot-proxy-smoke
```

`MAX_CONTROL_BODY_BYTES` is optional.

## Notes

The policies intentionally mutate Pods on admission rather than higher-level
controllers. GitOps manifests remain readable, while created Pods are forced
through the local S3 endpoint in opted-in namespaces.

The sidecar must be first because init containers are ordered. The environment
policy patches pre-existing init containers with JSON Patch by `elementIndex`;
do not use strategic merge for that list, since it can reorder init containers
and defeat the startup guarantee.

The namespace label is the coarse opt-in. The Pod annotation is the escape
hatch for jobs or debugging Pods that need direct S3 behavior.
