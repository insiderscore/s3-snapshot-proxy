# S3 Conformance Harness

This directory carries an advisory S3 compatibility lane for writable clone buckets. The current harness uses a pinned checkout of `ceph/s3-tests` from `upstream.json` instead of a git submodule. That keeps normal checkouts small, avoids submodule sync friction in CI, and makes upstream bumps a metadata change plus a build rerun. A submodule is still reasonable later if the suite becomes a permanent gating dependency or CI must run without network access.

## Shape

- `upstream.json` pins the upstream repository and revision.
- `allowlist.txt` selects object, conditional, ListObjects, ListObjectsV2, multi-object delete, versioned-object listing, basic multipart upload, explicit SSE-S3 upload, and overlay object tagging cases.
- `unsupported.md` is the explicit ledger for APIs outside the current support claim.
- `run_s3tests.py` generates `s3tests.conf`, injects the fixture shim, runs pytest, and emits JUnit, JSON, and Markdown artifacts.
- `s3tests_shim.py` patches only the fixture boundary: buckets are provisioned in origin with versioning enabled, object operations still target the proxy, and cleanup removes pending multipart uploads plus origin versions and overlay keys under `<bucket>/`.

The compose `conformance-runner` profile reuses the existing MinIO origin, MinIO overlay, and proxy services. The proxy already waits for `overlay-bucket-init`, so the overlay bucket exists before the proxy starts.

## Running

```sh
docker compose -f docker-compose-test.yml --profile conformance build conformance-runner
docker compose -f docker-compose-test.yml --profile conformance run --rm conformance-runner
```

Artifacts are written under `tests/conformance/artifacts/`:

- `s3tests.conf`
- `junit.xml`
- `pytest.log`
- `selected-tests.txt`
- `summary.json`
- `summary.md`
- `unsupported.md`

The default lane is advisory: harness/setup failures fail the runner, but selected object-API failures are reported with exit 0. To promote a green allowlist to gating:

```sh
CONFORMANCE_ADVISORY=false docker compose -f docker-compose-test.yml --profile conformance run --rm conformance-runner
```

To reproduce one upstream case:

```sh
docker compose -f docker-compose-test.yml --profile conformance run --rm conformance-runner s3tests/functional/test_s3.py::test_object_write_read_update_read_delete
```

## Remediation Loop

1. A high-capability supervisor reviews `summary.json` and `summary.md`, clusters failures, and assigns each cluster as product bug, adapter bug, unsupported API, or flaky/environmental.
2. Each worker takes one cluster with a single-test compose command and a narrow file scope.
3. Use lower-cost workers for isolated product or adapter fixes, then have the verifier rerun the affected conformance IDs plus the current regression suite.
4. Changes to `unsupported.md` need an explicit reason. Do not hide failures by silently moving tests out of the allowlist.
