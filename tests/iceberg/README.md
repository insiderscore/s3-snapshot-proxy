# Iceberg/Trino Workload Harness

This directory contains a heavy end-to-end workload lane for checking whether
snapshot proxies are plausible for correctness-intensive Iceberg workloads.

The lane:

- creates a versioned `iceberg-origin` bucket and three versioned overlay
  buckets;
- seeds several Iceberg tables in the origin bucket using Spark with Iceberg's
  Hadoop catalog, so catalog state lives in object storage;
- continuously mutates the origin tables while three snapshot proxies start at
  staggered times;
- runs concurrent Spark read/write workloads through the snapshot proxy
  catalogs, again using the Hadoop catalog;
- starts Trino with one origin Iceberg catalog and three snapshot Iceberg
  catalogs for validation;
- registers Trino tables against the latest Iceberg metadata file visible
  through each endpoint; and
- verifies via Trino that overlay writes do not leak back to origin or across
  proxy overlays.

Current Trino releases do not expose Iceberg's Hadoop catalog as a connector
catalog type, so Spark drives the Hadoop-catalog writes. Trino remains in the
lane as the query engine and validates the resulting metadata, manifests, and
Parquet files by registering explicit metadata files into JDBC-backed Trino
catalogs.

Run with:

```sh
docker compose -f docker-compose-test.yml --profile iceberg run --rm iceberg-workload-runner
```

This lane is intentionally not advisory S3 conformance. It is a workload
compatibility test that exercises Iceberg Hadoop catalog metadata, Trino,
Parquet files, range reads, metadata tables, multipart writes, deletes, and
proxy overlay isolation.
