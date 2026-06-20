#!/usr/bin/env python

import argparse
import os
import time

from pyspark.sql import SparkSession


BUCKET = os.environ.get("ICEBERG_ORIGIN_BUCKET", "iceberg-origin")
SCHEMA = os.environ.get("ICEBERG_SCHEMA", "lake")
WAREHOUSE = os.environ.get("ICEBERG_WAREHOUSE", f"s3://{BUCKET}/warehouse")
TABLES = ["events_a", "events_b", "events_c", "events_d"]


def table_index(table):
    return TABLES.index(table) + 1


def spark(endpoint):
    builder = (
        SparkSession.builder.appName("s3-snapshot-proxy-iceberg")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lakecat", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakecat.type", "hadoop")
        .config("spark.sql.catalog.lakecat.warehouse", WAREHOUSE)
        .config("spark.sql.defaultCatalog", "lakecat")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID", "origin-access"))
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY", "origin-secret"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.shuffle.partitions", "4")
    )
    return builder.getOrCreate()


def table_name(table):
    return f"lakecat.{SCHEMA}.{table}"


def table_location(table):
    return f"{WAREHOUSE}/{SCHEMA}/{table}"


def create_namespace(spark_session):
    spark_session.sql(f"CREATE NAMESPACE IF NOT EXISTS lakecat.{SCHEMA}")


def create_table(spark_session, table):
    idx = table_index(table)
    spark_session.sql(f"DROP TABLE IF EXISTS {table_name(table)} PURGE")
    spark_session.sql(
        f"""
        CREATE TABLE {table_name(table)} (
            id BIGINT,
            table_id INT,
            batch INT,
            payload STRING,
            mutated_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (bucket(8, id))
        LOCATION '{table_location(table)}'
        TBLPROPERTIES (
            'format-version' = '2',
            'write.object-storage.enabled' = 'true',
            'write.parquet.compression-codec' = 'snappy'
        )
        """
    )
    spark_session.sql(
        f"""
        INSERT INTO {table_name(table)}
        SELECT
            CAST(id + {idx * 1000000} AS BIGINT),
            {idx},
            0,
            concat('seed-{table}-', CAST(id AS STRING)),
            current_timestamp()
        FROM range(1, 33) AS t(id)
        """
    )


def insert_batch(spark_session, table, batch, count, prefix):
    idx = table_index(table)
    spark_session.sql(
        f"""
        INSERT INTO {table_name(table)}
        SELECT
            CAST(id + {idx * 1000000} + {batch * 10000} AS BIGINT),
            {idx},
            {batch},
            concat('{prefix}-{table}-{batch}-', CAST(id AS STRING)),
            current_timestamp()
        FROM range(1, {count + 1}) AS t(id)
        """
    )


def delete_half_of_batch(spark_session, table, batch):
    spark_session.sql(
        f"""
        DELETE FROM {table_name(table)}
        WHERE batch = {batch}
        AND id % 2 = 0
        """
    )


def bootstrap(args):
    session = spark(args.endpoint)
    try:
        create_namespace(session)
        for table in TABLES:
            create_table(session, table)

        for batch in range(1, 4):
            for table in TABLES:
                insert_batch(session, table, batch, 8, "seed-mutation")
            time.sleep(1)

        for table in TABLES:
            delete_half_of_batch(session, table, 1)
            count = session.sql(f"SELECT count(*) FROM {table_name(table)}").collect()[0][0]
            print(f"{table_name(table)}: {count} rows")
    finally:
        session.stop()


def mutate_origin(args):
    session = spark(args.endpoint)
    deadline = time.monotonic() + args.duration_seconds
    batch = 100
    try:
        while time.monotonic() < deadline:
            for table in TABLES:
                insert_batch(session, table, batch, 4, "origin-live")
                if batch > 103:
                    delete_half_of_batch(session, table, batch - 3)
                batch += 1
                time.sleep(args.sleep_seconds)
    finally:
        session.stop()


def snapshot_workload(args):
    session = spark(args.endpoint)
    try:
        for table in TABLES:
            before = session.sql(f"SELECT count(*) FROM {table_name(table)}").collect()[0][0]
            files_before = session.sql(f"SELECT count(*) FROM {table_name(table)}.files").collect()[0][0]
            snapshots_before = session.sql(f"SELECT count(*) FROM {table_name(table)}.snapshots").collect()[0][0]

            insert_batch(session, table, args.batch, 6, args.catalog)
            inserted = session.sql(
                f"SELECT count(*) FROM {table_name(table)} WHERE batch = {args.batch}"
            ).collect()[0][0]
            assert inserted == 6, f"{args.catalog}.{table} expected 6 inserted rows, got {inserted}"

            delete_half_of_batch(session, table, args.batch)
            remaining = session.sql(
                f"SELECT count(*) FROM {table_name(table)} WHERE batch = {args.batch}"
            ).collect()[0][0]
            assert remaining == 3, f"{args.catalog}.{table} expected 3 remaining rows, got {remaining}"

            after = session.sql(f"SELECT count(*) FROM {table_name(table)}").collect()[0][0]
            files_after = session.sql(f"SELECT count(*) FROM {table_name(table)}.files").collect()[0][0]
            snapshots_after = session.sql(f"SELECT count(*) FROM {table_name(table)}.snapshots").collect()[0][0]

            assert before > 0
            assert after >= before
            assert files_before > 0
            assert files_after >= files_before
            assert snapshots_after > snapshots_before
            print(
                f"{args.catalog}.{table}: before={before} after={after} "
                f"files={files_before}->{files_after} snapshots={snapshots_before}->{snapshots_after}"
            )
    finally:
        session.stop()


def parse_args():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    bootstrap_parser = subparsers.add_parser("bootstrap")
    bootstrap_parser.add_argument("--endpoint", default="http://minio-origin:9000")

    mutate_parser = subparsers.add_parser("mutate-origin")
    mutate_parser.add_argument("--endpoint", default="http://minio-origin:9000")
    mutate_parser.add_argument("--duration-seconds", type=int, default=int(os.environ.get("ICEBERG_MUTATION_SECONDS", "75")))
    mutate_parser.add_argument("--sleep-seconds", type=float, default=float(os.environ.get("ICEBERG_MUTATION_SLEEP_SECONDS", "0.75")))

    snapshot_parser = subparsers.add_parser("snapshot-workload")
    snapshot_parser.add_argument("--endpoint", required=True)
    snapshot_parser.add_argument("--catalog", required=True)
    snapshot_parser.add_argument("--batch", type=int, required=True)

    return parser.parse_args()


def main():
    args = parse_args()
    if args.command == "bootstrap":
        bootstrap(args)
    elif args.command == "mutate-origin":
        mutate_origin(args)
    elif args.command == "snapshot-workload":
        snapshot_workload(args)
    else:
        raise ValueError(args.command)


if __name__ == "__main__":
    main()
