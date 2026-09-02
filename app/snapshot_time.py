from datetime import datetime, timezone
from typing import Optional

import botocore.exceptions


SNAPSHOT_TIME_OBJECT_KEY = "__s3_snapshot_proxy__/snapshot-time"


class SnapshotTimeError(RuntimeError):
    pass


def parse_snapshot_time(value: str, source: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"Invalid {source}; expected an ISO-8601 timestamp") from exc

    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"Invalid {source}; timestamp must include a UTC offset")

    parsed = parsed.astimezone(timezone.utc)
    if parsed > datetime.now(timezone.utc):
        raise ValueError(f"Invalid {source}; snapshot time must not be in the future")
    return parsed


def s3_error_code(exc: botocore.exceptions.ClientError) -> str:
    return exc.response.get("Error", {}).get("Code", "")


def s3_error_status(exc: botocore.exceptions.ClientError) -> Optional[int]:
    return exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")


def snapshot_time_object_missing(exc: botocore.exceptions.ClientError) -> bool:
    return s3_error_code(exc) == "NoSuchKey"


def read_snapshot_time_object(s3_client, bucket: str) -> Optional[datetime]:
    try:
        response = s3_client.get_object(
            Bucket=bucket,
            Key=SNAPSHOT_TIME_OBJECT_KEY,
        )
    except botocore.exceptions.ClientError as exc:
        if snapshot_time_object_missing(exc):
            return None
        raise

    body = response["Body"]
    try:
        raw_value = body.read() if hasattr(body, "read") else body
    finally:
        if hasattr(body, "close"):
            body.close()

    try:
        value = raw_value.decode("utf-8") if isinstance(raw_value, bytes) else str(raw_value)
        return parse_snapshot_time(value, f"snapshot time object {SNAPSHOT_TIME_OBJECT_KEY}")
    except (UnicodeDecodeError, ValueError) as exc:
        raise SnapshotTimeError(
            f"Overlay snapshot time object {SNAPSHOT_TIME_OBJECT_KEY!r} is invalid: {exc}"
        ) from exc


def create_and_read_snapshot_time_object(
    s3_client,
    bucket: str,
    candidate: datetime,
) -> datetime:
    body = f"{candidate.isoformat()}\n".encode("utf-8")
    last_conflict = None

    for _attempt in range(4):
        try:
            s3_client.put_object(
                Bucket=bucket,
                Key=SNAPSHOT_TIME_OBJECT_KEY,
                Body=body,
                ContentType="text/plain; charset=utf-8",
                IfNoneMatch="*",
            )
        except botocore.exceptions.ClientError as exc:
            code = s3_error_code(exc)
            status = s3_error_status(exc)
            # S3 documents PutObject as retryable when a concurrent delete
            # causes a 409 during an If-None-Match conditional write.
            if status == 409 or code in {"409", "ConditionalRequestConflict"}:
                last_conflict = exc
                continue
            if status != 412 and code not in {"412", "PreconditionFailed"}:
                raise

        stored = read_snapshot_time_object(s3_client, bucket)
        if stored is not None:
            return stored

    raise SnapshotTimeError(
        "Unable to create and read back the overlay snapshot time object after concurrent updates"
    ) from last_conflict


def initialize_snapshot_time(
    s3_client,
    bucket: str,
    configured_time: Optional[datetime],
    require_existing: bool,
) -> datetime:
    stored_time = read_snapshot_time_object(s3_client, bucket)
    if stored_time is not None:
        if configured_time is not None and configured_time != stored_time:
            raise SnapshotTimeError(
                "Configured START_TIME does not match the snapshot time already stored "
                f"in s3://{bucket}/{SNAPSHOT_TIME_OBJECT_KEY}"
            )
        return stored_time

    if require_existing:
        raise SnapshotTimeError(
            "No snapshot time object exists at "
            f"s3://{bucket}/{SNAPSHOT_TIME_OBJECT_KEY}"
        )

    candidate = configured_time or datetime.now(timezone.utc)
    stored_time = create_and_read_snapshot_time_object(
        s3_client,
        bucket,
        candidate,
    )
    if configured_time is not None and configured_time != stored_time:
        raise SnapshotTimeError(
            "Configured START_TIME lost a concurrent snapshot-time creation race; "
            f"stored value is {stored_time.isoformat()}"
        )
    return stored_time
