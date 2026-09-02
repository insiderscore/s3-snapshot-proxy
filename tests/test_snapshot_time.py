import io
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import botocore.exceptions
import pytest

os.environ.setdefault("START_TIME", "2024-01-10T00:00:00Z")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "origin-access")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "origin-secret")
os.environ.setdefault("OVERLAY_AWS_ACCESS_KEY_ID", "overlay-access")
os.environ.setdefault("OVERLAY_AWS_SECRET_ACCESS_KEY", "overlay-secret")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import main, snapshot_time


SNAPSHOT_A = datetime(2024, 1, 10, tzinfo=timezone.utc)
SNAPSHOT_B = datetime(2024, 1, 11, tzinfo=timezone.utc)


def client_error(code, status):
    return botocore.exceptions.ClientError(
        {
            "Error": {"Code": code, "Message": code},
            "ResponseMetadata": {"HTTPStatusCode": status},
        },
        "GetObject",
    )


class FakeSnapshotClient:
    def __init__(self, stored=None, put_errors=None, winner=None):
        self.stored = stored
        self.put_errors = list(put_errors or [])
        self.winner = winner
        self.put_calls = []

    def get_object(self, **kwargs):
        if self.stored is None:
            raise client_error("NoSuchKey", 404)
        return {"Body": io.BytesIO(f"{self.stored.isoformat()}\n".encode())}

    def put_object(self, **kwargs):
        self.put_calls.append(kwargs)
        if self.put_errors:
            error = self.put_errors.pop(0)
            if error.response["ResponseMetadata"]["HTTPStatusCode"] == 412:
                self.stored = self.winner
            raise error
        self.stored = self.winner or datetime.fromisoformat(
            kwargs["Body"].decode().strip()
        )


def test_reuses_existing_snapshot_time_without_writing():
    client = FakeSnapshotClient(stored=SNAPSHOT_A)

    result = snapshot_time.initialize_snapshot_time(client, "overlay", None, False)

    assert result == SNAPSHOT_A
    assert client.put_calls == []


def test_creates_snapshot_time_with_conditional_put_and_reads_it_back():
    client = FakeSnapshotClient()

    result = snapshot_time.initialize_snapshot_time(
        client,
        "overlay",
        SNAPSHOT_A,
        False,
    )

    assert result == SNAPSHOT_A
    assert client.put_calls[0]["IfNoneMatch"] == "*"
    assert client.put_calls[0]["Key"] == snapshot_time.SNAPSHOT_TIME_OBJECT_KEY


def test_concurrent_creator_wins_and_its_value_is_used():
    client = FakeSnapshotClient(
        put_errors=[client_error("PreconditionFailed", 412)],
        winner=SNAPSHOT_B,
    )

    result = snapshot_time.initialize_snapshot_time(client, "overlay", None, False)

    assert result == SNAPSHOT_B


def test_retries_conditional_put_after_conflict():
    client = FakeSnapshotClient(
        put_errors=[client_error("ConditionalRequestConflict", 409)],
    )

    result = snapshot_time.initialize_snapshot_time(
        client,
        "overlay",
        SNAPSHOT_A,
        False,
    )

    assert result == SNAPSHOT_A
    assert len(client.put_calls) == 2


def test_require_existing_snapshot_time_does_not_create_one():
    client = FakeSnapshotClient()

    with pytest.raises(
        snapshot_time.SnapshotTimeError,
        match="No snapshot time object exists",
    ):
        snapshot_time.initialize_snapshot_time(client, "overlay", None, True)

    assert client.put_calls == []


@pytest.mark.parametrize("require_existing", [False, True])
def test_missing_overlay_bucket_is_not_treated_as_a_missing_object(require_existing):
    class MissingBucketClient(FakeSnapshotClient):
        def get_object(self, **kwargs):
            raise client_error("NoSuchBucket", 404)

    client = MissingBucketClient()

    with pytest.raises(botocore.exceptions.ClientError) as raised:
        snapshot_time.initialize_snapshot_time(
            client,
            "missing-overlay",
            None,
            require_existing,
        )

    assert raised.value.response["Error"]["Code"] == "NoSuchBucket"
    assert client.put_calls == []


def test_configured_time_must_match_existing_snapshot():
    client = FakeSnapshotClient(stored=SNAPSHOT_B)

    with pytest.raises(snapshot_time.SnapshotTimeError, match="does not match"):
        snapshot_time.initialize_snapshot_time(
            client,
            "overlay",
            SNAPSHOT_A,
            False,
        )


def test_command_line_option_requires_existing_snapshot_time():
    args = main.build_argument_parser().parse_args(
        ["--require-existing-snapshot-time"]
    )

    assert args.require_existing_snapshot_time is True
