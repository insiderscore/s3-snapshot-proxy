import os
import time

import requests
import trino


TRINO_HOST = os.environ.get("TRINO_HOST", "trino-iceberg")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))
TRINO_USER = os.environ.get("TRINO_USER", "iceberg-test")


def wait_for_trino(timeout_seconds=180):
    deadline = time.monotonic() + timeout_seconds
    url = f"http://{TRINO_HOST}:{TRINO_PORT}/v1/info"
    last_error = None

    while time.monotonic() < deadline:
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                conn = connect()
                try:
                    cur = conn.cursor()
                    cur.execute("SELECT 1")
                    cur.fetchall()
                    return
                finally:
                    conn.close()
            last_error = RuntimeError(f"{url} returned {response.status_code}")
        except Exception as exc:
            last_error = exc
        time.sleep(2)

    raise TimeoutError(f"Timed out waiting for Trino at {url}: {last_error}")


def connect():
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        request_timeout=120,
    )


def query(sql, retries=0):
    last_error = None
    for attempt in range(retries + 1):
        conn = None
        try:
            conn = connect()
            cur = conn.cursor()
            cur.execute(sql)
            try:
                return cur.fetchall()
            except trino.exceptions.TrinoQueryError:
                raise
            except Exception:
                return []
        except Exception as exc:
            last_error = exc
            if attempt >= retries:
                raise
            time.sleep(min(2 + attempt, 8))
        finally:
            if conn is not None:
                conn.close()
    raise last_error


def scalar(sql, retries=0):
    rows = query(sql, retries=retries)
    if not rows:
        return None
    return rows[0][0]


def run_ignoring_missing(sql):
    try:
        query(sql)
    except Exception as exc:
        text = str(exc).lower()
        if "does not exist" not in text and "not found" not in text:
            raise
