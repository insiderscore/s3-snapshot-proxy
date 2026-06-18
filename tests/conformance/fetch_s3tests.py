#!/usr/bin/env python3
import json
import shutil
import subprocess
import sys
from pathlib import Path


HERE = Path(__file__).resolve().parent
UPSTREAM = HERE / "upstream.json"


def run(cmd, cwd=None):
    subprocess.run(cmd, cwd=cwd, check=True)


def current_revision(path):
    if not (path / ".git").exists():
        return None
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=path,
            text=True,
        ).strip()
    except subprocess.CalledProcessError:
        return None


def main():
    if len(sys.argv) != 2:
        raise SystemExit("usage: fetch_s3tests.py TARGET_DIR")

    target = Path(sys.argv[1]).resolve()
    metadata = json.loads(UPSTREAM.read_text())
    repo = metadata["repo"]
    revision = metadata["revision"]

    if current_revision(target) == revision:
        print(f"s3-tests already present at {revision}")
        return

    if target.exists():
        shutil.rmtree(target)

    target.parent.mkdir(parents=True, exist_ok=True)
    run(["git", "clone", "--no-checkout", repo, str(target)])
    run(["git", "checkout", revision], cwd=target)
    print(f"checked out {repo} at {revision} into {target}")


if __name__ == "__main__":
    main()
