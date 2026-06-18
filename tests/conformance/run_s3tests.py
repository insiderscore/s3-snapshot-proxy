#!/usr/bin/env python3
import json
import os
import shutil
import subprocess
import sys
import textwrap
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parents[1]
DEFAULT_ARTIFACT_DIR = HERE / "artifacts"


def truthy(value):
    return str(value).lower() not in {"0", "false", "no", "off"}


def read_json(path):
    return json.loads(path.read_text())


def load_allowlist(path):
    tests = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            tests.append(line)
    if not tests:
        raise RuntimeError(f"allowlist is empty: {path}")
    return tests


def generate_config(path):
    host = os.environ.get("CONFORMANCE_PROXY_HOST", "s3proxy")
    port = os.environ.get("CONFORMANCE_PROXY_PORT", "9000")
    access_key = os.environ.get("CONFORMANCE_MAIN_ACCESS_KEY", "origin-access")
    secret_key = os.environ.get("CONFORMANCE_MAIN_SECRET_KEY", "origin-secret")

    content = f"""
    [DEFAULT]
    host = {host}
    port = {port}
    is_secure = False
    ssl_verify = False

    [fixtures]
    bucket prefix = s3snap-{{random}}-
    iam name prefix = s3snap-
    iam path prefix = /s3snap/

    [s3 main]
    display_name = s3-snapshot-proxy main
    user_id = s3snap-main
    email = s3snap-main@example.invalid
    api_name = default
    access_key = {access_key}
    secret_key = {secret_key}

    [s3 alt]
    display_name = s3-snapshot-proxy alt
    user_id = s3snap-alt
    email = s3snap-alt@example.invalid
    access_key = {access_key}
    secret_key = {secret_key}

    [s3 tenant]
    display_name = s3-snapshot-proxy tenant
    user_id = s3snap-tenant
    email = s3snap-tenant@example.invalid
    tenant = s3snap
    access_key = {access_key}
    secret_key = {secret_key}

    [iam]
    display_name = s3-snapshot-proxy iam
    user_id = s3snap-iam
    email = s3snap-iam@example.invalid
    access_key = {access_key}
    secret_key = {secret_key}

    [iam root]
    user_id = s3snap-iam-root
    email = s3snap-iam-root@example.invalid
    account_id = s3snap-root
    access_key = {access_key}
    secret_key = {secret_key}

    [iam alt root]
    user_id = s3snap-iam-alt-root
    email = s3snap-iam-alt-root@example.invalid
    account_id = s3snap-alt-root
    access_key = {access_key}
    secret_key = {secret_key}
    """
    path.write_text(textwrap.dedent(content).strip() + "\n")


def ensure_upstream(root):
    if (root / "s3tests" / "functional" / "test_s3.py").exists():
        return
    subprocess.run(
        [sys.executable, str(HERE / "fetch_s3tests.py"), str(root)],
        check=True,
    )


def inject_conftest(root):
    conftest = root / "s3tests" / "functional" / "conftest.py"
    marker = "# s3-snapshot-proxy conformance shim"
    content = f"""
    {marker}
    import importlib.util
    from pathlib import Path

    shim_path = Path({str(HERE / "s3tests_shim.py")!r})
    spec = importlib.util.spec_from_file_location("s3_snapshot_proxy_s3tests_shim", shim_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.install()

    def pytest_collection_modifyitems(session, config, items):
        module.patch_collected_tests()
    """
    existing = conftest.read_text() if conftest.exists() else ""
    if marker not in existing:
        conftest.write_text(existing + "\n" + textwrap.dedent(content).strip() + "\n")


def selector_from_case(case):
    classname = case.attrib.get("classname", "")
    name = case.attrib.get("name", "")
    prefix = "s3tests.functional."
    if classname.startswith(prefix):
        module = classname[len(prefix):].replace(".", "/")
        return f"s3tests/functional/{module}.py::{name}"
    if classname:
        return f"{classname}::{name}"
    return name


def parse_junit(path):
    if not path.exists():
        return {
            "tests": 0,
            "failures": 0,
            "errors": 0,
            "skipped": 0,
            "failure_clusters": [],
            "failed_tests": [],
        }

    tree = ET.parse(path)
    root = tree.getroot()
    suites = list(root.iter("testsuite")) if root.tag != "testsuite" else [root]

    totals = {"tests": 0, "failures": 0, "errors": 0, "skipped": 0}
    for suite in suites:
        for key in totals:
            totals[key] += int(suite.attrib.get(key, "0"))

    clusters = defaultdict(list)
    failed_tests = []
    for case in root.iter("testcase"):
        node = f"{case.attrib.get('classname', '')}.{case.attrib.get('name', '')}".strip(".")
        selector = selector_from_case(case)
        for tag in ("failure", "error"):
            child = case.find(tag)
            if child is None:
                continue
            message = child.attrib.get("message") or (child.text or "").strip()
            first_line = message.splitlines()[0] if message else tag
            clusters[first_line].append(node)
            failed_tests.append({
                "node": node,
                "selector": selector,
                "kind": tag,
                "message": first_line,
            })

    totals["failure_clusters"] = [
        {"message": message, "count": len(nodes), "tests": sorted(nodes)}
        for message, nodes in sorted(clusters.items(), key=lambda item: (-len(item[1]), item[0]))
    ]
    totals["failed_tests"] = failed_tests
    return totals


def write_markdown_summary(path, summary):
    lines = [
        "# S3 Conformance Summary",
        "",
        f"- Generated: {summary['generated_at']}",
        f"- Mode: {'advisory' if summary['advisory'] else 'gating'}",
        f"- Upstream: {summary['upstream']['repo']} @ {summary['upstream']['revision']}",
        f"- Pytest exit code: {summary['pytest_exit_code']}",
        f"- Runner exit code: {summary['runner_exit_code']}",
        f"- Selected tests: {summary['selected_count']}",
        f"- Tests: {summary['junit']['tests']}",
        f"- Failures: {summary['junit']['failures']}",
        f"- Errors: {summary['junit']['errors']}",
        f"- Skipped: {summary['junit']['skipped']}",
        "",
        "## Failure Clusters",
        "",
    ]

    clusters = summary["junit"]["failure_clusters"]
    if not clusters:
        lines.append("No failures recorded.")
    else:
        for cluster in clusters:
            lines.append(f"- {cluster['count']} test(s): {cluster['message']}")

    lines.extend([
        "",
        "## Failed Test Reproduction",
        "",
    ])
    if not summary["junit"]["failed_tests"]:
        lines.append("No failing tests recorded.")
    else:
        for test in summary["junit"]["failed_tests"]:
            selector = test["selector"]
            lines.extend([
                f"### `{selector}`",
                "",
                "```sh",
                f"docker compose -f docker-compose-test.yml --profile conformance run --rm conformance-runner {selector}",
                "```",
                "",
            ])

    lines.extend([
        "",
        "## Reproduction",
        "",
        "```sh",
        "docker compose -f docker-compose-test.yml --profile conformance run --rm conformance-runner",
        "```",
        "",
        "For a single upstream case inside the conformance image:",
        "",
        "```sh",
        "python -m pytest -q s3tests/functional/test_s3.py::test_object_write_read_update_read_delete",
        "```",
        "",
    ])
    path.write_text("\n".join(lines))


def main(argv=None):
    argv = list(argv if argv is not None else sys.argv[1:])
    upstream = read_json(HERE / "upstream.json")
    selected = argv or load_allowlist(HERE / "allowlist.txt")

    s3tests_root = Path(os.environ.get("S3TESTS_ROOT", HERE / ".cache" / "s3-tests")).resolve()
    artifact_dir = Path(os.environ.get("CONFORMANCE_ARTIFACT_DIR", DEFAULT_ARTIFACT_DIR)).resolve()
    advisory = truthy(os.environ.get("CONFORMANCE_ADVISORY", "true"))

    artifact_dir.mkdir(parents=True, exist_ok=True)
    ensure_upstream(s3tests_root)
    inject_conftest(s3tests_root)

    config_path = artifact_dir / "s3tests.conf"
    junit_path = artifact_dir / "junit.xml"
    log_path = artifact_dir / "pytest.log"
    selected_path = artifact_dir / "selected-tests.txt"
    unsupported_copy = artifact_dir / "unsupported.md"
    summary_json_path = artifact_dir / "summary.json"
    summary_md_path = artifact_dir / "summary.md"

    generate_config(config_path)
    selected_path.write_text("\n".join(selected) + "\n")
    shutil.copyfile(HERE / "unsupported.md", unsupported_copy)

    env = os.environ.copy()
    env["S3TEST_CONF"] = str(config_path)
    env["PYTHONPATH"] = os.pathsep.join(
        [str(REPO_ROOT), str(s3tests_root), env.get("PYTHONPATH", "")]
    )

    cmd = [
        sys.executable,
        "-m",
        "pytest",
        "-q",
        "--disable-warnings",
        "--junitxml",
        str(junit_path),
        *selected,
    ]

    completed = subprocess.run(
        cmd,
        cwd=s3tests_root,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    log_path.write_text(completed.stdout)
    print(completed.stdout)

    junit = parse_junit(junit_path)
    harness_failure = completed.returncode in {2, 3, 4, 5} or (
        completed.returncode != 0 and not junit_path.exists()
    )
    runner_exit_code = completed.returncode
    if completed.returncode == 1 and advisory and not harness_failure:
        runner_exit_code = 0

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "advisory": advisory,
        "upstream": upstream,
        "s3tests_root": str(s3tests_root),
        "selected_count": len(selected),
        "pytest_exit_code": completed.returncode,
        "runner_exit_code": runner_exit_code,
        "harness_failure": harness_failure,
        "artifacts": {
            "config": str(config_path),
            "junit": str(junit_path),
            "log": str(log_path),
            "selected_tests": str(selected_path),
            "unsupported": str(unsupported_copy),
        },
        "junit": junit,
        "command": cmd,
    }
    summary_json_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    write_markdown_summary(summary_md_path, summary)

    if completed.returncode == 1 and advisory and not harness_failure:
        print("advisory mode: pytest reported conformance failures; runner exiting 0")

    raise SystemExit(runner_exit_code)


if __name__ == "__main__":
    main()
