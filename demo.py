"""Run isolated loopback replay, timeout, rejection and process-crash experiments."""

import argparse
import json
import sqlite3
import subprocess
import sys
import time
from contextlib import closing, contextmanager
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

BASE = Path(__file__).resolve().parent


def command(mode, root, *extra):
    return [
        sys.executable,
        str(BASE / "pipeline.py"),
        mode,
        "--root",
        str(root),
        *extra,
    ]


@contextmanager
def destination(root):
    root.mkdir(parents=True)
    with (root / "receiver.stderr.log").open("w", encoding="utf-8") as stderr:
        process = subprocess.Popen(
            command("receiver", root),
            stdout=subprocess.DEVNULL,
            stderr=stderr,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        try:
            deadline = time.monotonic() + 10
            while True:
                if process.poll() is not None:
                    raise RuntimeError("Loopback receiver exited before readiness")
                try:
                    ready = json.loads(
                        (root / "ready.json").read_text(encoding="utf-8")
                    )
                    break
                except (FileNotFoundError, json.JSONDecodeError):
                    if time.monotonic() >= deadline:
                        raise TimeoutError("Loopback receiver did not become ready")
                    time.sleep(0.05)
            yield f"http://127.0.0.1:{ready['port']}/"
        finally:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


def execute(mode, root, source, url, *, expected=0, extra=()):
    result = subprocess.run(
        command(mode, root, "--input", str(source), "--url", url, *extra),
        capture_output=True,
        check=False,
        text=True,
        timeout=20,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    if result.returncode != expected:
        raise RuntimeError(
            f"{mode} exited {result.returncode}; expected {expected}\n{result.stderr}"
        )
    return result.returncode


def effects(root):
    with closing(sqlite3.connect(root / "destination.sqlite")) as connection:
        return connection.execute("SELECT count(*) FROM receipts").fetchone()[0]


def run(root):
    """Create a fresh experiment directory and verify the stated fixture outcomes."""
    root.mkdir(parents=True, exist_ok=False)
    baseline_root = root / "baseline-destination"
    durable_root = root / "durable-destination"
    worker_root = root / "worker"
    replay = BASE / "fixtures" / "replay.jsonl"
    failures = BASE / "fixtures" / "failures.jsonl"
    with (
        destination(baseline_root) as baseline_url,
        destination(durable_root) as durable_url,
    ):
        for execution in range(3):
            execute(
                "baseline",
                root / "baseline",
                replay,
                baseline_url,
                extra=("--execution", str(execution)),
            )
            execute("worker", worker_root, replay, durable_url)
        replay_result = {
            "logical_operations": 2,
            "input_replays": 3,
            "baseline_effects": effects(baseline_root),
            "durable_effects": effects(durable_root),
        }
        assert replay_result["baseline_effects"] == 6, replay_result
        assert replay_result["durable_effects"] == 2, replay_result
        crash_exit = execute(
            "worker",
            worker_root,
            failures,
            durable_url,
            expected=77,
            extra=("--crash",),
        )
        execute("worker", worker_root, failures, durable_url)
        recovered = json.loads(
            (worker_root / "summary.json").read_text(encoding="utf-8")
        )
        recovery_result = {
            "crash_exit": crash_exit,
            "new_logical_operations": 3,
            "total_effects": effects(durable_root),
            **recovered,
        }
        assert recovered == {
            "states": {"done": 5, "rejected": 1},
            "rejections": 3,
            "pending": 0,
        }, recovered
        assert recovery_result["total_effects"] == 5, recovery_result
        execute("worker", worker_root, failures, durable_url)
        assert effects(durable_root) == 5, "Replaying recovered inputs added an effect"
    result = {"replay": replay_result, "recovery": recovery_result}
    (root / "result.json").write_text(
        json.dumps(result, indent=2) + "\n", encoding="utf-8"
    )
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    run_id = (
        datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S") + "-" + uuid4().hex[:8]
    )
    output = args.output or BASE / "runs" / run_id
    print(json.dumps(run(output), indent=2))
    print(f"Experiment artifacts: {output.resolve()}")
