"""Local draft workflow and loopback receiver. All inputs are synthetic fixtures."""

import argparse
import hashlib
import json
import os
import re
import sqlite3
import time
import urllib.error
import urllib.request
from contextlib import closing, contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def log(root, kind, **data):
    with (root / "events.jsonl").open("a", encoding="utf-8") as handle:
        handle.write(
            canonical({"time": time.time(), "pid": os.getpid(), "kind": kind, **data})
            + "\n"
        )
        handle.flush()


@contextmanager
def connect(path):
    with closing(sqlite3.connect(path, timeout=5)) as con:
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA journal_mode=WAL")
        with con:
            yield con


def validated(raw):
    if not isinstance(raw, dict) or set(raw) != {
        "event_id",
        "title",
        "severity",
        "body",
        "fixture",
    }:
        raise ValueError(
            "exact fields required: event_id, title, severity, body, fixture"
        )
    if not isinstance(raw["event_id"], str) or not re.fullmatch(
        r"[A-Za-z0-9_-]{1,50}", raw["event_id"]
    ):
        raise ValueError("invalid event_id")
    if raw["severity"] not in {"routine", "urgent"}:
        raise ValueError("invalid severity")
    if (
        not isinstance(raw["title"], str)
        or not raw["title"].strip()
        or len(raw["title"]) > 100
    ):
        raise ValueError("title must contain 1-100 characters")
    if (
        not isinstance(raw["body"], str)
        or not raw["body"].strip()
        or len(raw["body"]) > 2000
    ):
        raise ValueError("body must contain 1-2000 characters")
    if raw["fixture"] not in {"normal", "flaky", "slow", "crash", "permanent"}:
        raise ValueError("unknown controlled failure fixture")
    return raw


def draft(event):
    return {
        "event_id": event["event_id"],
        "fixture": event["fixture"],
        "markdown": f"# {event['title']}\n\nPriority: {event['severity']}\n\n{event['body']}\n\nStatus: draft for review\n",
    }


def receiver(root):
    root.mkdir(parents=True, exist_ok=True)
    with connect(root / "destination.sqlite") as con:
        con.executescript(
            "CREATE TABLE IF NOT EXISTS receipts (key TEXT PRIMARY KEY,event_id TEXT,hash TEXT,payload TEXT); CREATE TABLE IF NOT EXISTS attempts (key TEXT PRIMARY KEY,count INTEGER NOT NULL);"
        )

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            try:
                data = json.loads(
                    self.rfile.read(int(self.headers.get("Content-Length", "0")))
                )
                key = self.headers["Idempotency-Key"]
                payload_hash = digest(data)
                with connect(root / "destination.sqlite") as con:
                    con.execute("BEGIN IMMEDIATE")
                    con.execute(
                        "INSERT INTO attempts VALUES (?,1) ON CONFLICT(key) DO UPDATE SET count=count+1",
                        (key,),
                    )
                    count = con.execute(
                        "SELECT count FROM attempts WHERE key=?", (key,)
                    ).fetchone()[0]
                    previous = con.execute(
                        "SELECT hash FROM receipts WHERE key=?", (key,)
                    ).fetchone()
                    if previous and previous[0] != payload_hash:
                        status, result = 409, {"error": "key-content-conflict"}
                    elif data["fixture"] == "permanent":
                        status, result = 400, {"error": "fixture permanent rejection"}
                    elif data["fixture"] == "flaky" and count == 1:
                        status, result = 503, {"error": "fixture transient failure"}
                    else:
                        con.execute(
                            "INSERT OR IGNORE INTO receipts VALUES (?,?,?,?)",
                            (key, data["event_id"], payload_hash, canonical(data)),
                        )
                        status, result = (
                            200,
                            {"key": key, "cached": previous is not None},
                        )
                log(
                    root,
                    "destination_request",
                    key=key,
                    attempt=count,
                    status=status,
                    cached=previous is not None,
                )
                # The receipt has committed before this delay: timeout is ambiguous.
                if data["fixture"] == "slow" and count == 1:
                    time.sleep(0.3)
                encoded = canonical(result).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                try:
                    self.wfile.write(encoded)
                except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                    log(root, "response_lost", key=key)
            except Exception as exc:
                log(root, "receiver_error", error=str(exc))
                self.send_error(500)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    (root / "ready.json").write_text(
        canonical({"port": server.server_port, "pid": os.getpid()}), encoding="utf-8"
    )
    server.serve_forever()


def send(url, key, payload, timeout=0.08):
    request = urllib.request.Request(
        url,
        canonical(payload).encode(),
        {"Content-Type": "application/json", "Idempotency-Key": key},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read())


def worker(root, source, url, crash=False):
    root.mkdir(parents=True, exist_ok=True)
    reports = root / "reports"
    reports.mkdir(exist_ok=True)
    with connect(root / "state.sqlite") as con:
        con.executescript(
            "CREATE TABLE IF NOT EXISTS inbox (event_id TEXT PRIMARY KEY,hash TEXT); CREATE TABLE IF NOT EXISTS outbox (key TEXT PRIMARY KEY,event_id TEXT,payload TEXT,state TEXT,attempts INTEGER DEFAULT 0,error TEXT); CREATE TABLE IF NOT EXISTS rejections (raw_hash TEXT PRIMARY KEY,reason TEXT);"
        )
        for line in source.read_text(encoding="utf-8").splitlines():
            try:
                event = validated(json.loads(line))
                previous = con.execute(
                    "SELECT hash FROM inbox WHERE event_id=?", (event["event_id"],)
                ).fetchone()
                if previous and previous[0] != digest(event):
                    raise ValueError("event_id reused with changed content")
                if previous:
                    log(root, "input_replay", event_id=event["event_id"])
                    continue
                with con:
                    con.execute(
                        "INSERT INTO inbox VALUES (?,?)",
                        (event["event_id"], digest(event)),
                    )
                    con.execute(
                        "INSERT INTO outbox (key,event_id,payload,state) VALUES (?,?,?,'pending')",
                        (
                            "draft:v1:" + event["event_id"],
                            event["event_id"],
                            canonical(draft(event)),
                        ),
                    )
                log(root, "ingested", event_id=event["event_id"])
            except (ValueError, json.JSONDecodeError, TypeError) as exc:
                with con:
                    con.execute(
                        "INSERT OR IGNORE INTO rejections VALUES (?,?)",
                        (hashlib.sha256(line.encode()).hexdigest(), str(exc)),
                    )
                log(root, "input_rejected", reason=str(exc))
        pending = con.execute(
            "SELECT * FROM outbox WHERE state='pending' ORDER BY rowid"
        ).fetchall()
        for row in pending:
            payload = json.loads(row["payload"])
            for attempt in range(1, 4):
                with con:
                    con.execute(
                        "UPDATE outbox SET attempts=attempts+1 WHERE key=?",
                        (row["key"],),
                    )
                try:
                    receipt = send(url, row["key"], payload)
                    log(
                        root,
                        "destination_ack",
                        key=row["key"],
                        cached=receipt["cached"],
                    )
                    if (
                        crash
                        and payload["fixture"] == "crash"
                        and not (root / "crashed.once").exists()
                    ):
                        (root / "crashed.once").write_text(row["key"], encoding="utf-8")
                        log(root, "hard_exit_before_ack_commit", key=row["key"])
                        os._exit(77)
                    # Reports are derivable; atomic file replace permits repair after crash.
                    target = reports / (row["event_id"] + ".md")
                    temporary = target.with_suffix(".tmp")
                    temporary.write_text(payload["markdown"], encoding="utf-8")
                    temporary.replace(target)
                    with con:
                        con.execute(
                            "UPDATE outbox SET state='done',error=NULL WHERE key=?",
                            (row["key"],),
                        )
                    break
                except urllib.error.HTTPError as exc:
                    transient = exc.code in {408, 429} or exc.code >= 500
                    log(
                        root,
                        "delivery_failed",
                        key=row["key"],
                        status=exc.code,
                        retryable=transient,
                        attempt=attempt,
                    )
                    with con:
                        con.execute(
                            "UPDATE outbox SET error=? WHERE key=?",
                            (f"HTTP {exc.code}", row["key"]),
                        )
                        if not transient:
                            con.execute(
                                "UPDATE outbox SET state='rejected' WHERE key=?",
                                (row["key"],),
                            )
                    if not transient:
                        break
                except (TimeoutError, urllib.error.URLError, OSError) as exc:
                    log(
                        root,
                        "delivery_timeout_or_transport_error",
                        key=row["key"],
                        error=type(exc).__name__,
                        attempt=attempt,
                    )
                    with con:
                        con.execute(
                            "UPDATE outbox SET error=? WHERE key=?",
                            (type(exc).__name__, row["key"]),
                        )
                # Short controlled test backoff, not a production polling interval.
                if attempt < 3:
                    time.sleep(0.02 * attempt)
        summary = {
            "states": dict(
                con.execute(
                    "SELECT state,count(*) FROM outbox GROUP BY state"
                ).fetchall()
            ),
            "rejections": con.execute("SELECT count(*) FROM rejections").fetchone()[0],
            "pending": con.execute(
                "SELECT count(*) FROM outbox WHERE state='pending'"
            ).fetchone()[0],
        }
        (root / "summary.json").write_text(canonical(summary), encoding="utf-8")
        print(canonical(summary))


def baseline(root, source, url, execution):
    root.mkdir(parents=True, exist_ok=True)
    for index, line in enumerate(source.read_text(encoding="utf-8").splitlines()):
        payload = draft(validated(json.loads(line)))
        key = f"execution:{execution}:item:{index}"
        send(url, key, payload, timeout=1)
        (root / (key.replace(":", "_") + ".md")).write_text(
            payload["markdown"], encoding="utf-8"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["receiver", "worker", "baseline"])
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--input", type=Path)
    parser.add_argument("--url")
    parser.add_argument("--execution", default="1")
    parser.add_argument("--crash", action="store_true")
    args = parser.parse_args()
    if args.mode == "receiver":
        receiver(args.root)
    elif args.mode == "worker":
        worker(args.root, args.input, args.url, args.crash)
    else:
        baseline(args.root, args.input, args.url, args.execution)
