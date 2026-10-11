import json
import tempfile
import unittest
import urllib.error
from pathlib import Path

import demo
import pipeline


def event():
    return {
        "event_id": "fictional-note",
        "title": "Local note",
        "severity": "routine",
        "body": "An authored fixture.",
        "fixture": "normal",
    }


class PipelineTests(unittest.TestCase):
    def test_rejects_unsafe_report_identifiers(self):
        for identifier in ("../outside", "/absolute", "a/b", "a\\b", "", "."):
            with self.subTest(identifier=identifier):
                with self.assertRaises(ValueError):
                    pipeline.validated({**event(), "event_id": identifier})

    def test_destination_reuses_receipts_and_rejects_changed_payload(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "destination"
            with demo.destination(root) as url:
                payload = pipeline.draft(event())
                first = pipeline.send(
                    url, "operation:v1:fictional-note", payload, timeout=2
                )
                second = pipeline.send(
                    url, "operation:v1:fictional-note", payload, timeout=2
                )
                self.assertFalse(first["cached"])
                self.assertTrue(second["cached"])
                with self.assertRaises(urllib.error.HTTPError) as error:
                    pipeline.send(
                        url, first["key"], {**payload, "markdown": "changed"}, timeout=2
                    )
                self.assertEqual(error.exception.code, 409)
                self.assertEqual(demo.effects(root), 1)

    def test_failed_input_does_not_overwrite_reports(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "input.jsonl"
            source.write_text(
                json.dumps({**event(), "event_id": "../outside"}), encoding="utf-8"
            )
            pipeline.worker(root / "worker", source, "http://127.0.0.1:1/")
            self.assertFalse((root / "outside.md").exists())
            summary = json.loads(
                (root / "worker" / "summary.json").read_text(encoding="utf-8")
            )
            self.assertEqual(summary["rejections"], 1)

    def test_replay_and_real_process_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            result = demo.run(Path(directory) / "experiment")
            self.assertEqual(result["replay"]["baseline_effects"], 6)
            self.assertEqual(result["replay"]["durable_effects"], 2)
            self.assertEqual(result["recovery"]["crash_exit"], 77)
            self.assertEqual(result["recovery"]["pending"], 0)


if __name__ == "__main__":
    unittest.main()
