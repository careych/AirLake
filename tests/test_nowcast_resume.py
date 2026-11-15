"""Resume after the recorded smoke-check stop; mocked AWS only."""

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import nowcast_resume

from nowcast import deploy


def receipt(digest, ops=None, smoke=None):
    return {
        "plan_sha256": digest,
        "status": "RUNNING",
        "operations": [
            {"operation": o, "status": "RETURNED_SUCCESS"}
            for o in (ops or nowcast_resume.RECORDED_OPERATIONS)
        ],
        "smoke": [
            {"method": m, "path": p, "status": s}
            for m, p, s in (smoke or deploy.API_SMOKE)
        ],
        "first_refresh": {"status": "PUBLISHED"},
        "function_url": "https://abc.lambda-url.ap-southeast-2.on.aws/",
    }


class ResumeTests(unittest.TestCase):
    def test_original_defect_post_405_body_is_not_a_projection(self):
        with self.assertRaises(KeyError):
            deploy.check_projection({"error": "read_only"}, "p")

    def test_only_the_exact_recorded_stop_is_accepted(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp)
            (p / "receipt.json").write_text(json.dumps(receipt("D")))
            nowcast_resume.recorded_state(p, "D")
            for bad in (
                receipt("D", ops=nowcast_resume.RECORDED_OPERATIONS[:-1]),
                receipt("D", smoke=[*deploy.API_SMOKE[:-1], ("POST", "/v1/now", 200)]),
                {**receipt("D"), "status": "BACKEND_LIVE_SITE_RELEASE_NEXT"},
                receipt("OTHER"),
            ):
                (p / "receipt.json").write_text(json.dumps(bad))
                with self.assertRaises(ValueError):
                    nowcast_resume.recorded_state(p, "D")

    def test_alarms_once_then_site_gate_opens(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp)
            (p / "receipt.json").write_text(json.dumps(receipt("D")))
            plan = {
                "alarms": {
                    "a": {
                        "metric": "Errors",
                        "function": "f",
                        "period_s": 60,
                        "threshold": 1,
                    }
                }
            }
            c = {"cloudwatch": Mock()}
            self.assertEqual(
                nowcast_resume.alarms(c, p, plan, "D")["status"],
                "BACKEND_LIVE_SITE_RELEASE_NEXT",
            )
            self.assertFalse(
                c["cloudwatch"].put_metric_alarm.call_args.kwargs["ActionsEnabled"]
            )
            with self.assertRaises(ValueError):  # receipt exists: never twice
                nowcast_resume.alarms(c, p, plan, "D")


if __name__ == "__main__":
    unittest.main()
