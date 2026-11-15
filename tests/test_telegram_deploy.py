"""Telegram release tooling; mocked AWS only."""

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from telegram_bot import deploy

TOKEN = "arn:aws:secretsmanager:ap-southeast-2:701364614078:secret:airlake/telegram-20261115/bot-token-AbC123"


class TelegramDeployTests(unittest.TestCase):
    def test_bundle_imports_with_stdlib_only_and_excludes_deploy(self):
        code = deploy.lambda_zip()
        self.assertEqual(code, deploy.lambda_zip())
        with tempfile.TemporaryDirectory() as tmp:
            z = Path(tmp) / "b.zip"
            z.write_bytes(code)
            out = subprocess.run(
                [
                    sys.executable,
                    "-I",
                    "-S",
                    "-c",
                    f"import sys; sys.path.insert(0, {str(z)!r}); import telegram_bot.app; print('ok')",
                ],
                capture_output=True,
                text=True,
                cwd=tmp,
                check=False,
            )
            self.assertEqual(out.stdout.strip(), "ok", out.stderr[-400:])
        names = deploy.zipfile.ZipFile(deploy.io.BytesIO(code)).namelist()
        self.assertIn("telegram_bot/hour_pattern.json", names)
        self.assertNotIn("telegram_bot/deploy.py", names)

    def test_policy_is_scoped(self):
        p = deploy.policies(
            TOKEN, "arn:aws:secretsmanager:ap-southeast-2:701364614078:secret:w-AbC123"
        )
        text = json.dumps(p)
        self.assertNotIn('"*"', text)
        self.assertIn("nowcast/v1/current.json", text)
        self.assertNotIn("historical-demo", text)

    def test_plan_freezes_and_detects_drift(self):
        with tempfile.TemporaryDirectory() as tmp:
            digest = deploy.prepare(Path(tmp) / "p", TOKEN)
            plan = deploy.load(Path(tmp) / "p", digest)
            self.assertEqual(plan["schedule"]["expression"], "cron(5/15 * * * ? *)")
            self.assertEqual(plan["queues"]["max_receive_count"], 3)
            self.assertEqual(plan["e2e"]["manual_evaluations_max"], 4)
            self.assertIn("not used", plan["sns"])
            with self.assertRaises(ValueError):
                deploy.load(Path(tmp) / "p", "0" * 64)
        with tempfile.TemporaryDirectory() as tmp, self.assertRaises(ValueError):
            deploy.prepare(
                Path(tmp) / "p", "arn:aws:secretsmanager:us-east-1:1:secret:x"
            )


if __name__ == "__main__":
    unittest.main()
