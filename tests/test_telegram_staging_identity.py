"""Offline identity setup uses synthetic identities and blocks all sockets."""

import copy
import io
import json
import socket
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch

from scripts.verify_telegram_staging_identity import (
    IdentityError,
    load_identity,
    main,
    telegram,
    verify,
)

TOKEN = "900001:synthetic_test_token"
NONCE = "synthetic_consent_nonce"


def update(*, recipient=101, text="/start " + NONCE):
    return {
        "message": {
            "text": text,
            "chat": {"type": "private", "id": recipient},
            "from": {"id": recipient, "is_bot": False},
        }
    }


class IdentityTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(
            patch.object(socket, "socket", side_effect=AssertionError("Offline only"))
        )
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory())).resolve()
        self.directory = self.root / "identity"
        self.now = datetime.now(UTC)
        self.responses = {
            "getMe": {"is_bot": True, "id": 900001, "username": "tmkhuebot"},
            "getWebhookInfo": {"url": ""},
            "getUpdates": [update()],
        }
        self.calls = []
        self.messages = []

    def api(self, method, token, params):
        self.assertEqual(TOKEN, token)
        self.calls.append((method, params))
        return self.responses[method]

    def run_verify(self, **changes):
        args = {
            "ask_token": lambda _: TOKEN,
            "confirm": lambda _: "READY",
            "api": self.api,
            "emit": self.messages.append,
            "nonce_factory": lambda: NONCE,
            "now": lambda: self.now,
        }
        args.update(changes)
        verify(self.directory, **args)

    def test_success_stores_only_private_identity_and_never_token(self):
        self.run_verify()
        data = load_identity(self.directory / "identity.json", now=self.now)
        self.assertEqual("900001", data["bot_id"])
        self.assertEqual("101", data["chat_id"])
        self.assertEqual(0o700, self.directory.stat().st_mode & 0o777)
        self.assertEqual(
            0o600, (self.directory / "identity.json").stat().st_mode & 0o777
        )
        self.assertNotIn(TOKEN, (self.directory / "identity.json").read_text())
        self.assertNotIn(NONCE, (self.directory / "identity.json").read_text())
        self.assertEqual(
            [
                ("getMe", {}),
                ("getWebhookInfo", {}),
                ("getUpdates", {"limit": 100, "timeout": 0}),
            ],
            self.calls,
        )
        output = "\n".join(self.messages)
        for value in (TOKEN, "900001", "101"):
            self.assertNotIn(value, output)
        with self.assertRaises(IdentityError):
            self.run_verify()

    def test_bot_identity_mismatch_stops_before_webhook_or_updates(self):
        original = copy.deepcopy(self.responses["getMe"])
        for change in (
            {"is_bot": False},
            {"username": "anotherbot"},
            {"id": 900002},
            {"id": True},
        ):
            with self.subTest(change=change), self.assertRaises(IdentityError):
                self.responses["getMe"] = original | change
                self.calls.clear()
                self.run_verify()
            self.assertEqual(1, len(self.calls))
            self.assertFalse(self.directory.exists())

    def test_json_whitespace_newline_and_invalid_prefix_tokens_rejected(self):
        for token in (
            '{"token":"fixture"}',
            TOKEN + "\n",
            " " + TOKEN,
            "0900001:fixture",
            "0:fixture",
            "-1:fixture",
        ):
            with self.subTest(token=token), self.assertRaises(IdentityError):
                self.run_verify(ask_token=lambda _, value=token: value)
        self.assertEqual([], self.calls)

    def test_webhook_or_uncertain_webhook_stops_without_update_request(self):
        for webhook in ({"url": "https://example.invalid/webhook"}, {}, None):
            with self.subTest(webhook=webhook), self.assertRaises(IdentityError):
                self.responses["getWebhookInfo"] = webhook
                self.run_verify()
            self.assertNotIn("getUpdates", [m for m, _ in self.calls])

    def test_missing_wrong_nonce_duplicate_and_multiple_recipients_rejected(self):
        for updates in (
            [],
            [update(text="/start wrong")],
            [update(), update()],
            [update(), update(recipient=102)],
        ):
            with self.subTest(updates=updates), self.assertRaises(IdentityError):
                self.responses["getUpdates"] = updates
                self.run_verify()
            self.assertFalse(self.directory.exists())

    def test_group_bot_and_mismatched_sender_rejected(self):
        for field, value in (
            ("group", None),
            ("bot", None),
            ("sender", None),
            ("id", None),
        ):
            row = update()
            if field == "group":
                row["message"]["chat"].update(type="group", id=-101)
            elif field == "bot":
                row["message"]["from"]["is_bot"] = True
            elif field == "sender":
                row["message"]["from"]["id"] = 102
            else:
                row["message"]["chat"]["id"] = "101"
            with self.subTest(field=field), self.assertRaises(IdentityError):
                self.responses["getUpdates"] = [row]
                self.run_verify()

    def test_owner_confirmation_required_and_update_request_never_retries(self):
        with self.assertRaises(IdentityError):
            self.run_verify(confirm=lambda _: "")
        self.assertNotIn("getUpdates", [m for m, _ in self.calls])
        api = Mock(
            side_effect=[self.responses["getMe"], {"url": ""}, TimeoutError(TOKEN)]
        )
        with self.assertRaises(TimeoutError):
            self.run_verify(api=api)
        self.assertEqual(3, api.call_count)
        self.assertFalse(self.directory.exists())

    def test_private_directory_cannot_be_inside_git_or_symlink_existing_path(self):
        (self.root / ".git").mkdir()
        with self.assertRaises(IdentityError):
            self.run_verify()
        self.assertEqual([], self.calls)
        (self.root / ".git").rmdir()
        self.directory.symlink_to(self.root, target_is_directory=True)
        with self.assertRaises(IdentityError):
            self.run_verify()

    def test_identity_file_permissions_symlink_and_stale_evidence_rejected(self):
        self.run_verify()
        path = self.directory / "identity.json"
        path.chmod(0o644)
        with self.assertRaises(IdentityError):
            load_identity(path, now=self.now)
        path.chmod(0o600)
        for clock in (self.now + timedelta(days=2), self.now - timedelta(seconds=1)):
            with self.subTest(clock=clock), self.assertRaises(IdentityError):
                load_identity(path, now=clock)
        link = self.directory / "link.json"
        link.symlink_to(path)
        with self.assertRaises(OSError):
            load_identity(link, now=self.now)

    def test_identity_record_rejects_extra_fields_wrong_bot_or_nonprivate_recipient(
        self,
    ):
        self.run_verify()
        path = self.directory / "identity.json"
        original = json.loads(path.read_text())
        for change in (
            {"extra": "field"},
            {"bot_username": "anotherbot"},
            {"chat_id": "-101"},
            {"private_chat_verified": False},
            {"bot_id": "0"},
        ):
            path.write_text(json.dumps(original | change))
            with self.subTest(change=change), self.assertRaises(ValueError):
                load_identity(path, now=self.now)

    def test_cli_requires_terminal_and_sanitizes_all_failures(self):
        output = io.StringIO()
        with patch("sys.stdin.isatty", return_value=False), redirect_stdout(output):
            self.assertEqual(
                1, main(["--private-dir", str(self.directory), "--approve-private-dir"])
            )
        with (
            patch("sys.stdin.isatty", return_value=True),
            patch(
                "scripts.verify_telegram_staging_identity.verify",
                side_effect=RuntimeError(TOKEN + " 101"),
            ),
            redirect_stdout(output),
        ):
            self.assertEqual(
                1, main(["--private-dir", str(self.directory), "--approve-private-dir"])
            )
        for value in (TOKEN, "101"):
            self.assertNotIn(value, output.getvalue())
        self.assertNotIn("Traceback", output.getvalue())

    def test_transport_only_permits_identity_calls_and_sanitizes_cli_boundary(self):
        with self.assertRaises(IdentityError):
            telegram("sendMessage", TOKEN, {})
        response = Mock(status=200)
        response.read.return_value = b'{"ok":true,"result":{"is_bot":true}}'
        context = Mock()
        context.__enter__ = Mock(return_value=response)
        context.__exit__ = Mock(return_value=False)
        opener = Mock()
        opener.open.return_value = context
        with patch("urllib.request.build_opener", return_value=opener) as factory:
            self.assertEqual({"is_bot": True}, telegram("getMe", TOKEN, {}))
        self.assertEqual({}, factory.call_args.args[0].proxies)
        self.assertEqual(10, opener.open.call_args.kwargs["timeout"])


if __name__ == "__main__":
    unittest.main()
