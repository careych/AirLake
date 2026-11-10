"""Owner-operated identity check. Never sends messages or accesses AWS secrets."""

import argparse
import getpass
import json
import os
import re
import secrets
import stat
import sys
import urllib.request
import warnings
from datetime import UTC, datetime, timedelta
from pathlib import Path

from alerts.delivery import chat_id


class IdentityError(ValueError):
    """Details stay private; the CLI reports only a sanitized failure."""


def private_parent(path):
    if not path.is_absolute():
        raise IdentityError("An absolute private path is required")
    parent = path.parent.resolve(strict=True)
    info = parent.stat()
    if info.st_uid != os.getuid() or info.st_mode & 0o022:
        raise IdentityError("Private parent must be owner-controlled")
    if any((p / ".git").exists() for p in (parent, *parent.parents)):
        raise IdentityError("Private files must be outside Git")
    return parent / path.name


def load_identity(path, *, now=None):
    """Read only owner-private identity evidence, valid for 24 hours."""
    path = private_parent(path)
    parent = path.parent.stat()
    if stat.S_IMODE(parent.st_mode) != 0o700:
        raise IdentityError("Identity directory must have mode 0700")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd) as source:
        info = os.fstat(source.fileno())
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_size > 4096
        ):
            raise IdentityError("Identity file must be owner-private")
        data = json.load(source)
    if set(data) != {
        "schema_version",
        "bot_id",
        "bot_username",
        "chat_id",
        "verified_at",
        "private_chat_verified",
    }:
        raise IdentityError("Unexpected identity evidence")
    if (
        type(data["schema_version"]) is not int
        or data["schema_version"] != 1
        or data["bot_username"] != "tmkhuebot"
        or data["private_chat_verified"] is not True
        or chat_id(data["bot_id"]).startswith("-")
        or chat_id(data["chat_id"]).startswith("-")
    ):
        raise IdentityError("Invalid staging identity")
    verified = datetime.fromisoformat(data["verified_at"])
    now = now or datetime.now(UTC)
    if verified.utcoffset() != timedelta(0) or not (
        timedelta(0) <= now - verified <= timedelta(hours=24)
    ):
        raise IdentityError("Identity evidence must be fresh UTC evidence")
    return data


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise IdentityError("Telegram redirect rejected")


def telegram(method, token, params):
    # No environment proxy, redirects, transport debug logging or automatic retries.
    if method not in {"getMe", "getWebhookInfo", "getUpdates"}:
        raise IdentityError("Only identity methods are allowed")
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    request = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/{method}",
        data=json.dumps(params).encode(),
        headers={"Content-Type": "application/json"},
    )
    with opener.open(request, timeout=10) as response:
        raw = response.read(1048577)
        if response.status != 200 or len(raw) > 1048576:
            raise IdentityError("Telegram response rejected")
        payload = json.loads(raw)
    if not isinstance(payload, dict) or payload.get("ok") is not True:
        raise IdentityError("Telegram response rejected")
    return payload["result"]


def verify(
    directory,
    *,
    ask_token=getpass.getpass,
    confirm=input,
    api=telegram,
    emit=print,
    nonce_factory=lambda: secrets.token_urlsafe(18),
    now=lambda: datetime.now(UTC),
):
    directory = private_parent(directory)
    if directory.exists() or directory.is_symlink():
        raise IdentityError("Refuse to overwrite private identity evidence")
    with warnings.catch_warnings():
        warnings.simplefilter("error", getpass.GetPassWarning)
        token = ask_token("NEXT: Enter the staging token using hidden input: ")
    if not isinstance(token, str) or not re.fullmatch(
        r"[1-9][0-9]{0,19}:[A-Za-z0-9_-]{1,180}", token
    ):
        raise IdentityError("Raw staging token required")
    me = api("getMe", token, {})
    if (
        me.get("is_bot") is not True
        or me.get("username") != "tmkhuebot"
        or type(me.get("id")) is not int
        or str(me["id"]) != token.partition(":")[0]
    ):
        raise IdentityError("Staging bot identity mismatch")
    bot_id = chat_id(str(me["id"]))
    webhook = api("getWebhookInfo", token, {})
    if not isinstance(webhook, dict) or webhook.get("url") != "":
        raise IdentityError("Existing or uncertain webhook; stop without changes")
    nonce = nonce_factory()
    if not re.fullmatch(r"[A-Za-z0-9_-]{16,64}", nonce):
        raise IdentityError("Invalid consent challenge")
    emit(
        "NEXT: Ask the ONE consenting recipient to send /start "
        + nonce
        + " in a private DM to @tmkhuebot. Do not share API responses or identifiers."
    )
    if confirm("NEXT: After that private message is sent, type READY: ") != "READY":
        raise IdentityError("Recipient consent was not confirmed")
    # One request, no offset/allowed_updates changes, no confirmation of updates.
    updates = api("getUpdates", token, {"limit": 100, "timeout": 0})
    if not isinstance(updates, list) or len(updates) > 100:
        raise IdentityError("Invalid or unbounded update response")
    matches = []
    for update in updates:
        message = update.get("message", {})
        if message.get("text") != "/start " + nonce:
            continue
        sender, chat = message.get("from", {}), message.get("chat", {})
        if (
            chat.get("type") != "private"
            or sender.get("is_bot") is not False
            or type(chat.get("id")) is not int
            or type(sender.get("id")) is not int
            or sender["id"] != chat["id"]
        ):
            raise IdentityError("Consent must come from a private human recipient")
        recipient = chat_id(str(chat["id"]))
        if recipient.startswith("-"):
            raise IdentityError("Private recipient required")
        matches.append(recipient)
    if len(matches) != 1:
        raise IdentityError("Exactly one matching consent message is required")
    data = {
        "schema_version": 1,
        "bot_id": bot_id,
        "bot_username": "tmkhuebot",
        "chat_id": matches[0],
        "verified_at": now().isoformat(),
        "private_chat_verified": True,
    }
    directory.mkdir(mode=0o700)
    fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fchmod(fd, 0o700)
        identity_fd = os.open(
            "identity.json",
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
            dir_fd=fd,
        )
        with os.fdopen(identity_fd, "w") as target:
            os.fchmod(target.fileno(), 0o600)
            json.dump(data, target)
            target.write("\n")
            target.flush()
            os.fsync(target.fileno())
        os.fsync(fd)
    finally:
        os.close(fd)
    emit("PASS: Private bot and recipient verification saved; no message was sent.")
    emit(
        "NEXT: Prepare private deployment files and request separate AWS write approval."
    )


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--private-dir", type=Path, required=True)
    parser.add_argument("--approve-private-dir", action="store_true", required=True)
    args = parser.parse_args(argv)
    try:
        if not sys.stdin.isatty():
            raise IdentityError("A trusted interactive Terminal is required")
        verify(args.private_dir)
    except (Exception, KeyboardInterrupt):  # noqa: BLE001 -- never expose token URLs/bodies/IDs
        print("FAIL: Identity verification incomplete; no deployment is authorized.")
        print(
            "NEXT: Inspect privately; do not share tokens, identifiers or API responses."
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
