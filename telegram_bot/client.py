"""Minimal Telegram Bot API client: bounded requests, sanitized errors."""

import json
import urllib.request
from urllib.error import HTTPError


class Retryable(Exception):
    """Transient: 429, 5xx or network. Never contains the token URL."""


class Rejected(Exception):
    """Permanent for this recipient, e.g. 403 when the user blocked the bot."""

    def __init__(self, code):
        super().__init__(f"Telegram rejected the request ({code})")
        self.code = code


class Telegram:
    def __init__(self, token, opener=urllib.request.urlopen):
        if not isinstance(token, str) or ":" not in token or len(token) > 200:
            raise ValueError("Telegram token must be configured")
        self._base = f"https://api.telegram.org/bot{token}/"
        self.opener = opener

    def call(self, method, payload):
        request = urllib.request.Request(
            self._base + method,
            data=json.dumps(payload, ensure_ascii=False).encode(),
            headers={"Content-Type": "application/json"},
        )
        try:
            with self.opener(request, timeout=10) as response:
                body = json.loads(response.read(65537))
        except HTTPError as e:
            if e.code == 429 or e.code >= 500:
                raise Retryable(f"Telegram HTTP {e.code}") from None
            raise Rejected(e.code) from None
        except OSError:
            raise Retryable("Telegram network error") from None
        if body.get("ok") is not True:
            raise Rejected("not_ok")
        return body.get("result")

    def send(self, chat, text, markup=None):
        payload = {"chat_id": chat, "text": text, "disable_web_page_preview": True}
        if markup:
            payload["reply_markup"] = markup
        return self.call("sendMessage", payload)

    def answer(self, callback_id):
        return self.call("answerCallbackQuery", {"callback_query_id": callback_id})
