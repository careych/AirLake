"""Small stdlib transport for the existing OpenAQ API client in Lambda."""

import json
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class Session:
    def get(
        self, url, *, params=None, headers=None, timeout=None, allow_redirects=False
    ):
        if not url.startswith("https://api.openaq.org/v3/") or allow_redirects:
            raise ValueError("Unexpected upstream host or redirect")
        request = Request(
            url + ("?" + urlencode(params) if params else ""), headers=headers or {}
        )
        opener = build_opener(ProxyHandler({}), NoRedirect())
        try:
            response = opener.open(request, timeout=20)
        except HTTPError as error:
            response = error
        with response:
            body = response.read(2_000_001)
            if len(body) > 2_000_000:
                raise ValueError("OpenAQ response too large")

            class Result:
                def __init__(self):
                    self.status_code = response.code
                    self.headers = dict(response.headers)

                def json(self):
                    return json.loads(body)

            return Result()
