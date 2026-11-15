"""Stdlib HTTPS client for the refresh Lambda: fixed hosts, no redirects, bounded bodies."""

import json
from urllib.error import HTTPError
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

HOSTS = {"api.openaq.org", "data.sensor.community", "air-quality-api.open-meteo.com"}
UA = "AirLake Hanoi air-quality pilot (university project)"


class UpstreamError(Exception):
    def __init__(self, reason):
        super().__init__(reason)
        self.reason = reason


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def get(url, params=None, headers=None, timeout=10):
    if urlsplit(url).scheme != "https" or urlsplit(url).hostname not in HOSTS:
        raise ValueError("Unexpected upstream host")
    request = Request(
        url + ("?" + urlencode(params) if params else ""),
        headers={"User-Agent": UA, **(headers or {})},
    )
    try:
        with build_opener(ProxyHandler({}), NoRedirect()).open(
            request, timeout=timeout
        ) as response:
            body = response.read(2_000_001)
    except HTTPError as error:
        raise UpstreamError(f"HTTP {error.code}") from None
    except OSError:
        raise UpstreamError("network_error") from None
    if len(body) > 2_000_000:
        raise UpstreamError("response_too_large")
    return json.loads(body)
