"""Localhost HTTP interface. HTTP requests never call AWS or execute SQL."""

import csv
import io
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

from clean_api.data import FACT_SNAPSHOT, FIELDS


class LocalServer(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 16


def make_server(dataset, port=18090):
    if type(port) is not int or not 0 <= port <= 65535:
        raise ValueError("Invalid local port")

    class Handler(BaseHTTPRequestHandler):
        server_version = "AirLakeCleanAPI/1"
        sys_version = ""

        def setup(self):
            super().setup()
            self.connection.settimeout(10)

        def log_message(self, *_):
            # Do not retain request URLs, source rows or client-supplied values.
            pass

        def send_body(
            self,
            status,
            body,
            content_type="application/json; charset=utf-8",
            headers=None,
        ):
            if not isinstance(body, bytes):
                body = json.dumps(body, ensure_ascii=False, allow_nan=False).encode()
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            for key, value in (headers or {}).items():
                self.send_header(key, str(value))
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)

        def do_HEAD(self):
            self.do_GET()

        def do_GET(self):
            expected = {
                f"127.0.0.1:{self.server.server_port}",
                f"localhost:{self.server.server_port}",
            }
            if self.headers.get("Host") not in expected or self.headers.get(
                "Origin"
            ) not in {None, *("http://" + h for h in expected)}:
                self.send_body(403, {"error": "local_origin_required"})
                return
            if len(self.path) > 4096:
                self.send_body(414, {"error": "request_too_long"})
                return
            try:
                url = urlsplit(self.path)
                params = parse_qs(url.query, keep_blank_values=True, max_num_fields=10)
                if url.path in ("/api/v1/health", "/api/v1/stations") and params:
                    raise ValueError("This endpoint accepts no query parameters")
                if url.path == "/api/v1/health":
                    self.send_body(
                        200,
                        {
                            "status": "ok",
                            "dataset_loaded": True,
                            "hourly_rows": len(dataset.rows),
                            "stations": len(dataset.stations),
                            "athena_health": "not_checked",
                            "backend": "retained_cloud_acceptance_copy",
                        },
                    )
                elif url.path == "/api/v1/stations":
                    self.send_body(
                        200,
                        {
                            "data": dataset.stations,
                            "count": len(dataset.stations),
                            "metadata": dataset.metadata(),
                        },
                    )
                elif url.path == "/api/v1/gold/hourly":
                    if (
                        "format" not in params
                        and self.headers.get("Accept", "").split(",")[0].strip()
                        == "text/csv"
                    ):
                        params["format"] = ["csv"]
                    result, fmt = dataset.page(params)
                    if fmt == "json":
                        self.send_body(200, result)
                    else:
                        stream = io.StringIO(newline="")
                        writer = csv.DictWriter(stream, fieldnames=FIELDS)
                        writer.writeheader()
                        for row in result["data"]:
                            # CSV quoting alone does not stop spreadsheet formulas.
                            row = dict(row)
                            if (
                                row["station_name"]
                                .lstrip()
                                .startswith(("=", "+", "-", "@"))
                            ):
                                row["station_name"] = "'" + row["station_name"]
                            writer.writerow(row)
                        headers = {
                            "Content-Disposition": 'attachment; filename="airlake-historical-gold.csv"',
                            "X-Total-Count": result["total"],
                            "X-Returned-Count": result["count"],
                            "X-Gold-Snapshot": FACT_SNAPSHOT,
                            "X-PM25-Units": "micrograms/m3",
                            "X-AQI-Method": "simplified_hourly_mean_mvp-not-official-VN_AQI",
                            "X-Data-Classification": "historical-bounded-batch",
                            "X-Data-Source": "OpenAQ-v3-via-AirLake-Gold",
                            "X-Redistribution": "not-cleared; localhost-demo-only",
                        }
                        if result["next"]:
                            headers["Link"] = f'<{result["next"]}>; rel="next"'
                        self.send_body(
                            200,
                            stream.getvalue().encode(),
                            "text/csv; charset=utf-8",
                            headers,
                        )
                else:
                    self.send_body(404, {"error": "not_found"})
            except ValueError:
                self.send_body(
                    400,
                    {
                        "error": "invalid_parameters",
                        "help": "Use approved station IDs, UTC start/end inside the batch, limit 1..100, offset 0..1000, and format json|csv",
                    },
                )
            except Exception:  # noqa: BLE001 -- HTTP errors must not expose internal exception values
                self.send_body(500, {"error": "internal_error"})

        def reject_write(self):
            self.send_body(405, {"error": "read_only"}, headers={"Allow": "GET, HEAD"})

        do_POST = do_PUT = do_PATCH = do_DELETE = do_OPTIONS = reject_write

    return LocalServer(("127.0.0.1", port), Handler)
