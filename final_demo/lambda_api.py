"""IAM-authenticated Function URL adapter for a hash-verified Glue export."""

import csv
import io
import json
import os
from urllib.parse import parse_qs

from clean_api.data import ACCOUNT, EVIDENCE_SHA256, FIELDS, Dataset
from final_demo.contract import SOURCES, digest, new_plan

_cached = None


class CloudDataset(Dataset):
    def __init__(self, value, expected_sha):
        if value.get("evidence_sha256") != EVIDENCE_SHA256:
            raise ValueError("Unapproved source reference")
        p = new_plan(os.environ["AIRLAKE_FINAL_RUN"])
        provenance = value["provenance"]
        if (
            provenance["run"] != p["run"]
            or provenance["database"] != p["database"]
            or provenance["execution"] != "AWS_GLUE"
            or provenance["source_snapshots"] != SOURCES
        ):
            raise ValueError("Unexpected export provenance")
        for name in ("hourly", "dimensions", "breakpoints"):
            ids = provenance["output_snapshots"][name]
            if (
                not isinstance(ids, list)
                or len(ids) != 1
                or type(ids[0]) is not int
                or ids[0] <= 0
            ):
                raise ValueError("Export must identify exactly one initial snapshot")
        super().__init__(
            value["facts"],
            value["dimensions"],
            evidence_sha256=value["evidence_sha256"],
        )
        if len(self.rows) != 72 or len(self.stations) != 3:
            raise ValueError("Unexpected exported counts")
        self.export_provenance, self.export_sha = provenance, expected_sha

    def metadata(self):
        value = super().metadata()
        value["backend"] = "s3_verified_aws_glue_export"
        value["licensing"]["scope"] = (
            "private IAM-authenticated demonstration only; public redistribution not cleared"
        )
        value["provenance"].update(
            database=self.export_provenance["database"],
            hourly_snapshot=str(
                self.export_provenance["output_snapshots"]["hourly"][0]
            ),
            dimension_snapshot=str(
                self.export_provenance["output_snapshots"]["dimensions"][0]
            ),
            execution="AWS_GLUE",
            source_snapshots=SOURCES,
            export_sha256=self.export_sha,
            acceptance_completed_at_utc=None,
        )
        return value


def load_dataset(s3=None):
    global _cached
    if _cached is not None:
        return _cached
    p = new_plan(os.environ["AIRLAKE_FINAL_RUN"])
    if s3 is None:
        import boto3
        from botocore.config import Config

        s3 = boto3.client(
            "s3",
            region_name=p["region"],
            config=Config(
                retries={"total_max_attempts": 1}, connect_timeout=3, read_timeout=5
            ),
        )
    response = s3.get_object(
        Bucket=p["bucket"],
        Key=p["prefix"] + "exports/gold.json",
        ExpectedBucketOwner=ACCOUNT,
    )
    raw = response["Body"].read(100_001)
    if len(raw) > 100_000 or digest(raw) != os.environ["AIRLAKE_EXPORT_SHA256"]:
        raise ValueError("Export integrity mismatch")
    _cached = CloudDataset(json.loads(raw), digest(raw))
    return _cached


def reply(code, value, content_type="application/json; charset=utf-8", headers=None):
    body = (
        value
        if isinstance(value, str)
        else json.dumps(value, ensure_ascii=False, allow_nan=False)
    )
    return {
        "statusCode": code,
        "headers": {
            "content-type": content_type,
            "cache-control": "no-store",
            "x-content-type-options": "nosniff",
            **(headers or {}),
        },
        "body": body,
        "isBase64Encoded": False,
    }


def handler(event, context):
    request = event.get("requestContext", {})
    if request.get("authorizer", {}).get("iam", {}).get("accountId") != ACCOUNT:
        return reply(403, {"error": "iam_authenticated_same_account_required"})
    method = request.get("http", {}).get("method")
    if method not in ("GET", "HEAD"):
        return reply(405, {"error": "read_only"}, headers={"allow": "GET, HEAD"})
    try:
        raw_query = event.get("rawQueryString", "")
        if len(raw_query) > 4096:
            return reply(414, {"error": "request_too_long"})
        params = parse_qs(raw_query, keep_blank_values=True, max_num_fields=10)
        path = event.get("rawPath")
        if path not in ("/api/v1/health", "/api/v1/gold/hourly", "/api/v1/stations"):
            return reply(404, {"error": "not_found"})
        if path != "/api/v1/gold/hourly" and params:
            raise ValueError("Unexpected query parameters")
        try:
            data = load_dataset()
        except Exception:  # noqa: BLE001 -- invalid/missing S3 export is service unavailability
            return reply(503, {"error": "verified_dataset_unavailable"})
        if path == "/api/v1/health":
            result = reply(
                200,
                {
                    "status": "ok",
                    "dataset_loaded": True,
                    "hourly_rows": len(data.rows),
                    "stations": len(data.stations),
                    "athena_health": "not_checked",
                    "backend": "s3_verified_aws_glue_export",
                },
            )
        elif path == "/api/v1/stations":
            result = reply(
                200,
                {
                    "data": data.stations,
                    "count": len(data.stations),
                    "metadata": data.metadata(),
                },
            )
        else:
            if (
                "format" not in params
                and event.get("headers", {}).get("accept", "").split(",")[0].strip()
                == "text/csv"
            ):
                params["format"] = ["csv"]
            page, fmt = data.page(params)
            if fmt == "json":
                result = reply(200, page)
            else:
                buffer = io.StringIO(newline="")
                writer = csv.DictWriter(buffer, fieldnames=FIELDS)
                writer.writeheader()
                for row in page["data"]:
                    row = dict(row)
                    if row["station_name"].lstrip().startswith(("=", "+", "-", "@")):
                        row["station_name"] = "'" + row["station_name"]
                    writer.writerow(row)
                headers = {
                    "x-total-count": str(page["total"]),
                    "x-returned-count": str(page["count"]),
                    "x-gold-snapshot": data.metadata()["provenance"]["hourly_snapshot"],
                    "x-pm25-units": "micrograms/m3",
                    "x-aqi-method": "simplified_hourly_mean_mvp-not-official-VN_AQI",
                    "x-data-classification": "historical-bounded-batch",
                    "x-data-source": "OpenAQ-v3-via-AWS-Glue-Gold",
                    "x-redistribution": "not-cleared; private-demo-only",
                }
                if page["next"]:
                    headers["link"] = f'<{page["next"]}>; rel="next"'
                result = reply(
                    200, buffer.getvalue(), "text/csv; charset=utf-8", headers
                )
        if method == "HEAD":
            result["body"] = ""
        return result
    except ValueError:
        return reply(400, {"error": "invalid_parameters_or_export"})
    except Exception:  # noqa: BLE001 -- never return raw SDK messages or source content
        return reply(503, {"error": "verified_dataset_unavailable"})
