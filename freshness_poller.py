"""AirLake - Freshness Poller & CloudWatch Metric Exporter.

Polls OpenAQ API v3 for latest air-quality measurements (specifically PM2.5)
across target monitoring stations in Hanoi, computes sensor data age (hours),
and publishes `SensorAgeHours` metrics to AWS CloudWatch (Namespace: `AirLake`).

Can be run:
1. Directly via CLI (with optional --dry-run or --mock for visual testing)
2. As a scheduled AWS Lambda function (via `lambda_handler`) triggered by EventBridge
3. As a periodic service/cron job on EC2
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta
import json
import logging
import os
from pathlib import Path
import sys
from typing import Any

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

import requests

try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
except ImportError:
    boto3 = None
    BotoCoreError = Exception
    ClientError = Exception

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("airlake.freshness_poller")

OPENAQ_BASE_URL = "https://api.openaq.org/v3"
DEFAULT_AWS_REGION = os.environ.get("AWS_REGION", "ap-southeast-2")
METRIC_NAMESPACE = "AirLake"

# Target stations and active PM2.5 sensor IDs as defined in AirLake architecture
# and infra/10-foundation/monitoring.tf
TARGET_STATIONS = {
    4946812: {
        "name": "Nhân Chính",
        "pm25_sensor_id": 13502151,
        "warn_hours": 1,
        "crit_hours": 24,
    },
    4946813: {
        "name": "Giải Phóng",
        "pm25_sensor_id": 13502154,
        "warn_hours": 1,
        "crit_hours": 24,
    },
    6123215: {
        "name": "OceanPark",
        "pm25_sensor_id": 14581375,
        "warn_hours": 2,
        "crit_hours": 24,
    },
}


def load_env_file() -> None:
    """Load environment variables from a local .env file if present."""
    candidate_paths = [Path(".env"), Path(__file__).resolve().parent / ".env"]
    for env_path in candidate_paths:
        if env_path.is_file():
            try:
                for line in env_path.read_text(encoding="utf-8").splitlines():
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        k, v = line.split("=", 1)
                        k = k.strip()
                        v = v.strip().strip('"').strip("'")
                        if k and k not in os.environ and v:
                            os.environ[k] = v
                break
            except Exception as err:
                logger.debug("Could not read .env file at %s: %s", env_path, err)


# Automatically attempt to load .env on import
load_env_file()


def get_openaq_api_key(
    explicit_key: str | None = None,
    region_name: str = DEFAULT_AWS_REGION,
    allow_missing: bool = False,
) -> str | None:
    """Retrieve the OpenAQ API key from CLI argument, environment variables, or AWS SSM Parameter Store."""
    if explicit_key and explicit_key.strip():
        return explicit_key.strip()

    api_key = os.environ.get("OPENAQ_KEY")
    if api_key and api_key.strip():
        return api_key.strip()

    if boto3 is not None:
        ssm_param_name = os.environ.get("OPENAQ_SSM_PARAM", "/airlake/openaq_key")
        try:
            ssm = boto3.client("ssm", region_name=region_name)
            response = ssm.get_parameter(Name=ssm_param_name, WithDecryption=True)
            val = response["Parameter"]["Value"].strip()
            if val and val != "REPLACE_ME":
                return val
        except (BotoCoreError, ClientError) as err:
            logger.debug("SSM lookup for %s failed: %s", ssm_param_name, err)

    if allow_missing:
        return None

    raise RuntimeError(
        "Không tìm thấy OpenAQ API Key. Hãy đặt biến môi trường OPENAQ_KEY, "
        "hoặc truyền tham số --api-key <key>, hoặc thêm cờ --mock để chạy thử nghiệm với dữ liệu giả lập."
    )


def fetch_station_freshness(
    location_id: int,
    api_key: str,
    target_sensor_id: int | None = None,
) -> dict[str, Any] | None:
    """Query OpenAQ API for the latest measurements of a station and calculate sensor age in hours."""
    headers = {"X-API-Key": api_key}
    url = f"{OPENAQ_BASE_URL}/locations/{location_id}/latest"

    try:
        resp = requests.get(url, headers=headers, timeout=20)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as err:
        logger.error("Failed to query OpenAQ latest measurements for location %d: %s", location_id, err)
        return None

    results = data.get("results", [])
    now = datetime.now(UTC)

    station_info = TARGET_STATIONS.get(location_id, {})
    expected_sensor_id = target_sensor_id or station_info.get("pm25_sensor_id")

    pm25_record = None
    for item in results:
        sensor_id = item.get("sensorsId")
        if expected_sensor_id and sensor_id == expected_sensor_id:
            pm25_record = item
            break

    if not pm25_record:
        for item in results:
            param = (item.get("parameter") or {}).get("name") if isinstance(item.get("parameter"), dict) else item.get("parameter")
            if param == "pm25":
                pm25_record = item
                break

    if not pm25_record:
        logger.warning(
            "No PM2.5 measurement found in latest results for location %d (expected sensor: %s)",
            location_id,
            expected_sensor_id,
        )
        return None

    sensor_id = pm25_record.get("sensorsId", expected_sensor_id)
    raw_dt = pm25_record.get("datetime", {})
    ts_str = raw_dt.get("utc") if isinstance(raw_dt, dict) else raw_dt

    if not ts_str:
        logger.warning("No timestamp found in PM2.5 measurement for location %d", location_id)
        return None

    try:
        last_dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        age_hours = max(0.0, (now - last_dt).total_seconds() / 3600.0)
    except (ValueError, TypeError) as err:
        logger.error("Invalid timestamp '%s' for location %d: %s", ts_str, location_id, err)
        return None

    status = "TƯƠI" if age_hours <= 1.0 else ("GIÁN ĐOẠN" if age_hours <= 24.0 else "CHẾT")

    return {
        "location_id": location_id,
        "station_name": station_info.get("name", f"Station-{location_id}"),
        "sensor_id": sensor_id,
        "parameter": "pm25",
        "value": pm25_record.get("value"),
        "unit": pm25_record.get("unit", "µg/m³"),
        "timestamp_utc": last_dt.isoformat(),
        "age_hours": round(age_hours, 2),
        "status": status,
    }


def generate_mock_freshness_data() -> list[dict[str, Any]]:
    """Generate realistic simulated air quality data for visual demonstration."""
    now = datetime.now(UTC)
    mock_samples = [
        {
            "loc_id": 4946812,
            "minutes_ago": 25,
            "value": 28.4,
            "note": "Hoạt động bình thường",
        },
        {
            "loc_id": 4946813,
            "minutes_ago": 45,
            "value": 35.1,
            "note": "Hoạt động bình thường",
        },
        {
            "loc_id": 6123215,
            "minutes_ago": 190,  # ~3.1 hours ago (Gián đoạn: vượt ngưỡng warn 2h của OceanPark)
            "value": 52.0,
            "note": "Cảm biến gián đoạn cập nhật",
        },
    ]

    results: list[dict[str, Any]] = []
    for item in mock_samples:
        loc_id = item["loc_id"]
        info = TARGET_STATIONS[loc_id]
        last_dt = now - timedelta(minutes=item["minutes_ago"])
        age_h = item["minutes_ago"] / 60.0

        status = "TƯƠI" if age_h <= 1.0 else ("GIÁN ĐOẠN" if age_h <= 24.0 else "CHẾT")

        results.append(
            {
                "location_id": loc_id,
                "station_name": info["name"],
                "sensor_id": info["pm25_sensor_id"],
                "parameter": "pm25",
                "value": item["value"],
                "unit": "µg/m³",
                "timestamp_utc": last_dt.isoformat(),
                "age_hours": round(age_h, 2),
                "status": status,
            }
        )
    return results


def publish_metrics_to_cloudwatch(
    metrics_list: list[dict[str, Any]],
    region_name: str = DEFAULT_AWS_REGION,
    dry_run: bool = False,
) -> bool:
    """Send SensorAgeHours metric data to AWS CloudWatch."""
    if not metrics_list:
        logger.info("No metrics to publish.")
        return True

    now = datetime.now(UTC)
    metric_data = []

    for m in metrics_list:
        metric_data.append(
            {
                "MetricName": "SensorAgeHours",
                "Dimensions": [
                    {"Name": "location_id", "Value": str(m["location_id"])},
                    {"Name": "sensors_id", "Value": str(m["sensor_id"])},
                    {"Name": "parameter", "Value": m["parameter"]},
                ],
                "Timestamp": now,
                "Value": float(m["age_hours"]),
                "Unit": "None",
            }
        )

    if dry_run:
        print("\n" + "=" * 90)
        print(" [DRY-RUN] DỮ LIỆU METRIC SẼ ĐƯỢC ĐẨY LÊN AWS CLOUDWATCH (NAMESPACE: 'AirLake')")
        print("=" * 90)
        for item in metric_data:
            dims_str = ", ".join(f"{d['Name']}={d['Value']}" for d in item["Dimensions"])
            print(f" • Metric: {item['MetricName']:<16} | Value: {item['Value']:>5.2f}h | Dimensions: [{dims_str}]")
        print("=" * 90)
        return True

    if boto3 is None:
        logger.error("boto3 is not available. Install boto3 to send metrics to CloudWatch.")
        return False

    try:
        cw = boto3.client("cloudwatch", region_name=region_name)
        cw.put_metric_data(Namespace=METRIC_NAMESPACE, MetricData=metric_data)
        logger.info("Successfully published %d metrics to CloudWatch namespace '%s'.", len(metric_data), METRIC_NAMESPACE)
        return True
    except (BotoCoreError, ClientError) as err:
        logger.error("Failed to publish metrics to CloudWatch: %s", err)
        return False


def print_visual_dashboard(results: list[dict[str, Any]], is_mock: bool = False) -> None:
    """Render a clean, visual console dashboard representing CloudWatch status."""
    header_title = "BẢNG ĐIỀU KHIỂN TRỰC QUAN ĐỘ TƯƠI DỮ LIỆU (AirLake Freshness Dashboard)"
    if is_mock:
        header_title += " [CHẾ ĐỘ MÔ PHỎNG --MOCK]"

    print("\n" + "=" * 90)
    print(f" {header_title}")
    print("=" * 90)
    print(f" {'TRẠNG THÁI':<12} | {'TRẠM QUAN TRẮC':<14} | {'SENSOR ID':<10} | {'TUỔI DỮ LIỆU':<14} | {'PM2.5':<12} | {'CLOUDWATCH ALARM'}")
    print("-" * 90)

    for r in results:
        status_tag = f"[{r['status']}]"
        age_str = f"{r['age_hours']:>5.1f} giờ"
        pm25_str = f"{r['value']:>4.1f} {r['unit']}" if r.get("value") is not None else "N/A"

        alarm_state = "OK (Bình thường)"
        if r["status"] == "GIÁN ĐOẠN":
            alarm_state = "CẢNH BÁO: Gián đoạn (>1h)"
        elif r["status"] == "CHẾT":
            alarm_state = "NGUY CẤP: Cảm biến chết (>24h)"

        print(f" {status_tag:<12} | {r['station_name']:<14} | {r['sensor_id']:<10} | {age_str:<14} | {pm25_str:<12} | {alarm_state}")

    print("=" * 90)


def run_poll(
    api_key: str | None = None,
    region_name: str = DEFAULT_AWS_REGION,
    dry_run: bool = False,
    mock: bool = False,
) -> list[dict[str, Any]]:
    """Execute one polling and metric publishing cycle for all target stations."""
    resolved_key = get_openaq_api_key(explicit_key=api_key, region_name=region_name, allow_missing=(mock or dry_run))

    if mock or not resolved_key:
        if not resolved_key and not mock:
            print("\n[THÔNG BÁO] Không tìm thấy OPENAQ_KEY trong môi trường hoặc file .env.")
            print("[THÔNG BÁO] Tự động chuyển sang chế độ --mock để mô phỏng hiển thị dữ liệu trực quan...")
        collected = generate_mock_freshness_data()
        print_visual_dashboard(collected, is_mock=True)
        publish_metrics_to_cloudwatch(collected, region_name=region_name, dry_run=True)
        return collected

    collected = []
    logger.info("Starting OpenAQ freshness poll for %d stations...", len(TARGET_STATIONS))

    for loc_id, info in TARGET_STATIONS.items():
        res = fetch_station_freshness(loc_id, resolved_key, target_sensor_id=info["pm25_sensor_id"])
        if res:
            collected.append(res)

    print_visual_dashboard(collected, is_mock=False)
    publish_metrics_to_cloudwatch(collected, region_name=region_name, dry_run=dry_run)
    return collected


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """AWS Lambda entry point when triggered by EventBridge Schedule."""
    logger.info("Invoked via Lambda with event: %s", json.dumps(event))
    results = run_poll(region_name=DEFAULT_AWS_REGION, dry_run=False)
    return {
        "statusCode": 200,
        "body": {
            "message": "Freshness poll completed",
            "count": len(results),
            "stations": results,
        },
    }


def main() -> None:
    """CLI entry point for local execution and debugging."""
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser(description="AirLake Freshness Poller & CloudWatch Metric Exporter")
    parser.add_argument("--dry-run", action="store_true", help="Print metrics instead of sending to CloudWatch")
    parser.add_argument("--mock", action="store_true", help="Run with simulated realistic station data for offline visual testing")
    parser.add_argument("--api-key", help="OpenAQ API key (optional if OPENAQ_KEY is in env or .env file)")
    parser.add_argument("--region", default=DEFAULT_AWS_REGION, help=f"AWS Region (default: {DEFAULT_AWS_REGION})")
    args = parser.parse_args()

    try:
        results = run_poll(api_key=args.api_key, region_name=args.region, dry_run=args.dry_run, mock=args.mock)
        print(f"\nThành công: Đã xử lý {len(results)} trạm quan trắc.\n")
    except Exception as exc:
        logger.exception("Poller failed with error: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
