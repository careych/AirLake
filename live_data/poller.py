"""One bounded poll: immutable raw S3 first, latest state second, cursor last."""

import json
import os
from datetime import timedelta
from itertools import pairwise

from ingestion.openaq import API, PILOTS, UnavailableStation, bronze_record, stamp, utc
from live_data.contract import (
    MAX_PAGES,
    MAX_RECORDS,
    OVERLAP_SECONDS,
    PAGE_SIZE,
    PREFIX,
    RUN,
    config,
    encode,
    freshness,
    now_utc,
    sha,
    validate_window,
)


def source_record(item, metadata, sensor, start, end):
    if len(metadata.get("name", "")) > 300:
        raise ValueError("Station name is too large")
    record = bronze_record(item, metadata, sensor, start, end)
    identity = f"{record['location_id']}:{record['sensors_id']}:{record['datetime']}"
    key = PREFIX + "bronze/" + sha(identity.encode()) + ".json"
    return record, key


def poll(api, store, now, deadline):
    now = utc(now)
    if now >= deadline:
        raise ValueError("Bounded test deadline reached; no source or write calls")
    owner = store.acquire(now)
    report = {
        "status": "PASS",
        "observed_source": "OpenAQ v3",
        "polled_at": stamp(now),
        "accepted_new": 0,
        "replayed": 0,
        "rejected": 0,
        "stations": [],
    }
    failed = False
    try:
        for location, sensor in PILOTS.items():
            info = {
                "location_id": location,
                "sensor_id": sensor,
                "accepted_new": 0,
                "replayed": 0,
                "rejected": 0,
                "completed_window": False,
            }
            report["stations"].append(info)
            try:
                metadata = api.metadata(location, sensor)
            except UnavailableStation:
                info["status"] = "source_unavailable"
                store.poll_status(location, now, info["status"])
                continue
            state = store.cursor(sensor)
            if state.get("pending_end"):
                start, end, page = (
                    utc(state["pending_start"]),
                    utc(state["pending_end"]),
                    state["page"],
                )
            else:
                end = now - timedelta(minutes=1)
                base = utc(state["watermark"]) if state.get("watermark") else end
                start = max(
                    end - timedelta(hours=24), base - timedelta(seconds=OVERLAP_SECONDS)
                )
                page = 1
                store.save_cursor(
                    sensor,
                    {
                        **state,
                        "pending_start": stamp(start),
                        "pending_end": stamp(end),
                        "page": page,
                    },
                    owner,
                )
            start, end = validate_window(start, end)
            info.update(
                window_start=stamp(start),
                window_end=stamp(end),
                source_latest_at=(metadata.get("datetimeLast") or {}).get("utc"),
            )
            for _ in range(MAX_PAGES):
                data = api.get(
                    f"/sensors/{sensor}/measurements",
                    {
                        "datetime_from": stamp(start),
                        "datetime_to": stamp(end),
                        "limit": PAGE_SIZE,
                        "page": page,
                    },
                )
                rows = data["results"]
                if len(rows) > PAGE_SIZE:
                    raise ValueError("Source exceeded requested page bound")
                times = []
                for item in rows:
                    try:
                        valid, _ = source_record(item, metadata, sensor, start, end)
                        times.append(utc(valid["datetime"]))
                    except (ValueError, TypeError, KeyError, AttributeError):
                        pass
                ordered = sorted(set(times))
                gaps = [(b - a).total_seconds() for a, b in pairwise(ordered)]
                spacing = int(min(gaps)) if gaps else None
                for item in rows:
                    if report["accepted_new"] + report["replayed"] >= MAX_RECORDS:
                        info["status"] = "budget_reached_resume_same_page"
                        store.poll_status(location, now, info["status"])
                        return report
                    try:
                        record, key = source_record(item, metadata, sensor, start, end)
                    except (ValueError, TypeError, AttributeError, KeyError):
                        report["rejected"] += 1
                        info["rejected"] += 1
                        continue
                    envelope = {
                        "schema_version": 1,
                        "record": record,
                        "observed_at": record["datetime"],
                        "ingested_at": stamp(now),
                        "source": {
                            "provider": "OpenAQ v3",
                            "endpoint": f"/sensors/{sensor}/measurements",
                            "page": page,
                            "licenses": metadata.get("licenses", []),
                            "attribution": metadata.get("provider"),
                        },
                        "record_sha256": sha(encode(record)),
                    }
                    period = item.get("period") or {}
                    try:
                        cadence = (
                            utc(period["datetimeTo"]["utc"])
                            - utc(period["datetimeFrom"]["utc"])
                        ).total_seconds()
                        if 0 < cadence <= 86400:
                            envelope["source"]["measurement_period_seconds"] = int(
                                cadence
                            )
                    except (KeyError, ValueError, TypeError):
                        pass
                    if spacing is not None:
                        envelope["source"]["observed_spacing_seconds"] = spacing
                        envelope["source"]["cadence_basis"] = (
                            "minimum positive observation spacing in this bounded page; not measurement period or SLA"
                        )
                    if len(encode(envelope)) > 100000:
                        raise ValueError("Source envelope exceeds persistence bound")
                    # An existing different value for the same key is a correction conflict,
                    # not permission to overwrite raw data or the first-wins Silver contract.
                    accepted, persisted = store.raw(key, envelope)
                    store.latest(location, persisted, key)
                    metric = "accepted_new" if accepted else "replayed"
                    report[metric] += 1
                    info[metric] += 1
                page += 1
                if len(rows) < PAGE_SIZE:
                    store.save_cursor(sensor, {"watermark": stamp(end)}, owner)
                    info["completed_window"] = True
                    break
                store.save_cursor(
                    sensor,
                    {
                        **state,
                        "pending_start": stamp(start),
                        "pending_end": stamp(end),
                        "page": page,
                    },
                    owner,
                )
            info["status"] = (
                "window_complete" if info["completed_window"] else "page_budget_resume"
            )
            store.poll_status(location, now, info["status"])
        return report
    except Exception:
        failed = True
        store.halt(owner)
        raise
    finally:
        if not failed:
            store.release(owner)


def handler(event, context):
    from live_data.http import Session
    from live_data.store import AWSStore, clients

    arn, deadline = config(os.environ)
    if event not in ({}, {"mode": "poll"}):
        raise ValueError("Unsupported poll invocation; no historical replay mode")
    now = now_utc()
    if now >= deadline:
        return {"status": "STOP_DEADLINE", "aws_writes": 0}
    c = clients()
    secret = c["secretsmanager"].get_secret_value(SecretId=arn).get("SecretString")
    if (
        not isinstance(secret, str)
        or not secret.strip()
        or "\n" in secret
        or secret.startswith("{")
    ):
        raise ValueError("Expected plaintext OpenAQ key")
    try:
        store = AWSStore(c)
        report = poll(API(secret, session=Session()), store, now, deadline)
        for station in report["stations"]:
            accepted = store.read("LATEST", str(station["location_id"]))
            f = freshness(
                accepted.get("observed_at"), now, accepted.get("cadence_seconds")
            )
            metrics = [{"Name": "FreshnessKnown", "Unit": "Count"}]
            log = {
                "_aws": {
                    "Timestamp": int(now.timestamp() * 1000),
                    "CloudWatchMetrics": [
                        {
                            "Namespace": "AirLake/LiveDemo",
                            "Dimensions": [["Run", "Location"]],
                            "Metrics": metrics,
                        }
                    ],
                },
                "Run": RUN,
                "Location": str(station["location_id"]),
                "FreshnessKnown": int(f["age_seconds"] is not None),
                "status": f["status"],
            }
            if f["age_seconds"] is not None:
                metrics.append({"Name": "ObservationAgeSeconds", "Unit": "Seconds"})
                log["ObservationAgeSeconds"] = f["age_seconds"]
            print(json.dumps(log))
        # Counts/status only; never upstream responses, headers or credentials.
        print(
            json.dumps(
                {
                    "metric": "AirLakePoll",
                    "accepted_new": report["accepted_new"],
                    "replayed": report["replayed"],
                    "rejected": report["rejected"],
                    "status": report["status"],
                }
            )
        )
        return report
    except Exception as e:  # noqa: BLE001 -- raw HTTP/SDK messages may contain private data
        print(
            json.dumps(
                {
                    "metric": "AirLakePoll",
                    "status": "FAILED_OR_UNCERTAIN",
                    "exception_type": type(e).__name__,
                }
            )
        )
        raise RuntimeError(
            "Poll stopped; preserve state and inspect sanitized logs"
        ) from None
