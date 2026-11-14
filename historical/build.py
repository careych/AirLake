"""Build licensed historical analytics from a verified export; local preview is explicit."""

import argparse
import hashlib
import json
import math
import shutil
from datetime import timedelta
from pathlib import Path

from citizen.data import project
from historical.analytics import WHO_REFERENCE, summarize
from ingestion.openaq import stamp, utc

ROOT = Path(__file__).resolve().parents[1]


def build(
    output,
    export_path,
    audit_path,
    metadata_path,
    latest_path,
    source_metadata_path,
    evidence_path,
    mode,
    allow_local_preview=False,
    athena_proof_path=None,
):
    output = Path(output)
    if (
        output.exists()
        or output.is_symlink()
        or any(
            (p / ".git").exists() for p in (output.resolve(), *output.resolve().parents)
        )
    ):
        raise ValueError("New private output outside Git required")
    raw = Path(export_path).read_bytes()
    export = json.loads(raw)
    audit = json.loads(Path(audit_path).read_bytes())
    evidence = json.loads(Path(evidence_path).read_bytes())
    meta = json.loads(Path(metadata_path).read_bytes())
    inventory = json.loads(Path(latest_path).read_bytes())
    digest = hashlib.sha256(raw).hexdigest()
    provenance = export["provenance"]
    if (
        evidence["status"] != "PASS"
        or evidence["export_sha256"] != digest
        or provenance["source_records_sha256"] != audit["records_sha256"]
        or evidence["snapshots_before"] != evidence["snapshots_after"]
        or evidence["snapshots_after"] != provenance["snapshots"]
    ):
        raise ValueError("Backfill verification/source hashes mismatch")
    cloud = provenance["execution"] == "AWS_GLUE"
    if not cloud:
        if not allow_local_preview or provenance["execution"] != "LOCAL_ISOLATED_SPARK":
            raise ValueError(
                "Explicit local preview required; no AWS acceptance inferred"
            )
    else:
        proof = (
            json.loads(Path(athena_proof_path).read_bytes())
            if athena_proof_path
            else {}
        )
        if (
            proof.get("export_sha256") != digest
            or proof.get("hourly_all_rows_match") != "PASS"
            or proof.get("daily_all_rows_match") != "PASS"
        ):
            raise ValueError(
                "Independent Athena acceptance required before publication"
            )
    reference = {
        (r["location_id"], r["hour_start"]): r["pm25_avg"]
        for r in audit["hourly_reference"]
    }
    observed = {
        (r["location_id"], r["hour_start"]): r["pm25_avg"] for r in export["facts"]
    }
    if (
        set(reference) != set(observed)
        or len(observed) != len(export["facts"])
        or any(
            not math.isclose(observed[k], v, rel_tol=1e-12, abs_tol=1e-10)
            for k, v in reference.items()
        )
    ):
        raise ValueError("Actual hourly export differs from source reference")
    data = project(
        export,
        inventory,
        meta,
        mode,
        {k: v["actual_records"] for k, v in audit["stations"].items()},
    )
    # The immutable dataset verification time is input provenance, not wall-clock
    # build time or an unverified Amplify publication timestamp.
    data.pop("published_at", None)
    data["dataset_verified_at"] = (
        proof.get("verified_at", provenance["computed_at"])
        if cloud
        else provenance["computed_at"]
    )
    ids = {s["id"] for s in data["stations"]}
    selected = [r for r in export["facts"] if r["location_id"] in ids]
    lookup = {r["id"]: r for r in meta["locations"]}
    src = (
        json.loads(Path(source_metadata_path).read_bytes())
        if source_metadata_path
        else {}
    )
    current = {
        r["station_id"]: r for r in src.get("requests", []) if r.get("status") == 200
    }
    for s in data["stations"]:
        s["historical_name"] = s["name"]
        s["name"] = lookup[s["id"]]["name"].strip()
        s["sensor_id"] = audit["stations"][str(s["id"])]["sensor_id"]
        s["history_quality"] = {
            k: v
            for k, v in audit["stations"][str(s["id"])].items()
            if k not in ("metadata", "missing_hours_utc")
        }
        s["source_last_reported_at"] = (
            current.get(s["id"], {}).get("latest_source_observation") or {}
        ).get("utc")
    start, end = utc(audit["window_start_utc"]), utc(audit["window_end_utc"])
    data.update(
        schema_version=2,
        cloud_verified=cloud,
        window_start_utc=stamp(start),
        window_end_utc=stamp(end),
        source_metadata_checked_at=src.get("checked_at"),
        historical_source="OpenAQ archived genuine measurements; source timestamps retained; descriptive Gold hour buckets",
        historical_raw_records=sum(
            audit["stations"][str(i)]["actual_records"] for i in ids
        ),
        historical_timezone="Asia/Ho_Chi_Minh",
        daily=[
            {
                "station_id": r["location_id"],
                "date_local": r["date_local"],
                "pm25": r["pm25_avg"],
                "hours_present": r["hours_present"],
                "hours_expected": 24,
                "coverage_percent": r["coverage_percent"],
                "complete_24_hours": r["complete_24_hours"],
            }
            for r in export["daily"]
            if r["location_id"] in ids
        ],
        analytics={},
        methodology=WHO_REFERENCE,
        source_export_sha256=digest,
    )
    for days, label in ((1, "24h"), (7, "7d"), (30, "30d")):
        begin = max(start, end - timedelta(days=days))
        data["analytics"][label] = summarize(selected, stamp(begin), stamp(end), ids)
    body = json.dumps(
        data, ensure_ascii=False, sort_keys=True, allow_nan=False
    ).encode()
    h = hashlib.sha256(body).hexdigest()
    output.mkdir(parents=True)
    (output / "data").mkdir()
    for p in (ROOT / "citizen/web").iterdir():
        if p.is_file():
            shutil.copyfile(p, output / p.name)
    (output / "data" / (h + ".json")).write_bytes(body)
    (output / "config.js").write_text(
        "window.AIRLAKE_CONFIG="
        + json.dumps({"mode": mode, "dataUrl": "data/" + h + ".json", "sha256": h})
        + ";\n"
    )
    return {
        "status": "LOCAL_PREVIEW_NOT_AWS_VERIFIED"
        if not cloud
        else "AWS_VERIFIED_BUILD",
        "data_sha256": h,
        "stations": len(ids),
        "hourly_rows": len(data["hourly"]),
        "daily_rows": len(data["daily"]),
        "bytes": len(body),
    }


def main():
    a = argparse.ArgumentParser(description=__doc__)
    for key in (
        "output",
        "export",
        "audit",
        "metadata",
        "latest",
        "source-metadata",
        "evidence",
    ):
        a.add_argument("--" + key, type=Path, required=True)
    a.add_argument("--mode", choices=("public", "instructor"), required=True)
    a.add_argument("--allow-local-preview", action="store_true")
    a.add_argument("--athena-proof", type=Path)
    x = a.parse_args()
    print(
        json.dumps(
            build(
                x.output,
                x.export,
                x.audit,
                x.metadata,
                x.latest,
                x.source_metadata,
                x.evidence,
                x.mode,
                x.allow_local_preview,
                x.athena_proof,
            )
        )
    )


if __name__ == "__main__":
    main()
