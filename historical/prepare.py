"""Bounded cached-archive preparation; default reads only, resume verifies exact hashes."""

import argparse
import json
from pathlib import Path

from historical.data import save_scan, scan
from live_data.contract import sha


def resume(root, output):
    a = json.loads((output / "audit.json").read_bytes())
    body = (output / "records.jsonl").read_bytes()
    if sha(body) != a["records_sha256"]:
        raise ValueError("Prepared source changed")
    for f in a["scanned_files"]:
        p = root / f["path"]
        if (
            not p.resolve().is_relative_to(root.resolve())
            or sha(p.read_bytes()) != f["sha256"]
        ):
            raise ValueError("Original archive source changed; preserve prior version")
    return {
        "status": "VERIFIED_ALREADY_PREPARED_NO_REPLAY",
        "source_records": a["accepted_unique_records"],
        "records_sha256": a["records_sha256"],
        "aws_calls": 0,
    }


def main():
    a = argparse.ArgumentParser(description=__doc__)
    a.add_argument("--source-root", type=Path, required=True)
    a.add_argument("--metadata", type=Path)
    a.add_argument("--start")
    a.add_argument("--end")
    a.add_argument("--output", type=Path)
    a.add_argument("--write", action="store_true")
    a.add_argument("--resume", action="store_true")
    x = a.parse_args()
    if x.resume:
        print(json.dumps(resume(x.source_root, x.output)))
        return
    if not x.metadata or not x.start or not x.end:
        a.error("Metadata and frozen UTC range required")
    metadata = json.loads(x.metadata.read_bytes())
    result, rows = scan(
        x.source_root, x.start, x.end, station_metadata=metadata["locations"]
    )
    if x.write:
        if not x.output:
            a.error("--output is required for local writing")
        result = save_scan(x.output, result, rows)
    print(
        json.dumps(
            {
                "status": "PREPARED_LOCAL_SOURCE_NOT_AWS"
                if x.write
                else "READ_ONLY_AUDIT",
                "source_records": result["accepted_unique_records"],
                "hourly_reference_rows": len(result["hourly_reference"]),
                "source_files": len(result["scanned_files"]),
                "stations": {
                    k: {
                        field: v[field]
                        for field in (
                            "actual_records",
                            "hourly_keys",
                            "missing_hour_count",
                            "duplicate_identities",
                            "coordinate_serialization_normalized",
                        )
                    }
                    for k, v in result["stations"].items()
                },
                "aws_calls": 0,
            }
        )
    )


if __name__ == "__main__":
    main()
