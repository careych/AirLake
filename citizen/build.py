"""Build static immutable projections. No AWS calls, credentials or build dependencies."""

import argparse
import hashlib
import json
import shutil
from pathlib import Path

from citizen.data import project

ROOT = Path(__file__).resolve().parent


def verified_data(export_path, inventory_path, coverage_path, evidence_path, mode):
    raw = Path(export_path).read_bytes()
    proof = json.loads(Path(evidence_path).read_bytes())
    # Verify the exact bytes accepted by the independent stored Athena result comparison.
    if (
        hashlib.sha256(raw).hexdigest() != proof["export_sha256"]
        or proof["athena"]["independent_stored_query_result_reconciliation"] != "PASS"
        or type(proof["athena"]["rows"]) is not int
        or not 1 <= proof["athena"]["rows"] <= 1000
        or proof["athena"]["rows"] != len(json.loads(raw).get("facts", []))
    ):
        raise ValueError("Independent source verification missing")
    data = project(
        json.loads(raw),
        json.loads(Path(inventory_path).read_bytes()),
        json.loads(Path(coverage_path).read_bytes()),
        mode,
        proof["source"]["station_counts"],
    )
    return data, proof


def build(output, export_path, inventory_path, coverage_path, evidence_path, mode):
    output = Path(output)
    if output.exists() or output.is_symlink():
        raise ValueError("Preserve previous builds; choose a new output directory")
    if any(
        (p / ".git").exists() for p in (output.resolve(), *output.resolve().parents)
    ):
        raise ValueError("Deployable datasets belong outside Git")
    data, proof = verified_data(
        export_path, inventory_path, coverage_path, evidence_path, mode
    )
    body = json.dumps(
        data, ensure_ascii=False, sort_keys=True, allow_nan=False
    ).encode()
    digest = hashlib.sha256(body).hexdigest()
    output.mkdir(parents=True)
    (output / "data").mkdir()
    for file in ROOT.joinpath("web").iterdir():
        if file.is_file():
            shutil.copyfile(file, output / file.name)
    (output / "data" / (digest + ".json")).write_bytes(body)
    (output / "config.js").write_text(
        "window.AIRLAKE_CONFIG="
        + json.dumps(
            {"dataUrl": "data/" + digest + ".json", "sha256": digest, "mode": mode}
        )
        + ";\n"
    )
    return {
        "mode": mode,
        "stations": len(data["stations"]),
        "hourly": len(data["hourly"]),
        "data_sha256": digest,
        "source_sha256": proof["export_sha256"],
        "source_query_id": proof["athena"]["query_id"],
    }


def main():
    a = argparse.ArgumentParser(description=__doc__)
    for name in ("output", "export", "inventory", "coverage", "evidence"):
        a.add_argument("--" + name, type=Path, required=True)
    a.add_argument("--mode", choices=["public", "instructor"], required=True)
    x = a.parse_args()
    print(
        json.dumps(
            build(x.output, x.export, x.inventory, x.coverage, x.evidence, x.mode)
        )
    )


if __name__ == "__main__":
    main()
