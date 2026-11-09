"""Pack reviewed exported YAML into the Superset import format, without clients."""

import argparse
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent / "assets"


def pack(target):
    files = sorted(ROOT.rglob("*.yaml"))
    if not files:
        raise ValueError("No reviewed dashboard assets found")
    with zipfile.ZipFile(target, "x", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in files:
            if path.is_symlink():
                raise ValueError("Unsafe dashboard asset")
            archive.writestr(
                "dashboard_export/" + path.relative_to(ROOT).as_posix(),
                path.read_bytes(),
            )
    return len(files)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--output", type=Path, required=True)
    a = p.parse_args()
    print("Packed YAML files:", pack(a.output))
