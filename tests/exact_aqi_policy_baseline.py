"""Pre-policy source bytes for evidence recorded before the exact AQI policy."""

import subprocess
from pathlib import Path

REPOSITORY = Path(__file__).resolve().parents[1]
# Reviewed 2026-11-15 exact AQI rounding policy; earlier evidence predates it.
BASE = "6452341f9e01661ca4746fe32f67341b67a2b8b6"
FILES = {"scripts/gold_batch.py", "scripts/real_batch.py"}


def recorded_bytes(name: str) -> bytes:
    """Bytes the evidence describes: Git baseline for changed files, else disk."""
    if name in FILES:
        return subprocess.check_output(
            ["git", "-C", str(REPOSITORY), "show", f"{BASE}:{name}"]
        )
    return (REPOSITORY / name).read_bytes()
