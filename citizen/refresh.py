"""Offline, idempotent website refresh preparation; never deploys or polls AWS."""

import argparse
import fcntl
import json
import os
import re
import uuid
from pathlib import Path

from citizen.build import ROOT, build, verified_data
from citizen.hosting import zip_site
from live_data.contract import sha


def prepare(workdir, inputs, execute_local=False):
    workdir = Path(workdir)
    if workdir.is_symlink() or any(
        (p / ".git").exists() for p in (workdir.resolve(), *workdir.resolve().parents)
    ):
        raise ValueError("Use a private working directory outside Git and runtime data")
    if any(p in {"warehouse", "checkpoints", "data"} for p in workdir.parts):
        raise ValueError("Never use production runtime directories")
    data, _ = verified_data(*inputs, "instructor")
    data.pop("published_at")
    # A new check timestamp alone is not new measurement content.
    if data.get("operations_status"):
        data["operations_status"] = {
            k: v for k, v in data["operations_status"].items() if k != "verified_at"
        }
    fingerprint = sha(
        json.dumps(
            {
                "data": data,
                "assets": {
                    p.name: sha(p.read_bytes())
                    for p in sorted((ROOT / "web").iterdir())
                    if p.is_file()
                },
            },
            sort_keys=True,
            allow_nan=False,
        ).encode()
    )
    state = workdir / "current.json"

    def unchanged():
        if not state.exists():
            return False
        if state.is_symlink():
            raise ValueError("Private publication state cannot be a symlink")
        current = json.loads(state.read_bytes())
        old = current.get("fingerprint")
        if not isinstance(old, str) or not re.fullmatch("[a-f0-9]{64}", old):
            raise ValueError("Invalid local publication state")
        if old != fingerprint:
            return False
        for branch, mode in (("public", "public"), ("demo", "instructor")):
            _, detail = zip_site(workdir / old / branch, mode)
            if detail != current["builds"][branch]:
                raise ValueError(
                    "Prior local build changed; do not silently replace it"
                )
        return True

    if unchanged():
        return {"status": "SKIP_UNCHANGED", "fingerprint": fingerprint, "aws_calls": 0}
    if not execute_local:
        return {
            "status": "WOULD_PREPARE_NEW_LOCAL_BUILDS",
            "fingerprint": fingerprint,
            "aws_calls": 0,
        }
    workdir.mkdir(mode=0o700, parents=True, exist_ok=True)
    if workdir.stat().st_mode & 0o077:
        raise ValueError("Private working directory must have mode 0700")
    lock_path = workdir / ".prepare.lock"
    if lock_path.is_symlink():
        raise ValueError("Unsafe local lock")
    with lock_path.open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if unchanged():
            return {
                "status": "SKIP_UNCHANGED",
                "fingerprint": fingerprint,
                "aws_calls": 0,
            }
        version = workdir / fingerprint
        if version.exists():
            raise ValueError(
                "Preserve incomplete or previous build; inspect before retry"
            )
        builds = {}
        for branch, mode in (("public", "public"), ("demo", "instructor")):
            build(version / branch, *inputs, mode)
            _, builds[branch] = zip_site(version / branch, mode)
        result = {
            "status": "PREPARED_LOCAL_ONLY_NOT_DEPLOYED",
            "fingerprint": fingerprint,
            "builds": builds,
            "previous_fingerprint": json.loads(state.read_bytes())["fingerprint"]
            if state.exists()
            else None,
            "aws_calls": 0,
        }
        (version / "preparation.json").write_text(json.dumps(result, sort_keys=True))
        temporary = workdir / (".current-" + uuid.uuid4().hex)
        with temporary.open("x") as f:
            os.chmod(temporary, 0o600)
            f.write(json.dumps(result, sort_keys=True))
            f.flush()
            os.fsync(f.fileno())
        os.replace(temporary, state)
        return result


def main():
    a = argparse.ArgumentParser(description=__doc__)
    a.add_argument("--workdir", type=Path, required=True)
    for n in ("export", "inventory", "coverage", "evidence"):
        a.add_argument("--" + n, type=Path, required=True)
    a.add_argument("--execute-local", action="store_true")
    x = a.parse_args()
    print(
        json.dumps(
            prepare(
                x.workdir,
                (x.export, x.inventory, x.coverage, x.evidence),
                x.execute_local,
            )
        )
    )


if __name__ == "__main__":
    main()
