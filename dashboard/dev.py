"""Start only a new loopback Superset development instance; never AWS."""

import argparse
import os
import socket
import subprocess
from pathlib import Path

from dashboard.prepare import prepare

IMAGE = "apache/superset@sha256:16b50bbef6648912a79e3293d418fefa743ce555d36c5af6b85d859119ed7f88"
COMMAND = """superset db upgrade > /app/dev/init.log 2>&1 && superset fab create-admin --username airlake_dev --firstname AirLake --lastname Dev --email dev@localhost --password "$(python -c 'import json;print(json.load(open("/app/dev/private.json"))["ADMIN_PASSWORD"])')" >> /app/dev/init.log 2>&1 && superset init >> /app/dev/init.log 2>&1 && gunicorn --bind 0.0.0.0:8088 --workers 1 --threads 4 --timeout 60 'superset.app:create_app()'"""


def start(root, docker="docker", name="airlake-superset-dev"):
    if not name.startswith("airlake-superset-dev"):
        raise ValueError("Use an isolated development container name")
    existing = subprocess.run(
        [docker, "container", "inspect", name], capture_output=True, check=False
    )
    if existing.returncode == 0:
        raise ValueError("Container exists; preserve it and use the current dashboard")
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 18088))
    manifest = prepare(root)
    subprocess.run(
        [
            docker,
            "run",
            "-d",
            "--name",
            name,
            "--user",
            f"{os.getuid()}:{os.getgid()}",
            "-p",
            "127.0.0.1:18088:8088",
            "-v",
            f"{Path(root).resolve()}:/app/dev",
            "-e",
            "HOME=/app/dev",
            "-e",
            "SUPERSET_CONFIG_PATH=/app/dev/superset_config.py",
            "-e",
            "SUPERSET_LOAD_EXAMPLES=no",
            "-e",
            "TZ=UTC",
            IMAGE,
            "/bin/sh",
            "-c",
            COMMAND,
        ],
        check=True,
    )
    return manifest


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--root", type=Path, required=True)
    p.add_argument("--docker", default="docker")
    p.add_argument("--name", default="airlake-superset-dev")
    a = p.parse_args()
    start(a.root, a.docker, a.name)
