"""Amendment-gated recovery after the run-2 smoke check's SigV4 query failure.

`historical/cloud.py` pins its own hash in the frozen plan, so the recovery
commands live here and reuse its frozen loader unchanged.
"""

import argparse
import json
from pathlib import Path

from historical import publish
from historical.cloud import clients, load


def main():
    a = argparse.ArgumentParser(description=__doc__)
    a.add_argument("command", choices=("amend", "smoke-api-resume", "publish-site"))
    a.add_argument("--packet", type=Path, required=True)
    a.add_argument("--confirm-plan-sha256", required=True)
    a.add_argument("--confirm-amendment-sha256")
    x = a.parse_args()
    p, h = load(x.packet)
    if x.confirm_plan_sha256 != h:
        a.exit(1, "STOP: new bounded historical AWS approval required\n")
    if x.command == "amend":  # local only: freezes the bounded recovery for approval
        print(json.dumps({"amendment_sha256": publish.prepare_amendment(x.packet, h)}))
        return
    amendment = publish.load_amendment(x.packet, h, x.confirm_amendment_sha256)
    c = clients()
    if x.command == "smoke-api-resume":
        print(json.dumps(publish.smoke_api_resume(c, p, x.packet, h, amendment)))
        return
    c["amplify"] = __import__("citizen.hosting", fromlist=["clients"]).clients()[
        "amplify"
    ]
    print(json.dumps(publish.publish_site(c, p, x.packet, h, amendment)))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:  # noqa: BLE001 -- never expose SDK payloads, private paths or signed URLs
        print(
            json.dumps(
                {
                    "status": "STOP_FAILED_OR_UNCERTAIN",
                    "exception_type": type(e).__name__,
                }
            )
        )
        raise SystemExit(1) from None
