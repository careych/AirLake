"""Run the historical API offline, or explicitly verify it with two SELECTs."""

import argparse
import json

from clean_api.data import load_evidence
from clean_api.server import make_server


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    serve = sub.add_parser("serve", help="Serve verified retained data; no AWS calls")
    serve.add_argument("--evidence", required=True)
    serve.add_argument("--port", type=int, default=18090)
    smoke = sub.add_parser("smoke", help="Check the running API; no AWS calls")
    smoke.add_argument("--evidence", required=True)
    smoke.add_argument("--base-url", default="http://127.0.0.1:18090")
    verify = sub.add_parser("verify", help="Opt-in: at most two scoped Athena SELECTs")
    verify.add_argument("--evidence", required=True)
    verify.add_argument("--base-url", default="http://127.0.0.1:18090")
    verify.add_argument("--approve-selects", type=int, choices=[2], required=True)
    verify.add_argument("--report", required=True)
    args = parser.parse_args(argv)
    try:
        dataset = load_evidence(args.evidence)
    except (OSError, ValueError, KeyError, TypeError):
        parser.exit(
            1,
            "STOP: missing, invalid or unapproved acceptance evidence; no AWS calls\n",
        )
    if args.command == "serve":
        try:
            server = make_server(dataset, args.port)
        except OSError:
            parser.exit(1, "STOP: localhost port unavailable; no service was stopped\n")
        print(
            json.dumps(
                {
                    "status": "READY",
                    "url": f"http://127.0.0.1:{server.server_port}",
                    "hourly_rows": len(dataset.rows),
                    "stations": len(dataset.stations),
                    "source": "hash-verified retained real cloud acceptance",
                    "aws_calls": 0,
                }
            ),
            flush=True,
        )
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            server.server_close()
        return 0
    from clean_api.verify import private_report, sdk_clients
    from clean_api.verify import smoke as run_smoke
    from clean_api.verify import verify as run_verify

    if args.command == "smoke":
        try:
            print(json.dumps(run_smoke(dataset, args.base_url)))
        except Exception:  # noqa: BLE001 -- do not expose private endpoint/response details
            parser.exit(1, "STOP: local API smoke check failed; no AWS calls\n")
        return 0
    try:
        report, save = private_report(args.report)
    except (OSError, ValueError):
        parser.exit(
            1, "STOP: report must be new, outside Git, in an owned 0700 directory\n"
        )
    try:
        client, identity = sdk_clients()
        report["phase"] = "identity_and_api_preflight"
        save()
        run_verify(
            dataset,
            args.base_url,
            client,
            identity,
            report,
            save,
            approved_selects=args.approve_selects,
        )
    except Exception as error:  # noqa: BLE001 -- sanitize SDK failures and retain ambiguous query evidence
        report.update(status="FAILED_OR_UNCERTAIN", exception_type=type(error).__name__)
        save()
        print(
            "STOP: verification failed; inspect the sanitized private report; no automatic retry"
        )
        return 1
    print(
        json.dumps(
            {
                "status": "PASS",
                "athena_selects": report["athena_selects_attempted"],
                "hourly_rows": report["hourly_rows"],
                "stations": report["stations"],
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
