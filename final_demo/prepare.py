"""Build private deployment packets offline; no AWS calls."""

import argparse
import json
import shutil
import uuid
import zipfile
from pathlib import Path

from clean_api.data import load_evidence
from final_demo.contract import (
    ACCOUNT,
    BUCKET,
    REGION,
    SOURCE_ROOT,
    SOURCES,
    digest,
    encode,
    new_plan,
    uri,
)

JARS = {
    "runtime.jar": "8dd8c90e54ed1a20f2461309d047a255ee56382eb7c0a46162b27a4936fd86dc",
    "aws-bundle.jar": "dd7e49ffa4ac9b215bed846581991259e3aec9bb2fa08d6cec029c46cd66d957",
}
ROOT = Path(__file__).resolve().parents[1]


def policy(p, api=False):
    def statement(actions, resources, **extra):
        return {"Effect": "Allow", "Action": actions, "Resource": resources, **extra}

    log = f"arn:aws:logs:{REGION}:{ACCOUNT}:log-group:" + (
        f"/aws/lambda/{p['api']}" if api else p["log_prefix"] + "/*"
    )
    statements = [
        statement(
            ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
            [log, log + ":*"],
        )
    ]
    if api:
        statements.append(
            statement(
                ["s3:GetObject"],
                f"arn:aws:s3:::{BUCKET}/{p['prefix']}exports/gold.json",
            )
        )
    else:
        read_prefixes = [
            SOURCE_ROOT + s["database"] + ".db/*" for s in SOURCES.values()
        ]
        statements += [
            statement(
                ["s3:GetObject"],
                [
                    f"arn:aws:s3:::{BUCKET}/" + x
                    for x in [*read_prefixes, p["prefix"] + "*"]
                ],
            ),
            statement(
                ["s3:PutObject"],
                [
                    f"arn:aws:s3:::{BUCKET}/{p['prefix']}{suffix}*"
                    for suffix in ("warehouse/", "exports/", "evidence/", "temp/")
                ],
            ),
            statement(
                ["s3:DeleteObject", "s3:AbortMultipartUpload"],
                [
                    f"arn:aws:s3:::{BUCKET}/{p['prefix']}{suffix}*"
                    for suffix in ("warehouse/", "temp/")
                ],
            ),
            statement(
                ["s3:ListBucket"],
                f"arn:aws:s3:::{BUCKET}",
                Condition={
                    "StringLike": {"s3:prefix": [*read_prefixes, p["prefix"] + "*"]}
                },
            ),
            statement(["s3:GetBucketLocation"], f"arn:aws:s3:::{BUCKET}"),
            statement(
                [
                    "glue:GetDatabase",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:GetTableVersion",
                    "glue:GetTableVersions",
                ],
                [
                    f"arn:aws:glue:{REGION}:{ACCOUNT}:catalog",
                    *[
                        f"arn:aws:glue:{REGION}:{ACCOUNT}:database/{s['database']}"
                        for s in SOURCES.values()
                    ],
                    *[
                        f"arn:aws:glue:{REGION}:{ACCOUNT}:table/{s['database']}/{s['table']}"
                        for s in SOURCES.values()
                    ],
                ],
            ),
            statement(
                [
                    "glue:GetDatabase",
                    "glue:CreateDatabase",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:CreateTable",
                    "glue:UpdateTable",
                    "glue:GetTableVersion",
                    "glue:GetTableVersions",
                ],
                [
                    f"arn:aws:glue:{REGION}:{ACCOUNT}:catalog",
                    f"arn:aws:glue:{REGION}:{ACCOUNT}:database/{p['database']}",
                    f"arn:aws:glue:{REGION}:{ACCOUNT}:table/{p['database']}/*",
                ],
            ),
        ]
    return {"Version": "2012-10-17", "Statement": statements}


def trust(service):
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": service + ".amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }


def job_definition(p, sha):
    config = {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.demo": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.demo.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
        "spark.sql.catalog.demo.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "spark.sql.catalog.demo.warehouse": uri(p, "warehouse/"),
        "spark.sql.catalog.demo.client.region": REGION,
        "spark.sql.catalog.demo.table-default.format-version": "2",
        "spark.sql.catalog.demo.table-default.write.format.default": "parquet",
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.shuffle.partitions": "2",
    }
    args = {
        "--plan_uri": uri(p, "artifacts/plan.json"),
        "--plan_sha256": sha,
        "--extra-jars": ",".join(uri(p, "artifacts/" + j) for j in JARS),
        "--extra-py-files": uri(p, "artifacts/payload.zip"),
        "--user-jars-first": "true",
        "--job-bookmark-option": "job-bookmark-disable",
        "--TempDir": uri(p, "temp/"),
        "--custom-logGroup-prefix": p["log_prefix"],
        "--conf": " --conf ".join(f"{k}={v}" for k, v in config.items()),
    }
    return {
        "Name": p["job"],
        "Role": f"arn:aws:iam::{ACCOUNT}:role/{p['glue_role']}",
        "Command": {
            "Name": "glueetl",
            "ScriptLocation": uri(p, "artifacts/glue_job.py"),
            "PythonVersion": "3",
        },
        "GlueVersion": "5.0",
        "WorkerType": "G.1X",
        "NumberOfWorkers": 2,
        "Timeout": 10,
        "ExecutionClass": "STANDARD",
        "MaxRetries": 0,
        "ExecutionProperty": {"MaxConcurrentRuns": 1},
        "NonOverridableArguments": args,
        "Tags": {"Project": "airlake", "Scope": p["run"]},
    }


def prepare(output, evidence, runtime, bundle, run=None):
    output = Path(output).resolve()
    if output.exists() or any((p / ".git").exists() for p in (output, *output.parents)):
        raise ValueError("Use a new private directory outside Git")
    data = load_evidence(evidence)
    for name, source in (("runtime.jar", runtime), ("aws-bundle.jar", bundle)):
        if digest(Path(source).read_bytes()) != JARS[name]:
            raise ValueError("Unverified Iceberg dependency")
    p = new_plan(run or "20261114_" + uuid.uuid4().hex[:8])
    output.mkdir(mode=0o700, parents=True)
    for name, source in (("runtime.jar", runtime), ("aws-bundle.jar", bundle)):
        shutil.copyfile(source, output / name)
    source = json.loads(Path(evidence).read_bytes())
    reference = {
        "facts": source["hourly_rows"],
        "dimensions": source["dimension_rows"],
        "evidence_sha256": data.evidence_sha256,
    }
    (output / "reference.json").write_bytes(encode(reference))
    shutil.copyfile(ROOT / "final_demo/glue_job.py", output / "glue_job.py")
    modules = [
        *sorted((ROOT / "clean_api").glob("*.py")),
        *sorted((ROOT / "final_demo").glob("*.py")),
    ]
    production = [ROOT / "scripts/gold_batch.py", ROOT / "scripts/silver_batch.py"]
    for name in ("payload.zip", "api.zip"):
        with zipfile.ZipFile(
            output / name, "w", compression=zipfile.ZIP_DEFLATED
        ) as archive:
            for file in modules + (production if name == "payload.zip" else []):
                archive.write(file, str(file.relative_to(ROOT)))
            if name == "payload.zip":
                archive.writestr("scripts/__init__.py", "")
    p["production_sha256"] = {
        str(f.relative_to(ROOT)): digest(f.read_bytes()) for f in production
    }
    for name in (
        "runtime.jar",
        "aws-bundle.jar",
        "reference.json",
        "payload.zip",
        "api.zip",
        "glue_job.py",
    ):
        p["artifacts"][name] = digest((output / name).read_bytes())
    (output / "plan.json").write_bytes(encode(p))
    sha = digest(encode(p))
    (output / "plan.sha256").write_text(sha + "\n")
    for name, value in (
        ("glue-policy.json", policy(p)),
        ("api-policy.json", policy(p, True)),
        ("glue-trust.json", trust("glue")),
        ("api-trust.json", trust("lambda")),
        ("job.json", job_definition(p, sha)),
    ):
        (output / name).write_bytes(encode(value))
    invoker = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": "lambda:InvokeFunctionUrl",
                "Resource": f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{p['api']}",
                "Condition": {
                    "StringEquals": {"lambda:FunctionUrlAuthType": "AWS_IAM"}
                },
            },
            {
                "Effect": "Allow",
                "Action": "lambda:InvokeFunction",
                "Resource": f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{p['api']}",
                "Condition": {"Bool": {"lambda:InvokedViaFunctionUrl": "true"}},
            },
        ],
    }
    (output / "api-invoker-policy.json").write_bytes(encode(invoker))
    for file in output.iterdir():
        file.chmod(0o600)
    return p, sha


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    parser.add_argument("--evidence", required=True)
    parser.add_argument("--runtime-jar", required=True)
    parser.add_argument("--aws-bundle", required=True)
    a = parser.parse_args()
    p, sha = prepare(a.output, a.evidence, a.runtime_jar, a.aws_bundle)
    print(
        json.dumps(
            {
                "status": "PREPARED_OFFLINE_ONLY",
                "plan_sha256": sha,
                "run": p["run"],
                "database": p["database"],
                "prefix": uri(p, ""),
                "job": p["job"],
            }
        )
    )


if __name__ == "__main__":
    main()
