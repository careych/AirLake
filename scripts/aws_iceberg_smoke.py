"""Plan an isolated AWS Iceberg probe; cloud execution requires explicit opt-in."""

import argparse
import configparser
import hashlib
import json
import os
import re
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import uuid
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path
from zipfile import BadZipFile, ZipFile

REPOSITORY = Path(__file__).resolve().parents[1]
ACCOUNT = "701364614078"
REGION = "ap-southeast-2"
PROFILE = "airlake"
BUCKET = f"airlake-{ACCOUNT}-lake"
RESULTS = f"s3://airlake-{ACCOUNT}-athena-results/"
CORE_NAME = "iceberg-spark-runtime-3.5_2.12-1.12.0.jar"
CORE_SHA256 = "8dd8c90e54ed1a20f2461309d047a255ee56382eb7c0a46162b27a4936fd86dc"
SDK_VERSION = "2.54.17"
ROWS = [(1, "alpha", 10.0), (2, "beta", 0.0), (3, "gamma", 25.0)]
CORE_CLASSES = (
    "org.apache.iceberg.spark.SparkCatalog",
    "org.apache.iceberg.aws.glue.GlueCatalog",
    "org.apache.iceberg.aws.s3.S3FileIO",
)
AWS_CLASSES = (
    "software.amazon.awssdk.services.glue.GlueClient",
    "software.amazon.awssdk.services.s3.S3Client",
    "software.amazon.awssdk.services.sts.StsClient",
    "software.amazon.awssdk.services.kms.KmsClient",
    "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider",
    "software.amazon.awssdk.auth.credentials.ProcessCredentialsProvider",
    "software.amazon.awssdk.http.apache5.Apache5HttpClient",
    "software.amazon.awssdk.core.util.VersionInfo",
)
LOGIN_CLASS = "software.amazon.awssdk.services.signin.auth.LoginCredentialsProvider"


class SmokeError(Exception):
    """Only fixed diagnostic codes cross the subprocess/logging boundary."""


def plan(run_id=None):
    if run_id is None:
        run_id = datetime.now(UTC).strftime("%Y%m%d") + "_" + uuid.uuid4().hex
    if not re.fullmatch(r"[0-9]{8}_[0-9a-f]{32}", run_id):
        raise SmokeError("invalid_run_id")
    database = f"airlake_smoke_{run_id}"
    prefix = f"integration-smoke/{run_id}/"
    return {
        "run_id": run_id,
        "account": ACCOUNT,
        "region": REGION,
        "profile": PROFILE,
        "database": database,
        "table": "probe",
        "prefix": prefix,
        "warehouse": f"s3://{BUCKET}/{prefix}warehouse/",
        "table_location": f"s3://{BUCKET}/{prefix}warehouse/probe",
        "athena_results": RESULTS,
        "expected_rows": [list(row) for row in ROWS],
        "expected_count": 3,
        "expected_sum": 35.0,
    }


def validate_plan(target):
    if target != plan(target.get("run_id", "")):
        raise SmokeError("unsafe_target")


def validate_environment(env):
    for key in env:
        if key.startswith(("SPARK_", "PYSPARK_", "HADOOP_", "HIVE_", "YARN_")):
            raise SmokeError("inherited_spark_configuration")
        if key in {
            "CLASSPATH",
            "JAVA_TOOL_OPTIONS",
            "JDK_JAVA_OPTIONS",
            "_JAVA_OPTIONS",
            "JAVA_OPTS",
            "PYTHONPATH",
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "ALL_PROXY",
            "http_proxy",
            "https_proxy",
            "all_proxy",
            "BOTO_CONFIG",
            "REQUESTS_CA_BUNDLE",
            "CURL_CA_BUNDLE",
            "SSL_CERT_FILE",
        }:
            raise SmokeError("inherited_runtime_configuration")
        if key.startswith("AWS_") and key not in {
            "AWS_PROFILE",
            "AWS_REGION",
            "AWS_DEFAULT_REGION",
        }:
            raise SmokeError("inherited_aws_configuration")
    for key in ("AWS_REGION", "AWS_DEFAULT_REGION"):
        if env.get(key, REGION) != REGION:
            raise SmokeError("wrong_region")
    if env.get("AWS_PROFILE", PROFILE) != PROFILE:
        raise SmokeError("wrong_profile")


def inspect_jar(path, required, expected_hash):
    if not path.is_file():
        return {"ready": False, "problem": "missing_jar"}
    if "," in str(path) or any(character.isspace() for character in str(path)):
        return {"ready": False, "problem": "unsupported_jar_path"}
    with path.open("rb") as handle:
        digest = hashlib.file_digest(handle, "sha256").hexdigest()
    result = {"sha256": digest, "ready": False}
    if not expected_hash or not re.fullmatch(r"[0-9a-f]{64}", expected_hash):
        return result | {"problem": "trusted_sha256_required"}
    if digest != expected_hash:
        return result | {"problem": "checksum_mismatch"}
    try:
        with ZipFile(path) as jar:
            names = set(jar.namelist())
    except (OSError, BadZipFile):
        return result | {"problem": "invalid_jar"}
    missing = [
        name for name in required if name.replace(".", "/") + ".class" not in names
    ]
    return result | {
        "ready": not missing,
        "problem": "missing_classes" if missing else None,
        "missing_classes": missing,
        "login_provider_present": LOGIN_CLASS.replace(".", "/") + ".class" in names,
    }


def dependencies(args):
    core = inspect_jar(args.runtime_jar, CORE_CLASSES, CORE_SHA256)
    bundle = inspect_jar(args.aws_bundle_jar, AWS_CLASSES, args.aws_bundle_sha256)
    try:
        pyspark_version = version("pyspark")
    except ModuleNotFoundError:
        pyspark_version = None
    return {
        "core": core,
        "aws_bundle": bundle,
        "pyspark": pyspark_version,
        "python_supported": sys.version_info[:2] == (3, 12),
        "java_home_available": (
            Path(os.environ.get("JAVA_HOME", "")) / "bin/java"
        ).is_file(),
        "aws_cli_available": shutil.which("aws") is not None,
    }


def require_dependencies(report, spark_profile):
    if not (
        report["core"]["ready"]
        and report["aws_bundle"]["ready"]
        and report["pyspark"] == "3.5.9"
        and report["python_supported"]
        and report["java_home_available"]
        and report["aws_cli_available"]
    ):
        raise SmokeError("local_dependencies_not_ready")
    if spark_profile == PROFILE and not report["aws_bundle"]["login_provider_present"]:
        raise SmokeError("signin_module_missing_use_reviewed_process_profile")


def validate_profiles(spark_profile, aws_path):
    config = configparser.RawConfigParser()
    config.read(Path.home() / ".aws/config")
    if any(
        config.get(section, "cli_history", fallback="disabled") == "enabled"
        for section in config.sections()
    ):
        raise SmokeError("aws_cli_history_enabled")
    section = f"profile {PROFILE}"
    if not config.has_section(section):
        raise SmokeError("login_profile_missing")
    source = dict(config.items(section))
    if source.get("region") != REGION:
        raise SmokeError("wrong_region")
    if set(source) != {"region", "login_session"} or not source["login_session"]:
        raise SmokeError("login_profile_requires_review")
    credentials = configparser.RawConfigParser()
    credentials.read(Path.home() / ".aws/credentials")
    for name in {PROFILE, spark_profile}:
        if credentials.has_section(name) and dict(credentials.items(name)):
            raise SmokeError("credential_file_profile_not_allowed")
    if spark_profile != PROFILE:
        section = f"profile {spark_profile}"
        if not config.has_section(section):
            raise SmokeError("process_profile_missing")
        process = dict(config.items(section))
        expected = [
            aws_path,
            "configure",
            "export-credentials",
            "--profile",
            PROFILE,
            "--format",
            "process",
        ]
        if (
            set(process) != {"region", "credential_process"}
            or process["region"] != REGION
            or shlex.split(process["credential_process"]) != expected
        ):
            raise SmokeError("process_profile_requires_review")


class Journal:
    def __init__(self, root, target, report):
        self.path = root / "manifest.json"
        self.data = {
            "manifest_version": 1,
            "target": target,
            "dependencies": report,
            "status": "prepared",
            "operations": [],
            "observations": {},
            "athena_queries": [],
            "recovery": "No automatic cloud deletion; inspect attempted resources.",
        }
        self.save()

    def save(self):
        temporary = self.path.with_suffix(".tmp")
        with temporary.open("w", encoding="utf-8") as handle:
            os.chmod(temporary, 0o600)
            json.dump(self.data, handle, indent=2, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(self.path)

    def attempt(self, name, action):
        entry = {"operation": name, "status": "attempted"}
        self.data["operations"].append(entry)
        self.save()
        try:
            result = action()
        except BaseException:
            entry["status"] = "failed_or_uncertain"
            self.data["status"] = "failed_or_uncertain"
            self.save()
            raise
        entry["status"] = "succeeded"
        self.save()
        return result


class AWSCLI:
    """Read-only inventory adapter; raw CLI output/errors are never logged."""

    ALLOWED = frozenset(
        {
            ("sts", "get-caller-identity"),
            ("s3api", "head-bucket"),
            ("s3api", "get-bucket-location"),
            ("s3api", "list-objects-v2"),
            ("s3api", "get-bucket-versioning"),
            ("glue", "get-database"),
            ("glue", "get-table"),
            ("athena", "get-work-group"),
        }
    )

    def __init__(self, executable):
        self.executable = executable

    def call(self, service, operation, *arguments):
        if (service, operation) not in self.ALLOWED:
            raise SmokeError("aws_operation_not_allowed")
        result = subprocess.run(
            [
                self.executable,
                "--profile",
                PROFILE,
                "--region",
                REGION,
                "--no-cli-pager",
                "--output",
                "json",
                "--cli-connect-timeout",
                "10",
                "--cli-read-timeout",
                "20",
                service,
                operation,
                *arguments,
            ],
            capture_output=True,
            text=True,
            timeout=35,
            check=False,
        )
        if result.returncode:
            error = re.search(
                r"An error occurred \(([A-Za-z0-9]+)\) when calling the ([A-Za-z0-9]+) operation",
                result.stderr,
            )
            if (
                service == "glue"
                and operation == "get-database"
                and error
                and error[1] == "EntityNotFoundException"
                and error[2] == "GetDatabase"
            ):
                return None
            raise SmokeError("aws_inventory_failed")
        try:
            return json.loads(result.stdout or "{}")
        except ValueError:
            raise SmokeError("aws_inventory_invalid_json") from None


def assert_unused(aws, target):
    validate_plan(target)
    if (
        aws.call(
            "glue",
            "get-database",
            "--catalog-id",
            ACCOUNT,
            "--name",
            target["database"],
        )
        is not None
    ):
        raise SmokeError("namespace_exists")
    listing = aws.call(
        "s3api",
        "list-objects-v2",
        "--bucket",
        BUCKET,
        "--expected-bucket-owner",
        ACCOUNT,
        "--prefix",
        target["prefix"],
        "--max-keys",
        "1",
        "--no-paginate",
    )
    if not isinstance(listing, dict) or (
        listing.get("Name") != BUCKET
        or listing.get("Prefix") != target["prefix"]
        or type(listing.get("KeyCount")) is not int
        or listing.get("KeyCount") != 0
        or listing.get("IsTruncated") is not False
        or listing.get("Contents")
        or listing.get("CommonPrefixes")
    ):
        raise SmokeError("prefix_not_proven_empty")


def preflight(aws, target):
    validate_plan(target)
    identity = aws.call("sts", "get-caller-identity")
    if identity.get("Account") != ACCOUNT:
        raise SmokeError("wrong_account")
    aws.call(
        "s3api", "head-bucket", "--bucket", BUCKET, "--expected-bucket-owner", ACCOUNT
    )
    location = aws.call(
        "s3api",
        "get-bucket-location",
        "--bucket",
        BUCKET,
        "--expected-bucket-owner",
        ACCOUNT,
    )
    if location.get("LocationConstraint") != REGION:
        raise SmokeError("wrong_bucket_region")
    versioning = aws.call(
        "s3api",
        "get-bucket-versioning",
        "--bucket",
        BUCKET,
        "--expected-bucket-owner",
        ACCOUNT,
    )
    if versioning != {}:
        raise SmokeError("versioned_bucket_requires_review")
    group = aws.call("athena", "get-work-group", "--work-group", "airlake").get(
        "WorkGroup", {}
    )
    conf = group.get("Configuration", {})
    if (
        group.get("State") != "ENABLED"
        or conf.get("EngineVersion", {}).get("EffectiveEngineVersion")
        != "Athena engine version 3"
        or conf.get("EnforceWorkGroupConfiguration") is not True
        or conf.get("ResultConfiguration", {}).get("OutputLocation") != RESULTS
        or conf.get("BytesScannedCutoffPerQuery") != 10737418240
    ):
        raise SmokeError("workgroup_configuration_changed")
    assert_unused(aws, target)


def spark_settings(root, target, jars):
    validate_plan(target)
    return {
        "spark.jars": ",".join(str(p) for p in jars),
        "spark.jars.packages": "",
        "spark.jars.ivy": str(root / "ivy"),
        "spark.ui.enabled": "false",
        "spark.eventLog.enabled": "false",
        "spark.sql.catalogImplementation": "in-memory",
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.defaultCatalog": "cloudsmoke",
        "spark.sql.warehouse.dir": (root / "spark-warehouse").as_uri(),
        "spark.local.dir": str(root / "spill"),
        "spark.hadoop.hadoop.tmp.dir": str(root / "hadoop"),
        "spark.hadoop.fs.defaultFS": "file:///",
        "spark.driver.host": "127.0.0.1",
        "spark.driver.bindAddress": "127.0.0.1",
        "spark.task.maxFailures": "1",
        "spark.stage.maxConsecutiveAttempts": "1",
        "spark.speculation": "false",
        "spark.sql.shuffle.partitions": "1",
        "spark.driver.extraJavaOptions": f"-Djava.io.tmpdir={root}/tmp -Dderby.system.home={root}/derby -Daws.maxAttempts=1",
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.local": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.local.type": "hadoop",
        "spark.sql.catalog.local.warehouse": (root / "unused-local").as_uri(),
        "spark.sql.catalog.cloudsmoke": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.cloudsmoke.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
        "spark.sql.catalog.cloudsmoke.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "spark.sql.catalog.cloudsmoke.warehouse": target["warehouse"],
        "spark.sql.catalog.cloudsmoke.glue.id": ACCOUNT,
        "spark.sql.catalog.cloudsmoke.client.region": REGION,
        "spark.sql.catalog.cloudsmoke.client.credentials-provider": "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider",
        "spark.sql.catalog.cloudsmoke.http-client.type": "apache",
        "spark.sql.catalog.cloudsmoke.s3.retry.num-retries": "0",
        "spark.sql.catalog.cloudsmoke.s3.crt.enabled": "false",
        "spark.sql.catalog.cloudsmoke.s3.analytics-accelerator.enabled": "false",
        "spark.sql.catalog.cloudsmoke.s3.staging-dir": str(root / "staging"),
        "spark.sql.catalog.cloudsmoke.table-default.commit.retry.num-retries": "0",
        "spark.sql.catalog.cloudsmoke.table-default.write.metadata.delete-after-commit.enabled": "false",
    }


def run_probe(spark, aws, target, journal):
    # Recheck after JVM startup; CREATE has no IF NOT EXISTS or overwrite path.
    journal.attempt("recheck_unused_targets", lambda: assert_unused(aws, target))
    namespace = f"cloudsmoke.{target['database']}"
    table = f"{namespace}.probe"
    journal.attempt(
        "create_namespace", lambda: spark.sql(f"CREATE NAMESPACE {namespace}")
    )
    journal.attempt(
        "create_table",
        lambda: spark.sql(
            f"CREATE TABLE {table} (id INT, label STRING, pm25 DOUBLE) USING iceberg "
            f"LOCATION '{target['table_location']}' TBLPROPERTIES "
            "('format-version'='2', 'write.format.default'='parquet', "
            "'commit.retry.num-retries'='0', 'write.metadata.delete-after-commit.enabled'='false')"
        ),
    )
    journal.attempt(
        "append_three_rows",
        lambda: (
            spark.createDataFrame(ROWS, "id INT, label STRING, pm25 DOUBLE")
            .coalesce(1)
            .writeTo(table)
            .append()
        ),
    )

    def verify():
        frame = spark.table(table)
        if [(f.name, f.dataType.simpleString()) for f in frame.schema.fields] != [
            ("id", "int"),
            ("label", "string"),
            ("pm25", "double"),
        ]:
            raise SmokeError("schema_mismatch")
        rows = [tuple(row) for row in frame.orderBy("id").collect()]
        if rows != ROWS or len(rows) != 3 or sum(row[2] for row in rows) != 35.0:
            raise SmokeError("row_mismatch")
        snapshots = (
            spark.table(f"{table}.snapshots")
            .select("snapshot_id", "operation")
            .collect()
        )
        files = spark.table(f"{table}.files").select("file_path").collect()
        if (
            len(snapshots) != 1
            or snapshots[0].operation != "append"
            or not files
            or any(
                not r.file_path.startswith(target["table_location"] + "/")
                for r in files
            )
        ):
            raise SmokeError("snapshot_or_file_scope_mismatch")
        registered = aws.call(
            "glue",
            "get-table",
            "--catalog-id",
            ACCOUNT,
            "--database-name",
            target["database"],
            "--name",
            "probe",
        )
        metadata = (
            registered.get("Table", {})
            .get("Parameters", {})
            .get("metadata_location", "")
        )
        if not metadata.startswith(target["table_location"] + "/metadata/"):
            raise SmokeError("glue_metadata_outside_scope")
        iceberg = spark.sparkContext._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
            spark._jsparkSession, table
        )
        if (
            iceberg.location() != target["table_location"]
            or iceberg.operations().current().formatVersion() != 2
            or iceberg.properties().get("write.format.default") != "parquet"
        ):
            raise SmokeError("iceberg_properties_mismatch")
        journal.data["observations"] = {
            "rows": [list(row) for row in rows],
            "count": 3,
            "pm25_sum": 35.0,
            "schema": [
                [f.name, f.dataType.simpleString()] for f in frame.schema.fields
            ],
            "snapshot_ids": [int(r.snapshot_id) for r in snapshots],
            "data_files": [r.file_path for r in files],
            "metadata_location": metadata,
            "format_version": 2,
        }

    journal.attempt("verify_spark_and_glue", verify)
    journal.data["status"] = "spark_glue_passed_athena_not_executed"
    journal.save()


def verify_spark_settings(spark, settings):
    conf = spark.sparkContext.getConf()
    jvm = spark.sparkContext._jvm
    for key, expected in settings.items():
        actual = conf.get(key)
        if actual == expected:
            continue
        if key == "spark.driver.extraJavaOptions" and isinstance(actual, str):
            defaults = (
                jvm.org.apache.spark.launcher.JavaModuleOptions.defaultModuleOptions()
            )
            # SparkSubmit adds its IPv6 preference and standard module options.
            prefix = shlex.split(defaults)
            probe = shlex.split(expected)
            if shlex.split(actual) in (
                prefix + probe,
                ["-Djava.net.preferIPv6Addresses=false"] + prefix + probe,
            ):
                continue
        raise SmokeError("spark_configuration_mismatch:" + key)
    for option in shlex.split(settings["spark.driver.extraJavaOptions"]):
        name, expected = option.removeprefix("-D").split("=", 1)
        if jvm.java.lang.System.getProperty(name) != expected:
            raise SmokeError("effective_jvm_property_mismatch:" + name)


def worker(root, target, args, journal, aws):
    from pyspark import SparkContext
    from pyspark.sql import SparkSession

    if (
        SparkContext._active_spark_context is not None
        or SparkSession.getActiveSession()
    ):
        raise SmokeError("existing_spark_session")
    settings = spark_settings(root, target, (args.runtime_jar, args.aws_bundle_jar))
    builder = SparkSession.builder.master("local[1]").appName(
        "AirLake-AWS-Isolated-Smoke"
    )
    for key, value in settings.items():
        builder = builder.config(key, value)
    spark = journal.attempt("start_isolated_spark", builder.getOrCreate)
    try:
        spark.sparkContext.setLogLevel("OFF")
        if spark.sparkContext.master != "local[1]":
            raise SmokeError("unsafe_spark_master")
        verify_spark_settings(spark, settings)
        jvm = spark.sparkContext._jvm
        loader = jvm.java.lang.Thread.currentThread().getContextClassLoader()
        for jar, names in (
            (args.runtime_jar, CORE_CLASSES),
            (
                args.aws_bundle_jar,
                AWS_CLASSES + ((LOGIN_CLASS,) if args.spark_profile == PROFILE else ()),
            ),
        ):
            for name in names:
                location = (
                    loader.loadClass(name)
                    .getProtectionDomain()
                    .getCodeSource()
                    .getLocation()
                )
                if (
                    location.getProtocol() != "file"
                    or Path(location.toURI().getPath()).resolve() != jar
                ):
                    raise SmokeError("unexpected_loaded_jar")
        loaded = {
            "java": jvm.java.lang.System.getProperty("java.specification.version"),
            "iceberg": jvm.org.apache.iceberg.IcebergBuild.version(),
            "aws_sdk": jvm.software.amazon.awssdk.core.util.VersionInfo.SDK_VERSION,
            "scala": jvm.scala.util.Properties.versionNumberString(),
        }
        if loaded != {
            "java": "17",
            "iceberg": "1.12.0",
            "aws_sdk": SDK_VERSION,
            "scala": "2.12.18",
        }:
            raise SmokeError("loaded_runtime_version_mismatch")
        journal.data["loaded_versions"] = loaded
        journal.save()

        def verify_java_identity():
            provider = jvm.software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider.create()
            client = (
                jvm.software.amazon.awssdk.services.sts.StsClient.builder()
                .region(jvm.software.amazon.awssdk.regions.Region.of(REGION))
                .credentialsProvider(provider)
                .build()
            )
            try:
                if client.getCallerIdentity().account() != ACCOUNT:
                    raise SmokeError("wrong_java_account")
            finally:
                client.close()
                provider.close()

        journal.attempt("verify_java_identity", verify_java_identity)
        run_probe(spark, aws, target, journal)
    finally:
        spark.stop()


def child_environment(root, spark_profile):
    env = {
        k: os.environ[k]
        for k in (
            "PATH",
            "HOME",
            "JAVA_HOME",
            "LANG",
            "LC_ALL",
            "LC_CTYPE",
            "__CF_USER_TEXT_ENCODING",
        )
        if k in os.environ
    }
    env.update(
        {
            "AWS_PROFILE": spark_profile,
            "AWS_REGION": REGION,
            "AWS_DEFAULT_REGION": REGION,
            "AWS_EC2_METADATA_DISABLED": "true",
            "AWS_MAX_ATTEMPTS": "1",
            "AWS_PAGER": "",
            "SPARK_CONF_DIR": str(root / "conf"),
            "SPARK_LOCAL_IP": "127.0.0.1",
            "SPARK_LOCAL_HOSTNAME": "localhost",
            "SPARK_LOCAL_DIRS": str(root / "spill"),
            "SPARK_LOG_DIR": str(root / "logs"),
            "TMPDIR": str(root / "tmp"),
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYSPARK_PYTHON": sys.executable,
            "PYSPARK_DRIVER_PYTHON": sys.executable,
            "PYSPARK_SUBMIT_ARGS": "--master local[1] pyspark-shell",
        }
    )
    return env


def execute(args, target, report):
    validate_environment(os.environ)
    require_dependencies(report, args.spark_profile)
    aws_path = shutil.which("aws")
    validate_profiles(args.spark_profile, aws_path)
    base = Path(tempfile.gettempdir()).resolve()
    if base.is_relative_to(REPOSITORY):
        raise SmokeError("unsafe_temporary_base")
    root = Path(tempfile.mkdtemp(prefix="airlake-aws-smoke-", dir=base)).resolve()
    if any(character.isspace() for character in str(root)):
        raise SmokeError("unsupported_temporary_path")
    for name in ("conf", "tmp", "spill", "hadoop", "staging", "logs"):
        (root / name).mkdir(mode=0o700)
    journal = Journal(root, target, report)
    print(f"Sanitized manifest retained at: {journal.path}", flush=True)
    command = [
        sys.executable,
        "-B",
        str(Path(__file__).resolve()),
        "--execute",
        "--confirm-account",
        ACCOUNT,
        "--worker-root",
        str(root),
        "--runtime-jar",
        str(args.runtime_jar),
        "--aws-bundle-jar",
        str(args.aws_bundle_jar),
        "--aws-bundle-sha256",
        args.aws_bundle_sha256,
        "--spark-profile",
        args.spark_profile,
    ]
    # Raw JVM/SDK diagnostics can contain sensitive text; retain only journal fields.
    child = subprocess.Popen(
        command,
        cwd=root,
        env=child_environment(root, args.spark_profile),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    try:
        code = child.wait(timeout=300)
    except (subprocess.TimeoutExpired, KeyboardInterrupt):
        try:
            os.killpg(child.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass
        try:
            os.killpg(child.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        child.wait()
        code = 1
    journal.data = json.loads(journal.path.read_text(encoding="utf-8"))
    if journal.data["status"] != "spark_glue_passed_athena_not_executed":
        code = code or 1
    if code:
        journal.data["status"] = "failed_or_uncertain"
        journal.save()
    print(f"Result: {journal.data['status']}; no automatic cleanup performed.")
    return code


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--confirm-account")
    parser.add_argument("--region", default=REGION)
    parser.add_argument(
        "--plan-file",
        type=Path,
        help="Use a reviewed dry-run target JSON so execution matches approved resources.",
    )
    parser.add_argument(
        "--spark-profile", choices=(PROFILE, "airlake-spark"), default=PROFILE
    )
    parser.add_argument(
        "--runtime-jar",
        type=lambda s: Path(s).expanduser().resolve(),
        default=Path(
            os.environ.get(
                "AIRLAKE_ICEBERG_RUNTIME_JAR",
                str(Path.home() / ".ivy2/jars" / f"org.apache.iceberg_{CORE_NAME}"),
            )
        )
        .expanduser()
        .resolve(),
    )
    parser.add_argument(
        "--aws-bundle-jar",
        type=lambda s: Path(s).expanduser().resolve(),
        default=Path(
            os.environ.get(
                "AIRLAKE_ICEBERG_AWS_BUNDLE_JAR", "iceberg-aws-bundle-1.12.0.jar"
            )
        )
        .expanduser()
        .resolve(),
    )
    parser.add_argument(
        "--aws-bundle-sha256",
        default=os.environ.get("AIRLAKE_ICEBERG_AWS_BUNDLE_SHA256", ""),
    )
    parser.add_argument("--worker-root", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    try:
        if args.region != REGION:
            raise SmokeError("wrong_region")
        if args.execute and args.confirm_account != ACCOUNT:
            raise SmokeError("explicit_account_confirmation_required")
        if args.worker_root is not None:
            if not args.execute:
                raise SmokeError("worker_requires_execute")
            root = args.worker_root.resolve()
            if (
                root.is_relative_to(REPOSITORY)
                or Path.cwd().resolve() != root
                or not root.name.startswith("airlake-aws-smoke-")
                or os.environ.get("SPARK_CONF_DIR") != str(root / "conf")
            ):
                raise SmokeError("unsafe_worker_directory")
            if dict(os.environ) != child_environment(root, args.spark_profile):
                raise SmokeError("unexpected_worker_environment")
            data = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
            target = data["target"]
            validate_plan(target)
            report = dependencies(args)
            require_dependencies(report, args.spark_profile)
            validate_profiles(args.spark_profile, shutil.which("aws"))
            aws = AWSCLI(shutil.which("aws"))
            journal = Journal(root, target, report)
            try:
                journal.attempt(
                    "aws_read_only_preflight", lambda: preflight(aws, target)
                )
                worker(root, target, args, journal, aws)
                return 0
            except BaseException as error:  # noqa: BLE001 -- retain scope, never raw SDK diagnostics
                journal.data["status"] = "failed_or_uncertain"
                journal.data["failure_code"] = (
                    str(error)
                    if isinstance(error, SmokeError)
                    else "worker_operation_failed"
                )
                journal.save()
                return 1
        target = (
            json.loads(args.plan_file.read_text(encoding="utf-8"))
            if args.plan_file is not None
            else plan()
        )
        validate_plan(target)
        report = dependencies(args)
        if not args.execute:
            try:
                require_dependencies(report, args.spark_profile)
                gate = "local_checks_only_cloud_and_java_auth_not_verified"
            except SmokeError as error:
                gate = str(error)
            print(
                json.dumps(
                    {
                        "mode": "dry_run_no_aws_no_spark",
                        "target": target,
                        "dependencies": report,
                        "spark_profile": args.spark_profile,
                        "execution_gate": gate,
                        "future_execution_command": (
                            ".venv/bin/python -B scripts/aws_iceberg_smoke.py "
                            f"--execute --confirm-account {ACCOUNT} --region {REGION} "
                            f"--spark-profile {args.spark_profile}"
                            + (
                                f" --plan-file {shlex.quote(str(args.plan_file))}"
                                if args.plan_file
                                else ""
                            )
                        ),
                        "future_operations": [
                            "read-only identity/bucket/Glue/workgroup checks",
                            "fresh local Spark; create disposable Glue namespace/table",
                            "append exactly three rows; Spark/Glue verification",
                            "retain manifest; Athena queries and cleanup are manual",
                        ],
                    },
                    indent=2,
                )
            )
            return 0
        return execute(args, target, report)
    except Exception as error:  # noqa: BLE001 -- CLI boundary must not expose credential-bearing errors
        code = str(error) if isinstance(error, SmokeError) else "local_check_failed"
        print(f"BLOCKED: {code}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
