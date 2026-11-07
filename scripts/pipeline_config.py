"""Catalog and identifier wiring; no data transformations or import-time I/O."""

import argparse
import hashlib
import importlib.util
import json
import os
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

KAFKA_JARS = {
    "org.apache.spark_spark-sql-kafka-0-10_2.12-3.5.9.jar": (
        "fed393dcc3ef70611fe3d4f35ae78a8ea7c55301fd4cdd66c5dc97fcfaa5a00f",
        "org/apache/spark/sql/kafka010/KafkaSourceProvider.class",
    ),
    "org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.5.9.jar": (
        "e82612e6070438c5ea48b08c74f367a6d214a390bdf6dcf3ff316dca592ddda5",
        "org/apache/spark/kafka010/KafkaTokenUtil.class",
    ),
    "org.apache.kafka_kafka-clients-3.4.1.jar": (
        "9d0a9058c8ede79db6e54ae378ed16fe8d1bead2894635a4aa6aa0b7981668c6",
        "org/apache/kafka/clients/consumer/KafkaConsumer.class",
    ),
    "org.apache.commons_commons-pool2-2.11.1.jar": (
        "ea0505ee7515e58b1ac0e686e4d1a5d9f7d808e251a61bc371aa0595b9963f83",
        "org/apache/commons/pool2/impl/GenericObjectPool.class",
    ),
    "com.google.code.findbugs_jsr305-3.0.0.jar": (
        "bec0b24dcb23f9670172724826584802b80ae6cbdaba03bdebdef9327b962f6a",
        "javax/annotation/Nullable.class",
    ),
}


def verified_cloud_jars():
    """Reuse the accepted local Kafka set plus the verified Iceberg AWS bundle."""
    runtime = Path(os.environ.get("AIRLAKE_ICEBERG_RUNTIME_JAR", ""))
    bundle = Path(os.environ.get("AIRLAKE_ICEBERG_AWS_BUNDLE_JAR", ""))
    if runtime == bundle:
        raise ValueError("Two verified Iceberg JARs are required")
    expected = {
        runtime: (
            "8dd8c90e54ed1a20f2461309d047a255ee56382eb7c0a46162b27a4936fd86dc",
            "org/apache/iceberg/aws/glue/GlueCatalog.class",
        ),
        bundle: (
            "dd7e49ffa4ac9b215bed846581991259e3aec9bb2fa08d6cec029c46cd66d957",
            "software/amazon/awssdk/auth/credentials/ProcessCredentialsProvider.class",
        ),
        **{
            Path.home() / ".ivy2/jars" / name: value
            for name, value in KAFKA_JARS.items()
        },
    }
    for jar, (digest, required_class) in expected.items():
        if not jar.is_file() or hashlib.sha256(jar.read_bytes()).hexdigest() != digest:
            raise ValueError("Missing or unverified cloud JAR: " + jar.name)
        with zipfile.ZipFile(jar) as archive:
            if required_class not in archive.namelist():
                raise ValueError(
                    "Required dependency class is missing: " + required_class
                )
    watched = {value[1]: value[0] for value in expected.values()}
    watched.update(
        {
            name: expected[bundle][0]
            for name in (
                "software/amazon/awssdk/services/s3/S3Client.class",
                "software/amazon/awssdk/services/glue/GlueClient.class",
                "software/amazon/awssdk/services/sts/StsClient.class",
            )
        }
    )
    bundled = set()
    spark_jars = Path(importlib.util.find_spec("pyspark").origin).parent / "jars"
    for jar in spark_jars.glob("*.jar"):
        with zipfile.ZipFile(jar) as archive:
            overlap = set(archive.namelist()) & watched.keys()
        if overlap:
            digest = hashlib.sha256(jar.read_bytes()).hexdigest()
            if any(watched[name] != digest for name in overlap):
                raise ValueError(
                    "Conflicting dependency in Spark distribution: " + jar.name
                )
            bundled.add(digest)
    return [
        jar.resolve() for jar, (digest, _) in expected.items() if digest not in bundled
    ]


def verify_java_identity(spark, config):
    """Read-only identity boundary; never expose credential-process output."""
    jvm = spark.sparkContext._jvm
    if jvm.java.lang.System.getProperty("aws.profile") != config.profile:
        raise ValueError("Effective Java AWS profile differs")
    sts = (
        jvm.software.amazon.awssdk.services.sts.StsClient.builder()
        .region(jvm.software.amazon.awssdk.regions.Region.of(config.region))
        .credentialsProvider(
            jvm.software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider.create(
                config.profile
            )
        )
        .build()
    )
    try:
        request = jvm.software.amazon.awssdk.services.sts.model.GetCallerIdentityRequest.builder().build()
        if sts.getCallerIdentity(request).account() != config.account:
            raise ValueError("Java AWS account differs from confirmation")
    finally:
        sts.close()


def identifier(value):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError("Catalog, namespace and table names must be SQL identifiers")
    return value


@dataclass(frozen=True)
class CatalogConfig:
    mode: str = "hadoop"
    catalog: str = "local"
    warehouse: str = "./warehouse"
    bronze_namespace: str = "bronze"
    silver_namespace: str = "silver"
    gold_namespace: str = "gold"
    bronze_table: str = "aq_measurements"
    silver_table: str = "aq_measurements"
    breakpoint_table: str = "aqi_breakpoints"
    dimension_table: str = "dim_location"
    hourly_table: str = "pm25_aqi_hourly"
    region: str = "ap-southeast-2"
    account: str = ""
    profile: str = "airlake-spark"
    allow_cloud_write: bool = False
    confirm_account: str = ""

    def validate(self):
        if self.mode not in ("hadoop", "glue"):
            raise ValueError("Unknown catalog mode")
        for name in (
            "catalog",
            "bronze_namespace",
            "silver_namespace",
            "gold_namespace",
            "bronze_table",
            "silver_table",
            "breakpoint_table",
            "dimension_table",
            "hourly_table",
        ):
            identifier(getattr(self, name))
        if self.catalog.lower() in ("spark_catalog", "global_temp"):
            raise ValueError("Catalog name is reserved by Spark")
        targets = [
            (self.bronze_namespace, self.bronze_table),
            (self.silver_namespace, self.silver_table),
            (self.silver_namespace, self.breakpoint_table),
            (self.gold_namespace, self.dimension_table),
            (self.gold_namespace, self.hourly_table),
        ]
        if (
            len({(namespace.lower(), table.lower()) for namespace, table in targets})
            != 5
        ):
            raise ValueError("All five pipeline table identifiers must be distinct")
        uri = urlparse(self.warehouse)
        if self.mode == "hadoop":
            if uri.scheme not in ("", "file") or uri.netloc not in ("", "localhost"):
                raise ValueError("Hadoop mode requires a local warehouse")
            if not self.warehouse.strip():
                raise ValueError("Warehouse must not be blank")
        else:
            if self.region != "ap-southeast-2" or not re.fullmatch(
                r"\d{12}", self.account
            ):
                raise ValueError("Glue requires an explicit account and Sydney region")
            if uri.scheme != "s3" or not uri.netloc or uri.query or uri.fragment:
                raise ValueError("Glue requires an explicit S3 warehouse")
            if not uri.path.startswith("/integration-real/") or ".." in uri.path.split(
                "/"
            ):
                raise ValueError(
                    "First real-data cloud runs require an integration-real prefix"
                )
            if any(
                not getattr(self, layer + "_namespace").startswith("airlake_it2_")
                for layer in ("bronze", "silver", "gold")
            ):
                raise ValueError(
                    "First cloud runs require disposable airlake_it2_ namespaces"
                )
            if (
                len({self.bronze_namespace, self.silver_namespace, self.gold_namespace})
                != 3
            ):
                raise ValueError("Cloud layer namespaces must be distinct")
            if not re.fullmatch(r"[A-Za-z0-9_-]+", self.profile):
                raise ValueError("Invalid AWS profile name")
        return self

    def tables(self):
        self.validate()
        layers = {
            "BRONZE_TABLE": "bronze",
            "SILVER_TABLE": "silver",
            "BREAKPOINT_TABLE": "silver",
            "DIMENSION_TABLE": "gold",
            "HOURLY_TABLE": "gold",
        }
        return {
            key: f"{self.catalog}.{getattr(self, layer + '_namespace')}."
            f"{getattr(self, key.lower())}"
            for key, layer in layers.items()
        }

    def bind(self, namespace):
        for key, value in self.tables().items():
            if key in namespace:
                namespace[key] = value

    def settings(self):
        self.validate()
        prefix = f"spark.sql.catalog.{self.catalog}"
        settings = {
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.sql.session.timeZone": "UTC",
            prefix: "org.apache.iceberg.spark.SparkCatalog",
            prefix + ".warehouse": self.warehouse,
        }
        if self.mode == "hadoop":
            settings[prefix + ".type"] = "hadoop"
        else:
            settings.update(
                {
                    prefix + ".catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
                    prefix + ".io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
                    prefix + ".glue.id": self.account,
                    prefix + ".client.region": self.region,
                    prefix
                    + ".client.credentials-provider": "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider",
                    prefix + ".http-client.type": "apache",
                    prefix + ".s3.retry.num-retries": "0",
                    prefix + ".s3.crt.enabled": "false",
                    prefix + ".s3.analytics-accelerator.enabled": "false",
                    prefix + ".table-default.commit.retry.num-retries": "0",
                    prefix + ".table-default.format-version": "2",
                    prefix + ".table-default.write.format.default": "parquet",
                    "spark.master": "local[1]",
                    "spark.sql.catalogImplementation": "in-memory",
                    "spark.hadoop.fs.defaultFS": "file:///",
                    "spark.jars.packages": "",
                    "spark.driver.extraJavaOptions": f"-Daws.profile={self.profile} -Daws.region={self.region} -Daws.maxAttempts=1",
                }
            )
        return settings

    def create_spark(self, app_name, extra=None):
        self.validate()
        if self.mode == "glue":
            if not self.allow_cloud_write or self.confirm_account != self.account:
                raise ValueError(
                    "Cloud execution requires --allow-cloud-write and --confirm-account"
                )
            unsafe = [
                k
                for k in os.environ
                if k.startswith("AWS_ENDPOINT_URL")
                or k
                in (
                    "AWS_ACCESS_KEY_ID",
                    "AWS_SECRET_ACCESS_KEY",
                    "AWS_SESSION_TOKEN",
                    "SPARK_CONF_DIR",
                    "HADOOP_CONF_DIR",
                    "PYSPARK_GATEWAY_PORT",
                    "PYSPARK_GATEWAY_SECRET",
                    "SPARK_REMOTE",
                    "SPARK_CONNECT_MODE_ENABLED",
                    "SPARK_MASTER",
                    "MASTER",
                    "SPARK_DIST_CLASSPATH",
                    "HADOOP_CLASSPATH",
                    "PYSPARK_SUBMIT_ARGS",
                    "SPARK_SUBMIT_OPTS",
                    "JAVA_TOOL_OPTIONS",
                    "_JAVA_OPTIONS",
                )
            ]
            if unsafe:
                raise ValueError(
                    "Unsafe inherited settings: " + ",".join(sorted(unsafe))
                )
            jars = verified_cloud_jars()
            root = Path.cwd().resolve()
            repository = Path(__file__).resolve().parents[1]
            if root.is_relative_to(repository) or not root.name.startswith(
                "airlake-it2-cloud-"
            ):
                raise ValueError(
                    "Cloud launch requires a fresh external airlake-it2-cloud- directory"
                )
            if any(
                (root / name).exists()
                for name in ("metastore_db", "derby.log", "warehouse", "checkpoints")
            ):
                raise ValueError(
                    "Cloud working directory contains existing runtime state"
                )
        from pyspark import SparkContext
        from pyspark.sql import SparkSession

        if SparkContext._active_spark_context is not None:
            raise ValueError("Start pipeline entrypoints in a fresh Spark process")
        builder = SparkSession.builder.appName(app_name)
        settings = self.settings()
        for key, value in (extra or {}).items():
            if key in settings and settings[key] != value:
                raise ValueError(
                    "Extra Spark settings cannot redirect the configured catalog"
                )
            settings[key] = value
        if self.mode == "glue":
            settings["spark.jars"] = ",".join(map(str, jars))
            for name in ("tmp", "spill", "hadoop", "staging"):
                (root / name).mkdir(exist_ok=True)
            settings.update(
                {
                    "spark.sql.warehouse.dir": str(root / "spark-warehouse"),
                    "spark.local.dir": str(root / "spill"),
                    "spark.hadoop.hadoop.tmp.dir": str(root / "hadoop"),
                    f"spark.sql.catalog.{self.catalog}.s3.staging-dir": str(
                        root / "staging"
                    ),
                }
            )
            settings["spark.driver.extraJavaOptions"] += (
                f" -Djava.io.tmpdir={root / 'tmp'} -Dderby.system.home={root / 'derby'}"
            )
        for key, value in settings.items():
            builder = builder.config(key, value)
        spark = builder.getOrCreate()
        try:
            for key, value in settings.items():
                if (
                    key != "spark.driver.extraJavaOptions"
                    and spark.sparkContext.getConf().get(key) != value
                ):
                    raise ValueError(
                        "Effective Spark configuration differs from the reviewed plan"
                    )
            if self.mode == "glue":
                if any(
                    k.startswith("spark.sql.catalog.") and k not in settings
                    for k, _ in spark.sparkContext.getConf().getAll()
                ):
                    raise ValueError("Unreviewed inherited Iceberg catalog settings")
                verify_java_identity(spark, self)
        except BaseException as exc:
            spark.stop()
            if self.mode == "glue" and isinstance(exc, Exception):
                raise RuntimeError(
                    "Cloud session/identity preflight failed: " + type(exc).__name__
                ) from None
            raise
        spark.sparkContext.setLogLevel("WARN")
        return spark


def parser(description):
    result = argparse.ArgumentParser(description=description)
    for name, field in CatalogConfig.__dataclass_fields__.items():
        flag = "--" + name.replace("_", "-")
        if field.type is bool:
            result.add_argument(flag, action="store_true")
        else:
            result.add_argument(flag, default=field.default)
    result.add_argument(
        "--plan",
        action="store_true",
        help="Print configuration without starting Spark or calling AWS",
    )
    return result


def from_args(args):
    return CatalogConfig(
        **{name: getattr(args, name) for name in CatalogConfig.__dataclass_fields__}
    ).validate()


def print_plan(config):
    print(
        json.dumps(
            {
                "tables": config.tables(),
                "spark_settings": config.settings(),
                "mode": "configuration_only_no_spark_no_aws",
            },
            indent=2,
        )
    )
