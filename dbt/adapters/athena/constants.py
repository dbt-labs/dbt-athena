from dbt.adapters.events.logging import AdapterLogger

DEFAULT_THREAD_COUNT = 4
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_POLLING_INTERVAL = 5
DEFAULT_SPARK_COORDINATOR_DPU_SIZE = 1
DEFAULT_SPARK_MAX_CONCURRENT_DPUS = 2
DEFAULT_SPARK_EXECUTOR_DPU_SIZE = 1
DEFAULT_CALCULATION_TIMEOUT = 43200  # seconds = 12 hours
SESSION_IDLE_TIMEOUT_MIN = 10  # minutes

ENFORCE_SPARK_PROPERTIES = {
    "spark.sql.sources.partitionOverwriteMode": "dynamic",
}

DEFAULT_SPARK_PROPERTIES = {
    # https://docs.aws.amazon.com/athena/latest/ug/notebooks-spark-table-formats.html
    "iceberg": {
        "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
        "spark.sql.catalog.spark_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
        "spark.sql.catalog.spark_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
        "spark.sql.iceberg.handle-timestamp-without-timezone": "true",
    },
    "hudi": {
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.sql.extensions": "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
    },
    "delta_lake": {
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    },
    # https://docs.aws.amazon.com/athena/latest/ug/notebooks-spark-encryption.html
    "spark_encryption": {
        "spark.authenticate": "true",
        "spark.io.encryption.enabled": "true",
        "spark.network.crypto.enabled": "true",
    },
    # https://docs.aws.amazon.com/athena/latest/ug/spark-notebooks-cross-account-glue.html
    "spark_cross_account_catalog": {"spark.hadoop.aws.glue.catalog.separator": "/"},
    # https://docs.aws.amazon.com/athena/latest/ug/notebooks-spark-requester-pays.html
    "spark_requester_pays": {"spark.hadoop.fs.s3.useRequesterPaysHeader": "true"},
}

EMR_SERVERLESS_SPARK_PROPERTIES = {
    "default": {
        "spark.executor.instances": "1",
        "spark.executor.cores": "1",
        "spark.executor.memory": "1g",
        "spark.driver.cores": "1",
        "spark.driver.memory": "1g",
        "spark.hadoop.hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
    },
    "iceberg": {
        "spark.jars": "/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar",
        **DEFAULT_SPARK_PROPERTIES["iceberg"],
    },
    "hudi": {"spark.jars": "/usr/lib/hudi/hudi-spark-bundle.jar", **DEFAULT_SPARK_PROPERTIES["hudi"]},
    "delta_lake": {
        "spark.jars": "/usr/share/aws/delta/lib/delta-spark.jar,/usr/share/aws/delta/lib/delta-storage.jar",
        **DEFAULT_SPARK_PROPERTIES["delta_lake"],
    },
    "spark_encryption": {**DEFAULT_SPARK_PROPERTIES["spark_encryption"]},
}

LOGGER = AdapterLogger(__name__)
