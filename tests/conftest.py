from collections.abc import Generator

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession]:
    builder = (
        SparkSession.builder.appName("bream-delta-tests")
        .master("local[4]")
        # Delta configs
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Performance configs for faster tests
        .config("spark.ui.enabled", "false")
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .config("spark.sql.shuffle.partitions", "5")
        .config("delta.log.cacheSize", "3")
        .config("spark.databricks.delta.delta.log.cacheSize", "3")
        .config("spark.sql.sources.parallelPartitionDiscovery.parallelism", "5")
        .config("spark.default.parallelism", "1")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.adaptive.enabled", "false")
    )
    spark_session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield spark_session
    spark_session.stop()
