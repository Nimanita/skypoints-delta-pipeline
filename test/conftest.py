"""Shared fixtures.

* `spark`        one local SparkSession for the whole run (Delta-enabled when delta-spark is installed).
* `delta_spark`  same session, but the test is skipped when Delta tables cannot be created.
* `db`           a throw-away database for the Delta tests, dropped at the end.

Run:  pip install pyspark==3.5.3 delta-spark==3.2.0 pytest   then   pytest -v
On a Databricks cluster the already running session is reused (getOrCreate)."""
import os
import sys
import uuid

import pytest
from pyspark.sql import SparkSession

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


def _build_session():
    builder = (
        SparkSession.builder.master("local[2]").appName("silver-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")      # advertise the same address we listen on
    )
    try:  # Delta is optional: only the @pytest.mark.delta tests need it
        from delta import configure_spark_with_delta_pip
        builder = (
            builder
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
        return configure_spark_with_delta_pip(builder).getOrCreate()
    except ImportError:
        return builder.getOrCreate()


@pytest.fixture(scope="session")
def spark():
    session = _build_session()
    yield session
    session.stop()


@pytest.fixture(scope="session")
def delta_spark(spark):
    probe = "_delta_probe_" + uuid.uuid4().hex[:8]
    try:
        spark.sql(f"CREATE TABLE {probe} (id INT) USING DELTA")
        spark.sql(f"DROP TABLE {probe}")
    except Exception as error:  # noqa: BLE001
        pytest.skip(f"Delta Lake is not available in this Spark session: {str(error)[:120]}")
    return spark


@pytest.fixture()
def db(delta_spark):
    name = "t_silver_" + uuid.uuid4().hex[:8]
    delta_spark.sql(f"CREATE DATABASE {name}")
    yield name
    delta_spark.sql(f"DROP DATABASE IF EXISTS {name} CASCADE")