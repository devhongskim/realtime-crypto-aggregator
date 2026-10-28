"""Shared pytest fixtures."""

import os
import sys

import pytest
from pyspark.sql import SparkSession

# Make Spark's Python workers use the same interpreter as pytest
# (matters on Windows and in virtualenvs).
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def spark():
    """
    One local SparkSession for the whole test run (startup is the slow part).
    Mirrors the job's settings that affect results: UTC session timezone,
    which is what the dt partition is derived from.
    """
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("crypto-pipeline-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
