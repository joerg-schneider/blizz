from pathlib import Path
import os
import pytest
from pyspark.sql import SparkSession
from test import test_feature_library


def path_to_test_data() -> Path:
    return Path(os.path.dirname(__file__)).joinpath(os.pardir, "data")


@pytest.fixture
def path_boston_test() -> Path:
    return path_to_test_data().joinpath("boston.csv").absolute()


@pytest.fixture
def path_test_feature_library() -> Path:
    return Path(os.path.dirname(test_feature_library.__file__))


def get_or_create_spark_session() -> SparkSession:
    """
    Create and/or retrieve an Apache Spark Session.
    :return: a live Spark Session
    """
    spark = SparkSession.builder.getOrCreate()
    # spark.conf.set("spark.sql.execution.arrow.enabled", "true")

    return spark


@pytest.fixture(scope="session")
def spark_session() -> SparkSession:
    spark = get_or_create_spark_session()
    yield spark
    # this will run whenever the last test of the session has
    # completed
    spark.stop()
