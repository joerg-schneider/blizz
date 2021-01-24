from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from test.test_spark_feature_library.data_sources import Boston


def test_table(path_boston_test: Path, spark_session: SparkSession) -> None:
    """

    :param path_boston_test:
    :return:
    """

    assert type(Boston.load()) == DataFrame
