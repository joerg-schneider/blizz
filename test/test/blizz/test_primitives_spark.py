from pyspark.sql import DataFrame, SparkSession
from test.test_spark_feature_library.data_sources import StudentCouncelingInformation


def test_table(spark_session: SparkSession) -> None:
    """
    # todo: docs
    :return:
    """

    assert type(StudentCouncelingInformation.load()) == DataFrame
