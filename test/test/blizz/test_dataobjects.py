from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DoubleType
from blizz.dataobjects import SourceTable, Field


def test_table(path_boston_test: Path, spark_session: SparkSession) -> None:
    """

    :param path_boston_test:
    :return:
    """

    class Boston(SourceTable):
        CRIM = Field(name="CRIM", datatype=DoubleType)
        ZN = Field(name="ZN", datatype=DoubleType)
        INDUS = Field(name="INDUS", datatype=DoubleType)
        CHAS = Field(name="CHAS", datatype=DoubleType)
        NOX = Field(name="NOX", datatype=DoubleType)
        RM = Field(name="RM", datatype=DoubleType)
        AGE = Field(name="AGE", datatype=DoubleType)
        DIS = Field(name="DIS", datatype=DoubleType)
        RAD = Field(name="RAD", datatype=DoubleType)
        TAX = Field(name="TAX", datatype=DoubleType)
        PTRATIO = Field(name="PTRATIO", datatype=DoubleType)
        B = Field(name="B", datatype=DoubleType)
        LSTAT = Field(name="LSTAT", datatype=DoubleType)
        MEDV = Field(name="MEDV", datatype=DoubleType)

        @classmethod
        def load(cls) -> DataFrame:
            return spark_session.read.csv(
                path=path_boston_test.as_posix(), inferSchema=True, header=True
            )

    assert type(Boston.load()) == DataFrame

    # check schema
    Boston.verify_schema(Boston.load())
