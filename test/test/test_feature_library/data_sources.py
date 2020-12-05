from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType

from blizz.dataobjects import SourceTable, Field
from test.conftest import path_to_test_data, get_or_create_spark_session


class Boston(SourceTable):
    """
    This is the example data source boston for testing.
    """

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
        return get_or_create_spark_session().read.csv(
            path=path_to_test_data().joinpath("boston.csv").as_posix(),
            inferSchema=True,
            header=True,
        )


__all__ = ["Boston"]
