import pytest

import blizz.check
from blizz import Field, Relation
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, IntegerType

from test.conftest import get_or_create_spark_session, path_student_performance_test
from test.test_spark_feature_library.data_sources import StudentPerformance


class StudentPerformanceFaulty1(Relation):
    """
    Example of a defined field missing.
    """

    THIS_IS_MISSING = Field(name="I'm missing")

    @classmethod
    @blizz.check.fields
    def load(cls) -> DataFrame:
        return get_or_create_spark_session().read.csv(
            path=path_student_performance_test().as_posix(),
            inferSchema=True,
            header=True,
        )


class StudentPerformanceFaulty2(Relation):
    """
    Example of a defined field with faulty datatype.
    """

    # this is actually a DoubleType:
    MARKS = Field(name="Marks", datatype=StringType)

    @classmethod
    @blizz.check.fields
    @blizz.check.types
    def load(cls) -> DataFrame:
        return get_or_create_spark_session().read.csv(
            path=path_student_performance_test().as_posix(),
            inferSchema=True,
            header=True,
        )


def test_field_existence_check() -> None:
    """
    """

    with pytest.raises(
        expected_exception=ValueError,
        match="Field 'I'm missing' is not part of loaded Relation 'StudentPerformanceFaulty1'",
    ):
        StudentPerformanceFaulty1.load()


def test_field_type_check() -> None:
    """
    """
    with pytest.raises(
        expected_exception=ValueError,
        match="Type error for 'StudentPerformanceFaulty2.Marks'*",
    ):
        StudentPerformanceFaulty2.load()


class StudentPerformanceFaulty3(Relation):
    """
    Example of a duplicated field defined as key.
    """

    STUDENT_ID = Field(name="Student_ID", datatype=StringType)
    # this is actually not the key:
    SEMSTER_NAME = Field("Semster_Name", datatype=StringType, key=True)
    PAPER_ID = Field(name="Paper_ID", datatype=StringType)
    MARKS = Field(name="Marks", datatype=IntegerType)

    @classmethod
    @blizz.check.fields
    @blizz.check.types
    @blizz.check.keys
    def load(cls) -> DataFrame:
        return get_or_create_spark_session().read.csv(
            path=path_student_performance_test().as_posix(),
            inferSchema=True,
            header=True,
        )


def test_key_check() -> None:
    """
    """
    with pytest.raises(
        expected_exception=ValueError,
        match="Key error for 'StudentPerformanceFaulty3'*",
    ):
        StudentPerformanceFaulty3.load()

def test_passes_checks() -> None:
    sdf = StudentPerformance.load()
    assert sdf is not None
