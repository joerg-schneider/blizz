from pathlib import Path

import pytest

import blizz.check
from blizz import Field, Relation
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType

from test.conftest import get_or_create_spark_session, path_to_test_data
from test.test_spark_feature_library.data_sources import Boston


class BostonFaulty1(Relation):
    """
    Example of a defined field missing.
    """

    THIS_IS_MISSING = Field(name="I'm missing")

    @classmethod
    @blizz.check.fields
    def load(cls) -> DataFrame:
        return get_or_create_spark_session().read.csv(
            path=path_to_test_data().joinpath("boston.csv").as_posix(),
            inferSchema=True,
            header=True,
        )


class BostonFaulty2(Relation):
    """
    Example of a defined field missing.
    """

    # this is actually a DoubleType:
    CRIM = Field(name="CRIM", datatype=StringType)

    @classmethod
    @blizz.check.fields
    @blizz.check.types
    def load(cls) -> DataFrame:
        return get_or_create_spark_session().read.csv(
            path=path_to_test_data().joinpath("boston.csv").as_posix(),
            inferSchema=True,
            header=True,
        )


def test_field_existence_check() -> None:
    """
    """

    with pytest.raises(
        expected_exception=ValueError,
        match="Field 'I'm missing' is not part of loaded Relation 'BostonFaulty1'",
    ):
        BostonFaulty1.load()


def test_field_type_check() -> None:
    """
    """
    with pytest.raises(
        expected_exception=ValueError, match="Type error for 'BostonFaulty2.CRIM'*",
    ):
        BostonFaulty2.load()


def test_passes_checks() -> None:
    Boston.load()
