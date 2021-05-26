import pytest
import pandas as pd
import blizz.check
from blizz import Field, Relation
import numpy as np
from test.conftest import path_to_test_data, path_student_performance_test
from test.test_pandas_feature_library import data_sources


class StudentPerformanceFaulty1(Relation):
    """
    Example of a defined field missing.
    """

    THIS_IS_MISSING = Field(name="I'm missing")

    @classmethod
    @blizz.check.fields
    def load(cls) -> pd.DataFrame:
        return pd.read_csv(path_student_performance_test().as_posix())


class StudentPerformanceFaulty2(Relation):
    """
    Example of a defined field with a wrong type.
    """

    # this is actually an int:
    MARKS = Field(name="Marks", datatype=float)

    @classmethod
    @blizz.check.fields
    @blizz.check.types
    def load(cls) -> pd.DataFrame:
        return pd.read_csv(path_student_performance_test().as_posix())


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


class StudentPerformanceFaultyWarn(Relation):
    """
    Example of a defined field missing.
    """

    THIS_IS_MISSING = Field(name="I'm missing")

    @classmethod
    @blizz.check.fields(on_fail=blizz.check.WARN)
    def load(cls) -> pd.DataFrame:
        return pd.read_csv(path_student_performance_test().as_posix())


def test_on_fail_warn() -> None:
    # type check defined with on_fail: WARN
    with pytest.warns(expected_warning=UserWarning):
        StudentPerformanceFaultyWarn.load()


def test_no_fail() -> None:
    df = data_sources.StudentPerformance.load()
    assert df is not None


class RelationTypeDefVariance1(Relation):
    """
    This is an example data source for testing,
    here specifically testing various ways of stating a Pandas datatype:
        -   quoted string, e.g. "int", "float",...
        -   numpy type, e.g. np.int, np.float
        -   numpy umbrella type, e.g. np.number
    """

    F_STRING_1 = Field(name="F_String_1", datatype=object)

    # type float using Python inbuilt:
    F_FLOAT_1 = Field(name="F_Float_1", datatype=float)
    # type float as string:
    F_FLOAT_2 = Field(name="F_Float_2", datatype="float")

    # type integer using Python inbuilt:
    F_INTEGER_1 = Field(name="F_Integer_1", datatype=int)

    # type integer from numpy:
    F_INTEGER_2 = Field(name="F_Integer_2", datatype=np.int)

    # instead of integer, use "umbrella" type np.number:
    F_INTEGER_3 = Field(name="F_Integer_3", datatype=np.number)

    @classmethod
    @blizz.check.fields
    @blizz.check.types
    def load(cls) -> pd.DataFrame:
        return pd.read_csv(
            path_to_test_data().joinpath("test_type_variance.csv").as_posix()
        )


def test_type_variances() -> None:
    RelationTypeDefVariance1.load()
