import pytest
import pandas as pd
import blizz.check
from blizz import Field, Relation
import numpy as np
from test.conftest import path_to_test_data
from test.test_pandas_feature_library import data_sources


class BostonFaulty1(Relation):
    """
    Example of a defined field missing.
    """

    THIS_IS_MISSING = Field(name="I'm missing")

    @classmethod
    @blizz.check.fields
    def load(cls) -> pd.DataFrame:
        return pd.read_csv(path_to_test_data().joinpath("boston.csv").as_posix())


class BostonFaulty2(Relation):
    """
    Example of a defined field with a wrong type.
    """

    # this is actually a float:
    CRIM = Field(name="CRIM", datatype=int)

    @classmethod
    @blizz.check.fields
    @blizz.check.types
    def load(cls) -> pd.DataFrame:
        return pd.read_csv(path_to_test_data().joinpath("boston.csv").as_posix())


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
        expected_exception=ValueError, match="Type error for 'BostonFaulty2.CRIM'*"
    ):
        BostonFaulty2.load()


class BostonFaultyWarn(Relation):
    """
    Example of a defined field missing.
    """

    THIS_IS_MISSING = Field(name="I'm missing")

    @classmethod
    @blizz.check.fields(on_fail=blizz.check.WARN)
    def load(cls) -> pd.DataFrame:
        return pd.read_csv(path_to_test_data().joinpath("boston.csv").as_posix())


def test_on_fail_warn() -> None:
    # type check defined with on_fail: WARN
    with pytest.warns(expected_warning=UserWarning):
        BostonFaultyWarn.load()


def test_no_fail() -> None:
    df = data_sources.Boston.load()
    assert df is not None


class BostonTypeDefVariance(Relation):
    """
    This is the example data source boston for testing,
    here specifically testing various ways of stating a Pandas datatype:
        -   quoted string, e.g. "int", "float",...
        -   numpy type, e.g. np.int, np.float
        -   numpy umbrella type, e.g. np.number
    """

    # type float using Python inbuilt:
    CRIM = Field(name="CRIM", datatype=float)
    # type float as string:
    ZN = Field(name="ZN", datatype="float")
    INDUS = Field(name="INDUS", datatype="float")

    # type integer using Python inbuilt:
    CHAS = Field(name="CHAS", datatype=int)
    NOX = Field(name="NOX", datatype="float")
    RM = Field(name="RM", datatype="float")
    AGE = Field(name="AGE", datatype="float")
    DIS = Field(name="DIS", datatype="float")

    # type integer from numpy:
    RAD = Field(name="RAD", datatype=np.int)

    # instead of integer, use "umbrella" type np.number:
    TAX = Field(name="TAX", datatype=np.number)

    PTRATIO = Field(name="PTRATIO", datatype=float)
    B = Field(name="B", datatype=float)
    LSTAT = Field(name="LSTAT", datatype=np.float)
    MEDV = Field(name="MEDV", datatype=np.float)

    @classmethod
    @blizz.check.fields
    @blizz.check.types
    def load(cls) -> pd.DataFrame:
        return pd.read_csv(path_to_test_data().joinpath("boston.csv").as_posix())


def test_type_variances() -> None:
    BostonTypeDefVariance.load()
