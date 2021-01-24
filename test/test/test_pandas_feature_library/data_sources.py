import pandas as pd
import blizz.check
from blizz import Relation, Field
from test.conftest import path_to_test_data
import numpy as np


class Boston(Relation):
    """
    This is the example data source boston for testing.
    """

    CRIM = Field(name="CRIM", datatype=float)
    ZN = Field(name="ZN", datatype=float)
    INDUS = Field(name="INDUS", datatype=float)
    CHAS = Field(name="CHAS", datatype=int)
    NOX = Field(name="NOX", datatype=float)
    RM = Field(name="RM", datatype=float)
    AGE = Field(name="AGE", datatype=float)
    DIS = Field(name="DIS", datatype=float)
    RAD = Field(name="RAD", datatype=int)
    TAX = Field(name="TAX", datatype=int)
    PTRATIO = Field(name="PTRATIO", datatype=float)
    B = Field(name="B", datatype=float)
    LSTAT = Field(name="LSTAT", datatype=float)
    MEDV = Field(name="MEDV", datatype=float)

    @classmethod
    @blizz.check.fields
    @blizz.check.types
    def load(cls) -> pd.DataFrame:
        return pd.read_csv(path_to_test_data().joinpath("boston.csv").as_posix())


__all__ = ["Boston"]
