import pandas as pd
from pandas import DataFrame

import blizz.check
from blizz import Relation, Field
from test.conftest import path_student_performance_test
import numpy as np


class StudentPerformance(Relation):
    """
    This is the example data source "StudentPerformance" for testing.
    """

    STUDENT_ID = Field(name="Student_ID", datatype=object)
    SEMSTER_NAME = Field("Semster_Name", datatype=object)
    PAPER_ID = Field(name="Paper_ID", datatype=object)
    MARKS = Field(name="Marks", datatype=int)

    @classmethod
    @blizz.check.fields
    @blizz.check.types
    def load(cls) -> DataFrame:
        return pd.read_csv(path_student_performance_test().as_posix())


__all__ = ["StudentPerformance"]
