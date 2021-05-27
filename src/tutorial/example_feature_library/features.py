from tutorial.example_feature_library.data_sources import *
from blizz import Feature, FeatureGroup
from typing import *
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F, Window

__all__ = ["StudentFeatureGroup"]


class StudentFeatureGroup(FeatureGroup):
    """ This is the StudentFeatureGroup. """

    data_sources = (StudentPerformance,)

    @classmethod
    def compute_base(cls) -> DataFrame:
        student_perf = cls.data_sources[0]
        return student_perf.load()

    class StudentMarksAverage(Feature):
        """ The average over all known student marks. """

        @classmethod
        def compute(cls, base: DataFrame, parameters: Dict[str, Any] = None) -> Column:
            return F.avg(StudentPerformance.MARKS).over(
                Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            )

    class NumberOfStudents(Feature):
        """ The total count of students. """

        @classmethod
        def compute(cls, base: DataFrame, parameters: Dict[str, Any] = None) -> Column:
            return F.count(StudentPerformance.STUDENT_ID).over(
                Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            )

    class StudentMarksScaled(Feature):
        """ All student marks scaled to be between 0-1. """

        @classmethod
        def compute(cls, base: DataFrame, parameters: Dict[str, Any] = None) -> Column:
            marks_min = base.select(F.min(StudentPerformance.MARKS)).collect()[0][0]
            marks_max = base.select(F.max(StudentPerformance.MARKS)).collect()[0][0]
            return F.round(
                (F.expr(f"{StudentPerformance.MARKS}") - marks_min)
                / (marks_max - marks_min),
                4,
            )

    class StudentMarksShifted(Feature):
        """ All student marks shifted by 1000. """

        @classmethod
        def compute(cls, base: DataFrame, parameters: Dict[str, Any] = None) -> Column:
            return F.col(StudentPerformance.MARKS.name) + 1000

    class ScaleMarksByFactor(Feature):
        """ Marks scaled by a (paramterizeable) factor. """

        class Parameters:
            FACTOR = "factor"

        @classmethod
        def compute(cls, base: DataFrame, parameters: Dict[str, Any] = None) -> Column:
            if parameters is None:
                raise ValueError(f"{cls.name()} requires parameters")

            return (
                F.col(StudentPerformance.MARKS.name) * parameters[cls.Parameters.FACTOR]
            )

    class AvgMarkPerStudent(Feature):
        """ The average mark per student. """

        aggregation_level = StudentPerformance.STUDENT_ID

        @classmethod
        def compute(
            cls, base: DataFrame, parameters: Dict[str, Any] = None
        ) -> DataFrame:
            data = base.groupby(F.col(StudentPerformance.STUDENT_ID)).agg(
                F.round(F.avg(F.col(StudentPerformance.MARKS)), 1)
            )
            return data
