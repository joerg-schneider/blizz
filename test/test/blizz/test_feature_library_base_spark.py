from pathlib import Path
from typing import Optional, Dict, Any, Iterable, Union, Tuple
from test.test_spark_feature_library.data_sources import *
from blizz import Feature, FeatureGroup, FeatureParameter
from pyspark.sql import DataFrame, Column, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window
from blizz import Relation, Field


def test_feature_group(spark_session: SparkSession):
    class TestFeatureGroup(FeatureGroup):
        """ Docstring of the feature group. """

        data_sources = (StudentPerformance,)

        @classmethod
        def compute_base(cls) -> DataFrame:
            student_perf = cls.data_sources[0]
            return student_perf.load()

        class StudentMarksAverage(Feature):
            """ The average over all known Student marks. """

            @classmethod
            def compute(
                cls, base: DataFrame, parameters: Dict[str, Any] = None
            ) -> Column:
                return F.avg(StudentPerformance.MARKS).over(
                    Window.rowsBetween(
                        Window.unboundedPreceding, Window.unboundedFollowing
                    )
                )

        class NumberOfStudents(Feature):
            """ The count of students """

            @classmethod
            def compute(
                cls, base: DataFrame, parameters: Dict[str, Any] = None
            ) -> Column:
                return F.count(StudentPerformance.STUDENT_ID).over(
                    Window.rowsBetween(
                        Window.unboundedPreceding, Window.unboundedFollowing
                    )
                )

        class StudentMarksScaled(Feature):
            """  """

            @classmethod
            def compute(
                cls, base: DataFrame, parameters: Dict[str, Any] = None
            ) -> Column:
                marks_min = base.select(F.min(StudentPerformance.MARKS)).collect()[0][0]
                marks_max = base.select(F.max(StudentPerformance.MARKS)).collect()[0][0]
                return F.round(
                    (F.expr(f"{StudentPerformance.MARKS}") - marks_min)
                    / (marks_max - marks_min),
                    4,
                )

        class StudentMarksShifted(Feature):
            """ Docstring of Feature 3. """

            @classmethod
            def compute(
                cls, base: DataFrame, parameters: Dict[str, Any] = None
            ) -> Column:
                return F.col(StudentPerformance.MARKS.name) + 1000

        class ScaleMarksByFactor(Feature):
            """ Docstring of Feature 4. """

            class Parameters:
                FACTOR = "factor"

            @classmethod
            def compute(
                cls, base: DataFrame, parameters: Dict[str, Any] = None
            ) -> Column:
                # todo: provide parameter check!
                if parameters is None:
                    raise ValueError(f"{cls.name()} requires parameters")

                return (
                    F.col(StudentPerformance.MARKS.name)
                    * parameters[cls.Parameters.FACTOR]
                )

        class AvgMarkPerStudent(Feature):
            """ Docstring of Feature 5. """

            aggregation_level = StudentPerformance.STUDENT_ID

            @classmethod
            def compute(
                cls, base: DataFrame, parameters: Dict[str, Any] = None
            ) -> DataFrame:
                data = base.groupby(F.col(StudentPerformance.STUDENT_ID)).agg(
                    F.round(F.avg(F.col(StudentPerformance.MARKS)), 1)
                )
                return data

    print([f.col_name() for f in TestFeatureGroup.get_features()])

    print(
        TestFeatureGroup.compute(
            keep=(StudentPerformance.STUDENT_ID,),
            parameters={
                TestFeatureGroup.ScaleMarksByFactor.name(): (
                    FeatureParameter(
                        parameter_grid={
                            TestFeatureGroup.ScaleMarksByFactor.Parameters.FACTOR: range(
                                0, 1
                            )
                        },
                        suffix_format="_times_{factor}",
                    ),
                )
            },
        ).show()
    )
