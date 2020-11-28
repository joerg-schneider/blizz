from test.test_feature_library.data_sources import *
from dataforger import *
from typing import *
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

__all__ = ["MyFeatureGroup"]


class MyFeatureGroup(FeatureGroup):
    """ Docstring of the feature group. """

    data_sources = (Boston,)

    @classmethod
    def compute_base(cls) -> DataFrame:
        boston = cls.data_sources[0]
        return boston.load()

    class MyFeature1(Feature):
        """ Docstring of Feature 1. """

        @classmethod
        def compute(cls, base: DataFrame, parameters: Dict[str, Any] = None) -> Column:
            return F.expr(f"{Boston.Columns.AGE} + 2")

    class MyFeature2(Feature):
        """ Docstring of Feature 2. """

        @classmethod
        def compute(cls, base: DataFrame, parameters: Dict[str, Any] = None) -> Column:
            return F.expr(f"{Boston.Columns.LSTAT} * 0.5")

    class MyFeature3(Feature):
        """ Docstring of Feature 3. """

        @classmethod
        def compute(cls, base: DataFrame, parameters: Dict[str, Any] = None) -> Column:
            return F.col(Boston.Columns.AGE.name) + 1000

    class MyFeature4(Feature):
        """ Docstring of Feature 4. """

        class Parameters:
            FACTOR = "factor"

        @classmethod
        def compute(cls, base: DataFrame, parameters: Dict[str, Any] = None) -> Column:
            # todo: provide parameter check!
            if parameters is None:
                raise ValueError(f"{cls.name()} requires parameters")

            return F.col(Boston.Columns.AGE.name) * parameters[cls.Parameters.FACTOR]

    class AvgTaxPerRAD(Feature):
        """ Docstring of Feature 5. """

        aggregation_level = Boston.Columns.RAD

        @classmethod
        def compute(
            cls, base: DataFrame, parameters: Dict[str, Any] = None
        ) -> DataFrame:
            data = base.groupby(F.col(Boston.Columns.RAD.name)).agg(
                F.round(F.avg(F.col(Boston.Columns.TAX.name)), 1)
            )
            return data
