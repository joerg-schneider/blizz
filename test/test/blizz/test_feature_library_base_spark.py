from pathlib import Path
from typing import Optional, Dict, Any, Iterable, Union, Tuple
from test.test_spark_feature_library.data_sources import *
from blizz import Feature, FeatureGroup, FeatureParameter
from pyspark.sql import DataFrame, Column, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType
from blizz import Relation, Field


def test_feature_group(spark_session: SparkSession, path_boston_test: Path):
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
            def compute(
                cls, base: DataFrame, parameters: Dict[str, Any] = None
            ) -> Column:
                return F.expr(f"{Boston.AGE} + 2")

        class MyFeature2(Feature):
            """ Docstring of Feature 2. """

            @classmethod
            def compute(
                cls, base: DataFrame, parameters: Dict[str, Any] = None
            ) -> Column:
                return F.expr(f"{Boston.LSTAT} * 0.5")

        class MyFeature3(Feature):
            """ Docstring of Feature 3. """

            @classmethod
            def compute(
                cls, base: DataFrame, parameters: Dict[str, Any] = None
            ) -> Column:
                return F.col(Boston.AGE.name) + 1000

        class MyFeature4(Feature):
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

                return F.col(Boston.AGE.name) * parameters[cls.Parameters.FACTOR]

        class AvgTaxPerRAD(Feature):
            """ Docstring of Feature 5. """

            aggregation_level = Boston.RAD

            @classmethod
            def compute(
                cls, base: DataFrame, parameters: Dict[str, Any] = None
            ) -> DataFrame:
                data = base.groupby(F.col(Boston.RAD.name)).agg(
                    F.round(F.avg(F.col(Boston.TAX.name)), 1)
                )
                return data

    print([f.col_name() for f in MyFeatureGroup.get_features()])

    print(
        MyFeatureGroup.compute(
            keep=(Boston.RAD,),
            parameters={
                MyFeatureGroup.MyFeature4.name(): (
                    FeatureParameter(
                        parameter_grid={
                            MyFeatureGroup.MyFeature4.Parameters.FACTOR: range(0, 1)
                        },
                        suffix_format="_times_{factor}",
                    ),
                )
            },
        ).show()
    )
    # print(MyFeatureGroup.compute().show())
    # print(MyFeatureGroup.compute(keep=Boston.get_fields()).show())