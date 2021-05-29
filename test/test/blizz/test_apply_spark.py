import pandas as pd

import blizz.apply
import blizz.check
from blizz import Field, Relation
import pyspark.sql


class TestApplyDefaults1(Relation):
    """

    """

    NAME = Field(name="name", default="")
    AGE = Field(name="age", default=20)

    @classmethod
    @blizz.check.fields
    @blizz.apply.defaults
    def load(cls, spark_session: pyspark.sql.SparkSession) -> pyspark.sql.DataFrame:
        return spark_session.createDataFrame(
            pd.DataFrame(data={"name": ["Tom", "Mike", "Sarah"], "age": [5, 31, None]})
        )


def test_apply_defaults(spark_session) -> None:
    """
    """

    filled = TestApplyDefaults1.load(spark_session)
    assert filled.toPandas()[TestApplyDefaults1.AGE].equals(
        pd.Series([5.0, 31.0, 20.0])
    )


class TestApplyDefaultsPartial(Relation):
    """

    """

    NAME = Field(name="name")
    AGE = Field(name="age", default=20)

    @classmethod
    @blizz.check.fields
    @blizz.apply.defaults
    def load(cls, spark_session: pyspark.sql.SparkSession) -> pyspark.sql.DataFrame:
        return spark_session.createDataFrame(
            pd.DataFrame(data={"name": ["Tom", None, "Sarah"], "age": [5, 31, None]})
        )


def test_apply_defaults_partial_1(spark_session) -> None:
    """
    """

    filled = TestApplyDefaultsPartial.load(spark_session).toPandas()
    assert filled[TestApplyDefaultsPartial.AGE].equals(pd.Series([5.0, 31.0, 20.0]))
    assert filled[TestApplyDefaultsPartial.NAME].equals(
        pd.Series(["Tom", None, "Sarah"])
    )


class TestApplyDefaultsPartial2(Relation):
    """

    """

    NAME = Field(name="name", default="Peter")
    AGE = Field(name="age", default=20)

    @classmethod
    @blizz.check.fields
    @blizz.apply.defaults(fill=NAME)
    def load(cls, spark_session: pyspark.sql.SparkSession) -> pyspark.sql.DataFrame:
        return spark_session.createDataFrame(
            pd.DataFrame(data={"name": ["Tom", None, "Sarah"], "age": [5, 31, None]})
        )


def test_apply_defaults_partial_2(spark_session) -> None:
    """
    """

    filled = TestApplyDefaultsPartial2.load(spark_session).toPandas()
    assert filled[TestApplyDefaultsPartial2.AGE].equals(pd.Series([5.0, 31.0, None]))
    assert filled[TestApplyDefaultsPartial2.NAME].equals(
        pd.Series(["Tom", "Peter", "Sarah"])
    )


class TestApplyDefaultsPartial3(Relation):
    """

    """

    NAME = Field(name="name", default="Peter")
    AGE = Field(name="age", default=20)

    @classmethod
    @blizz.check.fields
    @blizz.apply.defaults(fill=[NAME])
    @blizz.apply.defaults(fill=[AGE])
    def load(cls, spark_session: pyspark.sql.SparkSession) -> pyspark.sql.DataFrame:
        return spark_session.createDataFrame(
            pd.DataFrame(data={"name": ["Tom", None, "Sarah"], "age": [5, 31, None]})
        )


def test_apply_defaults_partial_3(spark_session) -> None:
    """
    """

    filled = TestApplyDefaultsPartial3.load(spark_session).toPandas()
    assert filled[TestApplyDefaultsPartial3.AGE].equals(pd.Series([5.0, 31.0, 20.0]))
    assert filled[TestApplyDefaultsPartial3.NAME].equals(
        pd.Series(["Tom", "Peter", "Sarah"])
    )


class TestApplyDedup1(Relation):
    """

    """

    NAME = Field(name="name", key=True)
    AGE = Field(name="age", default=20)

    @classmethod
    @blizz.check.fields
    @blizz.apply.defaults
    @blizz.apply.deduplication
    def load(cls, spark_session: pyspark.sql.SparkSession) -> pyspark.sql.DataFrame:
        return spark_session.createDataFrame(
            pd.DataFrame(data={"name": ["Tom", "Mike", "Mike"], "age": [5, 31, None]})
        )


def test_apply_dedup1(spark_session) -> None:
    """
    """

    assert (
        not TestApplyDedup1.load(spark_session)
        .toPandas()[TestApplyDedup1.NAME]
        .duplicated()
        .any()
    )


class TestApplyDedupNoKeyDefined(Relation):
    """

    """

    NAME = Field(name="name")
    AGE = Field(name="age", default=20)

    @classmethod
    @blizz.check.fields
    @blizz.apply.defaults
    @blizz.apply.deduplication
    def load(cls, spark_session: pyspark.sql.SparkSession) -> pyspark.sql.DataFrame:
        return spark_session.createDataFrame(
            pd.DataFrame(data={"name": ["Tom", "Mike", "Mike"], "age": [5, 31, 31]})
        )


def test_apply_dedup_no_key_defined(spark_session) -> None:
    """
    """

    assert (
        not TestApplyDedupNoKeyDefined.load(spark_session).toPandas().duplicated().any()
    )


class TestApplyDedupPartial(Relation):
    """

    """

    NAME = Field(name="name")
    AGE = Field(name="age", default=20)

    @classmethod
    @blizz.check.fields
    @blizz.apply.defaults
    @blizz.apply.deduplication(key=[AGE])
    def load(cls, spark_session: pyspark.sql.SparkSession) -> pyspark.sql.DataFrame:
        return spark_session.createDataFrame(
            pd.DataFrame(data={"name": ["Tom", "Sarah", "Mike"], "age": [5, 31, 31]})
        )


def test_apply_dedup_partial(spark_session) -> None:
    """
    """

    assert (
        TestApplyDedupPartial.load(spark_session)
        .toPandas()
        .sort_values(by=TestApplyDedupPartial.NAME)[[TestApplyDedupPartial.NAME]]
        .equals(pd.DataFrame(data={"name": ["Sarah", "Tom"]}))
    )


class TestApplyDedupSort(Relation):
    """

    """

    NAME = Field(name="name")
    AGE = Field(name="age", default=20)

    @classmethod
    @blizz.check.fields
    @blizz.apply.defaults
    @blizz.apply.deduplication(key=[NAME], sort_on=[AGE], sort_order=blizz.apply.ASC)
    def load(cls, spark_session: pyspark.sql.SparkSession) -> pyspark.sql.DataFrame:
        return spark_session.createDataFrame(
            pd.DataFrame(data={"name": ["Tom", "Mike", "Mike"], "age": [5, 25, 31]})
        )


def test_apply_dedup_sort(spark_session) -> None:
    """
    """

    assert (
        TestApplyDedupSort.load(spark_session)
        .toPandas()
        .equals(pd.DataFrame(data={"name": ["Tom", "Mike"], "age": [5, 25]}))
    )


class TestApplyRenameThroughField(Relation):
    """

    """

    NAME = Field(name="name_renamed", source_name="name")
    AGE = Field(name="age", default=20)

    @classmethod
    @blizz.check.fields
    @blizz.apply.defaults
    @blizz.apply.deduplication
    @blizz.apply.renames
    def load(cls, spark_session: pyspark.sql.SparkSession) -> pyspark.sql.DataFrame:
        return spark_session.createDataFrame(
            pd.DataFrame(data={"name": ["Tom", "Mike", "Mike"], "age": [5, 31, 31]})
        )


def test_apply_rename_through_field(spark_session) -> None:
    """
    """

    assert "name_renamed" in TestApplyRenameThroughField.load(spark_session).columns


class TestApplyRenameThroughArg(Relation):
    """

    """

    NAME = Field(name="name_renamed")
    AGE = Field(name="age", default=20)

    @classmethod
    @blizz.check.fields
    @blizz.apply.defaults
    @blizz.apply.deduplication
    @blizz.apply.renames(columns={"name": "name_renamed"})
    def load(cls, spark_session: pyspark.sql.SparkSession) -> pyspark.sql.DataFrame:
        return spark_session.createDataFrame(
            pd.DataFrame(data={"name": ["Tom", "Mike", "Mike"], "age": [5, 31, 31]})
        )


def test_apply_rename_through_arg(spark_session) -> None:
    """
    """

    assert "name_renamed" in TestApplyRenameThroughArg.load(spark_session).columns
