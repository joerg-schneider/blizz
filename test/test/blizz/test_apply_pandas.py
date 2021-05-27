import pandas as pd
import blizz.check
import blizz.apply
from blizz import Field, Relation


class TestApplyDefaults1(Relation):
    """

    """

    NAME = Field(name="name", default="")
    AGE = Field(name="age", default=20)

    @classmethod
    @blizz.check.fields
    @blizz.apply.defaults
    def load(cls) -> pd.DataFrame:
        return pd.DataFrame(
            data={"name": ["Tom", "Mike", "Sarah"], "age": [5, 31, None]}
        )


def test_apply_defaults() -> None:
    """
    """

    filled = TestApplyDefaults1.load()
    assert filled[TestApplyDefaults1.AGE].equals(pd.Series([5.0, 31.0, 20.0]))


class TestApplyDefaultsPartial(Relation):
    """

    """

    NAME = Field(name="name")
    AGE = Field(name="age", default=20)

    @classmethod
    @blizz.check.fields
    @blizz.apply.defaults
    def load(cls) -> pd.DataFrame:
        return pd.DataFrame(data={"name": ["Tom", None, "Sarah"], "age": [5, 31, None]})


def test_apply_defaults_partial_1() -> None:
    """
    """

    filled = TestApplyDefaultsPartial.load()
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
    def load(cls) -> pd.DataFrame:
        return pd.DataFrame(data={"name": ["Tom", None, "Sarah"], "age": [5, 31, None]})


def test_apply_defaults_partial_2() -> None:
    """
    """

    filled = TestApplyDefaultsPartial2.load()
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
    def load(cls) -> pd.DataFrame:
        return pd.DataFrame(data={"name": ["Tom", None, "Sarah"], "age": [5, 31, None]})


def test_apply_defaults_partial_3() -> None:
    """
    """

    filled = TestApplyDefaultsPartial3.load()
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
    def load(cls) -> pd.DataFrame:
        return pd.DataFrame(
            data={"name": ["Tom", "Mike", "Mike"], "age": [5, 31, None]}
        )


def test_apply_dedup1() -> None:
    """
    """

    assert not TestApplyDedup1.load()[TestApplyDedup1.NAME].duplicated().any()


class TestApplyDedupNoKeyDefined(Relation):
    """

    """

    NAME = Field(name="name")
    AGE = Field(name="age", default=20)

    @classmethod
    @blizz.check.fields
    @blizz.apply.defaults
    @blizz.apply.deduplication
    def load(cls) -> pd.DataFrame:
        return pd.DataFrame(data={"name": ["Tom", "Mike", "Mike"], "age": [5, 31, 31]})


def test_apply_dedup_no_key_defined() -> None:
    """
    """

    assert not TestApplyDedupNoKeyDefined.load().duplicated().any()


class TestApplyDedupPartial(Relation):
    """

    """

    NAME = Field(name="name")
    AGE = Field(name="age", default=20)

    @classmethod
    @blizz.check.fields
    @blizz.apply.defaults
    @blizz.apply.deduplication(key=[AGE])
    def load(cls) -> pd.DataFrame:
        return pd.DataFrame(data={"name": ["Tom", "Sarah", "Mike"], "age": [5, 31, 31]})


def test_apply_dedup_partial() -> None:
    """
    """

    assert TestApplyDedupPartial.load().equals(
        pd.DataFrame(data={"name": ["Tom", "Sarah"], "age": [5, 31]})
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
    def load(cls) -> pd.DataFrame:
        return pd.DataFrame(data={"name": ["Tom", "Mike", "Mike"], "age": [5, 25, 31]})


def test_apply_dedup_sort() -> None:
    """
    """

    assert TestApplyDedupSort.load().equals(
        pd.DataFrame(data={"name": ["Tom", "Mike"], "age": [5, 25]})
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
    def load(cls) -> pd.DataFrame:
        return pd.DataFrame(data={"name": ["Tom", "Mike", "Mike"], "age": [5, 31, 31]})


def test_apply_rename_through_field() -> None:
    """
    """

    assert "name_renamed" in TestApplyRenameThroughField.load().columns


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
    def load(cls) -> pd.DataFrame:
        return pd.DataFrame(data={"name": ["Tom", "Mike", "Mike"], "age": [5, 31, 31]})


def test_apply_rename_through_arg() -> None:
    """
    """

    assert "name_renamed" in TestApplyRenameThroughArg.load().columns
