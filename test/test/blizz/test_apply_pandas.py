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
