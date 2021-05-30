from typing import Union, Callable

import pandas as pd
import pytest
from test.blizz.test_feature_library_base_spark import StudentPerformance
from blizz import Relation, Field
from blizz._primitives import check_dataframe_type


def test_relation():
    class MyTestRelation1(Relation):

        COL1 = Field("column1", datatype=int, key=True)
        COL2 = Field("column2", datatype=object, default="test_default")
        COL3 = Field("column3", source_name="column2")
        COL4 = Field("column4", description="My description", mock_func=lambda _: 100)

        @classmethod
        def load(cls) -> Union["pyspark.sql.DataFrame", "pandas.DataFrame"]:
            return pd.DataFrame(data={"column1": [1, 2, 3], "column2": ["a", "b", "c"]})

    my_df = MyTestRelation1.load()
    # column list access
    print(my_df[[MyTestRelation1.COL1, MyTestRelation1.COL2]])

    # single series access
    print(my_df[MyTestRelation1.COL1])

    # equality
    assert not MyTestRelation1.equals(StudentPerformance)
    assert not MyTestRelation1.contained_in([StudentPerformance])
    assert my_df[MyTestRelation1.COL1].equals(my_df["column1"])

    class MyTestRelation2(Relation):
        COL1 = Field("my_field", datatype="int")

    class MyTestRelation3(Relation):
        COL1 = Field("my_field", datatype=float)

    assert not MyTestRelation2 == MyTestRelation3

    assert MyTestRelation1.COL2 != MyTestRelation1.COL4

    # types
    assert MyTestRelation1.COL1 in (
        my_df.select_dtypes(include=MyTestRelation1.COL1.datatype)
    )

    assert MyTestRelation1.COL2 in (
        my_df.select_dtypes(include=MyTestRelation1.COL2.datatype)
    )

    assert MyTestRelation1.get_key_field_names() == [MyTestRelation1.COL1]
    defined_types = MyTestRelation1.get_types()
    print(defined_types)

    assert MyTestRelation1.COL4.description == "My description"

    assert defined_types[MyTestRelation1.COL1] == int
    defined_renames = MyTestRelation1.get_field_renames()
    assert defined_renames["column2"] == "column3"

    assert (
        MyTestRelation1.get_default(MyTestRelation1.COL2)
        == MyTestRelation1.COL2.default
        == "test_default"
        == MyTestRelation1.get_defaults()[MyTestRelation1.COL2]
    )

    assert isinstance(MyTestRelation1.COL4.mock_func, Callable)

    with pytest.raises(expected_exception=ValueError):
        MyTestRelation1.get_default(MyTestRelation1.COL1)

    # check data frame type error handling
    with pytest.raises(ValueError):
        check_dataframe_type(data=[], relation=MyTestRelation1)
