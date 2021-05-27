from typing import Union
import pandas as pd
from blizz import Relation, Field


def test_relation():
    class MyTestRelation1(Relation):

        COL1 = Field("column1", datatype=int, key=True)
        COL2 = Field("column2", datatype=object, default="")
        COL3 = Field("column3", source_name="column2")

        @classmethod
        def load(cls) -> Union["pyspark.sql.DataFrame", "pandas.DataFrame"]:
            return pd.DataFrame(data={"column1": [1, 2, 3], "column2": ["a", "b", "c"]})

    my_df = MyTestRelation1.load()
    # column list access
    print(my_df[[MyTestRelation1.COL1, MyTestRelation1.COL2]])

    # single series access
    print(my_df[MyTestRelation1.COL1])

    # equality
    assert my_df[MyTestRelation1.COL1].equals(my_df["column1"])

    # types
    assert MyTestRelation1.COL1 in (
        my_df.select_dtypes(include=MyTestRelation1.COL1.datatype)
    )

    assert MyTestRelation1.COL2 in (
        my_df.select_dtypes(include=MyTestRelation1.COL2.datatype)
    )

    assert MyTestRelation1.get_key_field_names() == [MyTestRelation1.COL1]
    assert MyTestRelation1.COL2.default == ""
    defined_types = MyTestRelation1.get_types()
    print(defined_types)
    assert defined_types[MyTestRelation1.COL1] == int
    defined_renames = MyTestRelation1.get_field_renames()
    assert defined_renames["column2"] == "column3"
