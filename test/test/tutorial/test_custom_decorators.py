from typing import Type

import numpy as np
import pandas as pd

import blizz.apply
import blizz.check
from blizz import Relation, Field


def my_custom_apply(relation: Type[Relation], data: pd.DataFrame) -> pd.DataFrame:
    """ Example of a custom apply – round all Fields defined as floats """

    # example use of the relation parameter: get all Fields defined as float
    float_fields = [
        field
        for field, data_type in relation.get_types().items()
        if field.datatype in (float, "float")
    ]

    for float_field in float_fields:
        data = data.assign(**{float_field: data[float_field].round(decimals=0)})

    return data


def my_custom_check(relation: Type[Relation], data: pd.DataFrame) -> pd.DataFrame:
    """ Example of a custom check – check that all numeric fields are rounded. """

    rounded = data.select_dtypes(include=np.number).round(decimals=0)
    not_rounded = data.select_dtypes(include=np.number)

    # a simple assertion suffices –
    #  the 'on_fail=' argument on the decorator defines the behaviour
    assert rounded.equals(
        not_rounded
    ), f"Not all numeric data for {relation.name()} is rounded."

    return data


class Iris(Relation):

    SEPAL_LENGTH = Field("sepal_length", datatype=float)
    SEPAL_WIDTH = Field("sepal_width", datatype=float)
    PETAL_LENGTH = Field("petal_length", default=0.0)
    PETAL_WIDTH = Field("petal_width", datatype=float)
    SPECIES = Field("species_renamed", datatype=object, key=True, source_name="species")

    @classmethod
    @blizz.check.func(function=my_custom_check, on_fail=blizz.check.WARN)
    @blizz.apply.func(function=my_custom_apply)
    def load(cls) -> pd.DataFrame:
        return pd.read_csv(
            "https://gist.githubusercontent.com/curran/a08a1080b88344b0c8a7"
            "/raw/0e7a9b0a5d22642a06d3d5b9bcbad9890c8ee534/iris.csv"
        )


# note: the above will (demonstratively) fail the check, as `PETAL_LENGTH` was not defined
#       as float and hence not rounded
def test_custom_decorators():
    print(Iris.load())
