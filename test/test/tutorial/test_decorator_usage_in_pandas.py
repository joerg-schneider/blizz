import pandas as pd

import blizz.apply
import blizz.check
from blizz import Relation, Field


class Iris(Relation):

    SEPAL_LENGTH = Field("sepal_length", datatype=float)
    SEPAL_WIDTH = Field("sepal_width", datatype=float)
    PETAL_LENGTH = Field("petal_length", default=0.0)
    PETAL_WIDTH = Field("petal_width", datatype=float)
    SPECIES = Field("species_renamed", datatype=object, key=True, source_name="species")

    @classmethod
    @blizz.check.types
    @blizz.check.fields
    @blizz.check.keys
    @blizz.apply.defaults(fill=PETAL_LENGTH)  # you can use field as arguments, too!
    @blizz.apply.deduplication
    @blizz.apply.renames  # renames should be applied first
    def load(cls) -> pd.DataFrame:
        return pd.read_csv(
            "https://gist.githubusercontent.com/curran/a08a1080b88344b0c8a7"
            "/raw/0e7a9b0a5d22642a06d3d5b9bcbad9890c8ee534/iris.csv"
        )


def decorator_usage_in_pandas():
    print(Iris.load())
