from blizz import Relation, Field
import blizz.check, blizz.apply
import pandas as pd


class Iris(Relation):

    SEPAL_LENGTH = Field("sepal_length", datatype=float)
    SEPAL_WIDTH = Field("sepal_width", datatype=float)
    PETAL_LENGTH = Field("petal_length", default=0.0)
    PETAL_WIDTH = Field("petal_width", datatype=float)
    SPECIES = Field("species", datatype=object, key=True)

    @classmethod
    @blizz.check.types
    @blizz.check.fields
    @blizz.check.keys
    @blizz.apply.defaults
    @blizz.apply.deduplication
    def load(cls) -> pd.DataFrame:
        return pd.read_csv(
            "https://gist.githubusercontent.com/curran/a08a1080b88344b0c8a7"
            "/raw/0e7a9b0a5d22642a06d3d5b9bcbad9890c8ee534/iris.csv"
        )


print(Iris.load())
