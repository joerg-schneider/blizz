from blizz import Relation, Field
import pandas as pd


class Iris(Relation):

    SEPAL_LENGTH = Field("sepal_length", datatype=float)
    SEPAL_WIDTH = Field("sepal_width", description="The Sepal length")
    PETAL_LENGTH = Field("petal_length", default=0.0)
    PETAL_WIDTH = Field("petal_width")
    SPECIES = Field("species")

    @classmethod
    def load(cls) -> pd.DataFrame:
        return pd.read_csv(
            "https://gist.githubusercontent.com/curran/a08a1080b88344b0c8a7"
            "/raw/0e7a9b0a5d22642a06d3d5b9bcbad9890c8ee534/iris.csv"
        )


# based on Iris, we can define a derived second Relation `IrisSepal`:


class IrisSepal(Relation):
    """ All records of Iris but filtered on a subset of Fields, just for the Sepal."""

    # we can simply reference the existing definitions that were made:
    SEPAL_LENGTH = Iris.SEPAL_LENGTH
    SEPAL_WIDTH = Iris.SEPAL_WIDTH
    SPECIES = Iris.SPECIES

    @classmethod
    def load(cls) -> pd.DataFrame:
        # we call Iris's load() method but filter it down to `IrisSepal`'s fields:
        return Iris.load()[cls.get_field_names()]


print(IrisSepal.load())
