from blizz import Relation, Field
import pandas as pd


class Iris(Relation):

    SEPAL_LENGTH = Field("sepal_length")
    SEPAL_WIDTH = Field("sepal_width")
    PETAL_LENGTH = Field("petal_length")
    PETAL_WIDTH = Field("petal_width")
    SPECIES = Field("species")

    @classmethod
    def load(cls) -> pd.DataFrame:
        return pd.read_csv(
            "https://gist.githubusercontent.com/curran/a08a1080b88344b0c8a7"
            "/raw/0e7a9b0a5d22642a06d3d5b9bcbad9890c8ee534/iris.csv"
        )


# calling load(), we can retrieve a dataframe for the Relation:
iris_df = Iris.load()

# using the Relation's Schema, we can access/modify iris_df referencing fields we like:
print(iris_df[[Iris.SEPAL_LENGTH, Iris.SEPAL_WIDTH]])
