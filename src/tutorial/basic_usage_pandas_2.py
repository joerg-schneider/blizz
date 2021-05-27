from blizz import Relation, Field
import pandas as pd


class Iris(Relation):
    SEPAL_LENGTH = Field("sepal_length")
    SEPAL_WIDTH = Field("sepal_width")
    PETAL_LENGTH = Field("petal_length")
    PETAL_WIDTH = Field("petal_width")
    SPECIES = Field("species")

    # a newly defined field:
    SEPAL_PETAL_WIDTH = Field("sepal_petal_width")

    @classmethod
    def load(cls) -> pd.DataFrame:
        iris = pd.read_csv(
            "https://gist.githubusercontent.com/curran/a08a1080b88344b0c8a7"
            "/raw/0e7a9b0a5d22642a06d3d5b9bcbad9890c8ee534/iris.csv"
        )
        # calculating the new field, and using references from Iris:
        iris[cls.SEPAL_PETAL_WIDTH] = iris[cls.SEPAL_WIDTH] + iris[cls.PETAL_WIDTH]
        return iris


# calling load(), we can retrieve a dataframe for the Relation:
iris_df = Iris.load()

print(iris_df)
