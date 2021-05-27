from blizz import Relation, Field
import pandas as pd


class Iris(Relation):
    # setting some example additional Field attributes:
    # 1. of course most importantly, one can set a datatype to complete the
    #    Relation's schema definition in the classical sense:
    SEPAL_LENGTH = Field("sepal_length", datatype=float)

    # for use with Pandas, `datatype` accepts Python inbuilts (e.g. float, object, int),
    # quoted names of Pandas datatypes, and also instances of numpy types, such as
    # numpy.int, numpy.number, etc.

    # 2. to capture metadata, and for documentation purposes, a description can be set:
    SEPAL_WIDTH = Field("sepal_width", description="The Sepal length")

    # 3. `default` also to capture a default row value for a Field:
    PETAL_LENGTH = Field("petal_length", default=0.0)
    PETAL_WIDTH = Field("petal_width")

    # 4. the boolean flag `key` allows to specify key fields:
    SPECIES = Field("species", key=True)

    @classmethod
    def load(cls) -> pd.DataFrame:
        iris = pd.read_csv(
            "https://gist.githubusercontent.com/curran/a08a1080b88344b0c8a7"
            "/raw/0e7a9b0a5d22642a06d3d5b9bcbad9890c8ee534/iris.csv"
        )
        # using all defined key fields, one could run a simply deduplication command:
        iris_dedup = iris.drop_duplicates(subset=cls.get_key_fields())
        return iris_dedup


# calling load(), we can retrieve a dataframe for the Relation:
iris_df = Iris.load()

print(iris_df)
