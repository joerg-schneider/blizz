from pyspark import SparkFiles

from blizz import Relation, Field
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType

import blizz.check


class Iris(Relation):

    SEPAL_LENGTH = Field("sepal_length", datatype=DoubleType)
    SEPAL_WIDTH = Field("sepal_width", datatype="double")
    PETAL_LENGTH = Field("petal_length", datatype=DoubleType)
    PETAL_WIDTH = Field("petal_width", datatype=DoubleType)
    SPECIES = Field("species", datatype="string")

    @classmethod
    @blizz.check.types
    @blizz.check.fields
    def load(cls, spark_session) -> DataFrame:

        spark_session.sparkContext.addFile(
            "https://gist.githubusercontent.com/curran/a08a1080b88344b0c8a7"
            "/raw/0e7a9b0a5d22642a06d3d5b9bcbad9890c8ee534/iris.csv"
        )
        df = spark_session.read.csv(
            SparkFiles.get("iris.csv"), inferSchema=True, header=True
        )

        return df


# set up a simple spark session:
spark = SparkSession.builder.getOrCreate()
# calling load(), we can retrieve a dataframe for the Relation:
iris_df = Iris.load(spark)
print(iris_df)
# using the Relation's Schema, we can access/modify iris_df referencing fields we like:
iris_df.select(Iris.SEPAL_WIDTH, Iris.SEPAL_LENGTH).show()
spark.stop()
