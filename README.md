blizz – _be blizzful_
=====================
**effortless Python table schema management**


If you are looking for…

:clipboard: …an lightweight option to capture data schemas and field annotations for either Pandas or PySpark,

:star: …which makes table definitions available not just at runtime, but already while developing code
– with IDE type checking and autocomplete!

:snowflake: …which allows dynamic schema inheritance/reference, to express several levels of derived data assets –
as common in stable data pipelines – tracing the full data field lineage across your project and boosting
refactoring and debugging productivity

:rocket: …which offers pre-built PySpark & Pandas decorators for most common checks & transformations – all based
on metadata

then **blizz** is for you!
# Installation
You can install the latest stable version of _blizz_ simply using Pip:

``pip install blizz``

Note, that either `pandas` or `pyspark` need to be installed separately, if you do
not have them already.

A test installation from the latest sources is possible as well:

``pip install git+https://github.com/joerg-schneider/blizz/#egg=blizz``

# Usage Tutorial
The following is an introduction and basic tutorial to _blizz_. You can find also some example scripts in
[src/tutorial](src/tutorial) of this repository.

## blizz primitives: **Relation** and **Field**
`blizz.Relation` and `blizz.Field` are the main primitives implementing its behaviour. 

A relation – in the computer scientific sense – refers here to any table/dataframe/... one
likes to define, made up of rows and columns, the latter defined by `blizz.Field`.
DataFrames from PySpark and Pandas are the supported means of capturing data instances to
which a given _Relation_ applies to.

The following sections explain how to use `blizz.Relation` and `blizz.Field`.

### Basic usage with Pandas

The following is a minimal example of creating a Relation with _blizz_:

- define your _Relation_ as a subclass from _blizz's_ `Relation` superclass
- add some field names as members
- add `load()` as a classmethod and implement it to return either a Spark or Pandas DataFrame

```python
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
```


Calling `load()` on the Relation, we can retrieve a dataframe:
```python
iris_df = Iris.load()
```

Most importantly, now using the Relation's Schema, we can access/modify `iris_df` referencing fields we like:
```python
print(iris_df[[Iris.SEPAL_LENGTH, Iris.SEPAL_WIDTH]])
```

A command like above now references the member variables on Iris to access fields – these will also be autocompleted
by your IDE, checked if they exist (avoiding typos) and allow easy refactoring (e.g. if the column name changes in
the source – just change it once in `Field(...)`!).

### Using Field definitions inside of load()



# FAQs
