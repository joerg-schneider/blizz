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
Since `load()` is a class method, it is easily possible to refer to defined __Fields__ already in it:

```python
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
        iris[cls.SEPAL_PETAL_WIDTH] = iris[cls.SEPAL_WIDTH] + iris[cls.PETAL_WIDTH]
        return iris
```
In this example, one can see how to add another, derived field to the _Relation_ which is just
built on loading, leveraging existing _Field_ references.

### Defining Metadata properties on Field
`Field` is implemented in an elegant way: It cleanly folds down to a Python string when you use it as part
of an expression (e.g. inside of square brackets for Pandas), but actually, _it is not a string_.

In the previous examples, _Fields_ have been only constructed using its default first attribute, `.name`. 
However, it does also support a big variety of useful declarations one might like to make (see
`API Reference on Relation and Field` below!), such as defining datatypes, adding a description or
the _Field's_ default value:

```python
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
        iris_dedup = iris.drop_duplicates(subset=cls.get_defined_key_fields())
        return iris_dedup
```

### Relation hierarchies
Since Fields are Python variables (class members of `Relation`), this makes building connected Relations very easy:

```python
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
```

### API Reference on Relation and Field

# FAQs
