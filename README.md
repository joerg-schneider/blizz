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

**Contents:**

- [Installation](#installation)
- [Usage Tutorial](#usage-tutorial)
    - [blizz primitives: Relation and Field](#blizz-primitives-relation-and-field)
        - [Basic usage with pandas](#basic-usage-with-pandas)
        - [Using field definitions](#using-field-definitions-inside-of-load)
        - [Defining metadata properties](#defining-metadata-properties-on-field)
        - [Relation Hierarchies](#relation-hierarchies)
        - [Making use of metadata declarations: blizz.check & blizz.apply](#making-use-of-metadata-declarations-blizzcheck--blizzapply)
        - [Using blizz with Pyspark](#using-blizz-with-pyspark)
        - [API Reference on Relation and Field](#api-reference-on-relation-and-field)
    - [blizz Feature Store](#blizz-feature-library)
    
# Installation
You can install the latest stable version of _blizz_ simply using Pip:

``pip install blizz``

Note, that either `pandas` or `pyspark` need to be installed separately, if you do
not have them already.

If you have any issues, questions or suggestions – do not hesitate to open an GitHub issue.

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
        iris_dedup = iris.drop_duplicates(subset=cls.get_key_fields())
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
In the example above, the `load()` method additionally shows that each `Relation` offers helpful
methods to interact with metadata – in this case `get_field_names()` can retrieve all defined
fields as strings. See _API Reference on Relation and Field_ for the full list of available
methods.

### Making use of metadata declarations: blizz.check & blizz.apply

Having defined structured metadata on a Relation's fields can be very powerful to carry out
basic operations.

For this purpose, `blizz` defines two modules with Python function decorators, meant to be
added on-top of the `load()` method:

- `blizz.check`: defines several utilities to check a loaded Relation against the definition. 
- `blizz.apply`: defines several utilities for often occuring transformations to the Relation's 
  dataframe

All of these utlities work are implemented with equal functionality on Pandas and Pyspark.

Consider this example, which takes care of schema checking (field names, field types, field keys),
deduplication and checking for duplicates:
```python
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
```

It is important to note, that these decorator utilities __execute after the load() function
has finished__, and the order of writing them matters. As an example, it makes more sense
to execute `blizz.apply.deduplication` first, and then check its result with `blizz.check.keys`
(unless you already expect it deduplicated, then avoid `blizz.apply.deduplication` entirely!).

In this use case, your decorators have to be defined in this order (lowest one runs first):

```python
    @classmethod
    @blizz.check.keys
    @blizz.apply.deduplication
    def load(cls)->pd.DataFrame:
        pass
```

### Using blizz with PySpark

Using _blizz_ with PySpark works very similarly to the examples shown above – the only difference
lies in using different datatypes for _Field_ definitions (either instances of `pyspark.sql.types.DataType`
or the PySpark datatypes `simpleString` representation (e.g. "int", "string", ...)), and of course
having to implement `load()` on each Relation differently, to return a `pyspark.sql.DataFrame`:

```python
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
```

### API Reference on Relation and Field

The `Field()` constructor accepts the following arguments (besides `name`, all optional):

- `name`: the target field name, referencing a field in the Relation as retrieved by `load()`. If
  you want to retrieve an originally differently name field, then capture its source name in 
  `source_name`
- `source_name`: the field name as in the source – useful for `load()` to retrieve the Relation,
  before fields get renamed – such renaming can be done by `blizz.apply.rename`
- `datatype`: the Field's datatype – for Pandas, can be defined as Python built-in, numpy type, or
  as a String value ("int"). For PySpark, use `pyspark.sql.types.DataType` or the PySpark datatypes
  `simpleString` representation (e.g. "int", "string", ...).
- `default`: allows one to define a literal as a default value on NULL
- `description`: allows one to provide a string as a description/comment on the Field
- `key`: a boolean flag indicating if this field is part of the primary key
- `mock`: allows one to define a callable mocking data for this Field – useful for testing of Relation's
    with synthetic data.

Each `Relation` defines the following useful methods on class level:

- `Relation.get_fields()`: returns all _Fields_ as defined
- `Relation.get_field_names()`: returns all _Fields_ names as defined
- `Relation.get_types()`: returns all _Fields_ datatypes
- `Relation.get_key_fields()`: returns all _Fields_, defined as key
- `Relation.get_key_field_names()`: returns all names of _Fields_, defined as key
- `Relation.get_defaults()`: return a Dictionary mapping from _Field_ to its default value, where defined  
- `Relation.get_default(<Field>)`: return the default value for a _Field_
- `Relation.mock()`: to be overridden by each `Relation` – allows one to define a mocked version of `load()`
to produce synthetic data for testing.

Of course it is entirely possibly, to define a subclass of `Relation` for your own purposes, which 
deals with common use-cases by own class functions it adds! For instance, you might want to create
a `SQLRelation` which already bundles useful methods or definitions to allow you quickling 
retrieving these kind of Relations from a SQL RDBMS.

## blizz Feature Library
**EXPERIMENTAL – only supported with PySpark**

