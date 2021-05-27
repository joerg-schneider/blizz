blizz – _be blizzful_
=====================
**effortless Python table schema management**


If you are looking for…

📋 …an lightweight option to capture data schemas and field annotations for either Pandas or PySpark,

⭐ …which makes table definitions available not just at runtime, but already while developing code
– with IDE type checking and autocomplete!

❄️ …which allows dynamic schema inheritance/reference, to express several levels of derived data assets –
as common in stable data pipelines – tracing the full data field lineage across your project and boosting
refactoring and debugging productivity,

🚀 …which offers pre-built PySpark & Pandas decorators for most common data checks & transformations – all based
on metadata,

🏃 …which supports your workflow with boostrapping tools to generate basic code and Sphinx documentations

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
        - [API Reference on blizz.check & blizz.apply](#api-reference-on-blizzcheck--blizzapply)
    - [Boostrapping – generate schema definitions from DataFrames](#bootstrapping--generate-schema-definitions-from-dataframes)    
    - [Beyond: blizz Feature Store](#beyond-blizz-feature-library)
    - [Beyond: Generate Documentation](#beyond-generate-documentation)
    
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
  before fields get renamed – such renaming can be done by `blizz.apply.renames`
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

### API Reference on blizz.check & blizz.apply

`blizz.check` offers the following decorators for checking:

- `blizz.check.fields`: validates on the DataFrame returned by `load()`, that it contains at least all fields 
  (by name) as defined on the Relation
- `blizz.check.types`: validates that all fields that have defined data types on the Relation match with their
  data types as in the DataFrame returned by `load()`
- `blizz.check.keys`: validates that the subset of all Fields marked as keys is unique

All `blizz.check` decorators accept the argument `on_fail`, which can be set to:
- `blizz.check.WARN`: raise a warning on failing the check
- `blizz.check.RAISE`: raise an exception on failing the check

`blizz.apply` offers the following decorators for transformations:
- `blizz.apply.deduplication`: deduplicate the DataFrame returned by `load()` either by the Fields already marked
  as keys of the Relation, or the subset of Field given as argument `key:List[str]` to the decorator (which
  has precedence). Additionally the
  decorator supports the arguments `sort_on:List[str]` and `sort_order` (which can be either `blizz.apply.ASC` or 
  `blizz.apply.DESC`) – this can ensure which non-key records are preferrably kept on duplication. E.g. when you
  find a duplicate based on the key-column, then pick among those duplicates the most recently updated row 
  – here one might use something like: `sort_on = ["update_timestamp"], sort_order = blizz.apply.DESC`.
- `blizz.apply.defaults`: fill defined defaults for NULL values in the DataFrame returned by `load()`, either
  using the defined value defaults from the Relation's Fields (if exist) or as given with as the argument
  `fill: List[str]` to the decorator (which has precedence).
- `blizz.apply.renames`: perform Field renames for columns in the DataFrame as returned by `load()`, either
  using the defined pairs of `Field.source_name` -> `Field.name` (where exists) or using the rename definitions
  as given as argument `columns:Dict[str,str]` to the decorator, which will be added to the renames to perform.


## Bootstrapping – generate schema definitions from DataFrames

As you've seen above, _blizz_ can be a powerful tool in your Python data pipelines. However it needs properly
defined Relations, which can be tedious at the start.

For this, _blizz_ can boostrap Relations from existing PySpark or Pandas DataFrames using 
`blizz.relation_from_dataframe()`.

```shell
>>> blizz.relation_from_dataframe(df=departments_df, name="Departments")

import blizz.check
from blizz import Relation, Field
from pyspark.sql import DataFrame
from pyspark.sql.types import *


class Departments(Relation):
    """
    todo: describe relation Departments in this docstring
    """
    DEPARTMENT_ID = Field(name="Department_ID", datatype=StringType)
    DEPARTMENT_NAME = Field(name="Department_Name", datatype=StringType)
    DOE = Field(name="DOE", datatype=TimestampType)

    @classmethod
    @blizz.check.keys(on_fail = blizz.check.WARN)
    @blizz.check.types(on_fail = blizz.check.WARN)
    @blizz.check.fields(on_fail = blizz.check.WARN)
    def load(cls) -> DataFrame:
        """
        todo: describe relation Departments load process in this docstring
        """
        # todo: implement data source loading here
        pass
```

You can then simply copy and paste and adjust this as needed.

## Beyond: blizz Feature Library
**EXPERIMENTAL – only supported with PySpark**

On top of __blizz__'s primitives, the plan is to build a lightweight Feature Library component for
Machine Learning. There is an example of this experimental API included as part of
[src/tutorial](src/tutorial/example_feature_library). Conceptually the API allows to express 
features programmatically, but in a well structured way using lightweight classes. This makes it 
possible then, to decouple the feature generation from the pure feature definition.

`blizz` has also a CLI – serving as a client to instruct a PySpark process which features to build,
all based on a defined blizz Feature Library.

In order to do this, one:

1. defines a Feature Library in code as done with
   [src/tutorial/example_feature_library](src/tutorial/example_feature_library)
2. defines a feature list definition: this is a YAML file of all FeatureGroups/Features one wants to
   have calculated, being also able to specify parameters for parameterizable features and to
   specify the output storage format. An example is given here
   [src/tutorial/ExampleFeatureList.yaml](src/tutorial/ExampleFeatureList.yaml)
3. calls the `blizz` CLI with both and the output path: 
   `blizz build src/tutorial/ExampleFeatureList.yaml src/tutorial/example_feature_library my_features`

This brings together the code-based definition of all data sources (by __blizz__ primitives) and features (by __blizz__ 
FeatureGroup & Feature) with the instruction in the form of the YAML file what and how (parameterized features)
to calculate.

**Why is this approach powerful?**

1. how to retrieve data sources, check them, and build features on-top is purely captured in code. This means it
  is **fully supported by IDE type checks, autocompletion and refactoring abilities.**
2. due to this code centric means of definition, the **whole end to end pipeline is easily managed and versioned** – 
   as a Python package.
3. expressing ML features in a structured way allows to leverage a **common, powerful Spark based factory** to build 
   them – whoever wants features, only needs to call `blizz build ...` with their YAML build instructions and
   pointing to a blizz Feature Library codebase (which can be checked out at a version as desired). As internally,
   _blizz_ pipelines are just nested function calls on top of `pyspark.sql`, this **heavily benefits from both
   lazy evaluation and execution plan optimizations**. This happens completely transparently thanks to Spark.
4. in this way, definitions to calculate features can be centralized and shared purely on the code level. Consumers
   of features, e.g. specialized ML models, just need to install `blizz`, write YAML build instructions and point
   to the FeatureLibrary – **their model does not even need to be in Python**.

## Beyond: Generate Documentation

Thanks to its rich metadata about Relations, Fields and Features, the task of documentation becomes easier
for data practitioners. On top of structured information serving as documentation, Python docstrings will
be automatically used, e.g. on class level underneath a `Relation`, when describing `Relation.load()` and
similarly for `FeatureGroup` and `Feature`.

The philosophy here is, not write documentation twice – e.g. in code and somewhere else – but rather to
make it code driven as well.

A nice (experimental) feature is the following, which will create a Sphinx project from a `blizz.FeatureGroup`'s
metadata and serve it:

```shell
blizz docs src/tutorial/example_feature_library --serve
```
