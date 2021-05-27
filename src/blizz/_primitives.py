from abc import abstractmethod
from typing import Union, Type, Dict, Any, Optional, List, Iterable, Callable

from blizz._helpers import pyspark, pandas

try:
    import pyspark
except ImportError:  # pragma: no cover
    pyspark = None  # pragma: no cover

try:
    import pandas
except ImportError:  # pragma: no cover
    pandas = None  # pragma: no cover

try:
    import numpy
except ImportError:  # pragma: no cover
    numpy = None  # pragma: no cover


class Relation:
    """
    Base class of all blizz Relation definitions. Relation definitions in blizz
    are a core primitive capturing a Relation's Metadata through defining blizz Fields
    on class level. A relation, in the computer scientific sense, refers here to any
    table/dataframe/... one likes to define made up of rows and columns, the latter
    defined by blizz Fields. DataFrames from PySpark and Pandas are the supported
    means of capturing data instances to which a given Relation relates to.

    To establish this link, each blizz Relation needs the abstract class method
    ``Relation.load()`` to be implemented/overriden, which shall return
    either a PySpark or Pandas dataframe.

    Decorators of packages `blizz.check` and `blizz.apply` exist to coveniently
    leverage the Relation's definition on the execution of `Relation.load()`.

    These allow to respectively run check (for field existance, field types,
    record duplication) and apply (field renames, field default values,
    record deduplication) functions that can make use of the Relations
    schema definition and metadata.
    """

    @classmethod
    def name(cls) -> str:
        return cls.__name__

    # todo: for now, Relation just defines generic load, but Relation might be subclassed into
    #       1) FileRelation   2) SQLRelation 3)...
    #       where the load method is predefined and uses Relation properties that indicate
    #       the location!
    @classmethod
    @abstractmethod
    def load(cls, *args, **kwargs) -> Union["pyspark.sql.DataFrame", "pandas.DataFrame"]:
        """
        Method loading a dataframe for the given Relation – to be implemented by each subclass
        of `Relation`.
        :return: a PySpark or Pandas Dataframe
        """
        pass

    @classmethod
    def mock(cls) -> Union["pyspark.sql.DataFrame", "pandas.DataFrame"]:
        """
        Returns mock data for the given Relation – if needed, to be implemented by a
        subclass of `Relation`
        :return: a PySpark or Pandas Dataframe with mocked data
         matching the Relation's schema
        """
        pass

    @classmethod
    def get_types(
        cls
    ) -> Dict["Field", Union[Type["pyspark.sql.types.DataType"], "numpy.dtype", "str"]]:
        """
        :return: Dictionary where a Field maps to the pandas/np/PySpark datatype
        """
        col_name_to_type = {
            f: f.datatype
            for f in cls.get_fields()
            # check if "Field":
            if "blizz._primitives.Field.__" in str(type(cls.get_fields()[0]))
            and f.datatype is not None
        }

        return col_name_to_type

    @classmethod
    def get_field_renames(cls) -> Dict["str", "str"]:
        """
        :return: Dictionary [str,str] from source field name to target field name
        """
        source_field_name_to_target = {
            f.source_name: f.name
            for f in cls.get_fields()
            # check if "Field":
            if "blizz._primitives.Field.__" in str(type(cls.get_fields()[0]))
            and f.source_name is not None
        }

        return source_field_name_to_target

    @classmethod
    def get_key_fields(cls) -> List["Field"]:
        """
        Returns all fields of this Relation defined as keys.
        :return: a List of blizz Fields.
        """
        return [c for c in cls.get_fields() if c.key]

    @classmethod
    def get_key_field_names(cls) -> List[str]:
        """
        Returns all field names of this Relation defined as keys.
        :return: a List of field names as strings.
        """
        return [f.name for f in cls.get_key_fields()]

    @classmethod
    def get_field_names(cls) -> List[str]:
        """
        Return all defined field names of this Relation.
        :return: a List of strings with field names
        """
        return [f.name for f in cls.get_fields()]

    @classmethod
    def get_fields(cls) -> List["Field"]:
        """
        Return all defined blizz Fields of this Relation.

        :return: a List of blizz Fields
        """
        return [
            getattr(cls, d)
            for d in dir(cls)
            if getattr(cls, d).__class__.__name__ == "Field_"
        ]

    @classmethod
    def get_default(cls, field: Union[str, "Field"]) -> Optional[Any]:
        """
        Get the default row value for a Field as defined in this Relation.
        :param field: the blizz Field or Field name to fetch
        :return: the default row value as defined
        """
        for col in cls.get_fields():
            if col.name == field:
                return col.default

        raise ValueError(f"Field '{field}' not defined for Relation '{cls.name()}'.")

    @classmethod
    def get_defaults(cls) -> Dict["Field", Any]:
        """
        Get a mapping of all defined default row values for this Relation, where
        the blizz Field maps to its defined default.
        :return: a dictionary mapping from blizz Field to default value
        """
        return {
            field: field.default
            for field in cls.get_fields()
            if field.default is not None
        }

    @classmethod
    def equals(cls, other: Type["Relation"]) -> bool:
        # 1. same number of fields
        if len(cls.get_fields()) != len(other.get_fields()):
            return False
        else:
            this_fields = {f.name: f for f in cls.get_fields()}
            other_fields = {f.name: f for f in other.get_fields()}

            for field_name, field in this_fields.items():
                if field_name not in other_fields:
                    return False
                elif field != other_fields[field_name]:
                    return False

        return True

    @classmethod
    def contained_in(cls, collection: Iterable[Type["Relation"]]) -> bool:
        for t in collection:
            if cls.equals(t):
                return True
        return False


class Field(str):
    """ A blizz Field captures known metadata on a field which is part of
    a blizz Relation.
    """
    name: str
    datatype: Union[Type["pyspark.sql.types.DataType"], "numpy.dtype", "str"]
    default: Any
    description: str
    key: bool
    source_name: str
    mock: Callable[[], Any]

    def __new__(
        cls,
        name: str,
        datatype: Union[Type["pyspark.sql.types.DataType"], "numpy.dtype", str] = None,
        default: Any = None,
        description: str = None,
        key: bool = None,
        source_name: str = None,
        mock_func: Callable[
            [], Any
        ] = None,
    ):

        description_ = description
        name_ = name
        default_ = default
        datatype_ = datatype
        key_ = key
        source_name_ = source_name
        mock_func_ = mock_func

        class Field_(str):
            @property
            def description(self):
                return description_

            @property
            def default(self):
                return default_

            @property
            def datatype(self):
                return datatype_

            @property
            def key(self):
                return key_

            @property
            def name(self) -> str:
                return name_

            @property
            def source_name(self) -> str:
                return source_name_

            @property
            def mock_func(
                self
            ) -> Callable[[], Any]:
                return mock_func_

        return Field_(name)


def check_dataframe_type(data: Any, relation: Optional[Type[Relation]]) -> str:
    if pyspark is not None and isinstance(data, pyspark.sql.DataFrame):
        return "pyspark"
    elif pandas is not None and isinstance(data, pandas.DataFrame):
        return "pandas"
    else:
        relation_name = (
            relation.name() if issubclass(relation, Relation) else str(relation)
        )
        raise ValueError(
            f"Unsupported Python instance of type {data.__class__} for relation '{relation_name}'"
        )


def is_pyspark_df(data: Any, relation: Optional[Type[Relation]]) -> bool:
    return check_dataframe_type(data, relation) == "pyspark"


def is_pandas_df(data: Any, relation: Optional[Type[Relation]]) -> bool:
    return check_dataframe_type(data, relation) == "pandas"
