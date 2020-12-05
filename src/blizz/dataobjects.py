""" The dataobjects module defines helpful Table & Column classes. """
from typing import *

from pyspark.sql import DataFrame
from pyspark.sql.types import *
from abc import abstractmethod


class Relation:
    """ Base class to be inherited by all table definitions in the project."""

    # todo: improve docstrings and consistency in naming, e.g. fields vs column!!

    @classmethod
    def name(cls) -> str:
        return cls.__name__

    # todo: for now, table just defines generic load, but Table might be subclassed into
    #       1) FileTable   2) SQLTable 3)...
    #       where the load method is predefined and uses Table properties that indicate
    #       the location!
    @classmethod
    @abstractmethod
    def load(cls) -> DataFrame:
        pass

    @classmethod
    @abstractmethod
    def mock(cls) -> DataFrame:
        pass

    class Columns:
        """ Class to be overridden by subclasses of Table. In subclass,
            define columns using all-uppercase variables of type
            Column().
            E.g.:

            class MyTable(Table):
                class Columns:
                    COL_1 = Column("this_is_the_first_column_name")
                    COL_2 = Column("the second")

            ...
        """

        pass

    @classmethod
    def get_defined_types(cls) -> Dict[str, DataType]:
        """
        :return: Dictionary where a column name (str) maps to the pandas/np datatype
        """
        list_of_all_cols = [getattr(cls.Columns, d) for d in dir(cls.Columns)]
        col_name_to_type = {
            col.name: col.datatype
            for col in list_of_all_cols
            if isinstance(col, Field) and col.datatype is not None
        }

        return col_name_to_type

    @classmethod
    def get_defined_key_fields(cls) -> List["Field"]:
        return [c for c in cls.get_columns() if c.key]

    @classmethod
    def get_defined_key_column_names(cls) -> List[str]:
        return [c.name for c in cls.get_defined_key_fields()]

    @classmethod
    def list_column_names(cls) -> List[str]:
        """ A handy function that yields all defined column names as an iterable. """
        return [c.name for c in cls.get_columns()]

    @classmethod
    def get_columns(cls) -> List["Field"]:
        """ A handy function that yields all defined columns as an iterable. """
        return [getattr(cls, d) for d in dir(cls) if isinstance(d, Field)]

    @classmethod
    def list_column_names_transformed_using_dict(
        cls, transform_dict: dict
    ) -> Iterable[str]:
        """ A function to convert the Schema columns names using a dict.
        This is handy when reading selected columns using parquet """
        list_of_columns_names = cls.list_column_names()
        out_list = []
        for name in list_of_columns_names:
            if transform_dict.get(name) is not None:
                out_list = out_list + [transform_dict.get(name)]
            else:
                out_list = out_list + [name]

        return out_list

    @classmethod
    def get_default(cls, column_name: str) -> Optional[Any]:
        """ Yield the default value for a column as defined in the Schema. """
        for col in cls.get_columns():
            if col.name == column_name:
                return col.default

        raise ValueError(f"Column name '{column_name}' not defined in Schema.")

    # todo: do this in datachecks.py!
    @classmethod
    def verify_schema(cls, data: DataFrame):
        for t_column in cls.get_columns():
            if t_column.name not in data.columns:
                raise ValueError(
                    f"Column '{t_column.name}' is not part of passed dataframe."
                )

    @classmethod
    def equals(cls, other: Type["Relation"]) -> bool:
        # 1. same number of fields
        if len(cls.get_columns()) != len(other.get_columns()):
            return False
        else:
            this_fields = {f.name: f for f in cls.get_columns()}
            other_fields = {f.name: f for f in other.get_columns()}

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
    # todo: add mock callable argument!
    name: str
    datatype: Type[DataType]
    default: Any
    description: str
    key: bool

    def __new__(
        self,
        name: str,
        datatype: Type[DataType] = None,
        default: Any = None,
        description: str = None,
        key: bool = None,
    ):

        description_ = description
        name_ = name
        default_ = default
        datatype_ = datatype
        key_ = key

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

        return Field_(name)


class ColumnRenames:
    """ Holding the transformation of input schema to used schema"""

    def __init__(self, input_to_used: dict):
        self.input_to_used = input_to_used

    @property
    def used_to_input(self) -> dict:
        return {val: key for key, val in self.input_to_used.items()}
