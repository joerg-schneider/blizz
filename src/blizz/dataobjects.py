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

    @classmethod
    # todo: evaluate datatype Field() here as opposed to str
    def get_defined_types(cls) -> Dict[str, DataType]:
        """
        :return: Dictionary where a Field maps to the pandas/np/pySpark datatype
        """
        col_name_to_type = {
            f.name: f.datatype
            for f in cls.get_fields()
            if isinstance(f, Field) and f.datatype is not None
        }

        return col_name_to_type

    @classmethod
    def get_defined_key_fields(cls) -> List["Field"]:
        return [c for c in cls.get_fields() if c.key]

    @classmethod
    def get_defined_key_field_names(cls) -> List[str]:
        return [f.name for f in cls.get_defined_key_fields()]

    @classmethod
    def get_field_names(cls) -> List[str]:
        return [f.name for f in cls.get_fields()]

    @classmethod
    def get_fields(cls) -> List["Field"]:
        return [getattr(cls, d) for d in dir(cls) if isinstance(d, Field)]

    @classmethod
    def list_field_names_transformed_using_dict(
        cls, transform_dict: dict
    ) -> Iterable[str]:

        out_list = []
        for name in cls.get_field_names():
            if transform_dict.get(name) is not None:
                out_list = out_list + [transform_dict.get(name)]
            else:
                out_list = out_list + [name]

        return out_list

    @classmethod
    def get_default(cls, field: Union[str, "Field"]) -> Optional[Any]:
        """ Get the default value for a field as defined in the Relation. """
        for col in cls.get_fields():
            if col.name == field:
                return col.default

        raise ValueError(f"Field '{field}' not defined for Relation '{cls.name()}'.")

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
    # todo: add mock callable argument!
    name: str
    datatype: Type[DataType]
    default: Any
    description: str
    key: bool

    def __new__(
        cls,
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


class FieldRenames:
    """ Holding the transformation of input schema to used schema"""

    def __init__(self, input_to_used: dict):
        self.input_to_used = input_to_used

    @property
    def used_to_input(self) -> dict:
        return {val: key for key, val in self.input_to_used.items()}
