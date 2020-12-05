from pathlib import Path
from blizz import FeatureGroup, Relation
from ._helpers import all_python_modules_in_path
from typing import Iterable, Type
import inspect


def find_source_tables_on_path(basepath: Path) -> Iterable[Type[Relation]]:
    for py_module in all_python_modules_in_path(basepath):
        for (member_name, member_value) in inspect.getmembers(py_module):
            if (
                inspect.isclass(member_value)
                and issubclass(member_value, Relation)
                and _member_defined_in_module(py_module, member_value)
            ):
                yield member_value


def find_feature_groups_on_path(basepath: Path) -> Iterable[Type[FeatureGroup]]:
    for py_module in all_python_modules_in_path(basepath):
        for (member_name, member_value) in inspect.getmembers(py_module):
            if (
                inspect.isclass(member_value)
                and issubclass(member_value, FeatureGroup)
                and _member_defined_in_module(py_module, member_value)
            ):
                yield member_value


def _member_defined_in_module(module, member) -> bool:
    # todo: if this occurs, need to extract module suffix for endswith
    assert "." not in str(module.__name__)
    return str(member.__module__).endswith(str(module.__name__))
