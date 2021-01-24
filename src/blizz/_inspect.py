import functools
import inspect
from pathlib import Path
from typing import Iterable, Type

from blizz import FeatureGroup, Relation
from ._helpers import all_python_modules_in_path


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


def get_class_that_defined_method(meth):
    if isinstance(meth, functools.partial):
        return get_class_that_defined_method(meth.func)
    if inspect.ismethod(meth) or (
        inspect.isbuiltin(meth)
        and getattr(meth, "__self__", None) is not None
        and getattr(meth.__self__, "__class__", None)
    ):
        for cls in inspect.getmro(meth.__self__.__class__):
            if meth.__name__ in cls.__dict__:
                return cls
        meth = getattr(meth, "__func__", meth)  # fallback to __qualname__ parsing
    if inspect.isfunction(meth):
        cls = getattr(
            inspect.getmodule(meth),
            meth.__qualname__.split(".<locals>", 1)[0].rsplit(".", 1)[0],
            None,
        )
        if isinstance(cls, type):
            return cls
    return getattr(meth, "__objclass__", None)  # handle special descriptor objects
