import functools
import importlib
import importlib.util
import logging
import os
import re
from glob import glob
from itertools import chain
from pathlib import Path
from typing import Any, Iterable

logger = logging.getLogger(__name__)

try:
    import pyspark
except ImportError:
    pyspark = None

try:
    import pandas
except ImportError:
    pandas = None


def camel_case_to_snake(name: str) -> str:
    # find all switches from lower to upper
    switch_indices = []
    for index, c in enumerate(name):
        c: str = c
        if index == 0:
            continue

        if c.isupper() and name[index - 1].islower():
            switch_indices.append(index)

    # insert underscores where switch occurred
    for nth_time_insert, insert_at_original_index in enumerate(switch_indices):
        name = (
            name[0 : insert_at_original_index + nth_time_insert]
            + "_"
            + name[insert_at_original_index + nth_time_insert :]
        )

    return name.lower()


def safe_name(name: str) -> str:
    return re.sub(r"[.\s-]", "_", name)


def recurse_dir_tree(base: Path) -> Iterable[Path]:
    return (
        Path(p)
        for p in chain.from_iterable(
            glob(os.path.join(x[0], "*")) for x in os.walk(base.as_posix())
        )
    )


def import_from_path(module_path: Path) -> Any:
    spec = importlib.util.spec_from_file_location(
        os.path.basename(module_path.as_posix()).replace(".py", ""), module_path
    )

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def all_python_modules_in_path(basepath: Path) -> Iterable[Any]:
    py_modules = [f for f in recurse_dir_tree(base=basepath) if str(f).endswith(".py")]
    return (import_from_path(py_module) for py_module in py_modules)


def pandas_dtype_to_spark_type():
    pass


def doublewrap(f):
    """
    a decorator decorator, allowing the decorator to be used as:
    @decorator(with, arguments, and=kwargs)
    or
    @decorator
    """

    @functools.wraps(f)
    def new_dec(*args, **kwargs):
        if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
            # actual decorated function
            return f(args[0])
        else:
            # decorator arguments
            return lambda realf: f(realf, *args, **kwargs)

    return new_dec
