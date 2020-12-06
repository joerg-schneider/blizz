import re
from itertools import chain
from glob import glob
import os
from pathlib import Path
import importlib.util
import importlib
from typing import Any, Iterable
import logging


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


def setup_logger(level: int = logging.INFO):
    """Set up a basic logger
    :param level: desired log level
    """

    logging.basicConfig(
        level=level,
        datefmt="%Y-%m-%d %H:%M:%S",
        format="[%(asctime)s] %(name)s:%(lineno)d %(levelname)s: %(message)s",
    )
