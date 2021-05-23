import functools
import inspect
import logging
import warnings
from typing import Union

from blizz import _inspect
from ._helpers import doublewrap
from ._primitives import Relation, Type, is_pandas_df, is_pyspark_df

try:
    import pyspark
except ImportError:  # pragma: no cover
    pyspark = None  # pragma: no cover

try:
    import pandas
except ImportError:  # pragma: no cover
    pandas = None  # pragma: no cover

logger = logging.getLogger(__name__)

WARN = "warn"
RAISE = "raise"


def _field_existence(
    r: Type[Relation], data: Union["pyspark.sql.DataFrame", "pandas.DataFrame"]
):
    if is_pyspark_df(data, r):
        data: pyspark.sql.DataFrame = data
        for t_column in r.get_fields():
            if t_column.name not in data.columns:
                raise ValueError(
                    f"Field '{t_column.name}' is not part of loaded Relation '{r.name()}'."
                )
    elif is_pandas_df(data, r):
        data: pandas.DataFrame = data
        for t_column in r.get_fields():
            if t_column.name not in data.columns:
                raise ValueError(
                    f"Field '{t_column.name}' is not part of loaded Relation '{r.name()}'."
                )

    logger.info(f"Relation {r.name()} has passed the field existance check.")


def _field_types(
    r: Type[Relation], data: Union["pyspark.sql.DataFrame", "pandas.DataFrame"]
):
    if is_pyspark_df(data, r):
        data: pyspark.sql.DataFrame = data
        for t_column in r.get_fields():
            if t_column.name in data.columns:
                if t_column.datatype is not None:
                    for spark_name, spark_type in data.dtypes:
                        if spark_name == t_column.name and not (
                            (t_column.datatype == spark_type)
                            or (
                                inspect.isclass(t_column.datatype)
                                and issubclass(
                                    t_column.datatype, pyspark.sql.types.DataType
                                )
                                and t_column.datatype().simpleString() == spark_type
                            )
                        ):
                            raise ValueError(
                                f"Type error for '{r.name()}.{t_column.name}': "
                                f"got: {spark_type}, expected: {t_column.datatype}"
                            )

    elif is_pandas_df(data, r):
        data: pandas.DataFrame = data
        for t_column in r.get_fields():
            if t_column.name in data.columns:
                if t_column.datatype is not None:
                    if (
                        t_column.name
                        not in data.select_dtypes(include=t_column.datatype).columns
                    ):
                        raise ValueError(
                            f"Type error for '{r.name()}.{t_column.name}': "
                            f"got: {data[t_column].dtype}, expected: {t_column.datatype}"
                        )

    logger.info(f"Relation {r.name()} has passed the field datatype check.")


def _keys(r: Type[Relation], data: Union["pyspark.sql.DataFrame", "pandas.DataFrame"]):
    if is_pyspark_df(data, r):
        from pyspark.sql.functions import column, count

        duplicated_rows = (
            data.groupby(r.get_key_field_names())
            .agg(count("*").alias("count"))
            .filter(column("count") > 1)
            .count()
        )

        if duplicated_rows > 0:
            raise ValueError(
                f"Key error for '{r.name()}': "
                f"using keys '{r.get_key_field_names()}'"
                f" there are {duplicated_rows} duplicates."
            )

    elif is_pandas_df(data, r):
        duplicated = data[r.get_key_field_names()].duplicated()
        duplicated_rows = len(data[duplicated])

        if duplicated_rows > 0:
            raise ValueError(
                f"Key error for '{r.name()}': "
                f"using keys '{r.get_key_field_names()}'"
                f" there are {duplicated_rows} duplicates."
            )

    logger.info(f"Relation {r.name()} has passed the key unqiue-ness check.")


@doublewrap
def fields(original_func=None, *, on_fail: str = RAISE):
    """ Check fields on a blizz Relation for existance. """
    _verify_args(on_fail)

    @functools.wraps(original_func)
    def _decorated(*args, **kwargs):
        relation = _inspect.get_class_that_defined_method(original_func)
        assert relation is not None
        res = original_func(*args, **kwargs)
        _run_check_and_handle_outcome(
            _field_existence, r=relation, data=res, on_fail=on_fail
        )

        return res

    return _decorated


@doublewrap
def types(original_func=None, *, on_fail: str = RAISE):
    """ Check datatypes on a blizz Relation. """
    _verify_args(on_fail)

    @functools.wraps(original_func)
    def _decorated(*args, **kwargs):
        relation = _inspect.get_class_that_defined_method(original_func)
        assert relation is not None
        res = original_func(*args, **kwargs)
        _run_check_and_handle_outcome(
            _field_types, r=relation, data=res, on_fail=on_fail
        )
        return res

    return _decorated


@doublewrap
def keys(original_func=None, *, on_fail: str = RAISE):
    """ Check keys on a blizz Relation. """
    _verify_args(on_fail)

    @functools.wraps(original_func)
    def _decorated(*args, **kwargs):
        relation = _inspect.get_class_that_defined_method(original_func)
        assert relation is not None
        res = original_func(*args, **kwargs)
        _run_check_and_handle_outcome(_keys, r=relation, data=res, on_fail=on_fail)

        return res

    return _decorated


def _run_check_and_handle_outcome(
    check: callable, r: Type[Relation], data, on_fail: str
) -> None:

    # skip any check, if data is None, e.g. if earlier blizz.check already has failed:
    if data is None:
        return None

    try:
        check(r=r, data=data)
    except Exception as error:
        if on_fail == RAISE:
            raise error
        if on_fail == WARN:
            warnings.warn(error.__repr__())


def _verify_args(on_fail: str) -> None:
    if on_fail not in [RAISE, WARN]:
        raise ValueError(
            f"Invalid argument for 'on_fail':{on_fail}. Allowed: {RAISE}, {WARN}"
        )
