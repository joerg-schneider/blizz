from typing import Union

from blizz import _inspect, _helpers
from ._primitives import Relation, Type
import functools

import logging

try:
    import pyspark
except ImportError as e:
    pyspark = None

try:
    import pandas
except ImportError as e:
    pandas = None

logger = logging.getLogger(__name__)


def _field_existence(
    r: Type[Relation], data: Union["pyspark.sql.DataFrame", "pandas.DataFrame"]
):
    if _helpers.is_pyspark_df(data):
        data: pyspark.sql.DataFrame = data
        for t_column in r.get_fields():
            if t_column.name not in data.columns:
                raise ValueError(
                    f"Field '{t_column.name}' is not part of loaded Relation '{r.name()}'."
                )
    elif _helpers.is_pandas_df(data):
        # todo: implement this for Pandas
        pass

    logger.info(f"Relation {r.name()} has passed the field existance check.")


def _field_types(
    r: Type[Relation], data: Union["pyspark.sql.DataFrame", "pandas.DataFrame"]
):
    if _helpers.is_pyspark_df(data):
        for t_column in r.get_fields():
            if t_column.name in data.columns:
                if t_column.datatype is not None:
                    for spark_name, spark_type in data.dtypes:
                        if (
                            spark_name == t_column.name
                            and t_column.datatype().simpleString() != spark_type
                        ):
                            raise ValueError(
                                f"Type error for '{r.name()}.{t_column.name}': "
                                f"got: {spark_type}, expected: {t_column.datatype().simpleString()}"
                            )

    elif _helpers.is_pandas_df(data):
        # todo: implement this for Pandas
        pass

    logger.info(f"Relation {r.name()} has passed the field datatype check.")


def _keys(r: Type[Relation], data: Union["pyspark.sql.DataFrame", "pandas.DataFrame"]):
    if _helpers.is_pyspark_df(data):
        # todo: implement this for Pandas
        pass
    elif _helpers.is_pandas_df(data):
        # todo: implement this for Pandas
        pass

    logger.info(f"Relation {r.name()} has passed the key unqiue-ness check.")


def fields(func):
    @functools.wraps(func)
    def _decorated(*args, **kwargs):
        relation = _inspect.get_class_that_defined_method(func)
        assert relation is not None
        res = func(*args, **kwargs)
        _field_existence(r=relation, data=res)
        return res

    return _decorated


def types(func):
    @functools.wraps(func)
    def _decorated(*args, **kwargs):
        relation = _inspect.get_class_that_defined_method(func)
        assert relation is not None
        res = func(*args, **kwargs)
        _field_types(r=relation, data=res)
        return res

    return _decorated


def keys(func):
    @functools.wraps(func)
    def _decorated(*args, **kwargs):
        relation = _inspect.get_class_that_defined_method(func)
        assert relation is not None
        res = func(*args, **kwargs)
        _keys(r=relation, data=res)
        return res

    return _decorated
