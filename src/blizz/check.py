from blizz import _inspect
from blizz.dataobjects import Relation, Type
from pyspark.sql import DataFrame
import functools


def _field_existence(r: Type[Relation], data: DataFrame):
    for t_column in r.get_fields():
        if t_column.name not in data.columns:
            raise ValueError(
                f"Field '{t_column.name}' is not part of loaded Relation '{r.name()}'."
            )


def _field_types(r: Type[Relation], data: DataFrame):
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


def _keys(r: Type[Relation], data: DataFrame):
    # todo: implement
    pass


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
