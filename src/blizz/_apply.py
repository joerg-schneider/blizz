import functools
import logging
from typing import List, Union, Dict

from blizz import _inspect, Field
from blizz._helpers import doublewrap
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

ASC = "asc"
DESC = "desc"


@doublewrap
def deduplication(
    original_func=None,
    *,
    key: List[Field] = None,
    sort_on: List[Field] = None,
    sort_order: str = ASC,
):
    """Apply deduplication to a loaded Blizz relation."""

    @functools.wraps(original_func)
    def _decorated(*args, **kwargs):
        relation = _inspect.get_class_that_defined_method(original_func)
        assert relation is not None
        res = original_func(*args, **kwargs)
        res = _deduplicate(
            r=relation, data=res, key=key, sort_on=sort_on, sort_order=sort_order
        )
        return res

    return _decorated


@doublewrap
def defaults(original_func=None, *, fill: List[Field] = None):
    """Apply default values to a loaded Blizz relation."""

    @functools.wraps(original_func)
    def _decorated(*args, **kwargs):
        relation = _inspect.get_class_that_defined_method(original_func)
        assert relation is not None
        res = original_func(*args, **kwargs)
        res = _fill_defaults(r=relation, data=res, fill=fill)

        return res

    return _decorated


def _deduplicate(
    r: Type[Relation],
    data: Union["pyspark.sql.DataFrame", "pandas.DataFrame"],
    key: List[Field] = None,
    sort_on: List[Field] = None,
    sort_order: str = ASC,
) -> Union["pyspark.sql.DataFrame", "pandas.DataFrame"]:

    if key is None:
        key_fields = r.get_key_fields()
        if len(key_fields) == 0:
            logger.info(f"No key fields defined – deduplicating based on all fields.")
            key = r.get_fields()
        else:
            logger.info(f"Deduplicating based on: {key_fields}")
            key = key_fields
    else:
        missing_fields = {
            key_field.name for key_field in key if key_field not in r.get_fields()
        }
        if missing_fields:
            raise ValueError(
                f"Cannot deduplicate based on {missing_fields} – not in relation."
            )

    if is_pyspark_df(data, r):
        data: pyspark.sql.DataFrame = data
        if sort_on is not None:
            # todo: sort
            raise NotImplementedError(
                f"sort_on/sort_order not implemented yet for deduplicate."
            )

        data = data.drop_duplicates(subset=key)
        return data

    elif is_pandas_df(data, r):
        data: pandas.DataFrame = data

        if sort_on is not None:
            if not isinstance(sort_on, List):
                sort_on = [sort_on]

            data = data.sort_values(by=sort_on, ascending=sort_order == ASC)

        data = data.drop_duplicates(subset=key, keep="first")
        return data

    logger.info(f"Applied deduplication to {r.name()}.")


def _fill_defaults(
    r: Type[Relation],
    data: Union["pyspark.sql.DataFrame", "pandas.DataFrame"],
    fill: List[Field] = None,
) -> Union["pyspark.sql.DataFrame", "pandas.DataFrame"]:

    if fill is None:
        fill = r.get_defaults()
    else:
        if not isinstance(fill, List):
            fill = [fill]

        # verify given list of fields to fill
        has_no_defaults = {
            field if isinstance(field, str) else field.name
            for field in fill
            if field not in r.get_defaults()
        }
        if has_no_defaults:
            raise ValueError(
                f"Cannot fill {has_no_defaults} – no defaults specified or not in relation."
            )
        fill = {
            field: default
            for field, default in r.get_defaults().items()
            if field in fill
        }

    if is_pyspark_df(data, r):
        data: pyspark.sql.DataFrame = data
        data = data.fillna(fill)
        return data

    elif is_pandas_df(data, r):
        data: pandas.DataFrame = data
        data = data.fillna(fill)
        return data

    logger.info(f"Applied default values to NAs in {r.name()}.")


@doublewrap
def renames(original_func=None, *, columns: Dict[str, str] = None):
    """Apply renames values to a loaded Blizz relation."""

    @functools.wraps(original_func)
    def _decorated(*args, **kwargs):
        relation = _inspect.get_class_that_defined_method(original_func)
        assert relation is not None
        res = original_func(*args, **kwargs)
        res = _rename_fields(r=relation, data=res, columns=columns)
        return res

    return _decorated


def _rename_fields(
    r: Type[Relation],
    data: Union["pyspark.sql.DataFrame", "pandas.DataFrame"],
    columns: Dict[str, str] = None,
) -> Union["pyspark.sql.DataFrame", "pandas.DataFrame"]:

    if columns is None:
        columns = dict()

    defined_renames_on_relation = r.get_field_renames()
    all_renames: Dict[str, str] = dict()
    all_renames.update(defined_renames_on_relation)
    all_renames.update(columns)

    cant_rename = {
        source_field_name
        for source_field_name in all_renames.keys()
        if source_field_name not in data.columns
    }

    if cant_rename:
        raise ValueError(f"Cannot renames {cant_rename} – not in loaded DataFrame.")

    if is_pyspark_df(data, r):
        data: pyspark.sql.DataFrame = data
        for from_field_name, to_field_name in all_renames.items():
            data = data.withColumnRenamed(from_field_name, to_field_name)
        return data

    elif is_pandas_df(data, r):
        data: pandas.DataFrame = data
        data = data.rename(columns=all_renames)
        return data

    logger.info(f"Applied the following field renames: {all_renames} to {r.name()}.")
