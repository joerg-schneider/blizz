import functools
import logging
from typing import List, Union, Dict, Callable

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
    __original_func=None,
    *,
    key: List[Field] = None,
    sort_on: List[Field] = None,
    sort_order: str = ASC,
):
    """Apply deduplication to a loaded Blizz relation."""

    @functools.wraps(__original_func)
    def _decorated(*args, **kwargs):
        relation = _inspect.get_class_that_defined_method(__original_func)
        assert relation is not None
        res = __original_func(*args, **kwargs)
        res = _deduplicate(
            r=relation, data=res, key=key, sort_on=sort_on, sort_order=sort_order
        )
        return res

    return _decorated


@doublewrap
def defaults(__original_func=None, *, fill: List[Field] = None):
    """Apply default values to a loaded Blizz relation."""

    @functools.wraps(__original_func)
    def _decorated(*args, **kwargs):
        relation = _inspect.get_class_that_defined_method(__original_func)
        assert relation is not None
        res = __original_func(*args, **kwargs)
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
            logger.info("No key fields defined – deduplicating based on all fields.")
            key = r.get_fields()
        else:
            logger.info(f"Deduplicating based on: {key_fields}")
            key = key_fields
    else:
        missing_fields = {
            key_field for key_field in key if key_field not in data.columns
        }
        if missing_fields:
            raise ValueError(
                f"Cannot deduplicate based on {missing_fields} – not in relation."
            )

    if is_pyspark_df(data, r):
        data: pyspark.sql.DataFrame = data
        from pyspark.sql.window import Window
        from pyspark.sql.functions import dense_rank, asc, desc

        if sort_on is not None:
            key_cols = [k if isinstance(k, str) else k.name for k in key]
            sort_cols = [s if isinstance(s, str) else s.name for s in sort_on]
            row_ranked = Field("bliz__row_number")

            window = Window.partitionBy(*key_cols).orderBy(*sort_cols)
            if sort_order == ASC:
                window = window.orderBy(asc(*sort_cols))
            else:
                window = window.orderBy(desc(*sort_cols))

            rank_expression = dense_rank().over(window)
            data_ranked = data.withColumn(row_ranked, rank_expression)
            data = data_ranked.where(f"{row_ranked} = 1").drop(row_ranked)
        else:
            data = data.drop_duplicates(subset=key)

    elif is_pandas_df(data, r):
        data: pandas.DataFrame = data

        if sort_on is not None:
            if not isinstance(sort_on, List):
                sort_on = [sort_on]

            data = data.sort_values(by=sort_on, ascending=sort_order == ASC)

        data = data.drop_duplicates(subset=key, keep="first")
    logger.info(f"Applied deduplication to {r.name()}.")
    return data


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

    elif is_pandas_df(data, r):
        data: pandas.DataFrame = data
        data = data.fillna(fill)

    logger.info(f"Applied default values to NAs in {r.name()}.")
    return data


@doublewrap
def renames(__original_func=None, *, columns: Dict[str, str] = None):
    """Apply renames values to a loaded Blizz relation."""

    @functools.wraps(__original_func)
    def _decorated(*args, **kwargs):
        relation = _inspect.get_class_that_defined_method(__original_func)
        assert relation is not None
        res = __original_func(*args, **kwargs)
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
    elif is_pandas_df(data, r):
        data: pandas.DataFrame = data
        data = data.rename(columns=all_renames)

    logger.info(f"Applied the following field renames: {all_renames} to {r.name()}.")
    return data


@doublewrap
def func(
    __original_func=None,
    *,
    function: Callable[
        [Type[Relation], Union["pyspark.sql.DataFrame", "pandas.DataFrame"]],
        Union["pyspark.sql.DataFrame", "pandas.DataFrame"],
    ],
):
    """Apply a user defined function to a loaded Blizz relation."""

    @functools.wraps(__original_func)
    def _decorated(*args, **kwargs):
        relation = _inspect.get_class_that_defined_method(__original_func)
        assert relation is not None
        res = __original_func(*args, **kwargs)
        res = function(relation, res)
        return res

    return _decorated
