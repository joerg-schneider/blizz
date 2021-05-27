"""
blizz – be blizzful.
"""

import logging as __logging
import os as __os

from ._bootstrapping import relation_from_dataframe
from ._constants import ALL_FIELDS
from ._feature_library import (
    Feature,
    FeatureGroup,
    FeatureParameter,
    AggregatedFeatureGroup,
)
from ._primitives import Relation, Field

__version__ = "0.2.2"
__DEFAULT_LOG_LEVEL = __os.environ.get("BLIZZ_LOG_LEVEL", __logging.INFO)

__logging.basicConfig(
    level=__DEFAULT_LOG_LEVEL,
    datefmt="%Y-%m-%d %H:%M:%S",
    format="[%(asctime)s] %(name)s:%(lineno)d %(levelname)s: %(message)s",
)


# blizz needs either PySpark or pandas, check and raise if missing:
try:
    import pyspark as __pyspark
except ImportError:
    __pyspark = None

try:
    import pandas as __pandas
except ImportError:
    __pandas = None

if __pandas is None and __pyspark is None:
    msg = "'pandas' or 'pyspark' is required for blizz, but neither found."
    raise ImportError(msg)


__all__ = [
    "Relation",
    "Field",
    "relation_from_dataframe",
    "Feature",
    "FeatureGroup",
    "FeatureParameter",
    "AggregatedFeatureGroup",
    "ALL_FIELDS",
]
