"""
blizz – be blizzful.
"""

import logging as __logging
import os as __os

from ._bootstrapping import relation_from_dataframe
from ._constants import ALL_FIELDS

from ._primitives import Relation, Field

__version__ = "0.2.4"
__DEFAULT_LOG_LEVEL = __os.environ.get("BLIZZ_LOG_LEVEL", __logging.INFO)

__logging.basicConfig(
    level=__DEFAULT_LOG_LEVEL,
    datefmt="%Y-%m-%d %H:%M:%S",
    format="[%(asctime)s] %(name)s:%(lineno)d %(levelname)s: %(message)s",
)


# blizz needs either PySpark or pandas, check and raise if missing:
try:
    import pyspark as __pyspark
    from ._feature_library import (
        Feature,
        FeatureGroup,
        FeatureParameter,
        AggregatedFeatureGroup,
    )
except ImportError:  # pragma: no cover
    Feature = None
    FeatureGroup = None
    FeatureParameter = None
    AggregatedFeatureGroup = None
    __pyspark = None  # pragma: no cover

try:
    import pandas as __pandas
except ImportError:  # pragma: no cover
    __pandas = None  # pragma: no cover

if __pandas is None and __pyspark is None:  # pragma: no cover
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
