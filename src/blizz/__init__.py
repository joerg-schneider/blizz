"""
blizz – if you need features, you came to the right place!
"""
import os
import logging
from ._helpers import setup_logger

from ._feature_library import (
    Feature,
    FeatureGroup,
    FeatureParameter,
    AggregatedFeatureGroup,
)
from ._primitives import Relation, Field
from ._bootstrapping import relation_from_dataframe
from ._constants import ALL_FIELDS

__version__ = "0.1.0"

DEFAULT_LOG_LEVEL = os.environ.get("BLIZZ_LOG_LEVEL", logging.INFO)

setup_logger(DEFAULT_LOG_LEVEL)
