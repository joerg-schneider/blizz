from blizz._inspect import find_source_tables_on_path, find_feature_groups_on_path
from test.test_spark_feature_library.data_sources import *
from test.test_spark_feature_library.features import *


def test_find_source_tables_on_path(path_test_spark_feature_library):
    source_tables = list(find_source_tables_on_path(path_test_spark_feature_library))
    assert StudentPerformance.contained_in(source_tables)


def test_find_feature_groups_on_path(path_test_spark_feature_library):
    feature_groups = list(find_feature_groups_on_path(path_test_spark_feature_library))
    assert StudentFeatureGroup.contained_in(feature_groups)
