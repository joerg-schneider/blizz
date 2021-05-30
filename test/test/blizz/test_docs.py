import shutil
import tempfile
from typing import Union

import pyspark
import os
import test
from blizz import Relation, Field, Feature
from test.test_spark_feature_library.data_sources import *
from test.test_spark_feature_library.features import StudentFeatureGroup
from pathlib import Path

from blizz._docs import (
    create_sphinx_html,
    feature_group_to_rst,
    make_aggregation_level_block,
    make_parameters_block,
    relation_to_rst,
    _field_type_to_string,
)

PROJECT_HOME_DIR = os.path.join(
    os.path.dirname(os.path.realpath(test.__file__)), os.pardir, os.pardir
)
TUTORIAL_DIR = os.path.abspath(os.path.join(PROJECT_HOME_DIR, "src"))
TEST_FEATURE_GROUP_LIST = os.path.abspath(
    os.path.join(PROJECT_HOME_DIR, "src/tutorial/ExampleFeatureList.yaml")
)
TEST_FEATURE_GROUP_DIR = os.path.abspath(
    os.path.join(PROJECT_HOME_DIR, "src/tutorial/example_feature_library")
)


def test_relation_to_rst():
    relation_to_rst(StudentPerformance)

    class RelationWithNoKey(Relation):
        @classmethod
        def load(
            cls, *args, **kwargs
        ) -> Union["pyspark.sql.DataFrame", "pandas.DataFrame"]:
            pass

        C1 = Field("test")

    relation_to_rst(RelationWithNoKey)


def test_feature_group_to_rst():
    feature_group_to_rst(StudentFeatureGroup)


def test_make_aggreation_level_block():
    make_aggregation_level_block(StudentFeatureGroup.AvgMarkPerStudent)
    make_aggregation_level_block(StudentFeatureGroup.ScaleMarksByFactor)
    make_aggregation_level_block(StudentFeatureGroup.StudentMarksScaled)

    class TestFeature(Feature):
        aggregation_level = StudentPerformance.STUDENT_ID.name

        @classmethod
        def compute(cls, base, parameters=None):
            pass

    make_aggregation_level_block(TestFeature)

    class TestFeature2(Feature):
        aggregation_level = (StudentPerformance.STUDENT_ID, StudentPerformance.PAPER_ID)

        @classmethod
        def compute(cls, base, parameters=None):
            pass

    make_aggregation_level_block(TestFeature2)

def test_make_parameters_block():
    make_parameters_block(StudentFeatureGroup.ScaleMarksByFactor)


def test_field_type_to_string():
    assert _field_type_to_string(Field("test").datatype) == "Undefined"
    assert _field_type_to_string(Field("test", datatype="str").datatype) == "str"
    assert (
        _field_type_to_string(
            Field("test", datatype=pyspark.sql.types.StringType).datatype
        )
        == "string"
    )


def test_create_sphinx_html():
    tempdir = None
    try:
        tempdir = tempfile.mkdtemp()
        output_dir = os.path.join(tempdir, "output")
        create_sphinx_html(
            source_dir=Path(TEST_FEATURE_GROUP_DIR), target_dir=Path(output_dir)
        )
    finally:
        if tempdir:
            shutil.rmtree(tempdir, ignore_errors=True)
