import shutil
from pathlib import Path
import os
import pytest
from pyspark.sql import SparkSession
from test import test_spark_feature_library


def path_to_test_data() -> Path:
    return Path(os.path.dirname(__file__)).joinpath(os.pardir, "data")


def path_department_test() -> Path:
    return path_to_test_data().joinpath("Department_Information.csv").absolute()


def path_employee_test() -> Path:
    return path_to_test_data().joinpath("Employee_Information.csv").absolute()


def path_student_counceling_test() -> Path:
    return path_to_test_data().joinpath("Student_Counceling_Information.csv").absolute()


def path_student_performance_test() -> Path:
    return path_to_test_data().joinpath("Student_Performance_Data.csv.gz").absolute()


@pytest.fixture
def path_test_spark_feature_library() -> Path:
    return Path(os.path.dirname(test_spark_feature_library.__file__))


@pytest.fixture
def path_test_feature_lists() -> Path:
    return Path(os.path.dirname(__file__)).joinpath("test_feature_lists")


@pytest.fixture(scope="session")
def path_tmp_folder() -> Path:
    return Path(os.path.dirname(__file__)).joinpath(os.pardir).joinpath("temp")


@pytest.fixture(scope="session")
def path_conf_folder() -> Path:
    return Path(os.path.dirname(__file__)).joinpath(os.pardir).joinpath("conf")


def get_or_create_spark_session() -> SparkSession:
    """
    Create and/or retrieve an Apache Spark Session.
    :return: a live Spark Session
    """
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    log4j = spark._jvm.org.apache.log4j
    logger = log4j.LogManager.getLogger("ERROR")
    # spark.conf.set("spark.sql.execution.arrow.enabled", "true")

    return spark


@pytest.fixture(scope="session")
def spark_session() -> SparkSession:
    spark = get_or_create_spark_session()
    yield spark
    # this will run whenever the last test of the session has
    # completed
    spark.stop()


@pytest.fixture(scope="session", autouse=True)
def clean_tmp_folder(path_tmp_folder: Path,) -> None:
    def _clean_tmp():
        if path_tmp_folder.exists():
            for subpath in os.listdir(path_tmp_folder.as_posix()):
                shutil.rmtree(
                    path_tmp_folder.joinpath(subpath).as_posix(), ignore_errors=True
                )

    _clean_tmp()
    yield
    _clean_tmp()


@pytest.fixture(scope="session", autouse=True)
def set_spark_conf_dir(path_conf_folder: Path):
    os.environ["SPARK_CONF_DIR"] = path_conf_folder.as_posix()
