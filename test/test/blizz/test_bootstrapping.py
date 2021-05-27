import blizz
from test.test_spark_feature_library.data_sources import DepartmentInformation
from test.test_pandas_feature_library.data_sources import StudentPerformance


def test_bootstrapping_from_spark():
    blizz.relation_from_dataframe(df=DepartmentInformation.load(), name="Departments")


def test_boostrapping_from_pandas():
    blizz.relation_from_dataframe(
        df=StudentPerformance.load(), name="StudentPerformance"
    )
