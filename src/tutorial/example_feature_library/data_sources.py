from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType, DateType
from pyspark.sql import functions as F
import blizz.check
from blizz import Relation, Field
import os

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
PATH_TEST_DATA = os.path.join(CURRENT_DIR, os.pardir,os.pardir, os.pardir, "test/data")


def get_or_create_spark_session() -> SparkSession:
    """
    Create and/or retrieve an Apache Spark Session.
    :return: a live Spark Session
    """
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    log4j = spark._jvm.org.apache.log4j
    logger = log4j.LogManager.getLogger("ERROR")

    return spark


class StudentCouncelingInformation(Relation):
    """
    This is the example data source "StudentCouncelingInformation" of the tutorial.
    """

    STUDENT_ID = Field(
        name="Student_ID",
        datatype=StringType,
        description="The ID of the student",
        key=True,
    )

    DATE_OF_ADMISSION = Field(
        "DOA", datatype=DateType, description="Date of admission to university."
    )

    DATE_OF_BIRTH = Field(
        name="DOB", datatype=DateType, description="Student's birth date."
    )

    DEPARTMENT_CHOICES = Field(
        name="Department_Choices",
        datatype=StringType,
        description="Choice of department a student submitted",
    )

    DEPARTMENT_ADMISSION = Field(
        name="Department_Admission",
        datatype=StringType,
        description="Department where student got admitted",
    )

    @classmethod
    @blizz.check.fields
    @blizz.check.types
    def load(cls) -> DataFrame:
        return (
            get_or_create_spark_session()
            .read.csv(
                path=os.path.join(PATH_TEST_DATA, "Student_Counceling_Information.csv"),
                inferSchema=True,
                header=True,
            )
            .withColumn(
                cls.DATE_OF_ADMISSION, F.expr(f"cast({cls.DATE_OF_ADMISSION} as date)")
            )
            .withColumn(cls.DATE_OF_BIRTH, F.expr(f"cast({cls.DATE_OF_BIRTH} as date)"))
        )


class StudentPerformance(Relation):
    """
    This is the example data source "StudentPerformance" of the tutorial.
    """

    STUDENT_ID = Field(name="Student_ID", datatype=StringType)
    SEMSTER_NAME = Field("Semster_Name", datatype=StringType)
    PAPER_ID = Field(name="Paper_ID", datatype=StringType)
    MARKS = Field(name="Marks", datatype=IntegerType)

    @classmethod
    @blizz.check.fields
    @blizz.check.types
    def load(cls) -> DataFrame:
        return get_or_create_spark_session().read.csv(
            path=os.path.join(PATH_TEST_DATA, "Student_Performance_Data.csv.gz"),
            inferSchema=True,
            header=True,
        )


class EmployeeInformation(Relation):
    """
    This is the example data source "EmployeeInformation" of the tutorial.
    """

    EMPLOYEE_ID = Field(name="Employee ID", datatype=StringType)
    DATE_OF_BIRTH = Field(
        name="DOB", datatype=DateType, description="Employee's birth date."
    )
    DOJ = Field(name="DOJ", datatype=DateType, description="Date Of Joining")
    DEPARTMENT_ID = Field(name="Department_ID", datatype=StringType)

    @classmethod
    @blizz.check.fields
    @blizz.check.types
    def load(cls) -> DataFrame:
        return get_or_create_spark_session().read.csv(
            path=os.path.join(PATH_TEST_DATA, "Employee_Information.csv"),
            inferSchema=True,
            header=True,
        )


class DepartmentInformation(Relation):
    """
    This is the example data source "DepartmentInformation" of the tutorial.
    """

    DEPARTMENT_ID = Field(name="Department_ID", datatype=StringType)
    DATE_OF_ESTABLISHMENT = Field(
        name="DOE", datatype=DateType, description="Department Establishment Date"
    )
    DEPARTMENT_NAME = Field(name="Department_Name", datatype=StringType)

    @classmethod
    @blizz.check.fields
    @blizz.check.types
    def load(cls) -> DataFrame:
        return get_or_create_spark_session().read.csv(
            path=os.path.join(PATH_TEST_DATA, "Department_Information.csv"),
            inferSchema=True,
            header=True,
        )


__all__ = [
    "StudentCouncelingInformation",
    "StudentPerformance",
    "EmployeeInformation",
    "DepartmentInformation",
]
