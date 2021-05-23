from pyspark.sql import DataFrame
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    DateType,
    TimestampType,
)
from pyspark.sql import functions as F
import blizz.check
from blizz import Relation, Field
from test.conftest import (
    path_student_counceling_test,
    path_student_performance_test,
    path_employee_test,
    path_department_test,
    get_or_create_spark_session,
)


class StudentCouncelingInformation(Relation):
    """
    This is the example data source "StudentCouncelingInformation" for testing.
    """

    STUDENT_ID = Field(
        name="Student_ID", datatype=StringType, description="The ID of the student"
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
                path=path_student_counceling_test().as_posix(),
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
    This is the example data source "StudentPerformance" for testing.
    """

    STUDENT_ID = Field(name="Student_ID", datatype=StringType, key=True)
    SEMSTER_NAME = Field("Semster_Name", datatype=StringType)
    PAPER_ID = Field(name="Paper_ID", datatype=StringType)
    MARKS = Field(name="Marks", datatype=IntegerType)

    @classmethod
    @blizz.check.fields
    @blizz.check.types
    def load(cls) -> DataFrame:
        return get_or_create_spark_session().read.csv(
            path=path_student_performance_test().as_posix(),
            inferSchema=True,
            header=True,
        )


class EmployeeInformation(Relation):
    """
    This is the example data source "EmployeeInformation" for testing.
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
            path=path_employee_test().as_posix(), inferSchema=True, header=True
        )


class DepartmentInformation(Relation):
    """
    This is the example data source "DepartmentInformation" for testing.
    """

    DEPARTMENT_ID = Field(name="Department_ID", datatype=StringType)
    DATE_OF_ESTABLISHMENT = Field(
        name="DOE", datatype=TimestampType, description="Department Establishment Date"
    )
    DEPARTMENT_NAME = Field(name="Department_Name", datatype=StringType)

    @classmethod
    @blizz.check.fields
    @blizz.check.types
    def load(cls) -> DataFrame:
        return get_or_create_spark_session().read.csv(
            path=path_department_test().as_posix(), inferSchema=True, header=True
        )


__all__ = [
    "StudentCouncelingInformation",
    "StudentPerformance",
    "EmployeeInformation",
    "DepartmentInformation",
]
