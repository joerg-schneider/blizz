from typing import Iterable
from pathlib import Path

import pandas

TEMPLATE_FIELD = '{field_name_var} = Field(name="{field_name}")'
TEMPLATE_FIELD_TYPED = (
    '{field_name_var} = Field(name="{field_name}", datatype={data_type})'
)
TEMPLATE_DATA_SOURCE = """
class {table_name}(SourceTable):
    \"\"\"
    todo: describe data source {table_name} in this docstring
    \"\"\"
    class Columns:
        {fields}

    @classmethod
    def load(cls) -> DataFrame:

        # todo: implement data source loading here
        pass
""".strip()


def data_source_definition_from_file(path: Path, table_name: str):
    temp_df = pandas.read_csv(filepath_or_buffer=str(path), nrows=10000)


def data_source_definition(
    table_name: str, field_names: Iterable[str], field_types: Iterable[str] = None
) -> str:
    if field_types is None:
        fields = [
            TEMPLATE_FIELD.format(field_name_var=fn.upper(), field_name=fn)
            for fn in field_names
        ]
    else:
        fields = [
            TEMPLATE_FIELD_TYPED.format(
                field_name_var=fn.upper(), field_name=fn, data_type=ft
            )
            for fn, ft in zip(field_names, field_types)
        ]

    class_string = TEMPLATE_DATA_SOURCE.format(
        table_name=table_name, fields="\n        ".join(fields)
    )
    return class_string
