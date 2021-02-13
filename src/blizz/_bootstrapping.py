from pathlib import Path
from typing import Iterable, Optional, Union

try:
    import pyspark
except ImportError:
    pyspark = None

try:
    import pandas
except ImportError:
    pandas = None

from ._helpers import safe_name

TEMPLATE_FIELD = '{field_name_var} = Field(name="{field_name}")'
TEMPLATE_FIELD_TYPED = (
    '    {field_name_var} = Field(name="{field_name}", datatype={data_type})'
)
TEMPLATE_DATA_SOURCE = """
class {table_name}(Relation):
    \"\"\"
    todo: describe relation {table_name} in this docstring
    \"\"\"
{fields}

    @classmethod
    @blizz.check.keys(on_fail = blizz.check.WARN)
    @blizz.check.types(on_fail = blizz.check.WARN)
    @blizz.check.fields(on_fail = blizz.check.WARN)
    def load(cls) -> DataFrame:
        \"\"\"
        todo: describe relation {table_name} load process in this docstring
        \"\"\"
        # todo: implement data source loading here
        pass
""".strip()


def data_source_definition_from_file(path: Path, table_name: str):
    pass
    # temp_df = pandas.read_csv(filepath_or_buffer=str(path), nrows=10000)


def data_source_definition(
    field_names: Iterable[str],
    dataframe_type: str,
    table_name: str = "BootstrappedTable",
    field_types: Iterable[str] = None,
    add_imports: bool = True,
) -> str:
    if field_types is None:
        fields = [
            TEMPLATE_FIELD.format(field_name_var=safe_name(fn.upper()), field_name=fn)
            for fn in field_names
        ]
    else:
        fields = [
            TEMPLATE_FIELD_TYPED.format(
                field_name_var=safe_name(fn.upper()), field_name=fn, data_type=ft
            )
            for fn, ft in zip(field_names, field_types)
        ]

    class_string = TEMPLATE_DATA_SOURCE.format(
        table_name=table_name, fields="\n".join(fields)
    )

    if add_imports:
        imports = ""
        imports += "import blizz.check\n"
        imports += "from blizz import Relation, Field\n"
        if dataframe_type == "spark":
            imports += "from pyspark.sql import DataFrame\n"
            imports += "from pyspark.sql.types import *\n"
        else:
            imports += "import pandas as pd\n"
            imports += "from pandas import DataFrame\n"

        class_string = imports + "\n\n" + class_string

    return class_string


def relation_from_dataframe(
    df: Union["pyspark.sql.DataFrame", "pandas.DataFrame"],
    name: Optional[str] = "BootstrappedTable",
    print_text: bool = True,
    add_imports: bool = True,
) -> Optional[str]:
    field_names = []
    field_types = []

    if isinstance(df, pyspark.sql.DataFrame):
        dataframe_type = "spark"
        for s in df.schema:
            field_names.append(s.name)
            field_types.append(str(s.dataType))

    elif isinstance(df, pandas.DataFrame):
        dataframe_type = "pandas"
        for c in df.columns:
            field_names.append(c)
            field_types.append(f'"{df[c].dtype.name}"')
    else:
        raise ValueError(f"Unsupported df passed of type: {type(df)}")

    txt = data_source_definition(
        table_name=name,
        dataframe_type=dataframe_type,
        field_names=field_names,
        field_types=field_types,
        add_imports=add_imports,
    )
    if print_text:
        print(txt)
    else:
        return txt
