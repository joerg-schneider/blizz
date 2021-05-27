import inspect
import os
import shutil
import subprocess
import tempfile
import webbrowser
from pathlib import Path
from typing import Type, Tuple, Iterable, List, Dict, Any

import tornado
from pyspark.sql.types import DataType
from tornado import web, ioloop

from blizz import FeatureGroup, Relation, Feature, Field
from blizz._inspect import find_source_tables_on_path, find_feature_groups_on_path

DIR_SPHINX_BASE = Path(os.path.dirname(os.path.abspath(__file__))).joinpath(
    "sphinx-base"
)
TABLE_TEMPLATE = DIR_SPHINX_BASE.joinpath("relations/relation-template.rst")
FG_TEMPLATE = DIR_SPHINX_BASE.joinpath("features/feature-group-template.rst")

PORT = 8080
HOST = "127.0.0.1"


def create_sphinx_html(source_dir: Path, target_dir: Path):
    source_tables = find_source_tables_on_path(basepath=source_dir)
    source_table_rsts: Dict[Type[Relation], str] = {
        st: relation_to_rst(st_in=st) for st in source_tables
    }
    feature_groups = find_feature_groups_on_path(basepath=source_dir)
    feature_rsts = {fg: feature_group_to_rst(fg_in=fg) for fg in feature_groups}

    ds_target_folder = target_dir.joinpath("relations")
    fg_target_folder = target_dir.joinpath("features")

    assert (
        not target_dir.exists() or os.listdir(target_dir.as_posix()) == []
    ), "rst output folder should not exist already!"

    os.makedirs(str(target_dir), exist_ok=True)
    os.makedirs(str(ds_target_folder))

    for st, st_rst in source_table_rsts.items():
        with open(str(ds_target_folder.joinpath(st.name() + ".rst")), "wt") as f:
            f.write(st_rst)

    os.makedirs(str(fg_target_folder))

    for fg, fg_rst in feature_rsts.items():
        with open(str(fg_target_folder.joinpath(fg.name() + ".rst")), "wt") as f:
            f.write(fg_rst)

    for folder in ["_static", "_templates"]:
        shutil.copytree(
            src=str(DIR_SPHINX_BASE.joinpath(folder)),
            dst=str(target_dir.joinpath(folder)),
        )

    for file in ["conf.py", "index.rst", "relations/index.rst", "features/index.rst"]:
        shutil.copy(
            src=str(DIR_SPHINX_BASE.joinpath(file)), dst=str(target_dir.joinpath(file))
        )

    os.chdir(str(target_dir))
    subprocess.run(["sphinx-build", ".", "html"])


def serve_sphinx_html(webroot: Path) -> None:
    app = web.Application(
        [
            (
                r"/(.*)",
                tornado.web.StaticFileHandler,
                {"path": str(webroot), "default_filename": "index.html"},
            )
        ]
    )
    app.listen(port=PORT, address=HOST)

    print("\nPress 'Ctrl+C' to stop")
    webbrowser.open(url=f"http://{HOST}:{PORT}")

    try:
        ioloop.IOLoop.current().start()
    except KeyboardInterrupt:
        ioloop.IOLoop.current().stop()
        print("Feature Library server stopped.")


def feature_group_to_rst(fg_in: Type[FeatureGroup]) -> str:
    def _make_data_sources_section(data_sources: Tuple[Type[Relation], ...]) -> str:
        ds_template = ":doc:`{name} <../relations/{name}>`"
        return "\n\n".join([ds_template.format(name=ds.name()) for ds in data_sources])

    def _make_features_section(features: Iterable[Type[Feature]]) -> str:
        feature_template = """
{name}
...........

{description}

Aggregation level
    {agg_level}

Available Parameters
    {parameters}

Feature Definition
    .. code-block:: python
       :linenos:

       {feature_code}
"""
        return "\n\n".join(
            [
                feature_template.format(
                    name=f.name(),
                    description=f.__doc__,
                    agg_level=make_aggregation_level_block(f),
                    parameters=make_parameters_block(f),
                    feature_code=inspect.getsource(f.compute).lstrip(),
                )
                for f in features
            ]
        )

    with open(FG_TEMPLATE, "r") as f:
        template_string = f.read()
        return template_string.format(
            name=fg_in.name(),
            description=fg_in.__doc__,
            data_sources_section=_make_data_sources_section(
                data_sources=fg_in.data_sources
            ),
            feature_section=_make_features_section(features=fg_in.get_features()),
        )


def make_aggregation_level_block(feat_in: Type[Feature]) -> str:
    if hasattr(feat_in, "aggregation_level"):
        aggregation_level = feat_in.aggregation_level
        if isinstance(aggregation_level, str):
            aggregation_level = [aggregation_level]
        elif isinstance(aggregation_level, Field):
            aggregation_level = [aggregation_level.name]
        elif isinstance(aggregation_level, Iterable):
            aggregation_level = list(aggregation_level)
            # check if "Field" type was used, if so, convert to string
            if isinstance(aggregation_level[0], Field):
                aggregation_level = [f.name for f in aggregation_level]

        return ",".join([col.name for col in aggregation_level])
    return "Not defined."


def make_parameters_block(feat_in: Type[Feature]) -> str:
    if hasattr(feat_in, "Parameters"):
        members = [
            getattr(feat_in.Parameters,attr)
            for attr in dir(feat_in.Parameters)
            if not callable(getattr(feat_in.Parameters, attr))
            and not attr.startswith("__")
        ]
        return ",".join(members)

    return "Not defined."


def relation_to_rst(st_in: Type[Relation]) -> str:
    def _make_pk_section(pk_fields: List[Field]) -> str:
        if len(pk_fields) == 0:
            return "Unknown – no fields have been flagged as keys yet."

        # the pk section should look like this for each known key (example):
        # customer_id : string
        #     Unique customer ID taken out of the CRM system.

        pk_key_template = "{field_name} : {field_type}\n\t{field_desc}"

        return "\n\n".join(
            [
                pk_key_template.format(
                    field_name=p.name,
                    field_type=_field_type_to_string(p.datatype),
                    field_desc=p.description,
                )
                for p in pk_fields
            ]
        )

    def _make_fields_rows(fields: List[Field]) -> str:
        field_row_template = """
    * - {name}
      - {type}
      - {default}
      - {description}
"""
        return "\n\n".join(
            [
                field_row_template.format(
                    name=p.name,
                    type=_field_type_to_string(p.datatype),
                    default=p.default if p.default is not None else "",
                    description=p.description if p.description is not None else "",
                )
                for p in fields
            ]
        )

    with open(TABLE_TEMPLATE, "r") as f:
        template_string = f.read()
        return template_string.format(
            name=st_in.name(),
            description=st_in.__doc__,
            primary_key_section=_make_pk_section(st_in.get_key_fields()),
            fields_table_rows=_make_fields_rows(st_in.get_fields()),
            load_code=inspect.getsource(st_in.load).lstrip(),
        )


def _field_type_to_string(in_type: Any) -> str:
    if issubclass(in_type, DataType):
        return in_type().simpleString()

    return str(in_type)
