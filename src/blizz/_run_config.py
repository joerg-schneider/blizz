from enum import Enum
from pathlib import Path
from typing import Dict, Any, NamedTuple, List, Type

import yaml
from schema import Schema, And, Use, Optional

from blizz import Feature, FeatureGroup, FeatureParameter
from blizz._inspect import find_feature_groups_on_path


class OutputFormat(Enum):
    CSV = "csv"
    PARQUET = "parquet"


def str_to_format(in_str: str) -> OutputFormat:
    for e in OutputFormat:
        e: OutputFormat = e
        if in_str == e.value:
            return e

    raise ValueError(f"Unknown format {in_str}")


class FeatureRequest(NamedTuple):
    feature: Type[Feature]
    parameters: List[FeatureParameter]


class FeatureGroupRequest(NamedTuple):
    feature_group: Type[FeatureGroup]
    features: List[FeatureRequest]
    passthrough: List[str] = []
    pre_filter: str = None
    post_filter: str = None


class RunConfig(NamedTuple):
    feature_groups: List[FeatureGroupRequest]
    output_format: OutputFormat


output_schema = Schema(
    {"format": And(str, lambda s: s in ("csv", "parquet"), Use(str_to_format))}
)

parameters_schema = Schema(
    And(
        {"name-suffix": And(str, len), "grid": And(dict)},
        Use(
            lambda s: FeatureParameter(
                suffix_format=s["name-suffix"], parameter_grid=s["grid"]
            )
        ),
    )
)

feature_schema = Schema(
    {"name": And(str, len), Optional("parameters"): [parameters_schema]}
)

feature_group_schema = Schema(
    {
        "name": And(str, len),
        Optional("pre-filter"): And(
            str, len, error="pre-filter should be non-empty or omitted."
        ),
        Optional("post-filter"): And(
            str, len, error="post-filter should be non-empty or omitted."
        ),
        "passthrough": [str],
        "features": [feature_schema],
    }
)

config_schema = Schema(
    {"output": output_schema, "feature-groups": [feature_group_schema]}
)


def read_config(file: Path) -> Dict[str, Any]:
    with open(str(file), "rt") as f:
        return yaml.safe_load(f)


def validate_schema(raw: Dict[str, Any]) -> Dict[str, Any]:
    return config_schema.validate(raw)


def run_config_from_file(file: Path, feature_library_base_path: Path) -> RunConfig:
    # source_tables = find_source_tables_on_path(basepath=feature_library_base_path)
    feature_groups = find_feature_groups_on_path(basepath=feature_library_base_path)

    def _feature_group_for_name(fg_name: str) -> Type[FeatureGroup]:
        # todo: validate feature group name validity earlier in Schema.validate() -
        #       pass feature_groups into validate_schema(..) func
        for f in feature_groups:
            if f.name() == fg_name:
                return f

    raw_config = read_config(file)
    parsed_config = validate_schema(raw_config)

    feature_group_requests = []

    for raw_fg in parsed_config["feature-groups"]:
        name = raw_fg["name"]
        feature_group = _feature_group_for_name(name)
        pre_filter = raw_fg.get("pre_filter", None)
        post_filter = raw_fg.get("post_filter", None)
        passthrough = raw_fg.get("passthrough", [])

        feature_requests = []

        for raw_f in raw_fg["features"]:
            f_name = raw_f["name"]
            feature = feature_group.get_feature(name=f_name)
            parameters = raw_f.get("parameters", [])

            feature_requests.append(
                FeatureRequest(feature=feature, parameters=parameters)
            )

        feature_group_requests.append(
            FeatureGroupRequest(
                feature_group=feature_group,
                features=feature_requests,
                passthrough=passthrough,
                pre_filter=pre_filter,
                post_filter=post_filter,
            )
        )

    output_format = parsed_config["output"]["format"]

    rc = RunConfig(feature_groups=feature_group_requests, output_format=output_format)

    return rc
