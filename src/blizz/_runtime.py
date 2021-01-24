import os
from pathlib import Path
from typing import Dict, Type

from pyspark.sql import DataFrame

from blizz._run_config import RunConfig, FeatureGroup, OutputFormat


def _cleanup_spark_files(in_path: Path) -> None:
    for file in os.listdir(str(in_path)):
        if file.startswith("_S") or file.startswith("._S") or file.startswith(".part-"):
            os.remove(str(in_path.joinpath(file)))


def build_features(config: RunConfig) -> Dict[Type[FeatureGroup], DataFrame]:
    results = {}
    for fg in config.feature_groups:
        feature_requests = [f for f in fg.features]
        feature_parameters = {
            f.feature.name(): f.parameters
            for f in feature_requests
            if len(f.parameters) > 0
        }

        computed = fg.feature_group.compute(
            features=[f.feature for f in feature_requests],
            keep=fg.passthrough,
            parameters=feature_parameters,
        )

        results[fg.feature_group] = computed

    return results


def write_results(
    config: RunConfig,
    out_path: Path,
    results: Dict[Type[FeatureGroup], DataFrame],
    overwrite: bool = False,
) -> None:
    for fg, sdf in results.items():

        full_output_path = str(out_path.joinpath(fg.name()))
        save_mode = "overwrite" if overwrite else None

        if config.output_format == OutputFormat.CSV:
            sdf.write.mode(save_mode).csv(path=full_output_path, header=True)
        if config.output_format == OutputFormat.PARQUET:
            sdf.write.mode(save_mode).parquet(path=full_output_path)

        _cleanup_spark_files(Path(full_output_path))
