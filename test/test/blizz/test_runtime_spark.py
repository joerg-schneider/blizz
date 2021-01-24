from pathlib import Path
from blizz._run_config import read_config, validate_schema, run_config_from_file
from blizz._runtime import build_features, write_results


def test_read_config(path_test_feature_lists: Path) -> None:
    config = read_config(path_test_feature_lists.joinpath("TestFeatureList-1.yaml"))
    print(config)


def test_validate_config(path_test_feature_lists: Path) -> None:
    config = read_config(path_test_feature_lists.joinpath("TestFeatureList-1.yaml"))
    print(validate_schema(config))


def test_runconfig_from_file(
    path_test_feature_lists: Path, path_test_spark_feature_library
) -> None:
    rc = run_config_from_file(
        file=path_test_feature_lists.joinpath("TestFeatureList-1.yaml"),
        feature_library_base_path=path_test_spark_feature_library,
    )

    print(rc)


def test_build_features(path_test_feature_lists: Path, path_test_spark_feature_library):
    rc = run_config_from_file(
        file=path_test_feature_lists.joinpath("TestFeatureList-1.yaml"),
        feature_library_base_path=path_test_spark_feature_library,
    )
    build_features(rc)


def test_write_results(
    path_test_feature_lists: Path,
    path_test_spark_feature_library,
    path_tmp_folder: Path,
):
    rc = run_config_from_file(
        file=path_test_feature_lists.joinpath("TestFeatureList-1.yaml"),
        feature_library_base_path=path_test_spark_feature_library,
    )
    results = build_features(rc)
    write_results(config=rc, results=results, out_path=path_tmp_folder)
