import inspect
from abc import ABC, abstractmethod
from itertools import product
from typing import Dict, Any, Tuple, NamedTuple, Union, Type
from typing import Iterable, Optional

from pyspark.sql import DataFrame, Column

from ._constants import ALL_FIELDS
from ._helpers import camel_case_to_snake, safe_name
from ._primitives import Field, Relation


class FeatureParameter(NamedTuple):
    parameter_grid: Dict[str, Any]
    suffix_format: str


class Feature(ABC):
    @classmethod
    def name(cls) -> str:
        return cls.__name__

    @classmethod
    def col_name(
        cls,
        uppercase: bool = False,
        parameter_grid: Dict[str, str] = None,
        parameter_suffix_format: str = None,
    ) -> str:
        if parameter_grid is not None:
            suffix = parameter_suffix_format.format(**parameter_grid)
        else:
            suffix = ""

        if uppercase:
            return (safe_name(camel_case_to_snake(cls.__name__) + suffix)).upper()
        else:
            return (safe_name(camel_case_to_snake(cls.__name__) + suffix)).lower()

    @classmethod
    @abstractmethod
    def compute(
        cls, base: DataFrame, parameters: Dict[str, Any] = None
    ) -> Union[Column, DataFrame]:
        pass


class FeatureGroup(ABC):

    data_sources: Tuple[Type[Relation], ...] = ()

    @classmethod
    def name(cls) -> str:
        return cls.__name__

    @classmethod
    def compute_base(cls) -> DataFrame:
        assert (
            len(cls.data_sources) == 1
        ), "If a feature group uses more than 1 relation, override 'compute_base()'"
        return cls.data_sources[0].load()

    # todo: Unit test this well!

    @classmethod
    def parameter_combinations(
        cls, grid: Iterable[FeatureParameter]
    ) -> Iterable[Tuple[Dict[str, Any], str]]:
        master_grid = []

        for fp in grid:
            flattened = [[(k, v) for v in vs] for k, vs in fp.parameter_grid.items()]
            dict_of_combinations = [dict(items) for items in product(*flattened)]
            for parameter_combination in dict_of_combinations:
                master_grid.append((parameter_combination, fp.suffix_format))

        return tuple(master_grid)

    @classmethod
    def compute(
        cls,
        features: Optional[Iterable[Type[Feature]]] = None,
        parameters: Optional[Dict[str, Iterable[FeatureParameter]]] = None,
        keep: Iterable[Union[Field, str]] = None,
    ) -> DataFrame:

        if parameters is None:
            parameters = {}

        base = cls.compute_base()

        if keep is None:
            keep = set()
        elif keep == ALL_FIELDS:
            keep = set(base.columns)
        else:
            keep = {str(f) for f in keep}

        if features is None:
            features = cls.get_features()

        base_with_features = base

        def _add_computed_feature_to_base(
            computed_feature: Union[Column, DataFrame],
            col_name: str,
            base_: DataFrame,
            feature_definition: Type[Feature],
        ):
            if isinstance(computed_feature, Column):
                base_ = base_.withColumn(col_name, computed_feature)
            elif isinstance(computed_feature, DataFrame):
                assert hasattr(feature_definition, "aggregation_level"), (
                    f"Feature {feature_definition} returns a DataFrame, "
                    f"but Feature class does not define required "
                    f"'aggregation_level'"
                )

                aggregation_level = getattr(feature_definition, "aggregation_level")

                if isinstance(aggregation_level, str):
                    aggregation_level = [aggregation_level]
                elif isinstance(aggregation_level, Field):
                    aggregation_level = [aggregation_level.name]
                elif isinstance(aggregation_level, Iterable):
                    aggregation_level = list(aggregation_level)
                    # check if "Field" type was used, if so, convert to string
                    if isinstance(aggregation_level[0], Field):
                        aggregation_level = [f.name for f in aggregation_level]

                # renames non-key column to target name
                non_key_cols = set(computed_feature.columns).difference(
                    aggregation_level
                )

                assert len(non_key_cols) == 1, (
                    "Feature.compute() must return a DataFrame "
                    "containing keys and a single feature column"
                )

                to_rename = list(non_key_cols)[0]
                return_df = computed_feature.withColumnRenamed(
                    existing=to_rename, new=col_name
                )
                base_ = base_.join(other=return_df, on=aggregation_level, how="left")
            return base_

        for f in features:
            f_params = parameters.get(f.name(), None)
            if f_params is not None:
                for parameter_grid, name_suffix in cls.parameter_combinations(
                    grid=f_params
                ):
                    # parameterized feature computation, once per parameter set
                    parameterized_column_name = f.col_name(
                        parameter_grid=parameter_grid,
                        parameter_suffix_format=name_suffix,
                    )
                    computed_feature = f.compute(base, parameter_grid)
                    base_with_features = _add_computed_feature_to_base(
                        computed_feature,
                        parameterized_column_name,
                        base_with_features,
                        f,
                    )
                    keep.add(parameterized_column_name)
            else:
                # non-parameterized feature computation
                computed_feature = f.compute(base)
                base_with_features = _add_computed_feature_to_base(
                    computed_feature, f.col_name(), base_with_features, f
                )
                keep.add(f.col_name())

        projected = base_with_features.select(*keep)

        return projected

    @classmethod
    def get_features(cls) -> Iterable[Type[Feature]]:
        return [
            cls_attribute
            for cls_attribute in cls.__dict__.values()
            if inspect.isclass(cls_attribute) and issubclass(cls_attribute, Feature)
        ]

    @classmethod
    def get_feature(cls, name: str) -> Type[Feature]:
        for f in cls.get_features():
            if f.name() == name:
                return f
        raise AttributeError(f"{cls.name()} does not have feature '{name}'")

    @classmethod
    def contained_in(cls, collection: Iterable[Type["FeatureGroup"]]) -> bool:
        for fg in collection:
            # todo: tidy up
            # equality condition: feature groups named the same and all their features
            self_features = [f.name() for f in cls.get_features()]
            fg_features = [f.name() for f in fg.get_features()]
            if cls.__name__ == fg.__name__ and self_features == fg_features:
                return True

        return False


class AggregatedFeatureGroup:
    pass
