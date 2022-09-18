from multiprocessing.sharedctypes import Value
import os
from typing import Union

import pandas
import pandas as pd
from dagster import Field, IOManager, InputContext, OutputContext
from dagster import _check as check
from dagster import io_manager
from dagster._seven.temp_dir import get_system_temp_directory


class PartitionedParquetIOManager(IOManager):
    """
    This IOManager will take in a pandas or pyspark dataframe and store it in parquet at the
    specified path.
    It stores outputs for different partitions in different filepaths.
    Downstream ops can either load this dataframe into a spark session or simply retrieve a path
    to where the data is stored.
    """

    def __init__(self, base_path):
        self._base_path = base_path

    def handle_output(
        self, context: OutputContext, obj: pandas.DataFrame
    ):
        path = self._get_path(context)
        if "://" not in self._base_path:
            os.makedirs(os.path.dirname(path), exist_ok=True)

        if isinstance(obj, pandas.DataFrame):
            row_count = len(obj)
            context.log.info(f"Row count: {row_count}")
            obj.to_parquet(path=path, index=False)
        else:
            raise Exception(f"Outputs of type {type(obj)} not supported.")

        context.add_output_metadata({"row_count": row_count, "path": path})

    def load_input(self, context) -> pandas.DataFrame:
        path = self._get_path(context)
        return pd.read_parquet(path)
        return check.failed(
            f"Inputs of type {context.dagster_type} not supported. Please specify a valid type "
            "for this input either on the argument of the @asset-decorated function."
        )

    def _get_path(self, context: Union[InputContext, OutputContext]):
        
        # context.log.info(f'Has asset key {context.has_asset_key}')
        # context.log.info(f'mapping key {context.mapping_key}')
        # context.log.info(f'step key {context.step_key}')
        # context.log.info(f'partition key {context.has_partition_key}')
        # context.log.info(f'partition key {context.step_context.partition_key}')
        # raise ValueError(context.__dir__())

        if context.has_asset_key:
            key = context.asset_key.path[-1]  # type: ignore
        else:
            key = context.step_key.split('.')[-1]

        if context.has_asset_partitions:
            start, end = context.asset_partitions_time_window
            dt_format = "%Y%m%d%H%M%S"
            partition_str = start.strftime(dt_format) + "_" + end.strftime(dt_format)
            return os.path.join(self._base_path, key, f"{partition_str}.pq")
        elif context.has_partition_key is not None:
            partition_str = context.step_context.partition_key
            return os.path.join(self._base_path, key, f"{partition_str}.pq")
        else:
            return os.path.join(self._base_path, f"{key}.pq")


@io_manager(
    config_schema={"base_path": Field(str, is_required=False)},
)
def local_partitioned_parquet_io_manager(init_context):
    return PartitionedParquetIOManager(
        base_path=init_context.resource_config.get("base_path", get_system_temp_directory())
    )

