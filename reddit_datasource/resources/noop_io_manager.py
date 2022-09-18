import os
from typing import Union

import pandas
import pandas as pd
from dagster import Field, IOManager, InputContext, OutputContext
from dagster import _check as check
from dagster import io_manager
from dagster._seven.temp_dir import get_system_temp_directory


class NoOpIOManager(IOManager):
    """
    This IOManager will take in a pandas or pyspark dataframe and store it in parquet at the
    specified path.
    It stores outputs for different partitions in different filepaths.
    Downstream ops can either load this dataframe into a spark session or simply retrieve a path
    to where the data is stored.
    """

    def handle_output(
        self, context: OutputContext, obj: pandas.DataFrame
    ):
        return

    def load_input(self, context) -> pandas.DataFrame:
        return None


@io_manager()
def noop_io_manager():
    return NoOpIOManager()

