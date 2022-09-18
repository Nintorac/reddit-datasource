from pathlib import Path
from dagster import load_assets_from_package_module, repository, with_resources

from reddit_datasource import assets
from reddit_datasource.resources import limiter, noop_io_manager, trino_dataframe_io
from reddit_datasource.resources.parquet_io import local_partitioned_parquet_io_manager


@repository
def reddit_datasource():


    r_assets = with_resources(
        load_assets_from_package_module(assets),
        resource_defs={
            "pandas_io_manager": local_partitioned_parquet_io_manager.configured({'base_path': Path('~/data/reddit').expanduser().as_posix()}),
            "pushshift_limiter": limiter,
            "noop_io_manager": noop_io_manager
        },
    )


    return [r_assets]
