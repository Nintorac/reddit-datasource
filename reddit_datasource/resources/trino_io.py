from gettext import Catalog
from typing import Union
from dagster import Any, IOManager, InputContext, MetadataValue, OutputContext, fs_io_manager, io_manager
import pandas as pd
from sqlalchemy import create_engine


class TrinoIOManager(IOManager):

    def __init__(self, schema='default', catalog='minio') -> None:
        super().__init__()
        self.schema = schema
        self.catalog = catalog

    def conn(self, catalog):
        #TODO resource
        conn = engine = create_engine(
        f'trino://admin@localhost:8080/{catalog}/'
        )
        return conn

    def handle_output(self, context, obj):
        catalog, schema, table = self._get_path(context)

        conn = self.conn(catalog)

        obj = obj.reset_index()

        try:
            create = pd.io.sql.get_schema(obj, table, con=conn, schema=schema)
            _with = f"""
            WITH (
                external_location = 's3a://cryptobro/{schema}/{table}',
                format = 'PARQUET'
            )
                """
            pd.read_sql_query(create + _with, conn, schema)
        except:
            # catch exception on to_sql
            # raise
            pass
        context.add_output_metadata({
            "path": MetadataValue.path(f"{catalog}.{schema}.{table}"),
            "rows": MetadataValue.int(len(obj))
        })
        

        obj.to_sql(
            con=conn, 
            schema=schema, 
            name=table,
            method="multi", 
            index=False, 
            if_exists='append',
            chunksize=1500
        )


    def load_input(self, context: "InputContext") -> Any:
        raise NotImplementedError()
        conn = self.conn()
        catalog, schema, table = self._get_path(context)
        return pd.read_sql(f"SELECT * from {catalog}.{schema}.{table}", conn)
        


    def _get_path(self, context: Union[InputContext, OutputContext]) -> str:
        """Automatically construct filepath."""
        if context.has_asset_key:
            path = context.get_asset_identifier()
        else:
            path = context.get_identifier()

        if context.has_asset_key:
            # ignore partition 
            # not good for loading :S
            *path, _ = path

        if len(path) > 3:
            raise ValueError("Don't support more than depth 3")
        
        table = None if len(path)<1 else path[-1] #hopefully never None
        schema = None if len(path)<2 else path[-2]
        catalog = None if len(path)<3 else path[-3]

        schema = schema or self.schema
        catalog = catalog or self.catalog
        
        def sanitize(name):
            #TODO
            return name.lower()

        return sanitize(catalog), sanitize(schema), sanitize(table)


@io_manager(
    # required_resource_keys={'schema', 'catalog'}
)
def trino_dataframe_io():
    schema = 'cryptobro'
    catalog = 'minio'

    return TrinoIOManager(schema, catalog)
