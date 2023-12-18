from typing import Any

import pandas as pd
import sqlalchemy.dialects.postgresql.types

from dagster import ConfigurableIOManager, InputContext, OutputContext
from dagster_postgres.utils import get_conn_string
from pangres import upsert
from sqlalchemy import create_engine


class PostgresIOManager(ConfigurableIOManager):
    username: str
    password: str
    hostname: str
    db_name: str
    port: str
    dbschema: str

    def connection_str(self) -> str:
        return get_conn_string(
            username=self.username,
            password=self.password,
            hostname=self.hostname,
            db_name=self.db_name,
            port=self.port
        )

    def load_input(self, context: "InputContext") -> pd.DataFrame:
        table_name = context.asset_key.path[-1]
        return pd.read_sql(f"SELECT * FROM {self.dbschema}.{table_name}", con=self.connection_str)

    def handle_output(self, context: "OutputContext", obj: Any):
        if isinstance(obj, pd.DataFrame):
            # write df to table
            table_name = context.asset_key.path[-1]
            context.log.info(f"Writing {len(obj)} rows to {self.dbschema}.{table_name}")
            engine = create_engine(self.connection_str())
            upsert(con=engine,
                   df=obj,
                   table_name=table_name,
                   if_row_exists='update',
                   schema=self.dbschema)

            context.log.info(f"Successfully wrote to database.")
        else:
            raise ValueError(f"Unsupported object type {type(obj)} for PostgresIOManager.")
