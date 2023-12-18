from dagster import Definitions, load_assets_from_modules, EnvVar, ScheduleDefinition, AssetSelection, define_asset_job, \
    repository

from . import assets
from . import assets
from .resources.postgres_io_manager import PostgresIOManager
from .resources.postgres_io_manager import PostgresIOManager

PG_IOMANAGER_CONFIG = {
    "username": EnvVar("DATA_PG_USERNAME"),
    "password": EnvVar("DATA_PG_PASSWORD"),
    "hostname": EnvVar("DATA_PG_HOST"),
    "port": EnvVar("DATA_PG_PORT"),
    "db_name": EnvVar("DATA_PG_DB"),
    "dbschema": EnvVar("DATA_PG_SCHEMA")
}

all_assets = load_assets_from_modules([assets])

entsoe_job = define_asset_job("entsoe_job", selection=AssetSelection.groups("entsoe"))
entsog_job = define_asset_job("entsog_job", selection=AssetSelection.groups("entsog"))
desfa_job = define_asset_job("desfa_job", selection=AssetSelection.groups("desfa"))
ipto_job = define_asset_job("ipto_job", selection=AssetSelection.groups("ipto"))


entsoe_schedule = ScheduleDefinition(
    job=entsoe_job,
    cron_schedule="0 0 * * *",  # daily at midnight
)

entsog_schedule = ScheduleDefinition(
    job=entsog_job,
    cron_schedule="0 1 * * *",
)

desfa_schedule = ScheduleDefinition(
    job=desfa_job,
    cron_schedule="0 2 * * *",
)

ipto_schedule = ScheduleDefinition(
    job=ipto_job,
    cron_schedule="0 3 * * *",
)

defs = Definitions(
    assets=all_assets,
    resources={
        "postgres_io_manager": PostgresIOManager(**PG_IOMANAGER_CONFIG)
    },
    schedules=[entsog_schedule, entsoe_schedule, desfa_schedule, ipto_schedule]
)
