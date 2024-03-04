from dagster import Definitions, load_assets_from_modules, EnvVar, ScheduleDefinition, AssetSelection, define_asset_job

from . import assets
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

entsoe_job = define_asset_job("entsoe_job",
                              selection=AssetSelection.groups("entsoe")-AssetSelection.keys("hydro_reservoir_storage"))
entsoe_hydro_job = define_asset_job("entsoe_hydro_reservoir", selection=AssetSelection.keys("hydro_reservoir_storage"))

entsog_job = define_asset_job("entsog_job", selection=AssetSelection.groups("entsog"))

desfa_job = define_asset_job("desfa_job",
                             selection=AssetSelection.groups("desfa") - AssetSelection.keys("desfa_ng_quality_yearly"))
desfa_ng_quality_yearly_job = define_asset_job("desfa_ng_quality_yearly_job",
                                               selection=AssetSelection.keys("desfa_ng_quality_yearly"))

ipto_job = define_asset_job("ipto_job", selection=AssetSelection.groups("ipto"))


entsoe_schedule = ScheduleDefinition(
    job=entsoe_job,
    cron_schedule="0 0 * * *",  # daily at midnight
)

entsoe_hydro_schedule = ScheduleDefinition(
    job=entsoe_hydro_job,
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

desfa_ng_quality_yearly_schedule = ScheduleDefinition(
    job=desfa_ng_quality_yearly_job,
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
    schedules=[entsog_schedule, entsoe_hydro_schedule, entsoe_schedule, desfa_schedule,
               desfa_ng_quality_yearly_schedule, ipto_schedule]
)
