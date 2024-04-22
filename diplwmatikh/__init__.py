from dagster import Definitions, EnvVar, ScheduleDefinition, AssetSelection, define_asset_job, \
    load_assets_from_package_module

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

all_assets = load_assets_from_package_module(assets)

entsoe_job = define_asset_job("entsoe_job",
                              selection=AssetSelection.groups("entsoe") - AssetSelection.keys(
                                  "hydro_reservoir_storage"))
entsoe_hydro_job = define_asset_job("entsoe_hydro_reservoir", selection=AssetSelection.keys("hydro_reservoir_storage"))

entsog_job = define_asset_job("entsog_job", selection=AssetSelection.groups("entsog"))

desfa_job = define_asset_job("desfa_job",
                             selection=AssetSelection.groups("desfa")
                                       - AssetSelection.keys("desfa_ng_quality_yearly")
                                       - AssetSelection.keys("desfa_ng_gcv_daily")
                                       - AssetSelection.keys("desfa_flows_daily")
                                       - AssetSelection.keys("desfa_ng_pressure_monthly")
                                       - AssetSelection.keys("desfa_estimated_vs_actual_offtakes")
                                       - AssetSelection.keys("desfa_nominations_daily"))

desfa_nominations_job = define_asset_job("desfa_nominations_job",
                                         selection=AssetSelection.keys("desfa_nominations_daily"))

desfa_ng_quality_yearly_job = define_asset_job("desfa_ng_quality_yearly_job",
                                               selection=AssetSelection.keys("desfa_ng_quality_yearly"))
desfa_ng_gcv_daily_job = define_asset_job("desfa_ng_gcv_daily_job",
                                          selection=AssetSelection.keys("desfa_ng_gcv_daily"))
desfa_flows_daily_job = define_asset_job("desfa_flows_daily_job",
                                         selection=AssetSelection.keys("desfa_flows_daily"))
desfa_ng_pressure_monthly_job = define_asset_job("desfa_ng_pressure_monthly_job",
                                                 selection=AssetSelection.keys("desfa_ng_pressure_monthly"))
desfa_estimated_vs_actual_offtakes_job = define_asset_job("desfa_estimated_vs_actual_offtakes_job",
                                                          selection=AssetSelection.keys("desfa_estimated_vs_actual_offtakes"))

ipto_job = define_asset_job("ipto_job", selection=AssetSelection.groups("ipto")
                                                  - AssetSelection.keys("ipto_daily_energy_balance")
                                                  - AssetSelection.keys("ipto_net_interconnection_flows")
                                                  - AssetSelection.keys("ipto_res_injections")
                                                  - AssetSelection.keys("ipto_unit_production"))
ipto_daily_energy_balance_job = define_asset_job("ipto_daily_energy_balance_job",
                                                 selection=AssetSelection.keys("ipto_daily_energy_balance"))
ipto_net_interconnection_flows_job = define_asset_job("ipto_net_interconnection_flows_job",
                                                      selection=AssetSelection.keys("ipto_net_interconnection_flows"))
ipto_res_injections_job = define_asset_job("ipto_res_injections_job",
                                           selection=AssetSelection.keys("ipto_res_injections"))
ipto_unit_production_job = define_asset_job("ipto_unit_production_job",
                                            selection=AssetSelection.keys("ipto_unit_production"))

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

desfa_nominations_schedule = ScheduleDefinition(
    job=desfa_nominations_job,
    cron_schedule="0 2 * * *"
)

desfa_ng_quality_yearly_schedule = ScheduleDefinition(
    job=desfa_ng_quality_yearly_job,
    cron_schedule="0 2 * * *",
)

desfa_ng_gcv_daily_schedule = ScheduleDefinition(
    job=desfa_ng_gcv_daily_job,
    cron_schedule="0 2 * * *",
)

desfa_flows_daily_schedule = ScheduleDefinition(
    job=desfa_flows_daily_job,
    cron_schedule="0 2 * * *",
)

desfa_ng_pressure_monthly_schedule = ScheduleDefinition(
    job=desfa_ng_pressure_monthly_job,
    cron_schedule="0 2 * * *",
)

desfa_estimated_vs_actual_offtakes_schedule = ScheduleDefinition(
    job=desfa_estimated_vs_actual_offtakes_job,
    cron_schedule="0 2 * * *",
)

ipto_schedule = ScheduleDefinition(
    job=ipto_job,
    cron_schedule="0 3 * * *",
)

ipto_daily_energy_balance_schedule = ScheduleDefinition(
    job=ipto_daily_energy_balance_job,
    cron_schedule="0 3 * * *",
)

ipto_net_interconnection_flows_schedule = ScheduleDefinition(
    job=ipto_net_interconnection_flows_job,
    cron_schedule="0 3 * * *",
)

ipto_res_injections_schedule = ScheduleDefinition(
    job=ipto_res_injections_job,
    cron_schedule="0 3 * * *",
)

ipto_unit_production_schedule = ScheduleDefinition(
    job=ipto_unit_production_job,
    cron_schedule="0 3 * * *",
)

defs = Definitions(
    assets=all_assets,
    resources={
        "postgres_io_manager": PostgresIOManager(**PG_IOMANAGER_CONFIG)
    },
    schedules=[entsog_schedule, entsoe_hydro_schedule, entsoe_schedule, desfa_schedule, desfa_nominations_schedule,
               desfa_ng_quality_yearly_schedule, desfa_ng_gcv_daily_schedule, desfa_flows_daily_schedule,
               desfa_ng_pressure_monthly_schedule, desfa_estimated_vs_actual_offtakes_schedule, ipto_schedule,
               ipto_daily_energy_balance_schedule, ipto_net_interconnection_flows_schedule,
               ipto_res_injections_schedule, ipto_unit_production_schedule]
)
