from functools import reduce

import pandas as pd
import pytz
from entsoe import EntsoeRawClient, EntsoePandasClient
from entsoe.exceptions import NoMatchingDataError
from pandas import Timestamp
from pandas.core.indexes.datetimes import DatetimeIndex
from .utils import entsoe_fixes
from .utils.entsoe_fixes import sanitize_df, sanitize_series, transform_columns_to_ids

from dagster import (
    AssetKey,
    DagsterInstance,
    MetadataValue,
    Output,
    asset,
    get_dagster_logger,
    DynamicPartitionsDefinition,
    MonthlyPartitionsDefinition,
    AssetExecutionContext, EnvVar, TimeWindow
)

# ENTSOE assets
country_code = 'GR'


def entsoe_client() -> EntsoePandasClient:
    return EntsoePandasClient(api_key=EnvVar("ENTSOE_API_KEY").get_value())


def timewindow_to_ts(tw: TimeWindow) -> (Timestamp, Timestamp):
    start = Timestamp(tw.start)
    end = Timestamp(tw.end)
    return start, end


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2014-11-30"),
    io_manager_key="postgres_io_manager",
    group_name="entsoe"
)
def day_ahead_prices(context: AssetExecutionContext):
    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")
    series: pd.Series = entsoe_client().query_day_ahead_prices(country_code, start=start, end=end)
    dataframe = sanitize_series(series, ['price'])
    return Output(value=dataframe)


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2014-11-30"),
    io_manager_key="postgres_io_manager",
    group_name="entsoe"
)
def total_load_actual(context: AssetExecutionContext):
    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")
    dataframe: pd.DataFrame = entsoe_client().query_load(country_code, start=start, end=end)
    sanitize_df(dataframe)
    dataframe.columns = ['actual_load']
    return Output(value=dataframe)


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2014-11-30"),
    io_manager_key="postgres_io_manager",
    group_name="entsoe"
)
def total_load_day_ahead(context: AssetExecutionContext):
    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")
    dataframe: pd.DataFrame = entsoe_client().query_load_forecast(country_code, start=start, end=end)
    sanitize_df(dataframe)
    dataframe.columns = ['total_load']
    return Output(value=dataframe)


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2014-11-30"),
    io_manager_key="postgres_io_manager",
    group_name="entsoe"
)
def total_load_week_ahead(context: AssetExecutionContext):
    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")
    # entsoe-py has outdated pandas and does not work
    raw_client = EntsoeRawClient(api_key=EnvVar("ENTSOE_API_KEY").get_value())
    dataframe: pd.DataFrame = entsoe_fixes.query_load_forecast(raw_client=raw_client,
                                                               country_code=country_code,
                                                               start=start,
                                                               end=end,
                                                               process_type='A31')
    return Output(value=dataframe)


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2014-11-30"),
    io_manager_key="postgres_io_manager",
    group_name="entsoe"
)
def total_load_month_ahead(context: AssetExecutionContext):
    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")
    # entsoe-py has outdated pandas and does not work
    raw_client = EntsoeRawClient(api_key=EnvVar("ENTSOE_API_KEY").get_value())
    dataframe: pd.DataFrame = entsoe_fixes.query_load_forecast(raw_client=raw_client,
                                                               country_code=country_code,
                                                               start=start,
                                                               end=end,
                                                               process_type='A32')
    return Output(value=dataframe)


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2014-11-30"),
    io_manager_key="postgres_io_manager",
    group_name="entsoe"
)
def total_load_year_ahead(context: AssetExecutionContext):
    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")
    # entsoe-py has outdated pandas and does not work
    raw_client = EntsoeRawClient(api_key=EnvVar("ENTSOE_API_KEY").get_value())
    dataframe: pd.DataFrame = entsoe_fixes.query_load_forecast(raw_client=raw_client,
                                                               country_code=country_code,
                                                               start=start,
                                                               end=end,
                                                               process_type='A33')
    return Output(value=dataframe)


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2014-11-30"),
    io_manager_key="postgres_io_manager",
    group_name="entsoe"
)
def generation_forecast_day_ahead(context: AssetExecutionContext):
    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")
    result = entsoe_client().query_generation_forecast(country_code, start=start, end=end)
    if isinstance(result, pd.Series):
        result = sanitize_series(result, ['scheduled_generation'])
    else:
        sanitize_df(result)
    return Output(value=result)


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2014-11-30"),
    io_manager_key="postgres_io_manager",
    group_name="entsoe"
)
def generation_forecast_windsolar(context: AssetExecutionContext):
    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")
    df_dayahead: pd.DataFrame = entsoe_client().query_wind_and_solar_forecast(country_code, start=start, end=end)
    df_intraday: pd.DataFrame = entsoe_client().query_intraday_wind_and_solar_forecast(country_code, start=start, end=end)
    sanitize_df(df_dayahead)
    sanitize_df(df_intraday)
    df_dayahead.columns = ['solar_dayahead', 'wind_dayahead']
    df_intraday.columns = ['solar_intraday', 'wind_intraday']
    concat_df = pd.concat([df_dayahead, df_intraday], axis=1)
    return Output(value=concat_df)


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2014-11-30"),
    io_manager_key="postgres_io_manager",
    group_name="entsoe"
)
def actual_generation_per_type(context: AssetExecutionContext):
    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")
    dataframe: pd.DataFrame = entsoe_client().query_generation(country_code, start=start, end=end)
    sanitize_df(dataframe)
    dataframe.columns = ['fossil_brown_coal_lignite', 'fossil_gas', 'fossil_oil', 'hydro_pumped_storage',
                         'hydro_water_reservoir', 'solar', 'wind_onshore']
    return Output(value=dataframe)


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2014-11-30"),
    io_manager_key="postgres_io_manager",
    group_name="entsoe"
)
def crossborder_flows(context: AssetExecutionContext):
    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")
    albania = 'AL'
    bulgaria = 'BG'
    north_mk = 'MK'
    turkey = 'TR'
    italy_south = 'IT_SUD'
    italy_greece = 'IT_GR'
    italy_brindisi = 'IT_BRNN'

    # sometimes there is no data and NoMatchingDataError is raised
    def safe_call(c_from, c_to, strt, nd) -> pd.Series:
        context.log.info(f"Fetching data from {c_from} to {c_to}")
        try:
            result = entsoe_client().query_crossborder_flows(country_code_from=c_from,
                                                             country_code_to=c_to,
                                                             start=strt,
                                                             end=nd)
        except NoMatchingDataError as e:
            context.log.info(f"No matching data found from {c_from} to {c_to}")
            result = pd.Series()
        return result


    # first we query all the italian routes and aggregate
    gr_it_1: pd.Series = safe_call(country_code, italy_brindisi, start, end)
    gr_it_2: pd.Series = safe_call(country_code, italy_south, start, end)
    gr_it_3: pd.Series = safe_call(country_code, italy_greece, start, end)

    gr_it = gr_it_1.add(gr_it_2, fill_value=0).add(gr_it_3, fill_value=0)
    gr_it = sanitize_series(gr_it, ['gr_it'])
    # reverse
    it_gr_1: pd.Series = safe_call(italy_brindisi, country_code, start, end)
    it_gr_2: pd.Series = safe_call(italy_south, country_code, start, end)
    it_gr_3: pd.Series = safe_call(italy_greece, country_code, start, end)
    it_gr = it_gr_1.add(it_gr_2, fill_value=0).add(it_gr_3, fill_value=0)
    it_gr = sanitize_series(it_gr, ['it_gr'])

    gr_al = sanitize_series(safe_call(country_code, albania, start, end), ['gr_al'])
    al_gr = sanitize_series(safe_call(albania, country_code, start, end), ['al_gr'])
    gr_bg = sanitize_series(safe_call(country_code, bulgaria, start, end), ['gr_bg'])
    bg_gr = sanitize_series(safe_call(bulgaria, country_code, start, end), ['bg_gr'])
    gr_mk = sanitize_series(safe_call(country_code, north_mk, start, end), ['gr_mk'])
    mk_gr = sanitize_series(safe_call(north_mk, country_code, start, end), ['mk_gr'])
    gr_tr = sanitize_series(safe_call(country_code, turkey, start, end), ['gr_tr'])
    tr_gr = sanitize_series(safe_call(turkey, country_code, start, end), ['tr_gr'])

    dataframes = [gr_al, gr_bg, gr_it, gr_mk, gr_tr, al_gr, bg_gr, it_gr, mk_gr, tr_gr]

    dataframe = reduce(lambda left, right: left.join(right, how='outer'), dataframes)

    return Output(value=dataframe)


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2014-11-30"),
    io_manager_key="postgres_io_manager",
    group_name="entsoe"
)
def hydro_reservoir_storage(context: AssetExecutionContext):
    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")
    series = entsoe_client().query_aggregate_water_reservoirs_and_hydro_storage(country_code,
                                                                                start=start,
                                                                                end=end)
    # Push to midnight
    series.index = series.index.round('24H')
    dataframe = sanitize_series(series, ['stored_energy'])
    print(dataframe.iloc[:3])
    return Output(value=dataframe)


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2014-11-30"),
    io_manager_key="postgres_io_manager",
    group_name="entsoe"
)
def actual_generation_per_generation_unit(context: AssetExecutionContext):
    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")
    dataframe: pd.DataFrame = entsoe_client().query_generation_per_plant(country_code, start=start, end=end, include_eic=True)

    sanitize_df(dataframe)
    # keep only the first level of the columns, in lowercase, whitespace replaced with underscore
    dataframe.columns = dataframe.columns.get_level_values(0).map(lambda x: x.lower()).str.replace(' ', '_')

    dataframe = transform_columns_to_ids(dataframe)

    return Output(value=dataframe)


# IPTO assets

@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2014-11-30"),
    io_manager_key="postgres_io_manager",
    group_name="ipto"
)
def ipto_day_ahead_load_forecast(context: AssetExecutionContext):
    context.log.info(context.partition_time_window)

    entsoe_client = EntsoePandasClient(api_key=EnvVar("ENTSOE_API_KEY").get_value())
    country_code = 'GR'
    start = Timestamp(context.partition_time_window.start)
    end = Timestamp(context.partition_time_window.end)
    dataframe: pd.DataFrame = entsoe_client.query_load(country_code, start=start, end=end)
    dataframe.index.rename(name='timestamp', inplace=True)
    dataframe.columns = ['actual_load']
    return Output(value=dataframe)


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2014-11-30"),
    io_manager_key="postgres_io_manager",
    group_name="ipto"
)
def ipto_day_ahead_res_forecast(context: AssetExecutionContext):
    context.log.info(context.partition_time_window)

    entsoe_client = EntsoePandasClient(api_key=EnvVar("ENTSOE_API_KEY").get_value())
    country_code = 'GR'
    start = Timestamp(context.partition_time_window.start)
    end = Timestamp(context.partition_time_window.end)
    dataframe: pd.DataFrame = entsoe_client.query_load(country_code, start=start, end=end)
    dataframe.index.rename(name='timestamp', inplace=True)
    dataframe.columns = ['actual_load']
    return Output(value=dataframe)


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2014-11-30"),
    io_manager_key="postgres_io_manager",
    group_name="ipto"
)
def ipto_week_ahead_load_forecast(context: AssetExecutionContext):
    context.log.info(context.partition_time_window)

    entsoe_client = EntsoePandasClient(api_key=EnvVar("ENTSOE_API_KEY").get_value())
    country_code = 'GR'
    start = Timestamp(context.partition_time_window.start)
    end = Timestamp(context.partition_time_window.end)
    dataframe: pd.DataFrame = entsoe_client.query_load(country_code, start=start, end=end)
    dataframe.index.rename(name='timestamp', inplace=True)
    dataframe.columns = ['actual_load']
    return Output(value=dataframe)


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2014-11-30"),
    io_manager_key="postgres_io_manager",
    group_name="ipto"
)
def ipto_net_interconnection_flows(context: AssetExecutionContext):
    context.log.info(context.partition_time_window)

    entsoe_client = EntsoePandasClient(api_key=EnvVar("ENTSOE_API_KEY").get_value())
    country_code = 'GR'
    start = Timestamp(context.partition_time_window.start)
    end = Timestamp(context.partition_time_window.end)
    dataframe: pd.DataFrame = entsoe_client.query_load(country_code, start=start, end=end)
    dataframe.index.rename(name='timestamp', inplace=True)
    dataframe.columns = ['actual_load']
    return Output(value=dataframe)


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2014-11-30"),
    io_manager_key="postgres_io_manager",
    group_name="ipto"
)
def ipto_res_injections(context: AssetExecutionContext):
    context.log.info(context.partition_time_window)

    entsoe_client = EntsoePandasClient(api_key=EnvVar("ENTSOE_API_KEY").get_value())
    country_code = 'GR'
    start = Timestamp(context.partition_time_window.start)
    end = Timestamp(context.partition_time_window.end)
    dataframe: pd.DataFrame = entsoe_client.query_load(country_code, start=start, end=end)
    dataframe.index.rename(name='timestamp', inplace=True)
    dataframe.columns = ['actual_load']
    return Output(value=dataframe)


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2014-11-30"),
    io_manager_key="postgres_io_manager",
    group_name="ipto"
)
def ipto_unit_production(context: AssetExecutionContext):
    context.log.info(context.partition_time_window)

    entsoe_client = EntsoePandasClient(api_key=EnvVar("ENTSOE_API_KEY").get_value())
    country_code = 'GR'
    start = Timestamp(context.partition_time_window.start)
    end = Timestamp(context.partition_time_window.end)
    dataframe: pd.DataFrame = entsoe_client.query_load(country_code, start=start, end=end)
    dataframe.index.rename(name='timestamp', inplace=True)
    dataframe.columns = ['actual_load']
    return Output(value=dataframe)


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2014-11-30"),
    io_manager_key="postgres_io_manager",
    group_name="ipto"
)
def ipto_daily_energy_balance(context: AssetExecutionContext):
    context.log.info(context.partition_time_window)

    entsoe_client = EntsoePandasClient(api_key=EnvVar("ENTSOE_API_KEY").get_value())
    country_code = 'GR'
    start = Timestamp(context.partition_time_window.start)
    end = Timestamp(context.partition_time_window.end)
    dataframe: pd.DataFrame = entsoe_client.query_load(country_code, start=start, end=end)
    dataframe.index.rename(name='timestamp', inplace=True)
    dataframe.columns = ['actual_load']
    return Output(value=dataframe)


# ENTSOG assets

@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2016-12-31"),
    io_manager_key="postgres_io_manager",
    group_name="entsog",
    description="Deliveries / Off-takes (imports for entry points/off-takes per exit points) per day since 2017"
)
def entsog_flows_daily(context: AssetExecutionContext):
    context.log.info(context.partition_time_window)

    entsoe_client = EntsoePandasClient(api_key=EnvVar("ENTSOE_API_KEY").get_value())
    country_code = 'GR'
    start = Timestamp(context.partition_time_window.start)
    end = Timestamp(context.partition_time_window.end)
    dataframe: pd.DataFrame = entsoe_client.query_load(country_code, start=start, end=end)
    dataframe.index.rename(name='timestamp', inplace=True)
    dataframe.columns = ['actual_load']
    return Output(value=dataframe)


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2016-12-31"),
    io_manager_key="postgres_io_manager",
    group_name="entsog",
    description="Daily nominations for entry and exit points since 2017"
)
def entsog_nominations_daily(context: AssetExecutionContext):
    context.log.info(context.partition_time_window)

    entsoe_client = EntsoePandasClient(api_key=EnvVar("ENTSOE_API_KEY").get_value())
    country_code = 'GR'
    start = Timestamp(context.partition_time_window.start)
    end = Timestamp(context.partition_time_window.end)
    dataframe: pd.DataFrame = entsoe_client.query_load(country_code, start=start, end=end)
    dataframe.index.rename(name='timestamp', inplace=True)
    dataframe.columns = ['actual_load']
    return Output(value=dataframe)


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2016-12-31"),
    io_manager_key="postgres_io_manager",
    group_name="entsog",
    description="Daily allocations for entry and exit points since 2017"
)
def entsog_allocations_daily(context: AssetExecutionContext):
    context.log.info(context.partition_time_window)

    entsoe_client = EntsoePandasClient(api_key=EnvVar("ENTSOE_API_KEY").get_value())
    country_code = 'GR'
    start = Timestamp(context.partition_time_window.start)
    end = Timestamp(context.partition_time_window.end)
    dataframe: pd.DataFrame = entsoe_client.query_load(country_code, start=start, end=end)
    dataframe.index.rename(name='timestamp', inplace=True)
    dataframe.columns = ['actual_load']
    return Output(value=dataframe)


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2016-12-31"),
    io_manager_key="postgres_io_manager",
    group_name="entsog",
    description="Daily renominations for entry and exit points since 2017"
)
def entsog_renominations_daily(context: AssetExecutionContext):
    context.log.info(context.partition_time_window)

    entsoe_client = EntsoePandasClient(api_key=EnvVar("ENTSOE_API_KEY").get_value())
    country_code = 'GR'
    start = Timestamp(context.partition_time_window.start)
    end = Timestamp(context.partition_time_window.end)
    dataframe: pd.DataFrame = entsoe_client.query_load(country_code, start=start, end=end)
    dataframe.index.rename(name='timestamp', inplace=True)
    dataframe.columns = ['actual_load']
    return Output(value=dataframe)


# DESFA assets

@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2014-11-30"),
    io_manager_key="postgres_io_manager",
    group_name="desfa"
)
def desfa_flows_daily(context: AssetExecutionContext):
    context.log.info(context.partition_time_window)

    entsoe_client = EntsoePandasClient(api_key=EnvVar("ENTSOE_API_KEY").get_value())
    country_code = 'GR'
    start = Timestamp(context.partition_time_window.start)
    end = Timestamp(context.partition_time_window.end)
    dataframe: pd.DataFrame = entsoe_client.query_load(country_code, start=start, end=end)
    dataframe.index.rename(name='timestamp', inplace=True)
    dataframe.columns = ['actual_load']
    return Output(value=dataframe)


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2014-11-30"),
    io_manager_key="postgres_io_manager",
    group_name="desfa"
)
def desfa_flows_hourly(context: AssetExecutionContext):
    context.log.info(context.partition_time_window)

    entsoe_client = EntsoePandasClient(api_key=EnvVar("ENTSOE_API_KEY").get_value())
    country_code = 'GR'
    start = Timestamp(context.partition_time_window.start)
    end = Timestamp(context.partition_time_window.end)
    dataframe: pd.DataFrame = entsoe_client.query_load(country_code, start=start, end=end)
    dataframe.index.rename(name='timestamp', inplace=True)
    dataframe.columns = ['actual_load']
    return Output(value=dataframe)


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2014-11-30"),
    io_manager_key="postgres_io_manager",
    group_name="desfa"
)
def desfa_ng_quality_yearly(context: AssetExecutionContext):
    context.log.info(context.partition_time_window)

    entsoe_client = EntsoePandasClient(api_key=EnvVar("ENTSOE_API_KEY").get_value())
    country_code = 'GR'
    start = Timestamp(context.partition_time_window.start)
    end = Timestamp(context.partition_time_window.end)
    dataframe: pd.DataFrame = entsoe_client.query_load(country_code, start=start, end=end)
    dataframe.index.rename(name='timestamp', inplace=True)
    dataframe.columns = ['actual_load']
    return Output(value=dataframe)


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2014-11-30"),
    io_manager_key="postgres_io_manager",
    group_name="desfa"
)
def desfa_ng_pressure_monthly(context: AssetExecutionContext):
    context.log.info(context.partition_time_window)

    entsoe_client = EntsoePandasClient(api_key=EnvVar("ENTSOE_API_KEY").get_value())
    country_code = 'GR'
    start = Timestamp(context.partition_time_window.start)
    end = Timestamp(context.partition_time_window.end)
    dataframe: pd.DataFrame = entsoe_client.query_load(country_code, start=start, end=end)
    dataframe.index.rename(name='timestamp', inplace=True)
    dataframe.columns = ['actual_load']
    return Output(value=dataframe)


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2014-11-30"),
    io_manager_key="postgres_io_manager",
    group_name="desfa"
)
def desfa_ng_gcv_daily(context: AssetExecutionContext):
    context.log.info(context.partition_time_window)

    entsoe_client = EntsoePandasClient(api_key=EnvVar("ENTSOE_API_KEY").get_value())
    country_code = 'GR'
    start = Timestamp(context.partition_time_window.start)
    end = Timestamp(context.partition_time_window.end)
    dataframe: pd.DataFrame = entsoe_client.query_load(country_code, start=start, end=end)
    dataframe.index.rename(name='timestamp', inplace=True)
    dataframe.columns = ['actual_load']
    return Output(value=dataframe)


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2014-11-30"),
    io_manager_key="postgres_io_manager",
    group_name="desfa"
)
def desfa_nominations_daily(context: AssetExecutionContext):
    context.log.info(context.partition_time_window)

    entsoe_client = EntsoePandasClient(api_key=EnvVar("ENTSOE_API_KEY").get_value())
    country_code = 'GR'
    start = Timestamp(context.partition_time_window.start)
    end = Timestamp(context.partition_time_window.end)
    dataframe: pd.DataFrame = entsoe_client.query_load(country_code, start=start, end=end)
    dataframe.index.rename(name='timestamp', inplace=True)
    dataframe.columns = ['actual_load']
    return Output(value=dataframe)
