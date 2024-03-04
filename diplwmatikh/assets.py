from functools import reduce
from io import BytesIO

import pandas as pd
import numpy as np
import pytz
import requests
import datetime
from entsog import EntsogRawClient, EntsogPandasClient
from entsoe import EntsoeRawClient, EntsoePandasClient
from entsoe.exceptions import NoMatchingDataError
from pandas import Timestamp
from pandas.core.indexes.datetimes import DatetimeIndex
from .utils import entsoe_utils, entsog_utils
from .utils.entsoe_utils import transform_columns_to_ids
from .utils.common import sanitize_series, sanitize_df

from dagster import (
    AssetKey,
    DagsterInstance,
    MetadataValue,
    Output,
    asset,
    get_dagster_logger,
    StaticPartitionsDefinition,
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
    group_name="entsoe",
    op_tags={"dagster/concurrency_key": "entsoe", "concurrency_tag": "entsoe"}
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
    group_name="entsoe",
    op_tags={"dagster/concurrency_key": "entsoe", "concurrency_tag": "entsoe"}
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
    group_name="entsoe",
    op_tags={"dagster/concurrency_key": "entsoe", "concurrency_tag": "entsoe"}
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
    group_name="entsoe",
    op_tags={"dagster/concurrency_key": "entsoe", "concurrency_tag": "entsoe"}
)
def total_load_week_ahead(context: AssetExecutionContext):
    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")
    # entsoe-py has outdated pandas and does not work
    raw_client = EntsoeRawClient(api_key=EnvVar("ENTSOE_API_KEY").get_value())
    dataframe: pd.DataFrame = entsoe_utils.query_load_forecast(raw_client=raw_client,
                                                               country_code=country_code,
                                                               start=start,
                                                               end=end,
                                                               process_type='A31')
    return Output(value=dataframe)


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2014-11-30"),
    io_manager_key="postgres_io_manager",
    group_name="entsoe",
    op_tags={"dagster/concurrency_key": "entsoe", "concurrency_tag": "entsoe"}
)
def total_load_month_ahead(context: AssetExecutionContext):
    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")
    # entsoe-py has outdated pandas and does not work
    raw_client = EntsoeRawClient(api_key=EnvVar("ENTSOE_API_KEY").get_value())
    dataframe: pd.DataFrame = entsoe_utils.query_load_forecast(raw_client=raw_client,
                                                               country_code=country_code,
                                                               start=start,
                                                               end=end,
                                                               process_type='A32')
    return Output(value=dataframe)


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2014-11-30"),
    io_manager_key="postgres_io_manager",
    group_name="entsoe",
    op_tags={"dagster/concurrency_key": "entsoe", "concurrency_tag": "entsoe"}
)
def total_load_year_ahead(context: AssetExecutionContext):
    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")
    # entsoe-py has outdated pandas and does not work
    raw_client = EntsoeRawClient(api_key=EnvVar("ENTSOE_API_KEY").get_value())
    dataframe: pd.DataFrame = entsoe_utils.query_load_forecast(raw_client=raw_client,
                                                               country_code=country_code,
                                                               start=start,
                                                               end=end,
                                                               process_type='A33')
    return Output(value=dataframe)


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2014-11-30"),
    io_manager_key="postgres_io_manager",
    group_name="entsoe",
    op_tags={"dagster/concurrency_key": "entsoe", "concurrency_tag": "entsoe"}
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
    group_name="entsoe",
    op_tags={"dagster/concurrency_key": "entsoe", "concurrency_tag": "entsoe"}
)
def generation_forecast_windsolar(context: AssetExecutionContext):
    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")

    df_dayahead = None
    df_intraday = None

    try:
        df_dayahead = entsoe_client().query_wind_and_solar_forecast(country_code, start=start, end=end)
    except NoMatchingDataError:
        pass

    try:
        df_intraday = entsoe_client().query_intraday_wind_and_solar_forecast(country_code, start=start,
                                                                             end=end)
    except NoMatchingDataError:
        pass

    if df_dayahead is not None:
        sanitize_df(df_dayahead)
        df_dayahead.columns = ['solar_dayahead', 'wind_dayahead']

    if df_intraday is not None:
        sanitize_df(df_intraday)
        df_intraday.columns = ['solar_intraday', 'wind_intraday']

    if df_dayahead is not None and df_intraday is not None:
        concat_df = pd.concat([df_dayahead, df_intraday], axis=1)
    elif df_dayahead is not None:
        concat_df = df_dayahead
    elif df_intraday is not None:
        concat_df = df_intraday
    else:
        raise NoMatchingDataError

    return Output(value=concat_df)


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2014-11-30"),
    io_manager_key="postgres_io_manager",
    group_name="entsoe",
    op_tags={"dagster/concurrency_key": "entsoe", "concurrency_tag": "entsoe"}
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
    group_name="entsoe",
    op_tags={"dagster/concurrency_key": "entsoe", "concurrency_tag": "entsoe"}
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
    partitions_def=MonthlyPartitionsDefinition(start_date="2017-09-01"),
    io_manager_key="postgres_io_manager",
    group_name="entsoe",
    op_tags={"dagster/concurrency_key": "entsoe", "concurrency_tag": "entsoe"}
)
def hydro_reservoir_storage(context: AssetExecutionContext):
    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")
    # entsoe-py's function is mislabeled as returning a dataframe, it actually returns a series so the warning in
    # sanitize_series() should be ignored
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
    group_name="entsoe",
    op_tags={"dagster/concurrency_key": "entsoe", "concurrency_tag": "entsoe"}
)
def actual_generation_per_generation_unit(context: AssetExecutionContext):
    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")
    dataframe: pd.DataFrame = entsoe_client().query_generation_per_plant(country_code, start=start, end=end,
                                                                         include_eic=True)

    sanitize_df(dataframe)
    # keep only the first level of the columns, in lowercase, whitespace replaced with underscore
    dataframe.columns = dataframe.columns.get_level_values(0).map(lambda x: x.lower()).str.replace(' ', '_')

    dataframe = transform_columns_to_ids(dataframe)

    return Output(value=dataframe)


# IPTO assets

@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2014-11-30"),
    io_manager_key="postgres_io_manager",
    group_name="ipto",
    op_tags={"dagster/concurrency_key": "ipto", "concurrency_tag": "ipto"}
)
def ipto_day_ahead_load_forecast(context: AssetExecutionContext):
    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")

    entsoe_client = EntsoePandasClient(api_key=EnvVar("ENTSOE_API_KEY").get_value())
    country_code = 'GR'

    dataframe: pd.DataFrame = entsoe_client.query_load(country_code, start=start, end=end)
    dataframe.index.rename(name='timestamp', inplace=True)
    dataframe.columns = ['actual_load']
    return Output(value=dataframe)


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2014-11-30"),
    io_manager_key="postgres_io_manager",
    group_name="ipto",
    op_tags={"dagster/concurrency_key": "ipto", "concurrency_tag": "ipto"}
)
def ipto_day_ahead_res_forecast(context: AssetExecutionContext):
    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")

    entsoe_client = EntsoePandasClient(api_key=EnvVar("ENTSOE_API_KEY").get_value())
    country_code = 'GR'

    dataframe: pd.DataFrame = entsoe_client.query_load(country_code, start=start, end=end)
    dataframe.index.rename(name='timestamp', inplace=True)
    dataframe.columns = ['actual_load']
    return Output(value=dataframe)


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2014-11-30"),
    io_manager_key="postgres_io_manager",
    group_name="ipto",
    op_tags={"dagster/concurrency_key": "ipto", "concurrency_tag": "ipto"}
)
def ipto_week_ahead_load_forecast(context: AssetExecutionContext):
    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")

    entsoe_client = EntsoePandasClient(api_key=EnvVar("ENTSOE_API_KEY").get_value())
    country_code = 'GR'

    dataframe: pd.DataFrame = entsoe_client.query_load(country_code, start=start, end=end)
    dataframe.index.rename(name='timestamp', inplace=True)
    dataframe.columns = ['actual_load']
    return Output(value=dataframe)


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2014-11-30"),
    io_manager_key="postgres_io_manager",
    group_name="ipto",
    op_tags={"dagster/concurrency_key": "ipto", "concurrency_tag": "ipto"}
)
def ipto_net_interconnection_flows(context: AssetExecutionContext):
    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")

    entsoe_client = EntsoePandasClient(api_key=EnvVar("ENTSOE_API_KEY").get_value())
    country_code = 'GR'

    dataframe: pd.DataFrame = entsoe_client.query_load(country_code, start=start, end=end)
    dataframe.index.rename(name='timestamp', inplace=True)
    dataframe.columns = ['actual_load']
    return Output(value=dataframe)


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2014-11-30"),
    io_manager_key="postgres_io_manager",
    group_name="ipto",
    op_tags={"dagster/concurrency_key": "ipto", "concurrency_tag": "ipto"}
)
def ipto_res_injections(context: AssetExecutionContext):
    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")

    entsoe_client = EntsoePandasClient(api_key=EnvVar("ENTSOE_API_KEY").get_value())
    country_code = 'GR'

    dataframe: pd.DataFrame = entsoe_client.query_load(country_code, start=start, end=end)
    dataframe.index.rename(name='timestamp', inplace=True)
    dataframe.columns = ['actual_load']
    return Output(value=dataframe)


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2014-11-30"),
    io_manager_key="postgres_io_manager",
    group_name="ipto",
    op_tags={"dagster/concurrency_key": "ipto", "concurrency_tag": "ipto"}
)
def ipto_unit_production(context: AssetExecutionContext):
    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")

    entsoe_client = EntsoePandasClient(api_key=EnvVar("ENTSOE_API_KEY").get_value())
    country_code = 'GR'

    dataframe: pd.DataFrame = entsoe_client.query_load(country_code, start=start, end=end)
    dataframe.index.rename(name='timestamp', inplace=True)
    dataframe.columns = ['actual_load']
    return Output(value=dataframe)


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2014-11-30"),
    io_manager_key="postgres_io_manager",
    group_name="ipto",
    op_tags={"dagster/concurrency_key": "ipto", "concurrency_tag": "ipto"}
)
def ipto_daily_energy_balance(context: AssetExecutionContext):
    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")

    entsoe_client = EntsoePandasClient(api_key=EnvVar("ENTSOE_API_KEY").get_value())
    country_code = 'GR'

    dataframe: pd.DataFrame = entsoe_client.query_load(country_code, start=start, end=end)
    dataframe.index.rename(name='timestamp', inplace=True)
    dataframe.columns = ['actual_load']
    return Output(value=dataframe)


# ENTSOG assets

@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2016-12-31"),
    io_manager_key="postgres_io_manager",
    group_name="entsog",
    op_tags={"dagster/concurrency_key": "entsog", "concurrency_tag": "entsog"},
    description="Deliveries / Off-takes (imports for entry points/off-takes per exit points) per day since 2017"
)
def entsog_flows_daily(context: AssetExecutionContext):
    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")

    keys = entsog_utils.greek_operator_point_directions()

    # Breaking the date range down to avoid timeouts (one request per two days, as daily returns no data)
    days_list = pd.date_range(start, end, freq='2D')

    bidaily_data = []
    for day in days_list:
        data = entsog_utils.entsog_api_call_with_retries(day, day + pd.Timedelta(days=1), ['physical_flow'], keys, context)

        context.log.info(f"Fetched data from {day.strftime('%Y-%m-%d')} to {(day + pd.Timedelta(days=1)).strftime('%Y-%m-%d')}")
        # Rename columns to more applicable names
        data.rename(columns={"direction_key": "point_type", "period_from": "timestamp", "point_label": "point_id"},
                    inplace=True)
        # Set timestamp index and sanitize (to UTC/tz-naive)
        data['timestamp'] = pd.to_datetime(data['timestamp'], utc=True)
        data.set_index(pd.DatetimeIndex(data['timestamp']), inplace=True)
        sanitize_df(data)
        # Add remaining index columns to form primary key)
        data.set_index(['point_id', 'point_type'], inplace=True, append=True)
        # Keep only non-index column
        data = data[['value']]
        # Remove rows where 'value' contains an empty string
        data = data[data['value'] != '']
        bidaily_data.append(data)

    complete_data = pd.concat(bidaily_data)

    return Output(value=complete_data)


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2016-12-31"),
    io_manager_key="postgres_io_manager",
    group_name="entsog",
    op_tags={"dagster/concurrency_key": "entsog", "concurrency_tag": "entsog"},
    description="Daily nominations for entry and exit points since 2017"
)
def entsog_nominations_daily(context: AssetExecutionContext):
    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")

    keys = entsog_utils.greek_operator_point_directions()

    # Breaking the date range down to avoid timeouts (one request per two days, as daily returns no data)
    days_list = pd.date_range(start, end, freq='2D')

    bidaily_data = []
    for day in days_list:
        data = entsog_utils.entsog_api_call_with_retries(day, day + pd.Timedelta(days=1), ['nomination'], keys, context)

        context.log.info(f"Fetched data from {day.strftime('%Y-%m-%d')} to {(day + pd.Timedelta(days=1)).strftime('%Y-%m-%d')}")
        # Rename columns to more applicable names
        data.rename(columns={"direction_key": "point_type", "period_from": "timestamp", "point_label": "point_id"},
                    inplace=True)
        # Set timestamp index and sanitize (to UTC/tz-naive)
        data['timestamp'] = pd.to_datetime(data['timestamp'], utc=True)
        data.set_index(pd.DatetimeIndex(data['timestamp']), inplace=True)
        sanitize_df(data)
        # Add remaining index columns to form primary key)
        data.set_index(['point_id', 'point_type'], inplace=True, append=True)
        # Keep only non-index column
        data = data[['value']]
        # Remove rows where 'value' contains an empty string
        data = data[data['value'] != '']
        bidaily_data.append(data)

    complete_data = pd.concat(bidaily_data)

    return Output(value=complete_data)


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2016-12-31"),
    io_manager_key="postgres_io_manager",
    group_name="entsog",
    op_tags={"dagster/concurrency_key": "entsog", "concurrency_tag": "entsog"},
    description="Daily allocations for entry and exit points since 2017"
)
def entsog_allocations_daily(context: AssetExecutionContext):
    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")

    keys = entsog_utils.greek_operator_point_directions()

    # Breaking the date range down to avoid timeouts (one request per two days, as daily returns no data)
    days_list = pd.date_range(start, end, freq='2D')

    bidaily_data = []
    for day in days_list:
        data = entsog_utils.entsog_api_call_with_retries(day, day + pd.Timedelta(days=1), ['allocation'], keys, context)

        context.log.info(f"Fetched data from {day.strftime('%Y-%m-%d')} to {(day + pd.Timedelta(days=1)).strftime('%Y-%m-%d')}")
        # Rename columns to more applicable names
        data.rename(columns={"direction_key": "point_type", "period_from": "timestamp", "point_label": "point_id"},
                    inplace=True)
        # Set timestamp index and sanitize (to UTC/tz-naive)
        data['timestamp'] = pd.to_datetime(data['timestamp'], utc=True)
        data.set_index(pd.DatetimeIndex(data['timestamp']), inplace=True)
        sanitize_df(data)
        # Add remaining index columns to form primary key)
        data.set_index(['point_id', 'point_type'], inplace=True, append=True)
        # Keep only non-index column
        data = data[['value']]
        # Remove rows where 'value' contains an empty string
        data = data[data['value'] != '']
        bidaily_data.append(data)

    complete_data = pd.concat(bidaily_data)

    return Output(value=complete_data)


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2016-12-31"),
    io_manager_key="postgres_io_manager",
    group_name="entsog",
    op_tags={"dagster/concurrency_key": "entsog", "concurrency_tag": "entsog"},
    description="Daily renominations for entry and exit points since 2017"
)
def entsog_renominations_daily(context: AssetExecutionContext):
    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")

    keys = entsog_utils.greek_operator_point_directions()

    # Breaking the date range down to avoid timeouts (one request per two days, as daily returns no data)
    days_list = pd.date_range(start, end, freq='2D')

    bidaily_data = []
    for day in days_list:
        data = entsog_utils.entsog_api_call_with_retries(day, day + pd.Timedelta(days=1), ['renomination'], keys, context)

        context.log.info(f"Fetched data from {day.strftime('%Y-%m-%d')} to {(day + pd.Timedelta(days=1)).strftime('%Y-%m-%d')}")
        # Rename columns to more applicable names
        data.rename(columns={"direction_key": "point_type", "period_from": "timestamp", "point_label": "point_id"},
                    inplace=True)
        # Set timestamp index and sanitize (to UTC/tz-naive)
        data['timestamp'] = pd.to_datetime(data['timestamp'], utc=True)
        data.set_index(pd.DatetimeIndex(data['timestamp']), inplace=True)
        sanitize_df(data)
        # Add remaining index columns to form primary key)
        data.set_index(['point_id', 'point_type'], inplace=True, append=True)
        # Keep only non-index column
        data = data[['value']]
        # Remove rows where 'value' contains an empty string
        data = data[data['value'] != '']
        bidaily_data.append(data)
    context.log.info("Completed fetching data for ")

    complete_data = pd.concat(bidaily_data)

    return Output(value=complete_data)


# DESFA assets

@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2014-11-30"),
    io_manager_key="postgres_io_manager",
    group_name="desfa",
    op_tags={"dagster/concurrency_key": "desfa", "concurrency_tag": "desfa"}
)
def desfa_flows_daily(context: AssetExecutionContext):
    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")

    entsoe_client = EntsoePandasClient(api_key=EnvVar("ENTSOE_API_KEY").get_value())
    country_code = 'GR'

    dataframe: pd.DataFrame = entsoe_client.query_load(country_code, start=start, end=end)
    dataframe.index.rename(name='timestamp', inplace=True)
    dataframe.columns = ['actual_load']
    return Output(value=dataframe)


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2014-11-30"),
    io_manager_key="postgres_io_manager",
    group_name="desfa",
    op_tags={"dagster/concurrency_key": "desfa", "concurrency_tag": "desfa"}
)
def desfa_flows_hourly(context: AssetExecutionContext):
    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")

    entsoe_client = EntsoePandasClient(api_key=EnvVar("ENTSOE_API_KEY").get_value())
    country_code = 'GR'

    dataframe: pd.DataFrame = entsoe_client.query_load(country_code, start=start, end=end)
    dataframe.index.rename(name='timestamp', inplace=True)
    dataframe.columns = ['actual_load']
    return Output(value=dataframe)


@asset(
    partitions_def=StaticPartitionsDefinition(["desfa_ng_quality_yearly_monopartition"]),
    io_manager_key="postgres_io_manager",
    group_name="desfa",
    op_tags={"dagster/concurrency_key": "desfa", "concurrency_tag": "desfa"}
)
def desfa_ng_quality_yearly(context: AssetExecutionContext):
    context.log.info(f"Handling single partition.")

    url = 'https://www.desfa.gr/userfiles/pdflist/DDRA/NG-QUALITY.xls'

    # Entry points
    search_strings = ["AGIA TRIADA", "SIDIROKASTRO", "KIPI", "NEA MESIMVRIA"]

    # Fetch the content of the .xls file
    response = requests.get(url)
    dataframes = {}
    # Check if the request was successful
    if response.status_code == 200:
        # Use BytesIO to create a file-like object from the content
        file_content = BytesIO(response.content)
        full_df = pd.read_excel(file_content, sheet_name=0)

        for search_string in search_strings:
            # Find the row index for the search string
            start_row = None
            current_year = datetime.date.today().year
            for row_idx in range(len(full_df)):
                if f"Entry Point: {search_string}" in full_df.iloc[row_idx].to_string():
                    start_row = row_idx + 3  # Start from the row 3 rows below the found search string
                    break
            if start_row is not None:
                # Assuming the DataFrame has enough rows and columns, adjust as necessary
                # Nea mesimvria has data from 2021, the rest from 2008
                if "NEA MESIMVRIA" in search_string:
                    end_row = min(start_row + (current_year - 2021) + 1, len(full_df))  # To not go beyond the DataFrame
                else:
                    end_row = min(start_row + (current_year - 2008) + 1, len(full_df))  # To not go beyond the DataFrame
                # Assuming we want the first 16 columns, adjust the slicing as necessary
                block = full_df.iloc[start_row:end_row, :16].copy()

                block.columns = ["timestamp", "c1", "c2", "c3", "i_c4", "n_c4", "i_c5", "n_c5", "neo_c5",
                                 "c6_plus", "n2", "co2", "gross_heating_value", "wobbe_index", "water_dew_point",
                                 "hydrocarbon_dew_point_max"]

                # Convert the 'Year' column to datetime format, ensuring it starts on Jan 1st at 00:00
                block['timestamp'] = pd.to_datetime(block['timestamp'], format='%Y')
                # Set the 'timestamp' column as the index of the DataFrame
                block.set_index('timestamp', inplace=True)
                # Ensure the index is timezone-naive
                block.index = block.index.tz_localize(None)
                # Insert point_id column at start
                block.insert(0, 'point_id', search_string)

                # Replace '-' cells with NaN
                def replace_dash_with_nan(cell):
                    if isinstance(cell, str) and cell.strip() == "-":
                        return np.nan
                    else:
                        return cell

                # Apply the function to remove dashes each cell of the DataFrame
                block = block.map(replace_dash_with_nan)

                # Replace "<" prefix in the specific columns (if not NaN, in which case dtype is np.float64)
                if block["hydrocarbon_dew_point_max"].dtype == object:
                    block["hydrocarbon_dew_point_max"] = block["hydrocarbon_dew_point_max"].astype(str)
                    block["hydrocarbon_dew_point_max"] = block["hydrocarbon_dew_point_max"].str.replace('<', '')
                    block["hydrocarbon_dew_point_max"] = block["hydrocarbon_dew_point_max"].astype(np.float64)

                dataframes[search_string] = block
            else:
                # No data found for this search string
                context.log.warning(f"No data found for entry point '{search_string}'")

    else:
        raise Exception(f"Failed to fetch the file, status code: {response.status_code}")

    dataframe = pd.concat(dataframes.values())
    dataframe.set_index("point_id", inplace=True, append=True)

    return Output(value=dataframe)


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2014-11-30"),
    io_manager_key="postgres_io_manager",
    group_name="desfa",
    op_tags={"dagster/concurrency_key": "desfa", "concurrency_tag": "desfa"}
)
def desfa_ng_pressure_monthly(context: AssetExecutionContext):
    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")

    entsoe_client = EntsoePandasClient(api_key=EnvVar("ENTSOE_API_KEY").get_value())
    country_code = 'GR'

    dataframe: pd.DataFrame = entsoe_client.query_load(country_code, start=start, end=end)
    dataframe.index.rename(name='timestamp', inplace=True)
    dataframe.columns = ['actual_load']
    return Output(value=dataframe)


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2014-11-30"),
    io_manager_key="postgres_io_manager",
    group_name="desfa",
    op_tags={"dagster/concurrency_key": "desfa", "concurrency_tag": "desfa"}
)
def desfa_ng_gcv_daily(context: AssetExecutionContext):
    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")

    entsoe_client = EntsoePandasClient(api_key=EnvVar("ENTSOE_API_KEY").get_value())
    country_code = 'GR'

    dataframe: pd.DataFrame = entsoe_client.query_load(country_code, start=start, end=end)
    dataframe.index.rename(name='timestamp', inplace=True)
    dataframe.columns = ['actual_load']
    return Output(value=dataframe)


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2014-11-30"),
    io_manager_key="postgres_io_manager",
    group_name="desfa",
    op_tags={"dagster/concurrency_key": "desfa", "concurrency_tag": "desfa"}
)
def desfa_nominations_daily(context: AssetExecutionContext):
    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")

    entsoe_client = EntsoePandasClient(api_key=EnvVar("ENTSOE_API_KEY").get_value())
    country_code = 'GR'

    dataframe: pd.DataFrame = entsoe_client.query_load(country_code, start=start, end=end)
    dataframe.index.rename(name='timestamp', inplace=True)
    dataframe.columns = ['actual_load']
    return Output(value=dataframe)
