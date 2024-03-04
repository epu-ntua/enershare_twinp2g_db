from functools import reduce

import pandas as pd
from dagster import (
    Output,
    asset,
    MonthlyPartitionsDefinition,
    AssetExecutionContext, EnvVar
)
from entsoe import EntsoeRawClient, EntsoePandasClient
from entsoe.exceptions import NoMatchingDataError

from ..utils import entsoe_utils
from ..utils.common import sanitize_series, sanitize_df, timewindow_to_ts
from ..utils.entsoe_utils import transform_columns_to_ids

# ENTSOE assets
country_code = 'GR'


def entsoe_client() -> EntsoePandasClient:
    return EntsoePandasClient(api_key=EnvVar("ENTSOE_API_KEY").get_value())


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2014-11-30"),
    io_manager_key="postgres_io_manager",
    group_name="entsoe",
    op_tags={"dagster/concurrency_key": "entsoe", "concurrency_tag": "entsoe"},
    description="The day-ahead prices for each market time unit (60m). Day-ahead prices refer to the price per MWh as it is settled each hour in the energy market. In Greece, that is HEnEx. ENTSOE's data probably comes from there, as IPTO does not list them."
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
    op_tags={"dagster/concurrency_key": "entsoe", "concurrency_tag": "entsoe"},
    description="Average of real-time load values per market time unit (60m). Actual total load (including losses without stored energy) = net generation – exports + imports – absorbed energy"
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
    op_tags={"dagster/concurrency_key": "entsoe", "concurrency_tag": "entsoe"},
    description="Day-ahead forecast of average total load per market time unit (60m)"
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
    op_tags={"dagster/concurrency_key": "entsoe", "concurrency_tag": "entsoe"},
    description="Week ahead forecast of maximum and minimum load values per day"
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
    op_tags={"dagster/concurrency_key": "entsoe", "concurrency_tag": "entsoe"},
    description="Month ahead forecast of maximum and minimum load values per week"
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
    op_tags={"dagster/concurrency_key": "entsoe", "concurrency_tag": "entsoe"},
    description="Year ahead forecast of maximum and minimum load values per week"
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
    op_tags={"dagster/concurrency_key": "entsoe", "concurrency_tag": "entsoe"},
    description="An estimate of the total scheduled Net generation (MW), per each market time unit (60m) of the following day"
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
    op_tags={"dagster/concurrency_key": "entsoe", "concurrency_tag": "entsoe"},
    description="Averages of forecasts of wind and solar power net generation (MW), per each market time unit (60m) of the following (day-ahead) or same (intraday) day"
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
    op_tags={"dagster/concurrency_key": "entsoe", "concurrency_tag": "entsoe"},
    description="The actual aggregated net generation output, computed as the average of all available instantaneous net generation output values on each market time unit (60m)"
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
    op_tags={"dagster/concurrency_key": "entsoe", "concurrency_tag": "entsoe"},
    description="The measured real flow of energy between neighbouring bidding zones. Average values per market time unit (60m). One column for each directional flow (e.g. gr_al indicates energy flow from Greece to Albania)"
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
    op_tags={"dagster/concurrency_key": "entsoe", "concurrency_tag": "entsoe"},
    description="Aggregated weekly average filling rate of all water reservoir and hydro storage plants (MWh) including the figure for the same week of the previous year"
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
    op_tags={"dagster/concurrency_key": "entsoe", "concurrency_tag": "entsoe"},
    description="The actual net generation output per generation unit >= 100MW, as averaged per market time unit (60m)"
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
