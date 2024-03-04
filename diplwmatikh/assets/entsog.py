import pandas as pd
from dagster import (
    Output,
    asset,
    MonthlyPartitionsDefinition,
    AssetExecutionContext
)

from ..utils import entsog_utils
from ..utils.common import sanitize_df, timewindow_to_ts


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
        data = entsog_utils.entsog_api_call_with_retries(day, day + pd.Timedelta(days=1),
                                                         ['physical_flow'], keys, context)

        context.log.info(
            f"Fetched data from {day.strftime('%Y-%m-%d')} to {(day + pd.Timedelta(days=1)).strftime('%Y-%m-%d')}")
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

        context.log.info(
            f"Fetched data from {day.strftime('%Y-%m-%d')} to {(day + pd.Timedelta(days=1)).strftime('%Y-%m-%d')}")
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

        context.log.info(
            f"Fetched data from {day.strftime('%Y-%m-%d')} to {(day + pd.Timedelta(days=1)).strftime('%Y-%m-%d')}")
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
        data = entsog_utils.entsog_api_call_with_retries(day, day + pd.Timedelta(days=1),
                                                         ['renomination'], keys, context)

        context.log.info(
            f"Fetched data from {day.strftime('%Y-%m-%d')} to {(day + pd.Timedelta(days=1)).strftime('%Y-%m-%d')}")
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
