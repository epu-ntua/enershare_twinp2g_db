import json
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

from ..utils import entsoe_utils, entsog_utils
from ..utils.entsoe_utils import transform_columns_to_ids
from ..utils.ipto_utils import parameters, deduplicate_json
from ..utils.common import sanitize_series, sanitize_df, timewindow_to_ts

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

top_level_url = 'https://www.admie.gr/getOperationMarketFilewRange'

# IPTO assets

@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2020-10-01"),
    io_manager_key="postgres_io_manager",
    group_name="ipto",
    op_tags={"dagster/concurrency_key": "ipto", "concurrency_tag": "ipto"},
    description="Day-ahead System Total Load Forecast (First run)"
)
def ipto_1day_ahead_load_forecast(context: AssetExecutionContext):
    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")
    file_category = "ISP1DayAheadLoadForecast"

    params = parameters(start, end, file_category)

    context.log.info(f"Fetching file locations from {top_level_url}")
    response = requests.get(top_level_url, params=params)

    if response.status_code == 200:
        data_with_duplicates = response.json()
        data = deduplicate_json(data_with_duplicates, file_category)

        dataframes = []
        for file in data:
            xls_url = file['file_path']

            # Fetch the content of the .xls file
            inner_response = requests.get(xls_url)
            # Check if the request was successful
            if response.status_code == 200:
                # Use BytesIO to create a file-like object from the content
                file_content = BytesIO(inner_response.content)
                df = pd.read_excel(file_content, sheet_name=0, engine='openpyxl')

                # Contained date sometimes false. The correct date can be found via the first part of the filename.
                timestamp_str = xls_url.split('/')[-1].split('_')[0]
                timestamp = pd.Timestamp(timestamp_str)
                context.log.info(f"Got timestamp {timestamp} from URL: {xls_url}")

                # Asserting the format is as expected
                assert df.iloc[:, 36].isnull().all(), \
                    f"Unrecognized format: not all elements of column 36 are null as expected"
                assert df.columns[0] == 'Forecast Daily Analysis per Entity', \
                    f"Unrecognized format: first column is {df.columns[0]}not expected"

                df.drop(df.columns[36], axis=1, inplace=True)   # 36th column is empty, remove it
                df.drop(df.index[0:2], inplace=True)            # Discard first two rows (junk)
                df.drop(df.columns[0:2], axis=1, inplace=True)  # Discard first two columns (junk)

                # Reshape and properly label and index dataframe
                df_transposed = df.transpose()
                df_transposed.columns = ['load_forecast']
                date_range = pd.date_range(start=timestamp, periods=len(df_transposed), freq='30T')
                df_transposed.index = date_range
                df_transposed.index.name = 'timestamp'

                dataframes.append(df_transposed)

            else:
                raise Exception(f"Failed to fetch the xlsx file {xls_url}, status code: {inner_response.status_code}")
        final_df = pd.concat(dataframes, ignore_index=False)
        return Output(value=final_df)
    else:
        raise Exception(f"Failed to fetch the file from {top_level_url}, status code: {response.status_code}")

@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2020-10-01"),
    io_manager_key="postgres_io_manager",
    group_name="ipto",
    op_tags={"dagster/concurrency_key": "ipto", "concurrency_tag": "ipto"},
    description="Day-ahead System Total Load Forecast (Second run)"
)
def ipto_2day_ahead_load_forecast(context: AssetExecutionContext):
    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")
    file_category = "ISP2DayAheadLoadForecast"

    params = parameters(start, end, file_category)

    context.log.info(f"Fetching file locations from {top_level_url}")
    response = requests.get(top_level_url, params=params)

    if response.status_code == 200:
        data_with_duplicates = response.json()
        data = deduplicate_json(data_with_duplicates, file_category)

        dataframes = []
        for file in data:
            xls_url = file['file_path']

            # Fetch the content of the .xls file
            inner_response = requests.get(xls_url)
            # Check if the request was successful
            if response.status_code == 200:
                # Use BytesIO to create a file-like object from the content
                file_content = BytesIO(inner_response.content)
                df = pd.read_excel(file_content, sheet_name=0, engine='openpyxl')

                # Contained date sometimes false. The correct date can be found via the first part of the filename.
                timestamp_str = xls_url.split('/')[-1].split('_')[0]
                timestamp = pd.Timestamp(timestamp_str)
                context.log.info(f"Got timestamp {timestamp} from URL: {xls_url}")

                # Asserting the format is as expected
                assert df.iloc[:, 36].isnull().all(), \
                    f"Unrecognized format: not all elements of column 36 are null as expected"
                assert df.columns[0] == 'Forecast Daily Analysis per Entity', \
                    f"Unrecognized format: first column is {df.columns[0]}not expected"

                df.drop(df.columns[36], axis=1, inplace=True)  # 36th column is empty, remove it
                df.drop(df.index[0:2], inplace=True)  # Discard first two rows (junk)
                df.drop(df.columns[0:2], axis=1, inplace=True)  # Discard first two columns (junk)

                # Reshape and properly label and index dataframe
                df_transposed = df.transpose()
                df_transposed.columns = ['load_forecast']
                date_range = pd.date_range(start=timestamp, periods=len(df_transposed), freq='30T')
                df_transposed.index = date_range
                df_transposed.index.name = 'timestamp'

                dataframes.append(df_transposed)

            else:
                raise Exception(f"Failed to fetch the xlsx file {xls_url}, status code: {inner_response.status_code}")
        final_df = pd.concat(dataframes, ignore_index=False)
        return Output(value=final_df)
    else:
        raise Exception(f"Failed to fetch the file from {top_level_url}, status code: {response.status_code}")

@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2020-10-01"),
    io_manager_key="postgres_io_manager",
    group_name="ipto",
    op_tags={"dagster/concurrency_key": "ipto", "concurrency_tag": "ipto"},
    description="System Total Load Forecast (Third run/Intra-day)"
)
def ipto_3intraday_load_forecast(context: AssetExecutionContext):
    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")
    file_category = "ISP3IntraDayLoadForecast"

    params = parameters(start, end, file_category)

    context.log.info(f"Fetching file locations from {top_level_url}")
    response = requests.get(top_level_url, params=params)

    if response.status_code == 200:
        data_with_duplicates = response.json()
        data = deduplicate_json(data_with_duplicates, file_category)

        dataframes = []
        for file in data:
            xls_url = file['file_path']

            # Fetch the content of the .xls file
            inner_response = requests.get(xls_url)
            # Check if the request was successful
            if response.status_code == 200:
                # Use BytesIO to create a file-like object from the content
                file_content = BytesIO(inner_response.content)
                df = pd.read_excel(file_content, sheet_name=0, engine='openpyxl')

                # Contained date sometimes false. The correct date can be found via the first part of the filename.
                timestamp_str = xls_url.split('/')[-1].split('_')[0]
                timestamp = pd.Timestamp(timestamp_str)
                context.log.info(f"Got timestamp {timestamp} from URL: {xls_url}")

                # Asserting the format is as expected
                assert df.iloc[:, 36].isnull().all(), \
                    f"Unrecognized format: not all elements of column 36 are null as expected"
                assert df.columns[0] == 'Forecast Daily Analysis per Entity', \
                    f"Unrecognized format: first column is {df.columns[0]}not expected"

                df.drop(df.columns[36], axis=1, inplace=True)  # 36th column is empty, remove it
                df.drop(df.index[0:2], inplace=True)  # Discard first two rows (junk)
                df.drop(df.columns[0:2], axis=1, inplace=True)  # Discard first two columns (junk)

                # Reshape and properly label and index dataframe
                df_transposed = df.transpose()
                df_transposed.columns = ['load_forecast']
                date_range = pd.date_range(start=timestamp, periods=len(df_transposed), freq='30T')
                df_transposed.index = date_range
                df_transposed.index.name = 'timestamp'

                dataframes.append(df_transposed)

            else:
                raise Exception(f"Failed to fetch the xlsx file {xls_url}, status code: {inner_response.status_code}")
        final_df = pd.concat(dataframes, ignore_index=False)
        return Output(value=final_df)
    else:
        raise Exception(f"Failed to fetch the file from {top_level_url}, status code: {response.status_code}")


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2020-10-01"),
    io_manager_key="postgres_io_manager",
    group_name="ipto",
    op_tags={"dagster/concurrency_key": "ipto", "concurrency_tag": "ipto"},
    description="Day-ahead Renewables Forecast (First run)"
)
def ipto_1day_ahead_res_forecast(context: AssetExecutionContext):
    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")
    file_category = "ISP1DayAheadRESForecast"

    params = parameters(start, end, file_category)

    context.log.info(f"Fetching file locations from {top_level_url}")
    response = requests.get(top_level_url, params=params)

    if response.status_code == 200:
        data_with_duplicates = response.json()
        data = deduplicate_json(data_with_duplicates, file_category)

        dataframes = []
        for file in data:
            xls_url = file['file_path']

            # Fetch the content of the .xls file
            inner_response = requests.get(xls_url)
            # Check if the request was successful
            if response.status_code == 200:
                # Use BytesIO to create a file-like object from the content
                file_content = BytesIO(inner_response.content)
                df = pd.read_excel(file_content, sheet_name=0, engine='openpyxl')

                # Contained date sometimes false. The correct date can be found via the first part of the filename.
                timestamp_str = xls_url.split('/')[-1].split('_')[0]
                timestamp = pd.Timestamp(timestamp_str)
                context.log.info(f"Got timestamp {timestamp} from URL: {xls_url}")

                # Asserting the format is as expected
                assert df.iloc[:, 36].isnull().all(), \
                    f"Unrecognized format: not all elements of column 36 are null as expected"
                assert df.columns[0] == 'Forecast Daily Analysis per Entity', \
                    f"Unrecognized format: first column is {df.columns[0]}not expected"

                df.drop(df.columns[36], axis=1, inplace=True)  # 36th column is empty, remove it
                df.drop(df.index[0:2], inplace=True)  # Discard first two rows (junk)
                df.drop(df.columns[0:2], axis=1, inplace=True)  # Discard first two columns (junk)

                # Reshape and properly label and index dataframe
                df_transposed = df.transpose()
                df_transposed.columns = ['res_forecast']
                date_range = pd.date_range(start=timestamp, periods=len(df_transposed), freq='30T')
                df_transposed.index = date_range
                df_transposed.index.name = 'timestamp'

                dataframes.append(df_transposed)

            else:
                raise Exception(f"Failed to fetch the xlsx file {xls_url}, status code: {inner_response.status_code}")
        final_df = pd.concat(dataframes, ignore_index=False)
        return Output(value=final_df)
    else:
        raise Exception(f"Failed to fetch the file from {top_level_url}, status code: {response.status_code}")
@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2020-10-01"),
    io_manager_key="postgres_io_manager",
    group_name="ipto",
    op_tags={"dagster/concurrency_key": "ipto", "concurrency_tag": "ipto"},
    description="Day-ahead Renewables Forecast (Second run)"
)
def ipto_2day_ahead_res_forecast(context: AssetExecutionContext):
    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")
    file_category = "ISP2DayAheadRESForecast"

    params = parameters(start, end, file_category)

    context.log.info(f"Fetching file locations from {top_level_url}")
    response = requests.get(top_level_url, params=params)

    if response.status_code == 200:
        data_with_duplicates = response.json()
        data = deduplicate_json(data_with_duplicates, file_category)

        dataframes = []
        for file in data:
            xls_url = file['file_path']

            # Fetch the content of the .xls file
            inner_response = requests.get(xls_url)
            # Check if the request was successful
            if response.status_code == 200:
                # Use BytesIO to create a file-like object from the content
                file_content = BytesIO(inner_response.content)
                df = pd.read_excel(file_content, sheet_name=0, engine='openpyxl')

                # Contained date sometimes false. The correct date can be found via the first part of the filename.
                timestamp_str = xls_url.split('/')[-1].split('_')[0]
                timestamp = pd.Timestamp(timestamp_str)
                context.log.info(f"Got timestamp {timestamp} from URL: {xls_url}")

                # Asserting the format is as expected
                assert df.iloc[:, 36].isnull().all(), \
                    f"Unrecognized format: not all elements of column 36 are null as expected"
                assert df.columns[0] == 'Forecast Daily Analysis per Entity', \
                    f"Unrecognized format: first column is {df.columns[0]}not expected"

                df.drop(df.columns[36], axis=1, inplace=True)  # 36th column is empty, remove it
                df.drop(df.index[0:2], inplace=True)  # Discard first two rows (junk)
                df.drop(df.columns[0:2], axis=1, inplace=True)  # Discard first two columns (junk)

                # Reshape and properly label and index dataframe
                df_transposed = df.transpose()
                df_transposed.columns = ['res_forecast']
                date_range = pd.date_range(start=timestamp, periods=len(df_transposed), freq='30T')
                df_transposed.index = date_range
                df_transposed.index.name = 'timestamp'

                dataframes.append(df_transposed)

            else:
                raise Exception(f"Failed to fetch the xlsx file {xls_url}, status code: {inner_response.status_code}")
        final_df = pd.concat(dataframes, ignore_index=False)
        return Output(value=final_df)
    else:
        raise Exception(f"Failed to fetch the file from {top_level_url}, status code: {response.status_code}")

@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2020-10-01"),
    io_manager_key="postgres_io_manager",
    group_name="ipto",
    op_tags={"dagster/concurrency_key": "ipto", "concurrency_tag": "ipto"},
    description="Renewables Forecast (Third run/Intra-day)"
)
def ipto_3intraday_res_forecast(context: AssetExecutionContext):
    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")
    file_category = "ISP3IntraDayRESForecast"

    params = parameters(start, end, file_category)

    context.log.info(f"Fetching file locations from {top_level_url}")
    response = requests.get(top_level_url, params=params)

    if response.status_code == 200:
        data_with_duplicates = response.json()
        data = deduplicate_json(data_with_duplicates, file_category)

        dataframes = []
        for file in data:
            xls_url = file['file_path']

            # Fetch the content of the .xls file
            inner_response = requests.get(xls_url)
            # Check if the request was successful
            if response.status_code == 200:
                # Use BytesIO to create a file-like object from the content
                file_content = BytesIO(inner_response.content)
                df = pd.read_excel(file_content, sheet_name=0, engine='openpyxl')

                # Contained date sometimes false. The correct date can be found via the first part of the filename.
                timestamp_str = xls_url.split('/')[-1].split('_')[0]
                timestamp = pd.Timestamp(timestamp_str)
                context.log.info(f"Got timestamp {timestamp} from URL: {xls_url}")

                # Asserting the format is as expected
                assert df.iloc[:, 36].isnull().all(), \
                    f"Unrecognized format: not all elements of column 36 are null as expected"
                assert df.columns[0] == 'Forecast Daily Analysis per Entity', \
                    f"Unrecognized format: first column is {df.columns[0]}not expected"

                df.drop(df.columns[36], axis=1, inplace=True)  # 36th column is empty, remove it
                df.drop(df.index[0:2], inplace=True)  # Discard first two rows (junk)
                df.drop(df.columns[0:2], axis=1, inplace=True)  # Discard first two columns (junk)

                # Reshape and properly label and index dataframe
                df_transposed = df.transpose()
                df_transposed.columns = ['res_forecast']
                date_range = pd.date_range(start=timestamp, periods=len(df_transposed), freq='30T')
                df_transposed.index = date_range
                df_transposed.index.name = 'timestamp'

                dataframes.append(df_transposed)

            else:
                raise Exception(f"Failed to fetch the xlsx file {xls_url}, status code: {inner_response.status_code}")
        final_df = pd.concat(dataframes, ignore_index=False)
        return Output(value=final_df)
    else:
        raise Exception(f"Failed to fetch the file from {top_level_url}, status code: {response.status_code}")



@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2020-10-01"),
    io_manager_key="postgres_io_manager",
    group_name="ipto",
    op_tags={"dagster/concurrency_key": "ipto", "concurrency_tag": "ipto"},
    description="Daily week-ahead load forecast. timestamp denotes forecast run time, timestamp_target forecast target time"
)
def ipto_week_ahead_load_forecast(context: AssetExecutionContext):
    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")
    file_category = "ISPWeekAheadLoadForecast"

    params = parameters(start, end, file_category)
    response = requests.get(top_level_url, params=params)

    if response.status_code == 200:
        data_with_duplicates = response.json()

        data = deduplicate_json(data_with_duplicates, file_category)

        dataframes = []
        for file in data:
            xls_url = file['file_path']

            # Fetch the content of the .xls file
            inner_response = requests.get(xls_url)
            # Check if the request was successful
            if response.status_code == 200:
                # Use BytesIO to create a file-like object from the content
                file_content = BytesIO(inner_response.content)
                df = pd.read_excel(file_content, sheet_name=0, engine='openpyxl')

                timestamp_str = xls_url.split('/')[-1].split('_')[0]
                timestamp_start = pd.Timestamp(timestamp_str)
                context.log.info(f"Got timestamp {timestamp_start} from URL: {xls_url}")

                # Asserting the format is as expected
                assert df.iloc[:, 34].isnull().all(), \
                    f"Unrecognized format: not all elements of column 34 are null as expected"
                assert df.columns[0] == 'Forecast Daily Analysis per Entity', \
                    f"Unrecognized format: first column is {df.columns[0]}not expected"

                df.drop(df.columns[34], axis=1, inplace=True)  # 34th column is empty, remove it
                df.drop(df.index[0:1], inplace=True)  # Discard first row (junk)
                df.drop(df.columns[0:3], axis=1, inplace=True)  # Discard first three columns (junk)

                starting_ts_index = timestamp_start
                target_ts_index = pd.date_range(start=timestamp_start, periods=48 * 7, freq='30T')

                df_flattened = df.stack().reset_index()
                df_flattened.columns = ['timestamp', 'target_timestamp', 'load_forecast']
                df_flattened['timestamp'] = starting_ts_index
                df_flattened['target_timestamp'] = target_ts_index

                df_flattened.set_index(['timestamp', 'target_timestamp'], inplace=True)

                dataframes.append(df_flattened)

            else:
                raise Exception(f"Failed to fetch the xlsx file {xls_url}, status code: {inner_response.status_code}")
        final_df = pd.concat(dataframes, ignore_index=False)
        return Output(value=final_df)

    else:
        raise Exception(f"Failed to fetch the file from {top_level_url}, status code: {response.status_code}")


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2020-05-01"),
    io_manager_key="postgres_io_manager",
    group_name="ipto",
    op_tags={"dagster/concurrency_key": "ipto", "concurrency_tag": "ipto"},
    description="Provided by IPTO once a day (SCADA). One column per adjacent country with net flow of energy per hour."
)
def ipto_net_interconnection_flows(context: AssetExecutionContext):
    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")

    file_category = "RealTimeSCADAImportsExports"
    params = parameters(start, end, file_category)

    response = requests.get(top_level_url, params=params)

    if response.status_code == 200:
        data_with_duplicates = response.json()

        data = deduplicate_json(data_with_duplicates, file_category, 'xls')

        dataframes = []
        for file in data:
            xls_url = file['file_path']

            # Fetch the content of the .xls file
            inner_response = requests.get(xls_url)
            # Check if the request was successful
            if response.status_code == 200:
                # Use BytesIO to create a file-like object from the content
                file_content = BytesIO(inner_response.content)
                df = pd.read_excel(file_content, sheet_name=0)

                # flaggg = False
                timestamp_str = xls_url.split('/')[-1].split('_')[0]
                timestamp_start = pd.Timestamp(timestamp_str)
                context.log.info(f"Got timestamp {timestamp_start} from URL: {xls_url}")

                # Asserting the format is as expected

                assert df.columns[1] == 'ALBANIA REALTIME NET (MWh)', \
                    f"Unrecognized format: first column is {df.columns[1]}, not expected"
                assert df.iloc[2, 1] == 'FYROM REALTIME NET (MWh)', \
                    f"Unrecognized format: dataframe[2,1] is {df.iloc[2, 1]}, not expected"
                assert df.iloc[5, 1] == 'BULGARIA REALTIME NET (MWh)', \
                    f"Unrecognized format: dataframe[5,1] is {df.iloc[5, 1]}, not expected"
                assert df.iloc[8, 1] == 'TURKEY REALTIME NET (MWh)', \
                    f"Unrecognized format: dataframe[8,1] is {df.iloc[8, 1]}, not expected"
                assert df.iloc[11, 1] == 'ITALY REALTIME NET (MWh)', \
                    f"Unrecognized format: dataframe[11,1] is {df.iloc[11, 1]}, not expected"

                # Dropping excess rows/columns
                df.drop([0, 2, 3, 5, 6, 8, 9, 11, 12], inplace=True)
                df.drop(df.columns[0], axis=1, inplace=True)
                df.drop(df.columns[-1], axis=1, inplace=True)

                # Transposing, adding column names and timestamp index
                df_transposed = df.transpose()
                timestamp_index = pd.date_range(start=timestamp_start, periods=24, freq='60T')
                df_transposed.index = timestamp_index
                df_transposed.index.name = 'timestamp'
                df_transposed.columns = ['albania', 'fyrom', 'bulgaria', 'turkey', 'italy']

                dataframes.append(df_transposed)

            else:
                raise Exception(f"Failed to fetch the xls file {xls_url}, status code: {inner_response.status_code}")
        if len(dataframes) == 0:
            context.log.warning(f"No data found for this partition! The response from the IPTO API was:\n \
            {json.dumps(data_with_duplicates, indent=4, ensure_ascii=False)}")
            return Output(value=None)
        final_df = pd.concat(dataframes, ignore_index=False)
        return Output(value=final_df)
    else:
        raise Exception(f"Failed to fetch the file from {top_level_url}, status code: {response.status_code}")


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2020-10-01"),
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
    partitions_def=MonthlyPartitionsDefinition(start_date="2020-10-01"),
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
    partitions_def=MonthlyPartitionsDefinition(start_date="2020-11-01"),
    io_manager_key="postgres_io_manager",
    group_name="ipto",
    op_tags={"dagster/concurrency_key": "ipto", "concurrency_tag": "ipto"},
    description="Daily energy balance report (MWh for different energy sources)"
)
def ipto_daily_energy_balance(context: AssetExecutionContext):

    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")
    file_category = "DailyEnergyBalanceAnalysis"

    params = parameters(start, end, file_category)

    context.log.info(f"Fetching file locations from {top_level_url}")
    response = requests.get(top_level_url, params=params)

    if response.status_code == 200:
        data_with_duplicates = response.json()

        # Sometimes, there are multiple entries for a single day. E.g.
        # https://www.admie.gr/sites/default/files/attached-files/type-file/2020/12/20201201_DailyEnergyBalanceAnalysis_02.xlsx
        # https://www.admie.gr/sites/default/files/attached-files/type-file/2020/12/20201201_DailyEnergyBalanceAnalysis_01.xlsx
        # Notice the different number preceding the file format.
        # We deduplicate these entries and keep the latest version of the file
        data = deduplicate_json(data_with_duplicates, file_category)

        dataframes = []
        for file in data:
            xls_url: str = file['file_path']

            # Fetch the content of the .xls file
            context.log.info(f"Fetching single file {xls_url}")
            inner_response = requests.get(xls_url)
            # Check if the request was successful
            if response.status_code == 200:
                # Use BytesIO to create a file-like object from the content
                file_content = BytesIO(inner_response.content)
                df = pd.read_excel(file_content, sheet_name=0, engine='openpyxl')

                # Asserting that the file format is as expected
                assert df.iloc[19, 2] == 'ΛΙΓΝΙΤΗΣ', 'Wrong format! Cell [19,2] should be lignite'
                assert df.iloc[20, 2] == 'ΑΕΡΙΟ' or df.iloc[20, 2] == 'ΦΥΣΙΚΟ ΑΕΡΙΟ', 'Wrong format! Cell [20,2] should be natural_gas'
                assert df.iloc[21, 2] == 'ΥΔΡΟΗΛΕΚΤΡΙΚΑ', 'Wrong format! Cell [21,2] should be hydroelectric'
                assert df.iloc[22, 2] == 'ΑΙΟΛΙΚΑ' or df.iloc[22, 2] == 'ΑΠΕ', 'Wrong format! Cell [22,2] should be renewables'
                assert df.iloc[23, 2] == 'ΚΑΘΑΡΕΣ ΕΙΣΑΓΩΓΕΣ (ΕΙΣΑΓΩΓΕΣ-ΕΞΑΓΩΓΕΣ)', 'Wrong format! Cell [23,2] should be net_imports'

                # Contained date sometimes false. The correct date can be found via the first part of the filename.
                timestamp_str = xls_url.split('/')[-1].split('_')[0]
                timestamp = pd.Timestamp(timestamp_str)
                context.log.info(f"Got timestamp {timestamp} from URL: {xls_url}")

                new_df_data = {
                    'lignite': df.iloc[19, 4],
                    'natural_gas': df.iloc[20, 4],
                    'hydroelectric': df.iloc[21, 4],
                    'renewables': df.iloc[22, 4],
                    'net_imports': df.iloc[23, 4]
                }

                new_df = pd.DataFrame(new_df_data, index=pd.DatetimeIndex([timestamp], name='timestamp'))
                dataframes.append(new_df)

            else:
                raise Exception(f"Failed to fetch the xlsx file {xls_url}, status code: {inner_response.status_code}")
        final_df = pd.concat(dataframes, ignore_index=False)
        return Output(value=final_df)
    else:
        raise Exception(f"Failed to fetch the file from {top_level_url}, status code: {response.status_code}")

