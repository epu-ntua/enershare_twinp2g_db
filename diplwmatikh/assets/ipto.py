import json
from functools import reduce
from io import BytesIO
from typing import List, Tuple

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
from ..utils.common import sanitize_series, sanitize_df, timewindow_to_ts, find_string_in_df_by_index

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
    partitions_def=MonthlyPartitionsDefinition(start_date="2020-05-01"),
    io_manager_key="postgres_io_manager",
    group_name="ipto",
    op_tags={"dagster/concurrency_key": "ipto", "concurrency_tag": "ipto"},
    description="Energy generated by renewables, provided by IPTO (SCADA) once a day."
)
def ipto_res_injections(context: AssetExecutionContext):
    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")

    file_category = "RealTimeSCADARES"
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

                # Get timestamp from URL
                timestamp_str = xls_url.split('/')[-1].split('_')[0]
                timestamp_start = pd.Timestamp(timestamp_str)
                context.log.info(f"Got timestamp {timestamp_start} from URL: {xls_url}")

                # Asserting the format is as expected
                assert df.columns[1] == 'REAL TIME RES (MWh)', \
                    f"Unrecognized format: first column is {df.columns[1]}, not expected"
                assert df.iloc[0, 0] == 'Date', f"Unrecognized format: dataframe[2,1] is {df.iloc[2, 1]}, not expected"
                assert df.shape == (2, 26), f"Shape of dataframe is {df.shape}, not (2, 26) as expected"

                # Drop excess rows/columns
                df.drop([0], inplace=True)
                df.drop(df.columns[0], axis=1, inplace=True)
                df.drop(df.columns[-1], axis=1, inplace=True)

                # Finalize format, set index & labels
                df_transposed = df.transpose()
                timestamp_index = pd.date_range(start=timestamp_start, periods=24, freq='60T')
                df_transposed.index = timestamp_index
                df_transposed.index.name = "timestamp"
                df_transposed.columns = ['res_injections']

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
    partitions_def=MonthlyPartitionsDefinition(start_date="2011-01-01"),
    io_manager_key="postgres_io_manager",
    group_name="ipto",
    op_tags={"dagster/concurrency_key": "ipto", "concurrency_tag": "ipto"},
    description="Energy generated by each production unit (lignite, hydro, RES, gas) per hour, provided by IPTO (SCADA) once a day."
)
def ipto_unit_production(context: AssetExecutionContext):
    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")

    file_category = "SystemRealizationSCADA"
    params = parameters(start, end, file_category)

    response = requests.get(top_level_url, params=params)

    if response.status_code == 200:
        data_with_duplicates = response.json()
        # print(json.dumps(data_with_duplicates, indent=4, ensure_ascii=False))

        data = deduplicate_json(data_with_duplicates, file_category, 'xls')

        # print(json.dumps(data, indent=4, ensure_ascii=False))
        dataframes = []
        for file in data:
            xls_url = file['file_path']

            # Fetch the content of the .xls file
            inner_response = requests.get(xls_url)
            # Check if the request was successful
            if response.status_code == 200:
                # Use BytesIO to create a file-like object from the content
                file_content = BytesIO(inner_response.content)

                df = pd.read_excel(file_content, sheet_name=0, engine='xlrd')
                if df.iloc[0, 0] == "ARU-dbdrv":  # If the first sheet is an excel template, read the next sheet
                    df = pd.read_excel(file_content, sheet_name=1, engine='xlrd')

                timestamp_str = xls_url.split('/')[-1].split('_')[0]
                timestamp_start = pd.Timestamp(timestamp_str)
                context.log.info(f"Got timestamp {timestamp_start} from URL: {xls_url}")

                # Asserting the format is as expected.
                assert df.columns[1] == '(ΣΤΟΙΧΕΙΑ SCADA ΜΗ-ΠΙΣΤΟΠΟΙΗΜΕΝΑ)', \
                    f"Unrecognized format: first column is {df.columns[1]}, not expected"
                assert df.iloc[2, 2] == 'ΦΟΡΤΙΑ ΣΥΣΤΗΜΑΤΟΣ (MWh)', \
                    f"Unrecognized format: dataframe[2,2] is {df.iloc[2, 2]}, not expected"
                assert df.iloc[4, 1] == 'ΣΥΝΟΛΙΚΟ ΦΟΡΤΙΟ', \
                    f"Unrecognized format: dataframe[4,1] is {df.iloc[4, 1]}, not expected"
                assert len(df.columns) == 27, f"Unrecognized format: expected 27 columns, got {len(df.columns)}"

                #
                #
                #   Lignite production units subblock
                #
                #

                # Start based on finding lignite starting label
                lignite_label: List[Tuple[int, int]] = find_string_in_df_by_index(df, "ΛΙΓΝΙΤΙΚΕΣ ΜΟΝΑΔΕΣ")
                assert len(lignite_label) > 0, f"Unrecognized format: Did not find \"ΛΙΓΝΙΤΙΚΕΣ ΜΟΝΑΔΕΣ\" in dataframe"
                assert len(find_string_in_df_by_index(df, "TOTAL LIGNITE")) > 0 or \
                       len(find_string_in_df_by_index(df, "ΣΥΝΟΛΟ ΛΙΓΝΙΤΙΚΩΝ")) > 0, (f"Unrecognized format: " +
                                                    f"could not find either \"TOTAL LIGNITE\" or \"ΣΥΝΟΛΟ ΛΙΓΝΙΤΙΚΩΝ\"")

                # Lignite end label
                lignite_end_row = find_string_in_df_by_index(df, "TOTAL LIGNITE")[0][0] if \
                    len(find_string_in_df_by_index(df, "TOTAL LIGNITE")) > 0 else \
                    find_string_in_df_by_index(df, "ΣΥΝΟΛΟ ΛΙΓΝΙΤΙΚΩΝ")[0][0]

                # Select lignite subblock
                lignite_subblock_cols = list(range(lignite_label[0][1], 26))
                lignite_subblock_rows = list(range(lignite_label[0][0] + 1, lignite_end_row))
                lignite_subblock = df.iloc[lignite_subblock_rows, lignite_subblock_cols].copy()

                # Extract point names, flatten data, add LIGNITE prefix to point names
                lignite_subblock_points = lignite_subblock.iloc[:, 0].to_list()
                lignite_subblock_points = list(map(lambda x: f"LIGNITE {x}", lignite_subblock_points))
                lignite_subblock_data = lignite_subblock.iloc[:, 1:]
                lignite_subblock_data_flattened = lignite_subblock_data.stack(dropna=False).reset_index()

                # Generate timestamp index range
                ts_index = pd.date_range(start=timestamp_start, periods=24, freq='60T')

                # Based on point names and timestamp range, assign columns and make them into indices
                lignite_subblock_data_flattened.columns = ['point_id', 'timestamp', 'value']
                lignite_subblock_data_flattened['point_id'] = [x for x in lignite_subblock_points for _ in range(24)]
                lignite_subblock_data_flattened['timestamp'] = ts_index.to_list() * len(lignite_subblock_points)
                lignite_subblock_data_flattened.set_index(['point_id', 'timestamp'], inplace=True)

                #
                #
                #   Natural gas production units subblock
                #
                #

                # Start based on finding gas starting label
                gas_label: List[Tuple[int, int]] = find_string_in_df_by_index(df, "ΜΟΝΑΔΕΣ Φ. ΑΕΡΙΟΥ")
                assert len(gas_label) > 0, f"Unrecognized format: Did not find \"ΜΟΝΑΔΕΣ Φ. ΑΕΡΙΟΥ\" in dataframe"
                assert len(find_string_in_df_by_index(df, "TOTAL GAS")) > 0 or \
                       len(find_string_in_df_by_index(df, "ΣΥΝΟΛΟ Φ. ΑΕΡΙΟΥ")) > 0, (f"Unrecognized format: " +
                                                                                     f"could not find either \"TOTAL GAS\" or \"ΣΥΝΟΛΟ Φ. ΑΕΡΙΟΥ\"")

                # Gas end label
                gas_end_row = find_string_in_df_by_index(df, "TOTAL GAS")[0][0] if \
                    len(find_string_in_df_by_index(df, "TOTAL GAS")) > 0 else \
                    find_string_in_df_by_index(df, "ΣΥΝΟΛΟ Φ. ΑΕΡΙΟΥ")[0][0]

                # Select gas subblock
                gas_subblock_cols = list(range(gas_label[0][1], 26))
                gas_subblock_rows = list(range(gas_label[0][0] + 1, gas_end_row))
                gas_subblock = df.iloc[gas_subblock_rows, gas_subblock_cols].copy()

                # Extract point names, flatten data, add GAS prefix to point names
                gas_subblock_points = gas_subblock.iloc[:, 0].to_list()
                gas_subblock_points = list(map(lambda x: f"GAS {x}", gas_subblock_points))
                gas_subblock_data = gas_subblock.iloc[:, 1:]
                gas_subblock_data_flattened = gas_subblock_data.stack(dropna=False).reset_index()

                # Generate timestamp index range
                ts_index = pd.date_range(start=timestamp_start, periods=24, freq='60T')

                # Based on point names and timestamp range, assign columns and make them into indices
                gas_subblock_data_flattened.columns = ['point_id', 'timestamp', 'value']
                gas_subblock_data_flattened['point_id'] = [x for x in gas_subblock_points for _ in range(24)]
                gas_subblock_data_flattened['timestamp'] = ts_index.to_list() * len(gas_subblock_points)
                gas_subblock_data_flattened.set_index(['point_id', 'timestamp'], inplace=True)

                #
                #
                #   Hydroelectric production units subblock
                #
                #

                # Start based on finding hydro starting label
                hydro_label: List[Tuple[int, int]] = find_string_in_df_by_index(df, "ΥΔΡΟΗΛΕΚΤΡΙΚΕΣ ΜΟΝΑΔΕΣ")
                assert len(
                    hydro_label) > 0, f"Unrecognized format: Did not find \"ΥΔΡΟΗΛΕΚΤΡΙΚΕΣ ΜΟΝΑΔΕΣ\" in dataframe"
                assert len(find_string_in_df_by_index(df, "TOTAL HYDRO")) > 0 or \
                       len(find_string_in_df_by_index(df, "ΣΥΝΟΛΟ ΥΔΡΟΗΛΕΚΤΡΙΚΩΝ")) > 0, (f"Unrecognized format: " +
                                                                                          f"could not find either \"TOTAL HYDRO\" or \"ΣΥΝΟΛΟ ΥΔΡΟΗΛΕΚΤΡΙΚΩΝ\"")

                # Hydro end label
                hydro_end_row = find_string_in_df_by_index(df, "TOTAL HYDRO")[0][0] if \
                    len(find_string_in_df_by_index(df, "TOTAL HYDRO")) > 0 else \
                    find_string_in_df_by_index(df, "ΣΥΝΟΛΟ ΥΔΡΟΗΛΕΚΤΡΙΚΩΝ")[0][0]

                # Select hydro subblock
                hydro_subblock_cols = list(range(hydro_label[0][1], 26))
                hydro_subblock_rows = list(range(hydro_label[0][0] + 1, hydro_end_row))
                hydro_subblock = df.iloc[hydro_subblock_rows, hydro_subblock_cols].copy()

                # Extract point names, flatten data, add HYDRO prefix to point names
                hydro_subblock_points = hydro_subblock.iloc[:, 0].to_list()
                hydro_subblock_points = list(map(lambda x: f"HYDRO {x}", hydro_subblock_points))
                hydro_subblock_data = hydro_subblock.iloc[:, 1:]
                hydro_subblock_data_flattened = hydro_subblock_data.stack(dropna=False).reset_index()

                # Generate timestamp index range
                ts_index = pd.date_range(start=timestamp_start, periods=24, freq='60T')

                # Based on point names and timestamp range, assign columns and make them into indices
                hydro_subblock_data_flattened.columns = ['point_id', 'timestamp', 'value']
                hydro_subblock_data_flattened['point_id'] = [x for x in hydro_subblock_points for _ in range(24)]
                hydro_subblock_data_flattened['timestamp'] = ts_index.to_list() * len(hydro_subblock_points)
                hydro_subblock_data_flattened.set_index(['point_id', 'timestamp'], inplace=True)

                #
                #
                #   Renewables production units subblock
                #
                #

                # Start based on finding res starting label
                assert len(find_string_in_df_by_index(df, "ΑΝΑΝΕΩΣΙΜΑ")) > 0 or \
                       len(find_string_in_df_by_index(df, "ΑΙΟΛΙΚΑ ΠΑΡΚΑ")) > 0, (f"Unrecognized format: " +
                                                                                  f"could not find either \"ΑΝΑΝΕΩΣΙΜΑ\" or \"ΑΙΟΛΙΚΑ ΠΑΡΚΑ\"")

                res_label: List[Tuple[int, int]] = find_string_in_df_by_index(df, "ΑΝΑΝΕΩΣΙΜΑ") if \
                    len(find_string_in_df_by_index(df, "ΑΝΑΝΕΩΣΙΜΑ")) > 0 else \
                    find_string_in_df_by_index(df, "ΑΙΟΛΙΚΑ ΠΑΡΚΑ")

                assert len(find_string_in_df_by_index(df, "TOTAL RES")) > 0 or \
                       len(find_string_in_df_by_index(df, "ΣΥΝΟΛΟ ΑΙΟΛΙΚΩΝ")) > 0, (f"Unrecognized format: " +
                                                                                    f"could not find either \"TOTAL RES\" or \"ΣΥΝΟΛΟ ΑΙΟΛΙΚΩΝ\"")

                # RES end label
                res_end_row = find_string_in_df_by_index(df, "TOTAL RES")[0][0] if \
                    len(find_string_in_df_by_index(df, "TOTAL RES")) > 0 else \
                    find_string_in_df_by_index(df, "ΣΥΝΟΛΟ ΑΙΟΛΙΚΩΝ")[0][0]

                # Select res subblock
                res_subblock_cols = list(range(res_label[0][1], 26))
                res_subblock_rows = list(range(res_label[0][0] + 1, res_end_row))
                res_subblock = df.iloc[res_subblock_rows, res_subblock_cols].copy()

                # Extract point names, flatten data, add RES prefix to point names
                res_subblock_points = res_subblock.iloc[:, 0].to_list()
                res_subblock_points = list(map(lambda x: f"RES {x}", res_subblock_points))
                res_subblock_data = res_subblock.iloc[:, 1:]
                res_subblock_data_flattened = res_subblock_data.stack(dropna=False).reset_index()

                # Generate timestamp index range
                ts_index = pd.date_range(start=timestamp_start, periods=24, freq='60T')

                # Based on point names and timestamp range, assign columns and make them into indices
                res_subblock_data_flattened.columns = ['point_id', 'timestamp', 'value']
                res_subblock_data_flattened['point_id'] = [x for x in res_subblock_points for _ in range(24)]
                res_subblock_data_flattened['timestamp'] = ts_index.to_list() * len(res_subblock_points)
                res_subblock_data_flattened.set_index(['point_id', 'timestamp'], inplace=True)

                # Join ligninte, gas, hydro, res dataframes
                complete_df = pd.concat([lignite_subblock_data_flattened, gas_subblock_data_flattened,
                                         hydro_subblock_data_flattened, res_subblock_data_flattened],
                                        ignore_index=False)

                dataframes.append(complete_df)
            else:
                raise Exception(f"Failed to fetch the xls file {xls_url}, status code: {inner_response.status_code}")
        final_df = pd.concat(dataframes, ignore_index=False)
        return Output(value=final_df)

    else:
        raise Exception(f"Failed to fetch the file from {top_level_url}, status code: {response.status_code}")


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
