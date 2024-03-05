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
    partitions_def=MonthlyPartitionsDefinition(start_date="2020-11-01"),
    io_manager_key="postgres_io_manager",
    group_name="ipto",
    op_tags={"dagster/concurrency_key": "ipto", "concurrency_tag": "ipto"}
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
                raise Exception(f"Failed to fetch the xls file {xls_url}, status code: {inner_response.status_code}")
        final_df = pd.concat(dataframes, ignore_index=False)
        return Output(value=final_df)
    else:
        raise Exception(f"Failed to fetch the file from {top_level_url}, status code: {response.status_code}")

