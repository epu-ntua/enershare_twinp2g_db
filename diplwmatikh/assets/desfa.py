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
