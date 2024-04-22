import pathlib
from functools import reduce
from io import BytesIO
from zoneinfo import ZoneInfo

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
from ..utils.common import sanitize_series, sanitize_df, timewindow_to_ts, replace_dash_with_nan

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

entry_points = ["AGIA TRIADA", "SIDIROKASTRO", "KIPI", "NEA MESIMVRIA"]

# Different .xlsx files have different names for the same points... rename according to latest .xlsx
rename_dict = {'AGIOI THEODOROI': 'AG. THEODOROI', 'MEGALOPOLIS (PPC)': 'MEGALOPOLI (PPC)', 'THRIASIO': 'THRIASSIO',
               'ELPE HAR': 'ELPE-HAR', 'ELPE - HAR': 'ELPE-HAR', 'MEGALOPOLIS\n(PPC)': 'MEGALOPOLI (PPC)'}

# DESFA assets

@asset(
    partitions_def=StaticPartitionsDefinition(["desfa_flows_daily_monopartition"]),
    io_manager_key="postgres_io_manager",
    group_name="desfa",
    op_tags={"dagster/concurrency_key": "desfa", "concurrency_tag": "desfa"},
    description="Deliveries / Off-takes (imports for entry points/off-takes per exit points) per day since 2008"
)
def desfa_flows_daily(context: AssetExecutionContext):
    url = 'https://www.desfa.gr/userfiles/pdflist/DDRA/Flows.xlsx'

    # Fetch the content of the .xls file
    response = requests.get(url)
    dataframes = {}
    # Check if the request was successful
    if response.status_code == 200:
        # Use BytesIO to create a file-like object from the content
        file_content = BytesIO(response.content)
        df = pd.read_excel(file_content, sheet_name=0)

        df = df.drop(df.columns[5], axis=1)  # Remove 5th column starting from 0 (empty)
        df = df.drop(df.index[0:3])  # Remove first 3 rows (not needed for data)
        df = df[~df.iloc[:, 0].astype(str).str.contains('ΣΥΝΟΛΟ')]  # Remove lines that contain aggregates

        # Now to transform the dataframe to make the first row (barring the first element) into an index

        new_headers = df.iloc[0, 1:].tolist()
        # Adding suffix "_exit" to the first 4 headers and "_entry" to the rest, since a point may be bidirectional
        new_headers = [x + "_entry" for x in new_headers[:4]] + [x + "_exit" for x in new_headers[4:]]

        # We will remove the suffixes later
        def remove_suffix(point: str):
            if point.endswith("_entry"):
                return point[:-6]
            else:
                return point[:-5]

        df.columns = ['timestamp'] + new_headers  # Keep 'timestamp' for the first column and update the rest
        # Drop the first row
        df = df.drop(df.index[0])
        # Reset index if necessary
        df.reset_index(drop=True, inplace=True)

        df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y')

        # Melt the DataFrame to go from wide to long format
        df_long = pd.melt(df, id_vars=['timestamp'], var_name='point_id', value_name='value')

        # Set the new index using the 'timestamp' and 'point_id' columns to create a MultiIndex
        df_long.set_index(['timestamp', 'point_id'], inplace=True)

        # Now, to add "entry"/"exit" labels to our points
        def label_point_as_entry_or_exit(point: str):
            if point.endswith("_entry"):
                return "entry"
            else:
                return "exit"

        # Apply this function to the 'point_id' index level
        processed_header = df_long.index.get_level_values('point_id').map(label_point_as_entry_or_exit)
        # Add the processed values as a new level to the index
        df_long['point_type'] = processed_header
        # Then set this new column as an additional level of the index
        df_long.set_index('point_type', append=True, inplace=True)

        df_long.reset_index(drop=False, inplace=True)
        df_long['point_id'] = df_long['point_id'].apply(remove_suffix)

        df_long.set_index(['timestamp', 'point_id', 'point_type'], inplace=True)
        return Output(value=df_long)
    else:
        raise Exception(f"Failed to fetch the file, status code: {response.status_code}")


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2014-09-01", end_date="2018-08-01"),
    io_manager_key="postgres_io_manager",
    group_name="desfa",
    op_tags={"dagster/concurrency_key": "desfa", "concurrency_tag": "desfa"},
    description="Deliveries / Off-takes (imports for entry points/off-takes per exit points) per hour since 2008"
)
def desfa_flows_hourly_archive(context: AssetExecutionContext):
    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")

    start = start.replace(tzinfo=None)
    end = end.replace(tzinfo=None)

    month = start.strftime("%m")
    filepath = (pathlib.Path(__file__).parent / "archives_desfa" / "Hourly Flows" / f"Flows_{start.year}-Hourly" /
                f"Hourly-Flows-{month}_{start.year}.xls")
    if start > datetime.datetime(2016, 11, 30):  #.xlsx
        filepath = filepath.with_suffix(filepath.suffix + "x")
    context.log.info(f"Handling file {filepath}")
    dfs = pd.DataFrame()
    xls = pd.ExcelFile(filepath)
    for sheet_name in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet_name)
        day = datetime.datetime.strptime(sheet_name, "%d.%m.%Y")

        # dst has values "nodst", "todst" (march transitory day), "dst", "fromdst" (october transitory day)
        # needed in order to parse transitory days correctly and apply offset when dst
        day_check = day + datetime.timedelta(1)  # Day to be checked is the next day, due to the formatting of the xlss
        dst_value = day_check.replace(tzinfo=ZoneInfo("Europe/Athens")).dst()
        dst_calc = dst_value - (day_check.replace(tzinfo=ZoneInfo("Europe/Athens")) + datetime.timedelta(1)).dst()

        dst: str
        if dst_value == datetime.timedelta(hours=0):
            if dst_calc == datetime.timedelta(hours=0):
                dst = "nodst"
            else:
                dst = "todst"
        else:
            if dst_calc == datetime.timedelta(hours=0):
                dst = "dst"
            else:
                dst = "fromdst"

        df = df.drop(df.index[0:1])  # Remove first row (not needed for data)
        df = df.drop(df.columns[5], axis=1)  # Remove 5th column starting from 0 (empty)
        df = df.drop(df.columns[0], axis=1)  # Remove 0th column starting from 0 (empty)

        # Drop nan columns, if applicable
        columns_with_nan = df.columns[df.iloc[0].isna()]
        df = df.drop(columns=columns_with_nan)

        # Now we've dropped all the excess rows and columns.
        # Time to transform the dataframe to make the first row (barring the first element) into an index
        new_headers = df.iloc[0, 1:].tolist()

        # Adding suffix "_exit" to the first 3 headers and "_entry" to the rest, since a point may be bidirectional
        new_headers = [x + "_entry" for x in new_headers[:3]] + [x + "_exit" for x in new_headers[3:]]

        # We will remove the suffixes later
        def remove_suffix(point: str):
            if point.endswith("_entry"):
                return point[:-6]
            else:
                return point[:-5]

        df.columns = ['timestamp'] + new_headers  # Keep 'timestamp' for the first column and update the rest
        # Drop the first row
        df = df.drop(df.index[0])
        # Reset index if necessary
        df.reset_index(drop=True, inplace=True)

        lastrow = ((df['timestamp'] == 'Total') | (df['timestamp'] == 'Σύνολο')).idxmax()
        df = df.loc[:lastrow - 1]

        if dst == "todst":
            df = df.drop(df.index[18])  # empty row when transitioning to dst

        starting_hour = 9 if dst == "nodst" or dst == "todst" else 8
        iterations = 24
        if dst == "todst":
            iterations = 23
        elif dst == "fromdst":
            iterations = 25

        df['timestamp'] = [day + datetime.timedelta(hours=i) for i in range(starting_hour, iterations + starting_hour)]

        # Melt the DataFrame to go from wide to long format
        df_long = pd.melt(df, id_vars=['timestamp'], var_name='point_id', value_name='value')
        # Set the new index using the 'timestamp' and 'point_id' columns to create a MultiIndex
        df_long.set_index(['timestamp', 'point_id'], inplace=True)

        # Now, to add "entry"/"exit" labels to our points
        def label_point_as_entry_or_exit(point: str):
            if point.endswith("_entry"):
                return "entry"
            else:
                return "exit"

        # Apply this function to the 'point_id' index level
        processed_header = df_long.index.get_level_values('point_id').map(label_point_as_entry_or_exit)
        # Add the processed values as a new level to the index
        df_long['point_type'] = processed_header
        # Then set this new column as an additional level of the index
        df_long.set_index('point_type', append=True, inplace=True)

        # Applying reverse transformation to the 'point_id' index, i.e. removing suffix
        df_long.reset_index(drop=False, inplace=True)
        df_long['point_id'] = df_long['point_id'].apply(remove_suffix)

        df_long.set_index(['timestamp', 'point_id', 'point_type'], inplace=True)

        # Rename columns for compatibility with later .xlsx files
        df_renamed = df_long.rename(index=rename_dict, level='point_id')

        dfs = pd.concat([dfs, df_renamed])
    return Output(value=dfs)


@asset(
    partitions_def=StaticPartitionsDefinition(["desfa_ng_quality_yearly_monopartition"]),
    io_manager_key="postgres_io_manager",
    group_name="desfa",
    op_tags={"dagster/concurrency_key": "desfa", "concurrency_tag": "desfa"},
    description="Various NG quality indicators at entry points per year since 2008. The WDP and HCDP values are the mathematical average of the real values, while the values of the other quality data are the weighted average (to flow)."
)
def desfa_ng_quality_yearly(context: AssetExecutionContext):
    context.log.info(f"Handling single partition.")

    url = 'https://www.desfa.gr/userfiles/pdflist/DDRA/NG-QUALITY.xls'

    # Fetch the content of the .xls file
    response = requests.get(url)
    dataframes = {}
    # Check if the request was successful
    if response.status_code == 200:
        # Use BytesIO to create a file-like object from the content
        file_content = BytesIO(response.content)
        full_df = pd.read_excel(file_content, sheet_name=0)

        for search_string in entry_points:
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
    partitions_def=StaticPartitionsDefinition(["desfa_ng_pressure_monthly_monopartition"]),
    io_manager_key="postgres_io_manager",
    group_name="desfa",
    op_tags={"dagster/concurrency_key": "desfa", "concurrency_tag": "desfa"},
    description="Monthly Data of Natural Gas Pressure in Entry Points since 2008"
)
def desfa_ng_pressure_monthly(context: AssetExecutionContext):
    context.log.info(f"Handling single partition.")

    url = 'https://www.desfa.gr/userfiles/pdflist/DDRA/NG-Pressure.xls'

    # Fetch the content of the .xls file
    response = requests.get(url)
    # Check if the request was successful
    if response.status_code == 200:
        # Use BytesIO to create a file-like object from the content
        file_content = BytesIO(response.content)
        df = pd.read_excel(file_content, sheet_name=0)

        # Assuming 'df' is your DataFrame and the first column contains your mixed data
        # Function to convert only valid datetimes to the first of the month
        def convert_to_first_of_month(val):
            try:
                # Attempt to parse the datetime with the specific format
                date_val = pd.to_datetime(val, format='%m/%d/%Y')
                # If successful, return the date set to the first of the month
                return date_val.replace(day=1)
            except ValueError:
                # If parsing fails, return the original value
                return val

        # Apply the function to the first column
        df.iloc[:, 0] = df.iloc[:, 0].apply(convert_to_first_of_month)

        # Initialize a list to store tuples of (row index, matching substring)
        found_substrings = []

        # Iterate over each row in the DataFrame
        for index, row in df.iterrows():
            # Convert the value in the first column to string
            cell_value = str(row.iloc[0])
            # Check each entry_point
            for sub in entry_points:
                if sub in cell_value:
                    # If the entry point's substring is found, append (index, entry_point) to the list
                    found_substrings.append((index, sub))
                    # Break the loop if only the first match is needed
                    break

        subblocks = []  # tuples of (entry_point, start_row, end_row)
        for row_index, substring in found_substrings:
            # print(f"Row {row_index} contains the substring '{substring}'")
            start_row = row_index + 3

            # Apply the function to the 0th column to get a Series indicating whether each cell is a valid date
            def is_valid_datetime(cell):
                if isinstance(cell, pd.Timestamp):
                    return pd.notna(cell)
                else:
                    return False
            def find_non_datetime_index(df, n):
                for index, row in df.iloc[n:].iterrows():
                    # Check if the value in the first column is not an instance of datetime
                    if not is_valid_datetime(row.iloc[0]):
                        return index - 1  # Return the index where a non-datetime value is found
                return len(df) - 1  # Return None if all values are datetimes

            idx = find_non_datetime_index(df, start_row)
            # print(f"Last row meeting condition for {substring} is {idx}")
            subblocks.append((substring, start_row, idx))

        df.columns = ['timestamp', 'min_delivery_pressure', 'max_delivery_pressure']

        # Initialize an empty list to store each modified subblock dataframe
        subblock_dfs = []

        # Iterate through each subblock definition
        for label, start, end in subblocks:
            # Slice the dataframe to get the current subblock
            subblock_df = df.iloc[start:end + 1].copy()  # end+1 because iloc slicing is exclusive at the end
            # Add the label column with the current subblock's label
            subblock_df['point_id'] = label
            # Append the modified subblock dataframe to our list
            subblock_dfs.append(subblock_df)

        # Concatenate all subblock dataframes into a new dataframe
        merged_df = pd.concat(subblock_dfs, ignore_index=True)
        merged_df.set_index(['timestamp', 'point_id'], inplace=True)
        return Output(value=merged_df)
    else:
        raise Exception(f"Failed to fetch the file, status code: {response.status_code}")


@asset(
    partitions_def=StaticPartitionsDefinition(["desfa_ng_qcv_daily_monopartition"]),
    io_manager_key="postgres_io_manager",
    group_name="desfa",
    op_tags={"dagster/concurrency_key": "desfa", "concurrency_tag": "desfa"},
    description="Daily Data of Natural Gas GCV (Gross Calorific Value) in Entry/Exit Points since Nov. 2011"
)
def desfa_ng_gcv_daily(context: AssetExecutionContext):
    context.log.info(f"Handling single partition.")

    url = 'https://www.desfa.gr/userfiles/pdflist/DDRA/GCV.xlsx'

    # Fetch the content of the .xls file
    response = requests.get(url)
    # Check if the request was successful
    if response.status_code == 200:
        # Use BytesIO to create a file-like object from the content
        file_content = BytesIO(response.content)
        df = pd.read_excel(file_content, sheet_name=0)

        df = df.drop(df.columns[5], axis=1)  # Remove 5th column starting from 0 (empty)

        df = df.drop(df.index[0:3])  # Remove first 3 rows (not needed for data)
        df = df.iloc[:, :-1]  # This selects all rows and all columns except the last one
        df = df.iloc[:-4, :]  # This selects all columns and all rows except the last 4 rows

        # Now we've dropped all the excess rows and columns.
        # Time to transform the dataframe to make the first row (barring the first element) into an index

        new_headers = df.iloc[0, 1:].tolist()
        # Adding suffix "_exit" to the first 4 headers and "_entry" to the rest, since a point may be bidirectional
        new_headers = [x + "_entry" for x in new_headers[:4]] + [x + "_exit" for x in new_headers[4:]]

        # We will remove the suffixes later
        def remove_suffix(point: str):
            if point.endswith("_entry"):
                return point[:-6]
            else:
                return point[:-5]

        df.columns = ['timestamp'] + new_headers  # Keep 'timestamp' for the first column and update the rest
        # Drop the first row
        df = df.drop(df.index[0])
        # Reset index if necessary
        df.reset_index(drop=True, inplace=True)

        df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y')

        # Melt the DataFrame to go from wide to long format
        df_long = pd.melt(df, id_vars=['timestamp'], var_name='point_id', value_name='value')

        # Set the new index using the 'timestamp' and 'point_id' columns to create a MultiIndex
        df_long.set_index(['timestamp', 'point_id'], inplace=True)

        # Now, to add "entry"/"exit" labels to our points
        def label_point_as_entry_or_exit(point: str):
            if point.endswith("_entry"):
                return "entry"
            else:
                return "exit"

        # Apply this function to the 'point_id' index level
        processed_header = df_long.index.get_level_values('point_id').map(label_point_as_entry_or_exit)
        # Add the processed values as a new level to the index
        df_long['point_type'] = processed_header
        # Then set this new column as an additional level of the index
        df_long.set_index('point_type', append=True, inplace=True)

        # Applying reverse transformation to the 'point_id' index, i.e. removing suffix
        df_long.reset_index(drop=False, inplace=True)
        df_long['point_id'] = df_long['point_id'].apply(remove_suffix)

        df_long.set_index(['timestamp', 'point_id', 'point_type'], inplace=True)

        df_long = df_long.map(replace_dash_with_nan)
        return Output(value=df_long)
    else:
        raise Exception(f"Failed to fetch the file, status code: {response.status_code}")


@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2011-11-01"),
    io_manager_key="postgres_io_manager",
    group_name="desfa",
    op_tags={"dagster/concurrency_key": "desfa", "concurrency_tag": "desfa"},
    description="Daily nominations data of Entry/Exit Points since Nov. 2011"
)
def desfa_nominations_daily(context: AssetExecutionContext):
    start, end = timewindow_to_ts(context.partition_time_window)
    context.log.info(f"Handling partition from {start} to {end}")

    filepath_archived_1 = pathlib.Path(__file__).parent / 'archives_desfa' / 'Nominations' / '2011-2017_Nominations.xlsx'
    filepath_archived_2 = pathlib.Path(__file__).parent / 'archives_desfa' / 'Nominations' / 'Nominations_01_06_2017-31_12_2022.xlsx'
    url = 'https://www.desfa.gr/userfiles/pdflist/DERY/TS/Nominations-Allocations/Nominations%20(from%2001.01.2023).xlsx'

    start = start.replace(tzinfo=None)
    end = end.replace(tzinfo=None)

    if end < datetime.datetime(2017, 6, 1):
        df = pd.read_excel(filepath_archived_1, engine='openpyxl')

        df = df.drop(df.index[0:2])  # Remove first 2 rows (not needed for data)

        indices_to_drop = [0, 5, 49, 50, 51]  # Remove empty columns
        # Calculate indices to keep
        indices_to_keep = [i for i in range(df.shape[1]) if i not in indices_to_drop]
        df = df.iloc[:, indices_to_keep]

        # Now we've dropped all the excess rows and columns.
        # Time to transform the dataframe to make the first row (barring the first element) into an index
        new_headers = df.iloc[0, 1:].tolist()

        # Adding suffix "_exit" to the first 3 headers and "_entry" to the rest, since a point may be bidirectional
        new_headers = [x + "_entry" for x in new_headers[:3]] + [x + "_exit" for x in new_headers[3:]]

        # We will remove the suffixes later
        def remove_suffix(point: str):
            if point.endswith("_entry"):
                return point[:-6]
            else:
                return point[:-5]

        df.columns = ['timestamp'] + new_headers  # Keep 'timestamp' for the first column and update the rest
        # Drop the first row
        df = df.drop(df.index[0])
        # Reset index if necessary
        df.reset_index(drop=True, inplace=True)

        df['timestamp'] = pd.to_datetime(df['timestamp']).map(lambda x: x.tz_localize(None))

        # Melt the DataFrame to go from wide to long format
        df_long = pd.melt(df, id_vars=['timestamp'], var_name='point_id', value_name='value')
        # Set the new index using the 'timestamp' and 'point_id' columns to create a MultiIndex
        df_long.set_index(['timestamp', 'point_id'], inplace=True)

        # Now, to add "entry"/"exit" labels to our points
        def label_point_as_entry_or_exit(point: str):
            if point.endswith("_entry"):
                return "entry"
            else:
                return "exit"

        # Apply this function to the 'point_id' index level
        processed_header = df_long.index.get_level_values('point_id').map(label_point_as_entry_or_exit)
        # Add the processed values as a new level to the index
        df_long['point_type'] = processed_header
        # Then set this new column as an additional level of the index
        df_long.set_index('point_type', append=True, inplace=True)

        # Applying reverse transformation to the 'point_id' index, i.e. removing suffix
        df_long.reset_index(drop=False, inplace=True)
        df_long['point_id'] = df_long['point_id'].apply(remove_suffix)

        df_long.set_index(['timestamp', 'point_id', 'point_type'], inplace=True)

        filtered_df = df_long[(df_long.index.get_level_values('timestamp') >= start) & (
                df_long.index.get_level_values('timestamp') <= (end + datetime.timedelta(days=1)))]

        # Rename columns for compatibility with later .xlsx files
        filtered_df_renamed = filtered_df.rename(index=rename_dict, level='point_id')
        return Output(value=filtered_df_renamed)

    elif end < datetime.datetime(2023, 1, 1):
        df = pd.read_excel(filepath_archived_2, engine='openpyxl')

        df = df.drop(df.index[0:1])  # Remove first row (not needed for data)

        # Time to transform the dataframe to make the first row (barring the first element) into an index
        new_headers = df.iloc[0, 1:].tolist()

        # Adding suffix "_exit" to the first 4 headers and "_entry" to the rest, since a point may be bidirectional
        new_headers = [x + "_entry" for x in new_headers[:4]] + [x + "_exit" for x in new_headers[4:]]

        # We will remove the suffixes later
        def remove_suffix(point: str):
            if point.endswith("_entry"):
                return point[:-6]
            else:
                return point[:-5]

        df.columns = ['timestamp'] + new_headers  # Keep 'timestamp' for the first column and update the rest
        # Drop the first row
        df = df.drop(df.index[0])
        # Reset index if necessary
        df.reset_index(drop=True, inplace=True)

        df['timestamp'] = pd.to_datetime(df['timestamp']).map(lambda x: x.tz_localize(None))

        # Melt the DataFrame to go from wide to long format
        df_long = pd.melt(df, id_vars=['timestamp'], var_name='point_id', value_name='value')
        # Set the new index using the 'timestamp' and 'point_id' columns to create a MultiIndex
        df_long.set_index(['timestamp', 'point_id'], inplace=True)

        # Now, to add "entry"/"exit" labels to our points
        def label_point_as_entry_or_exit(point: str):
            if point.endswith("_entry"):
                return "entry"
            else:
                return "exit"

        # Apply this function to the 'point_id' index level
        processed_header = df_long.index.get_level_values('point_id').map(label_point_as_entry_or_exit)
        # Add the processed values as a new level to the index
        df_long['point_type'] = processed_header
        # Then set this new column as an additional level of the index
        df_long.set_index('point_type', append=True, inplace=True)

        # Applying reverse transformation to the 'point_id' index, i.e. removing suffix
        df_long.reset_index(drop=False, inplace=True)
        df_long['point_id'] = df_long['point_id'].apply(remove_suffix)

        df_long.set_index(['timestamp', 'point_id', 'point_type'], inplace=True)

        filtered_df = df_long[(df_long.index.get_level_values('timestamp') >= start) & (
                df_long.index.get_level_values('timestamp') <= (end + datetime.timedelta(days=1)))]

        return Output(value=filtered_df)

    else:
        # Fetch the content of the .xls file
        response = requests.get(url)
        # Check if the request was successful
        if response.status_code == 200:
            # Use BytesIO to create a file-like object from the content
            file_content = BytesIO(response.content)
            df = pd.read_excel(file_content, sheet_name=0)

            df = df.drop(df.index[0:1])  # Remove first row (not needed for data)

            # Time to transform the dataframe to make the first row (barring the first element) into an index
            new_headers = df.iloc[0, 1:].tolist()

            # Adding suffix "_exit" to the first 5 headers and "_entry" to the rest, since a point may be bidirectional
            new_headers = [x + "_entry" for x in new_headers[:5]] + [x + "_exit" for x in new_headers[5:]]

            # We will remove the suffixes later
            def remove_suffix(point: str):
                if point.endswith("_entry"):
                    return point[:-6]
                else:
                    return point[:-5]

            df.columns = ['timestamp'] + new_headers  # Keep 'timestamp' for the first column and update the rest
            # Drop the first row
            df = df.drop(df.index[0])
            # Reset index if necessary
            df.reset_index(drop=True, inplace=True)

            df['timestamp'] = pd.to_datetime(df['timestamp']).map(lambda x: x.tz_localize(None))

            # Melt the DataFrame to go from wide to long format
            df_long = pd.melt(df, id_vars=['timestamp'], var_name='point_id', value_name='value')
            # Set the new index using the 'timestamp' and 'point_id' columns to create a MultiIndex
            df_long.set_index(['timestamp', 'point_id'], inplace=True)

            # Now, to add "entry"/"exit" labels to our points
            def label_point_as_entry_or_exit(point: str):
                if point.endswith("_entry"):
                    return "entry"
                else:
                    return "exit"

            # Apply this function to the 'point_id' index level
            processed_header = df_long.index.get_level_values('point_id').map(label_point_as_entry_or_exit)
            # Add the processed values as a new level to the index
            df_long['point_type'] = processed_header
            # Then set this new column as an additional level of the index
            df_long.set_index('point_type', append=True, inplace=True)

            # Applying reverse transformation to the 'point_id' index, i.e. removing suffix
            df_long.reset_index(drop=False, inplace=True)
            df_long['point_id'] = df_long['point_id'].apply(remove_suffix)

            df_long.set_index(['timestamp', 'point_id', 'point_type'], inplace=True)

            filtered_df = df_long[(df_long.index.get_level_values('timestamp') >= start) & (
                    df_long.index.get_level_values('timestamp') <= (end + datetime.timedelta(days=1)))]
            return Output(value=filtered_df)

        else:
            raise Exception(f"Failed to fetch the file, status code: {response.status_code}")

@asset(
    partitions_def=StaticPartitionsDefinition(["desfa_estimated_vs_actual_offtakes_monopartition"]),
    io_manager_key="postgres_io_manager",
    group_name="desfa",
    op_tags={"dagster/concurrency_key": "desfa", "concurrency_tag": "desfa"},
    description="TSO's Estimations of N.G. Off-takes compared to Actual Off-takes"
)
def desfa_estimated_vs_actual_offtakes(context: AssetExecutionContext):
    context.log.info(f"Handling single partition.")

    url = 'https://www.desfa.gr/userfiles/pdflist/DDRA/Off_Takes_Estimation.xlsx'

    # Fetch the content of the .xls file
    response = requests.get(url)
    # Check if the request was successful
    if response.status_code == 200:
        # Use BytesIO to create a file-like object from the content
        file_content = BytesIO(response.content)
        df = pd.read_excel(file_content, sheet_name=0)

        assert df.iloc[0, 0:3].to_list() == ['Ημερομηνία\nDate', ' TSO Estimation', 'Physical off-takes'], (
            "Unrecognized format")

        # keep only the first three columns
        df.drop(df.columns[3:], axis=1, inplace=True)

        # drop first row
        df.drop(df.index[0], axis=0, inplace=True)

        df.columns = ['timestamp', 'estimated', 'actual']
        df.set_index(['timestamp'], inplace=True)

        return Output(value=df)
    else:
        raise Exception(f"Failed to fetch the file, status code: {response.status_code}")
