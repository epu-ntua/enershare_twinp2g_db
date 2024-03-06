from typing import List, Tuple

import pandas as pd
import numpy as np
import pytz
from dagster import TimeWindow
from pandas import Timestamp


# make timestamp UTC and tz-naive
# returns dataframe with timestamp as index
def sanitize_series(sr: pd.Series, cols) -> pd.DataFrame:
    sr = sr.to_frame().reset_index()
    sr.columns = ['timestamp'] + cols
    sr['timestamp'] = sr['timestamp'].apply(lambda x: x.astimezone(pytz.UTC))
    sr['timestamp'] = sr['timestamp'].apply(lambda x: x.tz_localize(None))
    sr.set_index(['timestamp'], inplace=True, drop=True)
    return sr


# make timestamp UTC and tz-naive
def sanitize_df(df: pd.DataFrame):
    df.index = df.index.tz_convert(pytz.UTC)
    df.index = df.index.tz_localize(None)
    df.index.rename(name='timestamp', inplace=True)
    return


def timewindow_to_ts(tw: TimeWindow) -> (Timestamp, Timestamp):
    start = Timestamp(tw.start)
    end = Timestamp(tw.end)
    return start, end


def replace_dash_with_nan(cell):
    if isinstance(cell, str) and cell.strip() == "-":
        return np.nan
    else:
        return cell

def find_string_in_df_by_index(df, search_string, case_sensitive=False) -> List[Tuple[int, int]]:
    """
    Find all occurrences of a string within a pandas DataFrame by indices.

    Parameters:
    - df: Pandas DataFrame to search in.
    - search_string: String to search for.
    - case_sensitive: Boolean indicating if the search should be case sensitive.

    Returns:
    - A list of tuples with (row index, column index) for each occurrence.
    """
    # Optionally convert both the DataFrame and search string to lowercase
    if not case_sensitive:
        temp_df = df.map(lambda x: str(x).lower())
        search_string = search_string.lower()
    else:
        temp_df = df.map(str)

    # Create a boolean mask where the condition is True
    mask = temp_df.map(lambda x: search_string in x)

    # Find row and column indices where the condition is True
    row_indices, col_indices = np.where(mask)

    # Combine row indices and column indices into a list of tuples
    result = list(zip(row_indices, col_indices))

    return result
