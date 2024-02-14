import datetime

import pandas as pd
from entsoe.mappings import lookup_area, Area
from entsoe.parsers import _extract_timeseries, _parse_load_timeseries
from entsoe import EntsoeRawClient
from typing import Union

from .common import sanitize_df


def parse_loads(xml_text, process_type='A01'):
    """
    Parameters
    ----------
    xml_text : str

    Returns
    -------
    pd.DataFrame
    """
    print(xml_text)
    if process_type == 'A01' or process_type == 'A16':
        series = []
        for soup in _extract_timeseries(xml_text):
            series.append(_parse_load_timeseries(soup))
        series = pd.concat(series)
        series = series.sort_index()
        return pd.DataFrame({
            'Forecasted Load' if process_type == 'A01' else 'Actual Load': series
        })
    else:
        series_min = pd.Series(dtype='object')
        series_max = pd.Series(dtype='object')
        for soup in _extract_timeseries(xml_text):
            t = _parse_load_timeseries(soup)
            if soup.find('businesstype').text == 'A60':
                # append is no longer in pandas
                # so below line does not work
                # series_min = series_min.append(t)
                print("series_min")
                print(series_min)
                print("t")
                print(t)
                if len(series_min) == 0:
                    series_min = t
                else:
                    series_min = pd.concat([series_min, t])
            elif soup.find('businesstype').text == 'A61':
                # append is no longer in pandas
                # so below line does not work
                # series_max = series_max.append(t)
                if len(series_max) == 0:
                    series_max = t
                else:
                    series_max = pd.concat([series_max, t])
            else:
                continue
        return pd.DataFrame({
            'min_total_load': series_min,
            'max_total_load': series_max
        })


def query_load_forecast(raw_client: EntsoeRawClient, country_code: Union[Area, str], start: pd.Timestamp,
                        end: pd.Timestamp, process_type: str = 'A01') -> pd.DataFrame:
    area = lookup_area(country_code)
    text = raw_client.query_load_forecast(
        country_code=area, start=start, end=end, process_type=process_type)

    df = parse_loads(text, process_type=process_type)
    print(df)
    df = df.tz_convert(area.tz)
    df = df.truncate(before=(start - datetime.timedelta(hours=3)), after=end)
    # Round to nearest midnight to represent start of day.
    sanitize_df(df)
    df.index = df.index.round('24H')
    return df


def transform_columns_to_ids(df: pd.DataFrame, column_label='point_id', value_label='value') -> pd.DataFrame:
    # Initialize an empty list to hold the transformed DataFrames
    dfs = []

    # Iterate over each column
    for column in df.columns:
        # Create a DataFrame for each column
        temp_df: pd.DataFrame = df[[column]].copy()
        # Rename the column to "value"
        temp_df.rename(columns={column: value_label}, inplace=True)
        # Add a $column_label column containing the name of the original column
        temp_df[column_label] = column
        # Append the DataFrame to the list
        dfs.append(temp_df)

    # Concatenate all the DataFrames along rows
    final_df = pd.concat(dfs, ignore_index=False)

    # Since every combo of index x $column_label is unique, it is better expressed as an index
    final_df.set_index(column_label, append=True, inplace=True)
    return final_df
