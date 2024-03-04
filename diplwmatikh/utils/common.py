import pandas as pd
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
