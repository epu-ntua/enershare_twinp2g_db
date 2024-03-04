from typing import List

import entsog
import pandas as pd
import time

from dagster import AssetExecutionContext
from entsog import EntsogPandasClient


def greek_operator_point_directions():
    points = entsog.EntsogPandasClient().query_operator_point_directions()
    mask1 = points['t_so_balancing_zone'].str.contains('Greece')
    mask2 = points['t_so_country'].str == 'GR'
    masked_points = points[mask1 | mask2]

    keys = []
    for idx, item in masked_points.iterrows():
        keys.append(f"{item['operator_key']}{item['point_key']}{item['direction_key']}")

    return keys


def entsog_api_call_with_retries(start: pd.Timestamp,
                                 end: pd.Timestamp,
                                 indicators: List[str],
                                 keys: List[str],
                                 context: AssetExecutionContext,
                                 max_retries: int = 5,
                                 delay_seconds: int = 1) -> pd.DataFrame:
    last_exception = None
    for attempt in range(max_retries):
        try:
            data = EntsogPandasClient().query_operational_point_data(start=start,
                                                                     end=end,
                                                                     indicators=indicators,
                                                                     point_directions=keys,
                                                                     verbose=False)
            if attempt > 0:
                context.log.info(f"Attempt no {attempt + 1} successful.")
            return data
        except Exception as e:
            context.log.info(f"Attempt no. {attempt + 1} failed with error: {e}")
            last_exception = e
            time.sleep(delay_seconds)

    raise last_exception
