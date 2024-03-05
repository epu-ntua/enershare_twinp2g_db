from typing import List

import entsog
import pandas as pd
import time

from dagster import AssetExecutionContext
from entsog import EntsogPandasClient

# Dictionary that matches TSO EIC codes to TSO names
tso_dict = {"21X-GR-A-A0A0A-G": "DESFA",
            "21X000000001376X": "TAP"}


# Some points can belong of different TSOs (e.g. both DESFA and TAP have Nea Mesimvria as a point)
# The below function preemptively labels these points by adding their TSO as a suffix, to avoid deduplication
# Needs to be called after the API response before any further processing is done on the dataframe
def label_potential_duplicates_with_tso(df: pd.DataFrame) -> pd.DataFrame:
    # So far, only Nea Mesimvria identified as a suspect
    def modify_row(row):
        if row['point_label'] == 'Nea Mesimvria':  # Condition to identify rows
            return row['point_label'] + "_" + tso_dict[row['tso_eic_code']]  # Modification based on TSO EIC
        else:
            return row['point_label']  # Keep original value for rows that don't meet the condition

    df['point_label'] = df.apply(modify_row, axis=1)
    return df


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
                                 max_retries: int = 10,
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
            context.log.info(f"Attempt no. {attempt + 1} failed with error: {type(e).__name__}: {e}")
            last_exception = e
            time.sleep(delay_seconds)

    raise last_exception
