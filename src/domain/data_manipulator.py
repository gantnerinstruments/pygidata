# This module implements domain functions of the python write section


import pandas as pd
from datetime import datetime


def calc_sums(arr, frequenzy, aggregation="D", ts_index=0):
    """Calculates sum of given measured frequenzy (Hz) and aggregation type(e.g. Hour)

    Args:
        arr ([type]): Array
        ts_offset ([type]): array index to skip
        frequenzy ([type]): (Hz) given as 's' 'm' ...
        aggregation ([type]): (Hour, Day, Month)
    """

    # Convert array into dataframe for sum operations
    df = pd.DataFrame(data=arr)

    if frequenzy == "s" or frequenzy == "m":
        df["datetime"] = pd.to_datetime(df[0] / 1000, unit="s")  # unit=(D,s,ms,us,ns)

    resample = df.set_index("datetime")
    resample = resample.resample(aggregation).sum()

    resample = resample.reset_index()

    # Fix timestamps (got summed up before)
    for index, row in resample.iterrows():
        resample[ts_index][index] = datetime.timestamp(resample["datetime"][index]) * 1000

    # Drop datetime column to get original array shape
    resample = resample.drop(["datetime"], axis=1)

    resample = resample.to_numpy()
    return resample
