import ephem  # TODO: replace with skyfield
import numpy as np
import pandas as pd
from typing import Sequence
import xarray as xr

from jllutils import dataframes  # monkey-patches dataframes to have the ``interpolate_to`` method
from jllutils import miscutils


class TimeOfDayError(Exception):
    pass


def compute_daily_anomalies(df: pd.DataFrame, anomaly_type: str = 'noon', lon: float = None, lat: float = None,
                            exclude_cols: Sequence[str] = ('year', 'day', 'hour', 'time', 'long_deg', 'lat_deg', 'start_time', 'end_time'),
                            inplace: bool = False, fail_action: str = 'nan'):
    """
    Convert values in a data frame to daily anomalies.

    :param df: A data frame indexed by time.
    :param anomaly_type: how to compute the anomalies. Options are "noon" (relative to solar noon), "mean" (relative to
     the daily mean), or "median" (relative to the daily median). Alternatively, pass a dataframe where the indices are
     the solar dates that has the baseline values for each day to difference against.
    :param lon: longitude of the site the anomalies are being computed for. Only used for ``anomaly_type = "noon"``, and
     then can be automatically inferred as the mean of the "long_deg" column in the dataframe.
    :param lat: latitude of the site. Same notes as ``lon`` apply.
    :param exclude_cols: columns to not compute an anomaly for. The defaults are chosen assuming a TCCON public data
     file was read in to cover variables for which a daily anomaly is not meaningful.
    :param inplace: whether to carry out the calculation in-place and so modify the given dataframe.
    :return: the modified dataframe, if ``inplace = False``.
    """
    # use ephem to find sunrises around the first time. Group all lines for that day, iterate until we've grouped every
    # row into solar days
    if lon is None:
        lon = df['long_deg'].mean()
    if lat is None:
        lat = df['lat_deg'].mean()

    # check that there are no duplicate indices because this will break the interpolation to noon
    # require the user to deal with because we can't assume the duplicate times can just be dropped
    if df.index.duplicated().any() and anomaly_type == 'noon':
        raise TimeOfDayError('The input dataframe contains duplicate indices which must be removed before computing '
                             'anomalies with the "noon" method')

    if not inplace:
        df = df.copy()

    columns_to_compute = [k for k in df.keys() if k not in exclude_cols]
    if isinstance(anomaly_type, pd.DataFrame):
        # Necessarily limit the columns to difference with the anomalies to those in the 
        # input baselines if that's how we're doing this.
        columns_to_compute = [col for col in columns_to_compute if col in anomaly_type.columns]

    observer = ephem.Observer()
    observer.lat, observer.long = '{:.4f}'.format(lat), '{:.4f}'.format(lon)

    solar_dates = get_solar_dates(df.index, observer=observer)
    df.loc[:, 'solar_date'] = solar_dates.astype('datetime64[ns]')
    baseline_df = pd.DataFrame(index=pd.DatetimeIndex(np.unique(solar_dates)), columns=columns_to_compute, dtype=np.float)
    pbar = miscutils.ProgressBar(solar_dates.size, prefix='Computing anomalies')
    for solar_day, day_df in df.groupby('solar_date'):
        xx = df.index.isin(day_df.index)
        pbar.print_bar(np.flatnonzero(xx)[0])

        if isinstance(anomaly_type, pd.DataFrame):
            try:
                baseline_row = anomaly_type.loc[solar_day, :]
            except KeyError:
                if fail_action == 'nan':
                    df.loc[xx, columns_to_compute] = np.nan
                    continue
                else:
                    raise
        elif anomaly_type == 'noon':
            try:
                baseline_row = _find_solar_noon(day_df, observer=observer)
            except TimeOfDayError as err:
                if fail_action == 'nan':
                    #print(err)
                    df.loc[xx, columns_to_compute] = np.nan
                    continue
                else:
                    raise
        elif anomaly_type == 'mean':
            baseline_row = day_df.mean()
        elif anomaly_type == 'median':
            baseline_row = day_df.median()
        else:
            raise ValueError('"{}" is not an allowed anomaly_type'.format(anomaly_type))
                
        # need to take the subset of columns on the RHS for both before subtracting to support 
        # passing in a baseline dataframe that has the computed columns but not all of the 
        # rest.
        df.loc[xx, columns_to_compute] = (day_df.loc[:, columns_to_compute] - baseline_row[columns_to_compute].values)
        baseline_df.loc[pd.Timestamp(solar_day), :] = baseline_row[columns_to_compute].values

    pbar.finish()
    if not inplace:
        return df, baseline_df
    else:
        return baseline_df


def get_solar_dates(times, lon=None, lat=None, observer=None, ):
    if observer is None:
        if lon is None or lat is None:
            raise TypeError('Must provide either an observer or lat+lon')
        observer = ephem.Observer()
        observer.lat, observer.long = '{:.4f}'.format(lat), '{:.4f}'.format(lon)

    sun = ephem.Sun()

    solar_dates = np.full(times.shape, None)

    inext = 0
    while True:
        last_sunrise = observer.previous_rising(sun, start=ephem.Date(times[inext])).datetime()
        next_sunrise = observer.next_rising(sun, start=ephem.Date(times[inext])).datetime()
        xx = (times >= last_sunrise) & (times < next_sunrise)
        solar_dates[xx] = last_sunrise.date()
        if next_sunrise > np.max(times):
            break
        else:
            inext = np.flatnonzero(xx)[-1] + 1
    return solar_dates


def _find_solar_noon(day_df, observer, max_delta=pd.Timedelta(hours=1)):
    sun = ephem.Sun()
    noon = observer.next_transit(sun, start=ephem.Date(day_df.index[0])).datetime()
    if np.abs(day_df.index - noon).min() > max_delta:
        raise TimeOfDayError('No points within {} of solar noon ({} UTC)'.format(max_delta, noon))
    return day_df.interpolate_to([noon], method='index')


def _limit_series(series, start, stop):
    # Can't simply use slice in case the indices are not monotonically increasing
    start = series.index.min() if start is None else start
    stop = series.index.max() if stop is None else stop
    xx = (series.index >= start) & (series.index <= stop)
    return series[xx]



def compute_hourly_avg_anomaly(xgas_anomaly, utc_offset=0, start=None, stop=None):
    """
    Compute the Xgas anomaly for each hour of day

    :param xgas_anomaly: a series of Xgas anomalies (relative to some daily baseline value) indexed by date
    :param utc_offset: how many hours off of UTC the data is. The returned series will be indexed by hour
     in local time.
    :param start: the earliest date from xgas_anomaly to include in the average.
    :param stop: the latest date from xgas_anomaly to include in the average.
    :return: a series of the anomalies averaged by hour of day, indexed by the local hour of day.
    """
    xgas_anomaly = _limit_series(xgas_anomaly, start, stop)
    return xgas_anomaly.groupby((xgas_anomaly.index.hour + utc_offset) % 24).mean().reindex(range(0, 24))
