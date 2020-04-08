import ephem  # TODO: replace with skyfield
import numpy as np
import pandas as pd

from jllutils import dataframes  # monkey-patches dataframes to have the ``interpolate_to`` method
from jllutils import miscutils


class TimeOfDayError(Exception):
    pass


def find_all_transits(dates, obs, obj=ephem.Sun()):
    """Identify all solar noon transits for a time period

    Parameters
    ----------
    dates : pandas.DatetimeIndex
        Index of dates. Solar transits will be identified for all
        dates between the beginning and end of this index, with an
        extra day added at the beginning and end of the record.

    obs : ephem.Observer
        A PyEphem Observer for the location we need to compute
        solar noon for.

    obj : ephem.Body
        The heavenly body to compute transits for. The sun, by default,
        so this means solar noons are calculated.

    Returns
    -------
    numpy.ndarray
        Array with `datetime64[ns]` type giving all transit times, in UTC,
        covering the date range described by `dates`.
    
    """
    dates = dates.round('D')
    start = dates.min() - pd.DateOffset(days=1)
    end = dates.max() + pd.DateOffset(days=1)
    dates = pd.date_range(start, end, freq='D')
    noons = np.zeros(dates.shape, dtype='datetime64[ns]')
    for idate, date in enumerate(dates):
        noons[idate] = obs.next_transit(obj, ephem.Date(date)).datetime()
        
    return noons


def calc_noon_anomalies(columns_ser, lon, lat, remove_dup=False):
    """Calculate the anomalies from solar noon in retrieved gas data

    Given a time series of retrieved gas data, this uses `find_all_transits`
    to identify the solar noon times. The column data is then interpolated
    to the solar noons. Each data point then has the solar noon value
    subtracted to compute its diurnal anomaly. The solar noon value used
    is determined by:

        1. Get the solar noon values on either side
        2. If one or both solar noon values is a NaN, points in between get 
           the value of the solar noon they are closest to (including NaN
           if that is the closest one).
        3. If neither neighboring solar noon values are NaNs, then the points
           in between are a time-distance-squared-weighted average of the 
           solar noon values.

    Parameters
    ----------
    columns_ser : pandas.Series
        A Pandas series indexed by date. The values will be used to compute 
        anomalies.

    lon : float
        The longitude of the site that we are computing anomalies for. 
        Necessary to determine the solar noon times.

    lat : float
        The latitude of the site that we are computing anomalies for.

    remove_dup : bool
        Whether to remove values in the series that have a duplicate index.
        If `True`, the first value of a duplicate index is kept and the others
        are dropped. If `False`, then an error is raised if there are duplicate
        indices

    Returns
    -------
    anomalies : pandas.Series
        A series of the diurnal anomalies, computed from `columns_ser`. Indexed by
        datetime.

    baselines : pandas.Series
        A series of the solar noon values, indexed by datetime.
    
    """
    dd = columns_ser.index.duplicated()
    columns_ser = columns_ser[~dd]
    
    obs = ephem.Observer()
    obs.lat = '{:.4f}'.format(lat)
    obs.lon = '{:.4f}'.format(lon)
    
    noons = find_all_transits(columns_ser.index, obs)
    noon_values = columns_ser.interpolate_to(noons, limit=1)
    
    anomalies = pd.Series(np.nan, index=columns_ser.index)
    baselines = pd.Series(np.nan, index=columns_ser.index)
    cuts = pd.cut(columns_ser.index, noons)
    for drange, sub_df in columns_ser.groupby(cuts):
        left_noon, right_noon = drange.left, drange.right
        left_noon_val = noon_values[left_noon]
        right_noon_val = noon_values[right_noon]
        
        left_delta_t = sub_df.index - left_noon
        right_delta_t = right_noon - sub_df.index
        
        if np.isnan(left_noon_val) or np.isnan(right_noon_val):
            these_noon_values = pd.Series(np.nan, index=sub_df.index)
            these_noon_values[left_delta_t < right_delta_t] = left_noon_val
            these_noon_values[left_delta_t >= right_delta_t] = right_noon_val
        else:
            wt = (right_delta_t / (right_noon - left_noon))**2
            these_noon_values = left_noon_val * wt + right_noon_val * (1 - wt)
        
        dd = columns_ser.index.isin(sub_df.index)
        baselines[dd] = these_noon_values.to_numpy()
        anomalies[dd] = columns_ser[dd] - these_noon_values
        
    return anomalies, baselines


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
