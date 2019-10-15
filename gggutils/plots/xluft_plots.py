from matplotlib import pyplot as plt
import numpy as np
import pandas as pd

from ..target_analysis import calc_delta_x


def _plot_xluft_timeser(all_matched_eofs, get_for_day_fxn):
    all_sites = all_matched_eofs.site.unique()
    nsites = len(all_sites)
    ny = nsites // 2 + nsites % 2
    fig = plt.figure()

    isite = -1
    site_names = []
    for site, site_df in all_matched_eofs.groupby('site'):
        isite += 1
        site_names.append(site)
        ax = fig.add_subplot(ny, 2, isite + 1)

        ndays = len(site_df.groupby(['year_new', 'day_new']))
        old_noon_xlufts = np.zeros([ndays])
        new_noon_xlufts = np.zeros([ndays])
        datetimes = []

        iday = -1
        for (year, doy), day_df in site_df.groupby(['year_new', 'day_new']):
            this_date = pd.Timestamp(int(year), 1, 1) + pd.Timedelta(days=doy - 1)
            iday += 1

            old_noon_xlufts[iday] = get_for_day_fxn(
                calc_delta_x(day_df, 'xluft_old', recalc_raw=True, recalc_scale=1.0, check_times=False))
            new_noon_xlufts[iday] = get_for_day_fxn(
                calc_delta_x(day_df, 'xluft_new', recalc_raw=True, recalc_scale=1.0, check_times=False))
            datetimes.append(this_date)

        ax.plot(datetimes, old_noon_xlufts, linestyle='none', marker='x', color='r', label='GGG2014')
        ax.plot(datetimes, new_noon_xlufts, linestyle='none', marker='+', color='b', label='GGGNext')

        ax.set_title(site)
        ax.set_ylabel('Xluft (raw, at solar noon)')
        ax.set_ylim(0.97, 1.01)
        ax.grid()
        ax.legend()

    fig.set_size_inches(16, 4 * ny)
    return fig


def plot_noon_xluft(matched_eofs):
    def get_noon(df):
        idx = np.argmin(df.delta_hours.abs().to_numpy())
        return df.xluft_raw.iloc[idx]

    return _plot_xluft_timeser(matched_eofs, get_noon)


def plot_xluft_in_sza(matched_eofs, sza_range=(40, 50)):
    def get_by_sza(df):
        xx = (df.asza_deg >= sza_range[0]) & (df.asza_deg <= sza_range[1])
        return df.xluft_raw[xx].mean()

    return _plot_xluft_timeser(matched_eofs, get_by_sza)
