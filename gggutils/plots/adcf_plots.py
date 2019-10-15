from matplotlib import pyplot as plt
import numpy as np
from jllutils import plots as jplots

from .. import target_analysis as tgts

_def_hist_bins = np.linspace(-0.20, 0.20, 40)


def plot_adcf_scatter(adcfs, gas, network_adcf=None, fix_ylim=False):
    nsites = adcfs.site.unique().size
    ny = nsites // 2 + nsites % 2
    fig = plt.figure()
    isite = 0

    adcfs['isoutlier'] = False
    adcf_ylims = (adcfs.adcf.min() * 0.99, adcfs.adcf.max() * 1.01)
    for site, site_df in adcfs.groupby('site'):
        xxout = tgts.is_outlier(site_df.adcf)
        xxsite = adcfs['site'] == site
        adcfs.loc[xxsite, 'isoutlier'] = xxout
        isite += 1
        ax = fig.add_subplot(ny, 2, isite)
        ax.plot(site_df.adcf[~xxout], marker='o', color='b', linestyle='none')
        ax.plot(site_df.adcf[xxout], marker='o', color='r', linestyle='none', label='Outlier')
        if network_adcf is not None:
            ax.axhline(network_adcf, color='r', linestyle='--', label='Network adcf ({:.4f})'.format(network_adcf))
        ax.grid()
        ax.legend()
        ax.set_ylabel('{} ADCF'.format(gas))
        ax.set_title(site)
        if fix_ylim:
            ax.set_ylim(adcf_ylims)

    fig.set_size_inches(16, ny * 4)
    return fig


def plot_adcf_site_hists(adcfs, gas, network_adcf=None, hist_bins=_def_hist_bins):
    all_sites = np.sort(adcfs.site.unique())
    nsites = all_sites.size
    ny = nsites // 4
    if nsites % 4 > 0:
        ny += 1

    fig = plt.figure()
    isite = 0
    for site, site_df in adcfs.groupby('site'):
        isite += 1
        ax = fig.add_subplot(ny, 4, isite)
        ax.hist(site_df.adcf, hist_bins, color='k')
        ax.set_title(site)
        ax.set_xlabel('{} ADCF'.format(gas))
        if network_adcf is not None:
            ax.axvline(network_adcf, color='r', linestyle='--', label='Network adcf ({:.4f})'.format(network_adcf))

    fig.set_size_inches(15, 4 * ny)
    plt.subplots_adjust(hspace=0.3)
    return fig


def plot_adcf_overall_scatter(adcfs, gas, network_adcf=None):
    all_sites = np.sort(adcfs.site.unique())
    nsites = all_sites.size

    # Find overall outliers
    adcfs['isoutlier'] = tgts.is_outlier(adcfs.adcf)

    fig, ax = plt.subplots(figsize=(12, 6))
    cm = jplots.ColorMapper(0, nsites - 1, cmap='jet')
    for site, site_df in adcfs.groupby('site'):
        xxout = tgts.is_outlier(site_df.adcf) | site_df.isoutlier
        color = cm(np.flatnonzero(all_sites == site).item())
        ax.plot(site_df[~xxout].index.month, site_df[~xxout].adcf, color=color, markersize=2, marker='o',
                linestyle='none', label=site)

    if network_adcf is not None:
        ax.axhline(network_adcf, color='k', linestyle='--', label='Network adcf ({:.4f})'.format(network_adcf))
    # Shrink current axis by 20%
    box = ax.get_position()
    ax.set_position([box.x0, box.y0, box.width * 0.8, box.height])
    # Put a legend to the right of the current axis
    ax.legend(loc='center left', bbox_to_anchor=(1, 0.5), ncol=2)
    ax.set_xticks(np.arange(1, 13))
    ax.set_xlabel('Month')
    ax.set_ylabel('{} ADCF'.format(gas))
    ax.grid()

    return fig


def plot_adcf_monthly_hist(adcfs, gas, network_adcf=None, hist_bins=_def_hist_bins):
    fig, axs = plt.subplots(4, 3, figsize=(12, 18))
    plt.subplots_adjust(hspace=0.3)

    for month, month_df in adcfs.groupby(lambda ind: ind.month):
        ax = axs[np.unravel_index(month - 1, (4, 3))]
        ax.hist(month_df.adcf, hist_bins, histtype='step', color='k')
        ax.set_title(pd.Timestamp(2000, month, 1).strftime('%B'))
        ax.set_xlabel('{} ADCF'.format(gas))
        ax.grid()
        if network_adcf is not None:
            ax.axvline(network_adcf, color='r', linestyle='--', label='Network adcf ({:.4f})'.format(network_adcf))

    return fig


def plot_adcf_seasonal_hist(adcfs, gas, network_adcf=None, hist_bins=_def_hist_bins):
    def _season(index):
        month = index.month
        if month in (12, 1, 2):
            return 'DJF'
        elif month in (3, 4, 5):
            return 'MAM'
        elif month in (6, 7, 8):
            return 'JJA'
        elif month in (9, 10, 11):
            return 'SON'

    fig, axs = plt.subplots(2, 2, figsize=(12, 9))
    plt.subplots_adjust(hspace=0.3)

    for ax, group in zip(axs.flat, adcfs.groupby(_season)):
        season, seas_df = group
        ax.hist(seas_df.adcf, hist_bins, histtype='step', color='k')
        ax.set_title(season)
        ax.grid()
        ax.set_xlabel('{} ADCF'.format(gas))
        if network_adcf is not None:
            ax.axvline(network_adcf, color='r', linestyle='--', label='Network adcf ({:.4f})'.format(network_adcf))

    return fig


def plot_adcf_overall_hist(adcfs, gas, network_adcf=None, hist_bins=_def_hist_bins):
    fig, ax = plt.subplots()
    ax.hist(adcfs.adcf, hist_bins, histtype='step', color='k')
    ax.set_xlabel('{} ACDF'.format(gas))
    if network_adcf is not None:
        ax.axvline(network_adcf, color='r', linestyle='--', label='Network adcf ({:.4f})'.format(network_adcf))
    ax.grid()

    return fig