from jllutils import plots as jplots
from matplotlib import pyplot as plt
import numpy as np

from typing import Sequence, Union

from ..target_analysis import match_test_to_delivered_by_site, recalc_x

plt.rcParams['font.size'] = 12


def plot_xluft_comparison(site_abbrevs: Sequence[str], qual_filter: str = 'both', plot_type: str = 'scatter',
                          calc_x: bool = True):
    """
    Plot a comparision of Xluft between two GGG versions.

    :param site_abbrevs: sequence of two letter site abbreviations, indicating which sites to compare. Uses
    :func:`match_test_to_delivered_by_site` to load the data.
    :param qual_filter: how to quality filter Xluft. See :func:`match_test_to_delivered_data`.
    :param plot_type: what type of plot to make. "scatter" will create a single panel scatter plot comparing the old and
     new Xluft values, colored by site. "hist" will make a two panel plot, with site-by-site histograms of old and new
     Xluft on the left and overall Xluft on the right. "sep
    :param calc_x:
    :return:
    """
    if plot_type == 'hist':
        nplots = 2
    elif plot_type == 'sephist':
        nplots = 3
    else:
        nplots = 1

    fig, axs = plt.subplots(1, nplots, figsize=(6 * nplots, 6))
    if np.size(axs) == 1:
        axs = [axs]

    if plot_type == 'sephist':
        ax = axs[:2]
    else:
        ax = axs[0]

    cm = jplots.ColorMapper(0, len(site_abbrevs) - 1, 'jet')

    xluft_old_all = np.array([])
    xluft_new_all = np.array([])
    for isite, site in enumerate(site_abbrevs):
        combo_df = match_test_to_delivered_by_site(site, do_qual_filter=qual_filter)

        if calc_x:
            xluft_old = recalc_x(combo_df, 'xluft_old', 1)
            xluft_new = recalc_x(combo_df, 'xluft_new', 1)
        else:
            xluft_old = combo_df['xluft_old'].to_numpy()
            xluft_new = combo_df['xluft_new'].to_numpy()

        xluft_old_all = np.concatenate([xluft_old_all, xluft_old])
        xluft_new_all = np.concatenate([xluft_new_all, xluft_new])

        if plot_type == 'scatter':
            _xluft_comp_scatter(ax, xluft_old, xluft_new, color=cm(isite), site=site)
        elif plot_type in ('hist', 'sephist'):
            _xluft_comp_hist(ax, xluft_old, xluft_new, color=cm(isite), site=site)

    if plot_type in ('hist', 'sephist'):
        old_bins, new_bins = _xluft_comp_hist(axs[-1], xluft_old_all, xluft_new_all, color='k', site=None, bins=20)
        _xluft_print_stats(axs[-1], old_bins, xluft_old_all)
        _xluft_print_stats(axs[-1], new_bins, xluft_new_all)
        axs[-1].set_title('Overall')
        axs[-1].grid()

    for ax in axs:
        ax.legend()
        ax.grid()


def _xluft_comp_scatter(ax, xluft_old, xluft_new, color, site):
    print('making scatter plot', site)
    ax.plot(xluft_old, xluft_new, linestyle='none', marker='.', markersize=1, color=color)
    ax.plot(np.nan, np.nan, linestyle='none', marker='o', color=color, label=site)
    ax.set_xlabel('GGG2014 Xluft')
    ax.set_ylabel('GGGNext Xluft')
    ax.set_aspect('equal')
    ax.set_xlim([0.95, 1.01])
    ax.set_ylim([0.95, 1.01])


def _xluft_comp_hist(ax, xluft_old, xluft_new, color, site, **kwargs):
    print('making hist plot', site)
    if np.size(ax) == 1:
        ax.set_xlabel('Xluft (dashed 2014, solid Next)')
        ax = [ax, ax]
        old_line = ':'
        old_legend = dict()
    else:
        old_line = '-'
        old_legend = {'label': site}
        ax[0].set_xlabel('Xluft (GGG2014)')
        ax[1].set_xlabel('Xluft (GGGNext)')
    old = ax[0].hist(xluft_old, histtype='step', linestyle=old_line, color=color, density=True, **old_legend, **kwargs)
    new = ax[1].hist(xluft_new, histtype='step', linestyle='-', color=color, label=site, density=True, **kwargs)

    return old, new


def _xluft_print_stats(ax, hist_results, xluft):
    mean = np.nanmean(xluft)
    std = np.nanstd(xluft, ddof=1)
    i = np.argmax(hist_results[0])  # find the mode bin
    x = hist_results[1][i]
    y = hist_results[0][i]
    ax.text(x, y, '{:.4f} $\pm$ {:.4f}'.format(mean, std))