from matplotlib import pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import numpy as np
import os
import pandas as pd

from .. import target_analysis as tgts


def full_p_timeser(all_matched_eofs: pd.DataFrame, mod_psurfs: pd.DataFrame, pout_var: str = 'pout_hPa_new',
                   pres_var: str = 'psurf'):
    """
    Plot the full time series of pout and FPIT psurf for multiple sites.

    Produces a plot where each panel shows the FPIT psurf and .eof.csv pout for all dates given for a single site.

    :param all_matched_eofs: a dataframe that has both the pout as the "pout_hPa_new" column.
    :param mod_psurfs: a dataframe that has the FPIT surface pressures. This and ``all_matched_eofs`` are typically the
     outputs from :func:`load_eofs_with_fpit` or :func:`add_fpit_pres`
    :param pout_var: column name from ``all_matched_eofs`` to plot.
    :param pres_var: column name from ``mod_psurfs`` to plot.
    :return: figure handle.
    """
    nsites = len(all_matched_eofs.groupby('site'))
    ny = nsites // 2 + nsites % 2

    fig = plt.figure()

    isite = 0
    for site, site_df in all_matched_eofs.groupby('site'):
        isite += 1
        ax = fig.add_subplot(ny, 2, isite)

        xx_site = mod_psurfs.site == site
        ax.plot(site_df.date_new, site_df[pout_var], marker='o', markersize=6, color='b', linestyle='none',
                label='Pout')
        ax.plot(mod_psurfs[xx_site][pres_var], marker='+', markersize=18, color='r', linestyle='none', label='FPIT')
        ax.legend()
        ax.grid()
        ax.set_ylabel('Surface pressure (hPa)')
        ax.set_title(site)

    fig.set_size_inches(16, 4 * ny)
    return fig


def save_pout_timeseries(all_matched_eofs: pd.DataFrame, mod_psurfs: pd.DataFrame, save_dir: str,
                         old_or_new: str = 'new', fix_ylim: str = 'by-site') -> None:
    """
    Save time series of pout and FPIT surface pressure to a .pdf file, one plot per day, one page per site.

    :param all_matched_eofs: a dataframe that has both the pout as the "pout_hPa_new" column.
    :param mod_psurfs: a dataframe that has the FPIT surface pressures. This and ``all_matched_eofs`` are typically the
     outputs from :func:`load_eofs_with_fpit` or :func:`add_fpit_pres`
    :param save_dir: directory to save the .pdf to.
    :param old_or_new: whether to plot GGG2014 ('old') or GGGNext ('new') pressure.
    :param fix_ylim: how to fix the y-limits. "global" will set the same y-limits for all sites, "by-site" will set the
     same y-limits for all days for a single site, and anything else will leave the y-limits at the defaults.
    :return: none
    """
    gggvers = 'GGG2014' if old_or_new == 'old' else 'GGGNext'

    pout_range = (700, 1100)

    with PdfPages(os.path.join(save_dir, '{}_pout_timeseries.pdf'.format(gggvers.lower()))) as pdf:
        for site, site_df in all_matched_eofs.groupby('site'):
            print('On', gggvers, site)
            fig = plt.figure()
            ndays = len(site_df.groupby(['year_new', 'day_new']))
            ny = ndays // 2 + ndays % 2

            # Get the min/max pressure ranges we'll want for this site
            pout_minmax = (site_df.pout_hPa_new.min(), site_df.pout_hPa_new.max())
            xx_site = mod_psurfs.site == site
            psurf_minmax = (mod_psurfs[xx_site].psurf.min(), mod_psurfs[xx_site].psurf.max())
            site_p_range = (
            np.floor(min(pout_minmax[0], psurf_minmax[0])) - 2, np.ceil(max(pout_minmax[1], psurf_minmax[1])) + 2)

            iday = 0
            for (year, doy), day_df in site_df.groupby(['year_new', 'day_new']):
                xx_psurf = (mod_psurfs.year == year) & (mod_psurfs.day == doy) & (mod_psurfs.site == site)
                mod_psurfs_sub = mod_psurfs[xx_psurf]
                this_date = pd.Timestamp(int(year), 1, 1) + pd.Timedelta(days=doy - 1)
                iday += 1
                ax = fig.add_subplot(ny, 2, iday)

                ax.plot(day_df.date_new, day_df.pout_hPa_new, linestyle='none', marker='.', color='b', label='Pout')
                ax.plot(mod_psurfs_sub.index, mod_psurfs_sub.psurf, linestyle='none', marker='P', markersize=12,
                        color='r', label='FPIT')
                ax.plot(day_df.date_new, day_df.fpit_surfp, linestyle='none', marker='.', color='orange',
                        label='interp. FPIT')

                ax.grid()
                ax.legend()

                plt.xticks(rotation=45)
                ax.set_ylabel('Surface pressure (hPa)')
                ax.set_title('{}: {}'.format(site, this_date.strftime('%Y-%m-%d')))
                # ax.set_xlim(*hour_range)
                if fix_ylim == 'global':
                    ax.set_ylim(pout_range)
                elif fix_ylim == 'by-site':
                    ax.set_ylim(site_p_range)

            fig.set_size_inches(16, 4 * ny)
            plt.subplots_adjust(hspace=0.4)

            pdf.savefig(fig, bbox_inches='tight')
            plt.close(fig)

        info = pdf.infodict()
        info['Title'] = '{} FPIT surf. pres. vs. pout timeseries of OCO-2 targets'.format(gggvers)
