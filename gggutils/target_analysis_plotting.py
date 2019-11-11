"""
This module is just an interface to the plotting submodules
"""
import os
from matplotlib.backends.backend_pdf import PdfPages

from .plots import adcf_plots as adcf
from .plots import psurf_plots as psurf
from .plots import xluft_plots as xluft
from .plots import comparison_plots as comp

from . import target_analysis as tgts


def make_standard_comparison_plots(test_root_dir, save_file, matched_df=None):
    if matched_df is None:
        sites = os.listdir(test_root_dir)
        sites = [s for s in sites if len(s) == 2]
        matched_df = tgts.match_test_to_delivered_multi_site(site_abbrevs=sites, test_root_dir=test_root_dir,
                                                             req_columns='all', do_qual_filter='both')

    # plots - start with differences in Xgas quantities (raw and corrected), VSFs, and RMSes
    diff = 'diff'
    hist = 'hists'

    hist_plots = ('xluft', 'xco2_ppm', 'xn2o_ppb', 'xch4_ppm', 'xhf_ppt', 'xco_ppb',  # xgases
                  'vsf_luft', 'vsf_hf', 'vsf_h2o', 'vsf_hdo', 'vsf_co', 'vsf_n2o',    # vsfs
                  'vsf_ch4', 'vsf_co2', 'vsf_o2', 'vsf_hcl', 'co_4233_VSF_co',        # since we had weird CO VSFs at caltech, show their individual window VSFs
                  'co_4290_VSF_co', 'LSE', 'LSU', 'o2_7885_SG')

    include_raw = ('xluft', 'xco2_ppm', 'xn2o_ppb', 'xch4_ppm', 'xh4_ppt', 'xco_ppb')

    plots = {k: hist for k in hist_plots}

    with PdfPages(save_file) as pdf:
        for column, plot_type in plots.items():
            print('Plotting {} for '.format(column), end='')
            for site, site_df in matched_df.groupby('site'):
                print(site, end=' ')
                comp.plot_comparison(matched_df=site_df, column=column, xraw=False, plot_type=plot_type, hlines=[0],
                                     pdf=pdf, suptitle=site)
                if column in include_raw:
                    comp.plot_comparison(matched_df=site_df, column=column, xraw=True, plot_type=plot_type, hlines=[0],
                                         pdf=pdf, suptitle=site)
            print('')
        print('Done.')
