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
                                                             req_columns='all')

    # plots - start with differences in Xgas quantities (raw and corrected), VSFs, and RMSes
    diff = 'diff'
    hist = 'hists'
    plots = {'xluft': diff, 'xco2_ppm': diff, 'xn2o_ppb': diff, 'xch4_ppb': diff, 'xhf_ppt': diff, 'xco_ppb': diff}

    with PdfPages(save_file) as pdf:
        for column, plot_type in plots.items():
            for site, site_df in matched_df.groupby('site'):
                comp.plot_comparison(matched_df=site_df, column=column, xraw=False, plot_type=plot_type, hlines=[0],
                                     pdf=pdf)
