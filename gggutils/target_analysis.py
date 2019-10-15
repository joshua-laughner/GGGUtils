from datetime import timedelta as tdel
from glob import glob
import numpy as np
import pandas as pd
import os
import re
from scipy import stats

from typing import Sequence, Union

from ginput.common_utils import mod_utils

from .runutils import find_by_glob
from .exceptions import TimeMatchError


# At some point, we'll want these to be configurable.
_root_dir = '/oco2-data/tccon/data_delivery/OCO2/'
_test_root_dir = '/oco2-data/tccon-nobak/scratch/beta-test-spectra/rc1/'
_abbrev_to_subdir = {'ae': 'Ascension',
                     'bi': 'Bialystok',
                     'br': 'Bremen',
                     'bu': 'Burgos',
                     'db': 'Darwin',
                     'et': 'EastTroutLake',
                     'df': 'Edwards',
                     'eu': 'Eureka',
                     'iz': 'Izana',
                     'ka': 'Karlsruhe',
                     'oc': 'Lamont',
                     'll': 'Lauder_125HR_ll',
                     'lr': 'Lauder_125HR_lr',
                     'ma': 'Manaus',
                     'or': 'Orleans',
                     'pr': 'Paris',
                     'pa': 'ParkFalls',
                     'ci': 'Pasadena',
                     'ra': 'Reunion',
                     'rj': 'Rikubetsu',
                     'js': 'Saga',
                     'so': 'Sodankyla',
                     'tk': 'Tsukuba',
                     'wg': 'Wollongong'}

all_sites = tuple(_abbrev_to_subdir.keys())


def is_outlier(y, zcut=2):
    xx = stats.zscore(np.abs(y)) < zcut
    return ~xx

def read_eof_csv(csv_file: str) -> pd.DataFrame:
    """
    Read a .eof.csv (engineering output file, comma-separated value format) file
    :param csv_file: the path to the .eof.csv file
    :return: a dataframe with all the information from the .eof.csv file, indexed with the dates of the measurments in
     the file.
    """
    nhead = mod_utils.get_num_header_lines(csv_file)
    df = pd.read_csv(csv_file, header=nhead - 1, sep=',')
    df.set_index(df_ydh_to_dtind(df), inplace=True, verify_integrity=True)
    return df


def read_eof_csv_by_sitedate(sitedate: str) -> pd.DataFrame:
    """
    Read the .eof.csv file for a particular site and OCO-2 target date

    :param sitedate: a string with the format ssYYYYMMDD, i.e. the two-letter site abbreviation followed by the year,
     month, and day. Example: "ci20140901" would look for an .eof.csv file uploaded for Caltech measurements on
     2014-09-01. This function uses the ``_root_dir`` and the ``_abbrev_to_subdir`` dictionary defined in the module to
     find these files. Right now, this is only set up for ccycle.
    :return: a dataframe with all the information from the .eof.csv file, indexed with the dates of the measurments in
     the file.
    """
    date_dir = os.path.join(_root_dir, _abbrev_to_subdir[sitedate[:2]], sitedate)
    if not os.path.exists(date_dir):
        raise IOError('No directory found for {} (looking for {})'.format(sitedate, date_dir))

    eof_csv_files = glob(os.path.join(date_dir, '*.eof.csv'))
    if len(eof_csv_files) == 1:
        return read_eof_csv(eof_csv_files[0])
    elif len(eof_csv_files) == 0:
        raise IOError('No .eof.csv file found in {}'.format(date_dir))
    else:
        raise IOError('Multiple .eof.csv files found in {}'.format(date_dir))


def read_all_eofs_for_site(site_abbrev: str) -> pd.DataFrame:
    """
    Read all delivered .eof.csv files for a given site.

    This finds all the .eof.csv files for a given site in the OCO-2 target directory and concatenates them together.
    This function uses the ``_root_dir`` and the ``_abbrev_to_subdir`` dictionary defined in the module to find these
    files. Right now, this is only set up for ccycle.

    :param site_abbrev: the two-letter abbreviation of the site to load .eof.csv files for.
    :return: a dataframe with all the information from the .eof.csv file, indexed with the dates of the measurments in
     the file.
    """
    date_dirs = glob(os.path.join(_root_dir, _abbrev_to_subdir[site_abbrev], '{}*'.format(site_abbrev)))
    date_dirs = [os.path.basename(d) for d in date_dirs]
    indiv_dfs = []
    for ddir in date_dirs:
        indiv_dfs.append(read_eof_csv_by_sitedate(ddir))

    print('Read .eof.csvs from {}'.format(','.join(date_dirs)))
    return pd.concat(indiv_dfs, sort=False)


def search_df_keys(df: pd.DataFrame, pattern: str, nocase: bool = True) -> Sequence[str]:
    """
    Search a dataframe for column keys matching a given pattern.

    :param df: the dataframe to search.
    :param pattern: the pattern to search for.
    :param nocase: when ``True``, the search is case-insensitive. Set this to ``False`` to retain case-sensitivity.
    :return: a list of keys matching the requested pattern.
    """
    re_flags = 0
    if nocase:
        re_flags |= re.IGNORECASE
    matches = [k for k in df.keys() if re.search(pattern, k, flags=re_flags)]

    return matches


def ydh_to_timestamp(year: int, day: int, hour: Union[int, float]) -> pd.Timestamp:
    """
    Convert a single year, day, and fractional hour into a Pandas timestamp.

    :param year: the year
    :param day: the day of year, 1-based.
    :param hour: the hour. May contain a fractional component and be negative.
    :return: the datetime
    """
    return pd.Timestamp(year, 1, 1) + pd.Timedelta(days=day - 1, hours=hour)


def df_ydh_to_dtind(df: pd.DataFrame) -> pd.DatetimeIndex:
    """
    Create a DatetimeIndex from a .eof.csv dataframe
    :param df: a dataframe containing "year", "day" and "hour" columns that are the year, day-of-year (1-based), and
     fractional hour of their rows.
    :return: a DatetimeIndex with the corresponding datetimes.
    """
    return pd.DatetimeIndex([ydh_to_timestamp(int(y), d, h) for y, d, h in zip(df.year, df.day, df.hour)])


_def_req_cols = ('flag', 'date', 'year', 'day', 'hour', 'column_o2', 'xluft', 'column_luft', 'xco2_ppm', 'column_co2')


def match_test_to_delivered_data(site_abbrev: str, new_eof_csv_file: str, req_columns: Sequence[str] = _def_req_cols,
                                 max_timedelta: Union[tdel, pd.Timedelta] = pd.Timedelta(seconds=3),
                                 do_qual_filter: str = 'none') -> pd.DataFrame:
    """
    Create a single dataframe containing data from both old (delivered) .eof.csv files and a new .eof.csv file

    This will match observations from the old and new data that are within a certain time of each other and return
    a single dataframe containing both sets of data. If any time in the new data matches multiple observations in the
    old data, an error is raised.

    Note that Xair and other "air" quantities in old .eof.csv files (GGG2014 and previous) will be replaced with
    "luft" to be consistent with the GGG2019+ standard.

    :param site_abbrev: the two-letter abbreviation for the site to load delivered .eof.csv files for.
    :param new_eof_csv_file: the path to the new .eof.csv file to load.
    :param req_columns: columns from the .eof.csv files to retain in the combined dataframe, as a sequence of strings.
     A special case is the key "date", which is not a column in the .eof.csv files, but which will have the datetime
     index of its dataframe, since the combined dataframe switches to a simple integer index.
    :param max_timedelta: the maximum difference in time considered a match between the old and new dataframe.
    :param do_qual_filter: whether or not to quality filter the final dataframe. "none" does no filtering, "old"
     requires that the quality flag in the old data be 0, "new" likewise checks the new quality flag, and "both"
     requires both old and new data to have a quality flag of 0.
    :return: a combined dataframe that has both old and new data. Column names will be suffixed with "_old" and "_new",
     respectively. Only the columns specified by ``req_columns`` will be included.
    """
    new_df = read_eof_csv(new_eof_csv_file)
    old_df = read_all_eofs_for_site(site_abbrev)

    # Not sure if there's a faster way to do this other than iterating through every single line and finding the
    # matching time
    old_inds = []
    new_inds = []
    new_unmatched = 0

    pbar = mod_utils.ProgressBar(new_df.shape[0], style='counter', prefix='Matching')
    for iline, new_time in enumerate(new_df.index):
        if iline % 10 == 0 or iline == (new_df.shape[0] - 1):
            pbar.print_bar(iline)

        timediffs = abs(old_df.index - new_time)
        old_ind = np.flatnonzero(timediffs < max_timedelta)

        if old_ind.size > 1:
            # Matched multiple times. Not good. Should only match one.
            raise TimeMatchError(
                '{} times matched for {} in the old data. Try reducing the max_timedelta.'.format(old_ind.size,
                                                                                                  new_time))
        elif old_ind.size < 1:
            # No match. Do not include this line from the new data frame
            new_unmatched += 1
        else:
            # Matched.
            old_ind = old_ind.item()
            old_inds.append(old_df.index[old_ind])
            new_inds.append(new_time)

    pbar.finish()

    # Cut down the dataframes to the same lines, reindex them to use just an integer index, but keep the times so we can
    # check. Cut them down to the desired columns, rename 'xair' and 'air' in the old dataframe to "xluft" and "luft",
    # respectively.
    old_columns = [c.replace('air', 'luft') for c in old_df.columns]
    old_df.columns = old_columns
    old_df = old_df.loc[old_inds, :].reset_index().rename(columns={'index': 'date'}).loc[:, req_columns]
    new_df = new_df.loc[old_inds, :].reset_index().rename(columns={'index': 'date'}).loc[:, req_columns]

    combo_df = old_df.join(new_df, how='inner', lsuffix='_old', rsuffix='_new')

    if do_qual_filter == 'none':
        return combo_df
    elif do_qual_filter == 'old':
        xx = combo_df.flag_old == 0
    elif do_qual_filter == 'new':
        xx = combo_df.flag_new == 0
    elif do_qual_filter == 'both':
        xx = (combo_df.flag_old == 0) & (combo_df.flag_new == 0)
    else:
        raise ValueError('do_qual_filter = "{}" is invalid'.format(do_qual_filter))

    combo_df['site'] = site_abbrev
    return combo_df.loc[xx, :]


def match_test_to_delivered_by_site(site_abbrev: str, **kwargs) -> pd.DataFrame:
    """
    Automatically match old and new data for a specific site.

    This wraps :func:`match_test_to_delivered_data`, automatically finding the right new .eof.csv file to combine with
    the original target data. This relies on the paths for the delivered data and test data defined in this module.
    Currently this is configured only for the GGG2019 test on ccycle.

    :param site_abbrev: the two-letter site abbreviation to load data for.
    :param kwargs: additional keyword arguments for :func:`match_test_to_delivered_data`. Note that ``new_eof_csv_file``
     cannot be included because that is determined by this function.
    :return: the combined dataframe, same as :func:`match_test_to_delivered_data`.
    """
    new_eof_csv_file = find_by_glob(os.path.join(_test_root_dir, site_abbrev, 'postproc', '*eof.csv'))
    return match_test_to_delivered_data(site_abbrev, new_eof_csv_file, **kwargs)


def match_test_to_delivered_multi_site(site_abbrevs: Sequence[str], **kwargs) -> pd.DataFrame:
    """
    Load old and new .eof.csv files for many sites
    :param site_abbrevs: list of two letter site abbreviations
    :param kwargs: keyword arguments for :func:`match_test_to_delivered_data`
    :return: a dataframe of all sites concatentated together
    """
    total_df = None
    for site in site_abbrevs:
        df = match_test_to_delivered_by_site(site, **kwargs)
        if total_df is None:
            total_df = df
        else:
            total_df = pd.concat([total_df, df])

    return total_df


def recalc_x(df: pd.DataFrame, xname: str, scale: float) -> pd.Series:
    """
    Recalculate an X-quantity (XLUFT, XCO2, etc.)

    To get a "raw" X-quantity that does not include an airmass -independent or -dependent correction, this function
    calculates a new X-quantity as column_? / column_o2 * 0.2095 * scale.

    :param df: the dataframe containing the column specie and column_o2 data.
    :param xname: the name of the X-quantity (e.g. XLUFT or XCO2_old) to calculate. This function supports both single
     .eof.csv dataframes and combined old & new data frames that have a column_o2_old and column_o2_new; it will use
     the right column_o2 for the right X-quantity.
    :param scale: a final scale factor to put the X-quantity in the right units.
    :return: the series of X-quantity values.
    """
    specie = re.search('(?<=x)\w+(?=_)', xname).group()
    old_or_new = re.search('_(old|new)$', xname)
    if old_or_new is None:
        old_or_new = ''
    else:
        old_or_new = old_or_new.group()
    colname = 'column_{}{}'.format(specie, old_or_new)
    o2name = 'column_o2{}'.format(old_or_new)
    return df[colname] / df[o2name] * 0.2095 * scale


##################
# ADCF Functions #
##################

def read_adcf_file(adcf_file: str) -> pd.DataFrame:
    """
    Read an airmass-dependent corrections file created by `derive_airmass_correction`
    :param adcf_file: the path to the ADCF file
    :return: a dataframe of the airmass corrections, indexed by date.
    """
    def _make_timestamp(ser):
        return pd.Timestamp(int(ser.year), 1, 1) + pd.Timedelta(days=ser.doy - 1)

    nhead = mod_utils.get_num_header_lines(adcf_file)
    df = pd.read_csv(adcf_file, header=nhead-1, sep='\s+')
    df['adcf'] = compute_adcf(df, remove_outliers=False)
    return df.set_index(df.apply(_make_timestamp, axis=1))


def iter_adcf_files(sites: Sequence[str], gas: str, ignore_missing: bool = False) -> (str, pd.DataFrame):
    """
    Iterate over the ADCF files for a list of sites for a specific gas
    :param sites: a sequence of site abbreviations
    :param gas: the gas name, must match the dac_xx_targets.vav_<gas>.out file name
    :param ignore_missing: whether to error or skip sites that do not have a ADCF file.
    :return: iteration over the site abbreviations and the corresponding data frames
    """
    for site in sites:
        pp_dir = os.path.join(_test_root_dir, site, 'postproc')
        gas_adcf_file = os.path.join(pp_dir, 'dac_{site}_targets.vav_{gas}.out'.format(site=site, gas=gas))
        try:
            df = read_adcf_file(gas_adcf_file)
        except FileNotFoundError:
            if ignore_missing:
                print('Cannot find an ADCF file for {}'.format(site))
            else:
                raise
        else:
            yield site, df


def compute_adcf(df, remove_outliers=False) -> pd.Series:
    """
    Compute airmass dependent correction factors for a given dataframe

    :param df: a dataframe resulting from reading in an ADCF file.
    :param remove_outliers: whether or not to remove outliers, defined as points with a z-score >= 2.
    :return: the airmass dependence correction factors as a series
    """
    adcf = df.sdc / df.ybar
    if remove_outliers:
        xx = is_outlier(adcf)
        adcf = adcf[~xx]

    return adcf


def load_all_adcfs(sites: Sequence[str], gas: str, ignore_missing: bool = True, req_num_spectra: int = 200) -> pd.DataFrame:
    """
    Load all ADCF files

    :param sites: sequence of site abbrevations indicating the sites to load
    :param gas: string indicating which gas to load, much match the name in the "dac*" file
    :param ignore_missing: if ``True``, any sites for which a DAC file cannot be found will just be skipped. If
     ``False``, an error is raised in that case.
    :param req_num_spectra: minimum number of good spectra required for a day to be included in the ADCF dataframe. If
     a day doesn't have enough spectra on either side of solar noon, then the ADCF will not be reliable.
    :return: a dataframe containing the ADCF data.
    """
    total_df = None
    for site, site_df in iter_adcf_files(sites, gas, ignore_missing=ignore_missing):
        if req_num_spectra > 0:
            # Read the .eof.csv file to get the number of spectra per day that are good
            eof_df = read_eof_csv(find_by_glob(os.path.join(_test_root_dir, site, 'postproc', '*.eof.csv')))
            xx_dates = pd.Series(False, index=site_df.index)
            for date in site_df.index:
                xx_eof = (eof_df.year == date.year) & (eof_df.day == date.dayofyear)
                xx_dates[date] = (eof_df[xx_eof].flag == 0).sum() > req_num_spectra
            site_df = site_df[xx_dates]
        site_df['site'] = site
        if total_df is None:
            total_df = site_df
        else:
            total_df = pd.concat([total_df, site_df])

    return total_df


def calc_delta_x(df: pd.DataFrame, xquantity: str, recalc_raw: bool = False, recalc_scale: float = 1.0,
                 check_times: bool = True, hour_key=None, sza_key=None) -> pd.DataFrame:
    """
    Create a dataframe with a given quantity calculated relative to local noon
    
    :param df: a dataframe containing the desired quantity, hour, and asza_deg columns. If the desired quantity
     is suffixed with "_old" or "_new", the hour and sza column names must be as well.
    :param xquantity: the X-quantity (e.g. Xluft, XCO2) to calculate as a delta. 
    :param recalc_raw: if ``True``, the X-quantity is calculated from column_<quantity> and column_o2, which must also have the 
     matching "_old" or "_new" suffixes if relevant.
    :param recalc_scale: if ``recalc_raw`` is ``True``, pass the multiplicative scale for the recalculated X-quantity
     as this parameter.
    :param check_times: ensure that hour monotonically increases along the dataframe. Set to ``False`` to disable this check.
    :param hour_key: key to use to find the hour of day. If not given, it is assumed to be "hour", suffixed by "_old" or 
     "_new" if the ``xquantity`` was.
    :param sza_key: key to use to find the SZA. If not given, it is assumed to be "asza_deg", suffixed by "_old" or 
     "_new" if the ``xquantity`` was.
    :return: a new dataframe with the x-quantity, delta x-quantity, hours from local noon, and solar zenith angle. "_old"
     and "_new" suffixes are removed.
    """
    old_or_new = re.search(r'_(old|new)', xquantity)
    if old_or_new is None:
        old_or_new = ''
    else:
        old_or_new = old_or_new.group()
        
    if hour_key is None:
        hour_key = 'hour' + old_or_new
    if sza_key is None:
        sza_key = 'asza_deg' + old_or_new
    
    # Check that the hour is monotonically increasing. If not, we probably have 
    # multiple days, which isn't allowed
    if check_times and (np.diff(df[hour_key]) < 0).any():
        raise ValueError('{} decreases at least once. This may be because the dataframe passed in contains multiple days, which is not permitted.')
        
    # Find the minimum solar zenith angle - that's solar noon
    noon_idx = df[sza_key].idxmin()
    
    # Get the x-quantity to calculate the delta of
    if recalc_raw:
        xdata = recalc_x(df, xquantity, scale=recalc_scale)
    else:
        xdata = df[xquantity]
        
    # Make a data frame to hold the original quantity, the delta quantity, the hour from local noon,
    # and the solar zenith angle.
    delta_df = pd.DataFrame(index=df.index)
    xname_out = xquantity.replace(old_or_new, '')
    if recalc_raw:
        xname_out = xname_out + '_raw'
    delta_xname = 'delta_' + xname_out
        
    delta_df[xname_out] = xdata
    delta_df[delta_xname] = xdata - xdata[noon_idx]
    delta_df['delta_hours'] = df[hour_key] - df[hour_key][noon_idx]
    delta_df['asza_deg'] = df[sza_key]
    return delta_df


def add_fpit_pres(matched_eofs: pd.DataFrame, interp_method: str = 'index') -> (pd.DataFrame, pd.DataFrame):
    """
    Add FPIT surface pressure from .mod files

    Note that this requires the necessary .mod files to be available in :file:`$GGGPATH/models/gnd`.

    :param matched_eofs: a data frame created by one of the ``match_test_*`` functions that the FPIT surface pressure
     will be added to. Must contain columns "site", "year_new", and "day_new".
    :param interp_method: how to interpolate FPIT pressure to the observation times.
    :return: two data frames. The first will be ``matched_eofs`` but with the fpit_surfp added as a new column. The
     second will be the individual FPIT surface pressure from the .mod files.
    """
    with pd.option_context('mode.chained_assignment', None):
        mod_dir = os.path.join(os.path.expandvars('$GGGPATH'), 'models', 'gnd')

        def round_to_3h(ts, nexthr=False):
            hr = ts.hour // 3 * 3
            ts = pd.Timestamp(ts.year, ts.month, ts.day, hr)
            if nexthr:
                ts += pd.Timedelta(hours=3)
            return ts

        matched_eofs = matched_eofs.reset_index(drop=True)
        matched_eofs['fpit_surfp'] = np.nan
        mod_df = pd.DataFrame(columns=['site', 'psurf', 'pbottom', 'mod_file'])
        sites_listed = set()
        for (site, year, doy), sub_df in matched_eofs.groupby(['site', 'year_new', 'day_new']):
            if site not in sites_listed:
                print('On', site)
                sites_listed.add(site)
            # get the site lat and lon. there must be one unique value, or .item() will raise a
            # ValueError
            site_lon = sub_df.long_deg_new.unique().item()
            site_lat = sub_df.lat_deg_new.unique().item()

            # We'll need to do a little work on the dataframe. We need to keep the integer index so
            # that we can put the values back in the main dataframe, but we need the actual index to
            # be the date so that we can insert the geos surface pressure by date and interpolate
            sub_df['rownum'] = sub_df.index
            sub_df = sub_df.set_index('date_new', drop=False)

            # load the surface pressures for the relevant times
            first_geos_time = round_to_3h(sub_df.date_new.min())
            last_geos_time = round_to_3h(sub_df.date_new.max(), nexthr=True)
            geos_times = pd.date_range(first_geos_time, last_geos_time, freq='3H')

            for geos_time in geos_times:
                mod_file_name = mod_utils.mod_file_name_for_priors(geos_time, site_lat, site_lon)
                mod_file_name = os.path.join(mod_dir, mod_file_name)
                mod_data = mod_utils.read_mod_file(mod_file_name)
                sub_df.loc[geos_time, 'fpit_surfp'] = mod_data['scalar']['Pressure']

                this_mod_dict = {'site': site, 'year': year, 'day': doy, 'psurf': mod_data['scalar']['Pressure'],
                                 'pbottom': mod_data['profile']['Pressure'][0], 'mod_file': mod_file_name}
                mod_df = pd.concat([mod_df, pd.DataFrame(this_mod_dict, index=[geos_time])], sort=True)

            sub_df['fpit_surfp'] = sub_df.fpit_surfp.sort_index().interpolate(method=interp_method)
            # Now that we've filled in the FPIT surface pressure, we need to get it back into the main dataframe.
            # We'll first remove rows that do not have a row number, because they were not in the original dataframe,
            # then turn those back into a integer index and use that to determine which rows the fpit pressures go in
            xx_orig = ~pd.isnull(sub_df.rownum)
            orig_rows = pd.Int64Index(sub_df[xx_orig].rownum)
            sub_df = sub_df[xx_orig].set_index(orig_rows)
            matched_eofs.loc[orig_rows, 'fpit_surfp'] = sub_df.fpit_surfp

    return matched_eofs, mod_df


def load_eofs_with_fpit(sites: Sequence[str], match_kws: dict = None, fpit_kws: dict = None) -> (pd.DataFrame, pd.DataFrame):
    """
    Simultaneously load .eof.csv files and the associated FPIT surface pressure

    :param sites: sequence of site abbreviations to load .eof.csv files from
    :param match_kws: keyword arguments for :func:`match_test_to_delivered_multi_site`
    :param fpit_kws: keyword arguments for :func:`add_fpit_pres`
    :return: two data frames. The first will be ``matched_eofs`` but with the fpit_surfp added as a new column. The
     second will be the individual FPIT surface pressure from the .mod files.
    """
    if match_kws is None:
        match_kws = dict()
    if fpit_kws is None:
        fpit_kws = dict()

    all_matched_eofs = match_test_to_delivered_multi_site(sites, **match_kws)
    return add_fpit_pres(all_matched_eofs, **fpit_kws)
