from datetime import timedelta as tdel
from glob import glob
import numpy as np
import pandas as pd
import os
import re

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


_def_req_cols = ('flag', 'date', 'column_o2', 'xluft', 'column_luft', 'xco2_ppm', 'column_co2')


def match_test_to_delivered_data(site_abbrev: str, new_eof_csv_file: str, req_columns: Sequence[str] = _def_req_cols,
                                 max_timedelta: Union[tdel, pd.Timedelta] = pd.Timedelta(seconds=5),
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