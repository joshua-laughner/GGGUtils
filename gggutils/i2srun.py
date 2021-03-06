from collections import OrderedDict
from configobj import ConfigObj
import datetime as dt
from glob import glob
from logging import getLogger
from multiprocessing import Pool
import os
import re
import shutil
from subprocess import Popen
import sys
from textui import uielements

from . import _i2s_halt_file
from . import runutils, exceptions, target_utils, igram_analysis
from .runutils import iter_i2s_dirs, load_config_file, date_subdir, get_date_cfg_option

logger = getLogger('i2srun')


def build_cfg_file(cfg_file, i2s_input_files, old_cfg_file=None, relpaths=False):
    """
    Build a starting config file specifying where the data are to run I2S for a large number of sites/days.

    :param cfg_file: the path to write the new config file to
    :type cfg_file: str

    :param i2s_input_files: a list of slice-i2s or opus-i2s input files that specify the days/sites to run. Note that
     these files MUST have the string xxYYYYMMDD somewhere in the file name, where "xx" is the site abbreviation and
     "YYYYMMDD" is the year-month-day date.
    :type i2s_input_files: list(str)

    :param old_cfg_file: if given, then this is a path to a previous version of the config file that should be merged
     into the new one. This way, any setting present in the old one will get copied into the new one, and any new config
     options get added.
    :type old_cfg_file: str

    :return: none, writes a new config file
    """
    # setup the common config elements
    cfg = ConfigObj()
    cfg.filename = cfg_file

    cfg['Run'] = {'run_top_dir': ''}
    cfg['Run'].comments = {'run_top_dir': ['# The directory where the data are linked to to run I2S/GGG']}
    cfg['I2S'] = dict()
    cfg['Sites'] = dict()

    # now make the site sections. include the default options for each group, then make each input date a subsection
    # with by default just the input file options
    grouped_files = _group_i2s_input_files(i2s_input_files)
    relative_to = os.path.dirname(cfg_file) if relpaths else None

    for site, site_dict in grouped_files.items():
        slices, subdir = _group_uses_slices(site_dict, relative_to=relative_to)
        site_opts = {'slices': slices,
                     'site_root_dir': '',
                     'no_date_dir': '0',
                     'subdir': subdir,
                     'slices_in_subdir': '0',
                     'flimit_file': ''}
        for site_date, input_file in site_dict.items():
            path = input_file if relpaths else os.path.abspath(input_file)
            site_opts[site_date] = {'i2s_input_file': path}
        cfg['Sites'][site] = site_opts

    if old_cfg_file is not None:
        old_cfg = ConfigObj(old_cfg_file)
        cfg.merge(old_cfg)

    cfg.write()


def update_cfg_run_files(cfg_file, i2s_run_files, keep_missing=False, new_cfg_file=None):
    """
    Update an existing config file so that all the site/dates use new run files

    :param cfg_file: the config file to update
    :type cfg_file: str

    :param i2s_run_files: list of I2S run files that will replace existing run files in the config.
    :type i2s_run_files: list(str)

    :param keep_missing: by default, if a site/date in the config file no longer has a corresponding run file in the
     given list of such files, it is removed from the config file. Set this keyword to ``True`` to keep such site/dates.
    :type keep_missing: bool

    :param new_cfg_file: a path to write the modified config file to. If not given, the original one is overwritten.
    :type new_cfg_file: str

    :return: none. Writes the updated config file.
    """

    cfg = load_config_file(cfg_file)
    if new_cfg_file is not None:
        cfg.filename = new_cfg_file

    grouped_files = _group_i2s_input_files(i2s_run_files)
    for site, site_dict in cfg['Sites'].items():
        site_files = grouped_files[site]
        for datestr in site_dict.sections:
            if datestr not in site_files:
                if keep_missing:
                    logger.debug('{} does not have an input file in the new list, not updating'.format(datestr))
                else:
                    logger.info('{} does not have an input file anymore, removing'.format(datestr))
                    site_dict.pop(datestr)
            else:
                site_dict[datestr]['i2s_input_file'] = site_files[datestr]

    cfg.write()


def _concat_i2s_run_files(i2s_files, last_header_param=runutils._default_last_header_param, parse_run_lines=True):
    """
    Concatenate scans from multiple I2S input files into a single list

    The header is always drawn from the first I2S input file.

    :param i2s_files: list of I2S input files to combine
    :type i2s_files: path-like

    :param last_header_param: number of general I2S parameters before the individual scans start in the I2S input files.
    :type last_header_param: int

    :param parse_run_lines: whether to parse the run lines into dicts or leave as strings
    :type parse_run_lines: bool

    :returns: list of header lines and list of run lines
    """
    # Read all the header info from the first file
    header_lines = []
    with open(i2s_files[0]) as robj:
        for param_num, subparam_num, value, comment, is_param in runutils.iter_i2s_input_params(robj, include_all_lines=True):
            if param_num > last_header_param:
                break

            if len(comment) > 0:
                value += ':' + comment
            header_lines.append(value.rstrip())

    # Iterate over all files and join their run lines together into a single list
    run_lines = []
    for fname in i2s_files:
        _, these_run_lines = runutils.read_i2s_input_params(fname, last_header=last_header_param,
                                                            verbatim_run_lines=not parse_run_lines)
        ok, nfirst, nbad, bad_index = _check_run_lines(these_run_lines)
        if not ok:
            raise IOError('Inconsistent number of elements in run lines of {file}: first line had {n} but the {i}th '
                          'line had {nbad}'.format(file=fname, n=nfirst, nbad=nbad, i=bad_index+1))
        run_lines += these_run_lines
        logger.info('{} scans found in {}'.format(len(these_run_lines), fname))

    return header_lines, run_lines


def create_header_and_full_scan_list(i2s_files, header_file, scan_list_file,
                                     last_header_param=runutils._default_last_header_param):
    """
    Create two files: I2S header and scan catalog from a collection of I2S input files

    The header is always drawn from the first I2S input file.

    :param i2s_files: list of I2S input files to combine
    :type i2s_files: path-like

    :param header_file: path to write the header file to
    :type header_file: path-like

    :param scan_list_file: path to write the list of scans to
    :type scan_list_file: path-like

    :param last_header_param: number of general I2S parameters before the individual scans start in the I2S input files.
    :type last_header_param: int
    """
    header_lines, run_lines = _concat_i2s_run_files(i2s_files, last_header_param=last_header_param, parse_run_lines=False)
    with open(header_file, 'w') as hobj:
        hobj.write('\n'.join(header_lines))
    with open(scan_list_file, 'w') as sobj:
        sobj.write('\n'.join(run_lines))


def _split_run_lines_by(run_lines, site, group_by):
    """
    Split up run lines into time periods

    :param run_lines: list of run lines
    :type run_lines: Sequence[str]

    :param site: two-letter site ID to use in the date strings
    :type site: str

    :param group_by: which
    """
    def make_split_key(year, month, day):
        year = int(year)
        month = int(month)
        day = int(day)

        if group_by == 'Y':
            return '{}{:04d}'.format(site, year)
        elif group_by == 'M':
            return '{}{:04d}{:02d}'.format(site, year, month)
        elif group_by == 'D':
            return '{}{:04d}{:02d}{:02d}'.format(site, year, month, day)
        else:
            raise ValueError('Unexpected value for `key`: "{}"'.format(group_by))

    def group(info_dict):
        if group_by == 'Y':
            return info_dict['year']
        elif group_by == 'M':
            return info_dict['year'], info_dict['month']
        elif group_by == 'D':
            return info_dict['year'], info_dict['month'], info_dict['day']

    splits = dict()
    current = []
    last_info = None
    for line in run_lines:
        info = runutils.parse_run_line(line)
        value = group(info)
        if last_info is not None and value != group(last_info):
            k = make_split_key(last_info['year'], last_info['month'], last_info['day'])
            splits[k] = current
            current = []

        current.append(line)
        last_info = info

    k = make_split_key(info['year'], info['month'], info['day'])
    splits[k] = current
    return splits


def _get_run_line_length(line):
    """
    Get the canonical length of a run line, whether given as a string or dictionary.
    """
    if isinstance(line, str):
        return len(runutils.parse_run_line(line))
    else:
        return len(line)


def _check_run_lines(run_lines):
    """
    Check that a list of run lines (as strings or dicts) all have the same length

    :return: True/False (whether the lines all match), number of elements in the first line, number of elements in the
     first *bad* line, and index of the first bad line. If all lines match, then the last two return values are the
     number of elements in the first line and ``None``.
    """
    n = _get_run_line_length(run_lines[0])
    for i, line in enumerate(run_lines):
        m = _get_run_line_length(line)
        if m != n:
            return False, n, m, i

    return True, n, n, None


def _build_cfg_by_split(output_dir, site_id, header_lines, run_lines, is_slices, split='D'):
    """
    Build a config file with scans split up into days, months, or years.
    """
    split_run_lines = _split_run_lines_by(run_lines, site_id, split)
    slice_or_opus = 'slice' if is_slices else 'opus'

    # Create input files first, then build the config. Since input files can be relative to the config file, we don't
    # need to mess with making absolute paths
    input_files = []
    for datestr, date_run_lines in split_run_lines.items():
        input_file_basename = '{datestr}.{type}-i2s.in'.format(datestr=datestr, type=slice_or_opus)
        input_file_fullname = os.path.join(output_dir, input_file_basename)
        input_files.append(input_file_basename)

        with open(input_file_fullname, 'w') as wobj:
            wobj.write('\n'.join(header_lines) + '\n')
            wobj.write('\n'.join(date_run_lines))

    build_cfg_file(os.path.join(output_dir, 'i2s_parallel.cfg'), input_files, relpaths=True)


def build_cfg_from_many_inputs(i2s_input_files, site_id, output_dir, split_by='D', is_slices=None,
                               last_param_num=runutils._default_last_header_param):
    """
    Build a config file from many I2S input files.

    This assumes that you have many I2S input files but want them reorganized into sensible splits (by day, month, or
    year) for parallelization.

    :param i2s_input_files: list of paths to I2S input files
    :type i2s_input_files: Sequence[path-like]

    :param site_id: the two-letter site ID for this header/scan combination.
    :type site_id: str

    :param output_dir: directory to save the I2S input files and the config file to.
    :type output_dir: str

    :param split_by: how to split the I2S data for parallelization. Options are 'D' (daily), 'M' (monthly) or 'Y'
     (yearly)
    :type split_by: str

    :param is_slices: whether the scans are slices or full Opus interferograms. If this is ``None``, then this is
     inferred from the scan catalog.
    :type is_slices: bool

    :param last_param_num: number of general I2S parameters before the individual scans start in the I2S input files.
    :type last_param_num: int
    """
    if is_slices is None:
        is_slices = runutils.i2s_use_slices(i2s_input_files[0])
    header_lines, run_lines = _concat_i2s_run_files(i2s_input_files, parse_run_lines=False,
                                                    last_header_param=last_param_num)

    ok, n, nbad, bad_index = _check_run_lines(run_lines)
    if not ok:
        raise IOError('Inconsistent numbers of elements in run lines of input files: the first line of {file1} had {n} '
                      'elements but the {i}th line read in had {nbad}'
                      .format(n=n, file1=i2s_input_files[0], i=bad_index+1, nbad=nbad))
    _build_cfg_by_split(output_dir=output_dir, site_id=site_id, header_lines=header_lines, run_lines=run_lines,
                        is_slices=is_slices, split=split_by)


def build_cfg_from_header_scan_list(header_file, scan_list, site_id, output_dir, split_by='D', is_slices=None):
    """
    Build an i2srun config file from a file containing the input header lines and one containing the catalog of scans

    :param header_file: path to the file with the header lines (general I2S parameters)
    :type header_file: path-like

    :param scan_list: path to the file with the catalog of scans to process
    :type scan_list: path-like

    :param site_id: the two-letter site ID for this header/scan combination.
    :type site_id: str

    :param output_dir: directory to save the I2S input files and the config file to.
    :type output_dir: str

    :param split_by: how to split the I2S data for parallelization. Options are 'D' (daily), 'M' (monthly) or 'Y'
     (yearly)
    :type split_by: str

    :param is_slices: whether the scans are slices or full Opus interferograms. If this is ``None``, then this is
     inferred from the scan catalog.
    :type is_slices: bool
    """
    if is_slices is None:
        is_slices = runutils.i2s_use_slices(scan_list, last_header=0)

    with open(header_file) as hobj:
        header_lines = [l.strip() for l in hobj]
    with open(scan_list) as sobj:
        run_lines = [l.strip() for l in sobj]

    ok, n, nbad, bad_index = _check_run_lines(run_lines)
    if not ok:
        raise IOError('Inconsistent numbers of elements in scan list: the first line had {n} but the {i}th line had '
                      '{nbad}.'.format(n=n, nbad=nbad, i=bad_index))

    _build_cfg_by_split(output_dir=output_dir, site_id=site_id, header_lines=header_lines, run_lines=run_lines,
                        is_slices=is_slices, split=split_by)


def make_one_i2s_run_file(target_data_dir, run_files, run_file_save_dir=None, overwrite=False, slice_dir='slices'):
    """
    Make a single I2S run file for a given target directory

    :param target_data_dir: the directory with the target data to make a run file for. The directory name must follow
     the xxYYYYMMDD pattern.
    :type target_data_dir: str

    :param run_files: list of run files or single run file to use a template(s) for the new run file, i.e. to copy the
     header parameters from.
    :type run_files: str or list(str)

    :param run_file_save_dir: where to save the new run files. If ``None``, they will be saved in the same directory
     as their template. Note however, that unlike :func:`make_i2s_run_files`, this function always tries to create a
     run file, even if there already is one for this target data directory in the list of run files given. So be sure
     that ``overwrite`` has the value you want.
    :type run_file_save_dir: str or None

    :param overwrite: whether or not to overwrite an existing file.
    :type overwrite: bool

    :param slice_dir: directory to find slice directories (named YYMMDD.R) to use to populate the list at the bottom of
     the run file. Has no effect if copying a template to run full interferograms. Note that this is interpreted
     differently based on its format:

        * If an absolute path, it is used unmodified.
        * If a relative path *starting with a period* (e.g. "./slices"), it is considered relative to the current
          working directory.
        * If a relative path that does *not* start with a period (e.g. "slices"), it is considered relative to the
          ``target_data_dir``.

    :type slice_dir: str

    :return: none, creates run files.
    """
    target_datestr = os.path.basename(target_data_dir.rstrip(os.sep))
    if not re.match(r'\w\w\d{8}$', target_datestr):
        raise ValueError('{} does not appear to be a target data directory'.format(target_data_dir))
    if not os.path.isabs(slice_dir) and not slice_dir.startswith('.'):
        slice_dir = os.path.join(target_data_dir, slice_dir)
    if isinstance(run_files, str):
        run_files = [run_files]

    run_file_dict = dict()
    for runf in run_files:
        base_runf = os.path.basename(runf)
        key = re.search(r'\w\w\d{8}', base_runf).group()
        run_file_dict[key] = runf
    _make_new_i2s_run_file(datestr=target_datestr, run_files=run_file_dict, save_dir=run_file_save_dir,
                           overwrite=overwrite, slice_dir=slice_dir)


def make_i2s_run_files(dirs_list, run_files, run_file_save_dir=None, overwrite=False, slice_dir=None,
                       exclude_dates=''):
    """
    Create new I2S run files using other files as templates
    
    :param dirs_list: file containing a list of target directories to make run files for, one per line. These are 
     directories that themselves have subdirectories of the xxYYYYMMDD.
    :type dirs_list: str
     
    :param run_files: a list of existing I2S run files. These are the files that will be used as templates to make files
     for dates in the directories listed in ``dirs_list``; the one closest by date to each of the missing directories 
     will be used as the template for each date missing a run file.
    :type run_files: list(str)
    
    :param run_file_save_dir: location to save the new run files to. If it is ``None``, they will be stored alongside
     their templates.
    :type run_file_save_dir: str
      
    :param overwrite: controls what happens if the new run file created would overwrite and existing file. If ``False``,
     the existing file is left alone.
    :type overwrite: bool
    
    :param slice_dir: used when creating a new file that uses slices to try to populate the list of slices at the bottom
     of the run file. If not given, then the rules of :func:`add_slice_info_to_i2s_run_file` are followed to guess where
     the slices might be; however, this will almost always need to be specified.
    :type slice_dir: str

    :param exclude_dates: full date strings (xxYYYYMMDD) to exclude, i.e. not make a run file for. Give as a list of
     date strings or a comma separated list (no spaces).
    :type exclude_dates: list(str) or str

    :return: none
    """
    if isinstance(exclude_dates, str):
        exclude_dates = exclude_dates.split(',')

    avail_target_dates = target_utils.build_target_dirs_dict(target_dirs=[], dirs_list=dirs_list,
                                                             flat=True, full_datestr=True)
    for site, site_dict in avail_target_dates.items():
        if len(site_dict) == 0:
            logger.info('No date folders found for {}'.format(site))
            continue
        run_file_dict = _list_existing_i2s_run_files(site_dict.keys(), run_files=run_files)

        # Now for each target date, if it has a file, reset the last key found to have a file to that, so that we use
        # the most recent file before a missing date as the template if we need to make a new input file. If there's not
        # a file, then copy one to be that file.
        for datestr, runfile in run_file_dict.items():
            if datestr in exclude_dates:
                logger.debug('{} excluded, skipping'.format(datestr))
                continue

            if runfile is None:
                _make_new_i2s_run_file(datestr=datestr, run_files=run_file_dict,
                                       save_dir=run_file_save_dir, overwrite=overwrite, slice_dir=slice_dir)


def _list_existing_i2s_run_files(target_dates, run_files):
    """
    Make a dictionary indicating which target days already have run files.

    :param target_dates: a list of full date strings (xxYYYYMMDD) to find run files for.
    :type target_dates: list(str)

    :param run_files: the list of run files available to be matched with target days
    :type run_files: list(str)

    :return: a ordered dictionary with the same date strings as keys as ``target_date_dict`` and the corresponding run
     file as the value, or ``None`` if no run file is available.
    :rtype: :class:`collections.OrderedDict`
    """

    target_dates = runutils.sort_datestr(target_dates)
    # Get the site abbreviation from the target dates. Assume its always the first two characters of the date strings.
    site_abbrev = set(td[:2] for td in target_dates)
    if len(site_abbrev) != 1:
        raise ValueError('target_date_dict appears to contain multiple sites: {}'.format(', '.join(site_abbrev)))
    else:
        site_abbrev = site_abbrev.pop()

    run_files = [f for f in run_files if re.search(site_abbrev + r'\d{8}', os.path.basename(f)) is not None]
    run_file_dict = {re.search(r'\w\w\d{8}', os.path.basename(f)).group(): f for f in run_files}
    run_file_dict = [(k, run_file_dict[k]) if k in run_file_dict else (k, None) for k in target_dates]
    run_file_dict = OrderedDict(run_file_dict)
    return run_file_dict


def _make_new_i2s_run_file(datestr, run_files, save_dir=None, overwrite=False, slice_dir=None,
                           file_type=None):
    """
    Create a new I2S run file using an existing run file as a template.

    This will find the run file closest by date to the requested date string, copy the header parameters from it into
    the new file, and either try to fill in the list of slices (if a slice file) or leave the list of opus files blank,
    if an opus-type file.

    :param datestr: the date string that we are creating a file for, in xxYYYYMMDD format.
    :type datestr: str

    :param run_files: list of available run files to use as templates
    :type run_files: list(str)

    :param save_dir: the directory to save the new run files to. If ``None``, then the new file will be saved in the
     same directory as the template it was created from.
    :type save_dir: str or None

    :param overwrite: if ``False``, then the new run file will not be created if doing so will overwrite an existing
     file. Set to ``True`` to change that behavior.
    :type overwrite: bool

    :param slice_dir: directory to search for slice files to populate the list of slices in a slice-type run file. This
     must be the directory that contains the slice date folders, named YYMMDD.R. If not given, the rules in
     :func:`add_slice_info_to_i2s_run_file` are used to try to find the slices, but this will usually need to be
     specified.
    :type slice_dir: str

    :param file_type: set to "slice" or "opus" to force this to treat the new files as that type; for slice-type files,
     it will populate the list of slices to run, for opus-type files, it will leave that list blank. If not given then
     this function tries to infer which file type the template is.
    :type file_type: str or None

    :return: none, creates new run files.
    """
    target_date = dt.datetime.strptime(datestr[2:], '%Y%m%d')
    
    def closest_in_time(k):
        d = dt.datetime.strptime(k[2:], '%Y%m%d')
        return abs(d - target_date)

    keys_by_time = [k for k in sorted(run_files.keys(), key=closest_in_time) if run_files[k] is not None]

    if len(keys_by_time) == 0:
        logger.warning('Cannot make a run file for {}: no existing files for that site'.format(datestr))
        return

    if file_type is None:
        found_key = False
        for key in keys_by_time:
            try:
                uses_slices = runutils.i2s_use_slices(run_files[key])
            except exceptions.I2SFormatException:
                pass
            else:
                found_key = True
                break

        file_type = 'slice' if uses_slices else 'opus'
        if not found_key:
            raise exceptions.I2SFormatException('None of the available run files lists any igrams, cannot determine file type')

    old_file = run_files[key]
    new_base_file = os.path.basename(old_file)
    new_base_file = re.sub(r'\w\w\d{8}', datestr, new_base_file)
    if save_dir is None:
        save_dir = os.path.dirname(old_file)
    new_file = os.path.join(save_dir, new_base_file)
    if os.path.exists(new_file):
        if not overwrite:
            logger.info('Not making {}, already exists'.format(new_file))
            return
        else:
            logger.debug('Overwriting {}'.format(new_file))
            os.remove(new_file)

    logger.info('Copying {} to {}'.format(old_file, new_file))
    # Need to duplicate this code here because add_slice_info will get the new location of the run file, which
    # definitely won't have the slices relative to it
    if slice_dir is None:
        run_params = runutils.read_i2s_input_params(old_file)
        slice_dir = run_params[1]
        if not os.path.isabs(slice_dir):
            slice_dir = os.path.join(os.path.dirname(old_file), slice_dir)

    if file_type == 'slice':
        try:
            add_slice_info_to_i2s_run_file(old_file, new_run_file=new_file, start_date=datestr[2:], end_date=datestr[2:],
                                           slice_dir=slice_dir)
        except exceptions.I2SSetupException as err:
            logger.warning(err.args[0])
    elif file_type == 'opus':
        # At the moment, I haven't implemented a way to figure out the input file list with opus format. So just copy
        # everything but the list of files.
        logger.warning('For {}, the list of input files will be blank because it is opus-format and auto-populating '
                       'a list of opus files is not implemented'.format(new_base_file))
        runutils.modify_i2s_input_params(old_file, new_file=new_file, include_input_files=False)
    else:
        raise ValueError('{} is not an allowed value for file_type. Must be "slice" or "opus"')


def copy_i2s_run_files_from_target_dirs(dirs_list, save_dir, interactive='choice', overwrite=None, prefix=None):
    """
    Copy I2S run files from disperate target directories into one collected directory.

    All I2S run files (*.in files) will be copied to the save directory and named opus-i2s.xxYYYYMMDD.in or
    slice-i2s.xxYYYYMMDD.in, depending on whether they are opus- or slice- type files.

    :param dirs_list: file containing a list of target directories to make run files for, one per line. These are
     directories that themselves have subdirectories of the xxYYYYMMDD.
    :type dirs_list: str

    :param save_dir: directory to save the run files to.
    :type save_dir: str

    :param interactive: controls what level of interactivity this has in normal operation. Options are "choice", "all",
     or "none". If "choice", then whenever multiple input files (files matching "*.in") are found, you will be prompted
     to choose which one to copy. If "all", then you will always be prompted, even if there is only 1 or 0 files. If
     "none", then you will never be prompted, but an error is raised if multiple .in files are found.
    :type interactive: str

    :param overwrite: controls what happens if the file already exists where it is being copied to. If ``None``, then
     you will be prompted whether or not to overwrite. Otherwise, set to ``True`` to ``False`` to always or never
     overwrite, respectively.
    :type overwrite: bool or None

    :param prefix: Prefix to use instead of "opus-i2s" or "slice-i2s" at the beginning of the file. May set to one of
     those to force the files to use the same prefix regardless of whether they are opus- or slice- type. If ``None``,
     then the prefix is automatically chosen based on the file type.
    :type prefix: str or None.

    :return: none, copies files
    """
    avail_target_dates = target_utils.build_target_dirs_dict([], dirs_list=dirs_list, flat=True,
                                                             full_datestr=True, key_by_basename=False)
    for site, site_dict in avail_target_dates.items():
        for site_date, revision in site_dict.items():
            full_site_dir = os.path.join(site, revision, site_date)
            site_input_files = glob(os.path.join(full_site_dir, '*.in'))

            if len(site_input_files) == 0:
                if interactive == 'all':
                    print('\nNo input files found in {}. Press ENTER to continue.'.format(full_site_dir), end='')
                    input()
                else:
                    logger.warning('No input files found in {}'.format(full_site_dir))
                continue
            elif len(site_input_files) == 1 and interactive != 'all':
                input_file = site_input_files[0]
            elif interactive == 'none':
                # more than two input files found
                raise NotImplementedError('More than two input files found in {} and interactive was "none"'
                                          .format(full_site_dir))
            else:
                input_file_basenames = [os.path.basename(f) for f in site_input_files]
                file_ind = uielements.user_input_list('\nChoose input files from {} to move.'.format(full_site_dir),
                                                      input_file_basenames, returntype='index')
                if file_ind is None:
                    user_ans = uielements.user_input_list('\nQuit or skip to the next site/date?',
                                                          ['Skip', 'Quit'], currentvalue='Skip')
                    if user_ans == 'Quit':
                        return
                    else:
                        continue
                else:
                    input_file = site_input_files[file_ind]

            if prefix is None:
                this_prefix = 'opus-i2s' if re.match('opus', os.path.basename(input_file)) else 'slice-i2s'
            else:
                this_prefix = prefix
            new_base_filename = '{}.{}.in'.format(this_prefix, site_date)
            new_fullname = os.path.join(save_dir, new_base_filename)
            if os.path.exists(new_fullname) and overwrite is None:
                this_ow = uielements.user_input_yn('{} exists. Overwrite?'.format(new_fullname))
            else:
                this_ow = overwrite

            if os.path.exists(new_fullname) and not this_ow:
                logger.debug('Not copying {} to {} - exists'.format(input_file, new_fullname))
            else:
                logger.info('Copying {} to {}'.format(input_file, new_fullname))
                shutil.copy(input_file, new_fullname)


def patch_i2s_run_header(header_example_file, catalogue_files, save_dir, overwrite=False,
                         last_new_header_param=runutils._default_last_header_param,
                         catalogue_start=runutils._default_last_header_param+1):
    """
    Create new i2s run files by combining a header from one file with a catalogue of slices/igrams from other files

    :param header_example_file: the file to copy the header from
    :type header_example_file: str

    :param catalogue_files: list of files to copy the catalogue of slices/igrams from
    :type catalogue_files: list(str)

    :param save_dir: directory to save the new files to
    :type save_dir: str

    :param overwrite: whether to overwrite files in the save directory
    :type overwrite: bool

    :param last_header_param: last parameter number considered to be in the header in the run files.
    :type last_header_param: int

    :return: none, writes new files
    """
    header_lines = []
    if isinstance(catalogue_start, str):
        if catalogue_start.startswith('l'):
            catalogue_by_line = True
            catalogue_start = catalogue_start[1:]
        else:
            catalogue_by_line = False

        catalogue_start = int(catalogue_start)
    elif isinstance(catalogue_start, int):
        catalogue_by_line = False
    else:
        raise TypeError('catalogue_start must be an int, a string interpretable as an int, that prepended with "l"')

    def write_catalogue_by_param(robj, wobj):
        for param_num, _, value, _ in runutils.iter_i2s_input_params(robj, include_all_lines=False):
            if param_num >= catalogue_start:
                # strip whitespace to get rid of carriage returns and homogenize new lines
                wobj.write(value.strip() + '\n')

    def write_catalogue_by_line(robj, wobj):
        for line_num, line in enumerate(robj, start=1):
            if line_num >= catalogue_start:
                # strip whitespace to get rid of carriage returns and homogenize new lines
                wobj.write(line.decode('utf8').strip() + '\n')

    # First we need to collect all of the lines before the list of slices/igrams, which will be rewritten at the
    # beginning of each new file.
    with open(header_example_file, 'rb') as fobj:
        for param_num, _, value, comment, _ in runutils.iter_i2s_input_params(fobj, include_all_lines=True):
            if param_num > last_new_header_param:
                break

            if len(comment) > 0:
                line = value + ':' + comment
            else:
                # If no comment we don't want a random colon at the end of the line
                line = value

            # strip whitespace to get rid of carriage returns and homogenize new lines
            header_lines.append(line.strip() + '\n')

    logger.debug('{} header lines read from {}'.format(len(header_lines), header_example_file))
    # Now we loop over the files that have the slices/igrams and copy those lines into new files that use the header
    # of the example file
    for cat_file in catalogue_files:
        basename = os.path.basename(cat_file)
        new_file = os.path.join(save_dir, basename)
        if not overwrite and os.path.exists(new_file):
            logger.warning('Not writing {} because it already exists'.format(new_file))
            continue
        else:
            logger.info('Writing {}'.format(new_file))

        with open(cat_file, 'rb') as robj, open(new_file, 'w') as wobj:
            wobj.writelines(header_lines)
            if catalogue_by_line:
                write_catalogue_by_line(robj, wobj)
            else:
                write_catalogue_by_param(robj, wobj)


def add_slice_info_to_i2s_run_file(run_file, new_run_file=None, start_date=None, end_date=None, start_run=None,
                                   end_run=None, slice_dir=None, scantype='Solar'):
    """
    Generate the slice_num of slice_num directories at the end of a slice_num-i2s input file.

    The range of data included depends on what start/end dates and runs are specified:

        * If no start or end date are given, then all data in the slices directory will be added.
        * If both a start and end date are given, then only data between those dates (inclusive) are added.
        * If both a start and end date are given, *and* start and/or end run numbers are given, then all data between
          start date + start run and end date + end run will be added, e.g. if the dates where 2019-01-01 to 2019-01-03
          and the runs 5 and 3, then runs >= 5 on 2019-01-01, all runs on 2019-01-02, and runs <= 3 are included.

    Start and end dates may be specified in a number of formats:

        * datetime-like (i.e. :class:`datetime.datetime`, :class:`pandas.Timestamp`, etc.)
        * A string in either YYMMDD or YYYYMMDD format.
        * A string in either of the previous formats, but including the run number after a decimal point, e.g.
          `190101.1` or `20190101.1`. *Note* that if a date is in this format and the corresponding run number is given
          as a separate argument, an error will be raised.

    Note that the date directories must currently include an :file:`IFSretr.log` file, as that is what is parsed to
    identify which slices make up a full interferogram.

    :param run_file: the run file to copy the header parameters (and comments) from.
    :type run_file: str

    :param new_run_file: the path to write the modified run file to. If not given, then the changes are written to
     ``run_file``.
    :type new_run_file: str

    :param start_date: first date to include in the list of files. See above for what happens if this is omitted and
     for allowed formats.

    :param end_date: last date (inclusive) to include in the list of files. See above for what happens if this is
     omitted and for allowed formats.

    :param start_run: first run number to include in the list of files. See above for what happens if this is omitted.
    :type start_run: int or str

    :param end_run: last run number (inclusive) to include in the list of files. See above for what happens if this is
     omitted.
    :type end_run: int or str

    :param slice_dir: directory to look for the slice_num run directories, i.e subdirectories named YYMMDD.R. Files *must*
     be in the proper organization (YYMMDD.R/scan/<slice_num files>). If this is not given, then the directory from the
     run file will be used. If that directory is relative, it will be interpreted as relative to the location of the
     run file.
    :type slice_dir: str

    :param scantype: which scan type (Solar, Cell, etc) to include in the list.
    :type scantype: str

    :return: none
    """
    start_date, end_date, start_run, end_run, slice_dir = _parse_add_slice_info_inputs(run_file=run_file, start_date=start_date,
                                                                                       end_date=end_date, start_run=start_run,
                                                                                       end_run=end_run, slice_dir=slice_dir)
    # Copy everything but the input files over to the new run file
    runutils.modify_i2s_input_params(filename=run_file, new_file=new_run_file, include_input_files=False)
    with open(new_run_file, 'a') as fobj:
        # Just make sure we start on a new line - a blank line shouldn't hurt
        fobj.write('\n')
        for date, run, slice_nums in _iter_slice_runs(slice_dir=slice_dir, start_date=start_date, end_date=end_date,
                                                      start_run=start_run, end_run=end_run, scantype=scantype):
            if len(slice_nums) == 0:
                logger.debug('{} run {}: Skipping a scan with no slices'.format(date.strftime('%Y-%m-%d'), run))
                continue
            # just get the integer values, don't need 0 padding (so no need to use strftime)
            year = date.year
            month = date.month
            day = date.day
            fobj.write('{yr} {mo} {day} {run} {slice_num}\n'.format(yr=year, mo=month, day=day, run=run,
                                                                    slice_num=slice_nums[0]))


def _parse_add_slice_info_inputs(run_file, start_date, end_date, start_run, end_run, slice_dir):
    """
    Parse inputs to :func:`add_slice_info_to_i2s_run_file`.

    Carries out all the input rules for :func:`add_slice_info_to_i2s_run_file` (i.e. choosing start/end dates/runs if
     not given). Returns the concrete values these should have.
    """
    def parse_datestr(datestr, input_name):
        if len(datestr) == 8:
            return dt.datetime.strptime(datestr, '%Y%m%d')
        elif len(datestr) == 6:
            return dt.datetime.strptime(datestr, '%y%m%d')
        else:
            raise ValueError('{} must be in YYMMDD or YYYYMMDD format'.format(input_name))

    def parse_date_input(date_in, input_name):
        if date_in is None:
            return None, None
        if not isinstance(date_in, str):
            try:
                date_in.strftime('%y%m%d')
                date_in + dt.timedelta(days=1)
            except (AttributeError, TypeError):
                raise TypeError('{} is not a string and does not have an "strftime" method or cannot be added to a '
                                'timedelta'.format(input_name))
            else:
                return date_in, None
        else:
            if '-' in date_in:
                raise ValueError('{} cannot contain dashes. Acceptable string formats are YYMMDD or YYYYMMDD.')
            ndots = date_in.count('.')
            if ndots == 1:
                date_str, run_str = date_in.split('.')
                try:
                    run_num = int(run_str)
                except ValueError:
                    raise ValueError('Cannot interpret run part of {} ({}) as an integer'.format(input_name, run_str))

            elif ndots == 0:
                date_str = date_in
                run_num = None
            else:
                raise ValueError('{} cannot have > 1 period in it'.format(input_name))

            date_in = parse_datestr(date_str, input_name)
            return date_in, run_num

    def finalize_run_num(run, run_from_date, start_or_end):
        if run is not None and run_from_date is not None:
            raise TypeError('Cannot include a run number as part of the {se}_date and give {se}_run, only one may be '
                            'given.'.format(se=start_or_end))

        if run is None:
            run = run_from_date

        if run is not None:
            try:
                run = int(run)
            except ValueError:
                raise ValueError('Cannot interpret {}_run as an integer'.format(start_or_end))

            if run < 1:
                raise ValueError('{}_run cannot be < 1'.format(start_or_end))

        return run

    if slice_dir is None:
        run_params = runutils.read_i2s_input_params(run_file)
        slice_dir = run_params[1]
        if not os.path.isabs(slice_dir):
            slice_dir = os.path.join(os.path.dirname(run_file), slice_dir)

    # Check if start/end date contain run numbers, also check their format
    start_date, run_from_start_date = parse_date_input(start_date, 'start_date')
    end_date, run_from_end_date = parse_date_input(end_date, 'end_date')
    start_run = finalize_run_num(start_run, run_from_start_date, 'start')
    end_run = finalize_run_num(end_run, run_from_end_date, 'end')

    # if any of these are None, we'll need to list all the run directories to figure out their values
    if any(v is None for v in [start_date, end_date, start_run, end_run]):
        run_dirs = glob(os.path.join(slice_dir, '*'))
        run_dates = dict()
        for full_fname in run_dirs:
            fname = os.path.basename(full_fname.rstrip(os.sep))
            if not re.match(r'\d{6}\.\d+$', fname):
                logger.debug('Skipping {} as its name cannot be parsed as a run directory'.format(full_fname))
                continue
            date_str, run_str = fname.split('.')
            key = dt.datetime.strptime(date_str, '%y%m%d')
            if key not in run_dates:
                run_dates[key] = []
            run_dates[key].append(int(run_str))

        if start_date is None:
            start_date = min(run_dates.keys())
        if end_date is None:
            end_date = max(run_dates.keys())
        
        # Limit the start/end dates to dates that actually exists
        run_dates = {k: v for k, v in run_dates.items() if start_date <= k <= end_date}
        if len(run_dates) == 0:
            raise exceptions.I2SSetupException('No data for date range {} to {}'.format(start_date.date(), end_date.date()))
        
        if start_run is None:
            start_run = min(run_dates[start_date])
        if end_run is None:
            end_run = max(run_dates[end_date])

    return start_date, end_date, start_run, end_run, slice_dir


def _iter_slice_runs(slice_dir, start_date, end_date, start_run, end_run, scantype='Solar'):
    """
    Iterate over the runs that need to be written to the bottom of a slice-type I2S input file.

    :param slice_dir: the directory containing the slice run dirs, i.e. the directories named YYMMDD.R.
    :type slice_dir: str

    :param start_date: the first date to include in the list of runs
    :type start_date: datetime-like

    :param end_date: the last date to include in the list of runs (inclusive)
    :type end_date: datetime-like

    :param start_run: the first run to include on the start date.
    :type start_run: int

    :param end_run: the last run to include on the end date (inclusive).
    :type end_run: int

    :param scantype: the type of scan to include. E.g. "Solar", "SolarInGaAs". Must match the scan type in the
     IFSretr.log file.
    :type scantype: str

    :return: iterator that yields the date, run number, and list of slices that make up each scan. The list of slices
     will be in order, and contain the slice number (i.e. for a slice "b123456789.0" the "123456789") as a string.
    :rtype: :class:`datetime.datetime`, int, list(str)
    """
    curr_date = start_date
    while curr_date <= end_date:
        gave_warning = False
        for run in range(start_run, end_run+1):
            curr_dir = os.path.join(slice_dir, '{}.{}'.format(curr_date.strftime('%y%m%d'), run))
            ifs_log_file = os.path.join(curr_dir, 'IFSretr.log')
            if not os.path.exists(ifs_log_file):
                if not gave_warning:
                    logger.warning('Cannot add data from {}, no IFSretr.log file'.format(curr_dir))
                    gave_warning = True
                continue
            with open(ifs_log_file, 'r') as robj:
                in_scan = False
                for line in robj:
                    status = line.split(':')[3:]
                    # the lines have the form "Mon Apr 23 17:37:59 2018: Solar". We want everything after the
                    # third colon as the status, so we get that, but have to paste it back together if there
                    # were colons in the status itself as in "Mon Apr 23 17:38:40 2018: Retrieving b3621010.0: 1 sec"
                    status = ':'.join(status).strip()
                    if in_scan:
                        if 'Request Completed' == status:
                            in_scan = False
                            yield curr_date, run, slice_num_list
                        else:
                            slice_num = re.search(r'(?<=b)\d+(?=\.0)', status)
                            if slice_num is None:
                                logger.warning('Run directory {}: Line inside a scan does not include a slice number! Line was: {}'
                                               .format(curr_dir, line))
                                # something odd happened in this run. We do not want to include it in the list, so reset
                                # in_scan without yielding
                                in_scan = False
                                continue
                            slice_num = slice_num.group()
                            slice_num_list.append(slice_num)
                    else:
                        if scantype == status:
                            in_scan = True
                            slice_num_list = []
        curr_date += dt.timedelta(days=1)


def link_i2s_input_files(cfg_file, overwrite=False, clean_links=False, clean_spectra=False, ignore_missing_igms=False,
                         create_runscript=True):
    """
    Link all the input interferograms/slices and the required input files for I2S into the batch run directory

    :param cfg_file: the path to the config file to use that specifies where the input data may be found
    :type cfg_file: str

    :param overwrite: whether or not to overwrite existing symbolic links
    :type overwrite: bool

    :param clean_links: whether or not to delete the existing interferograms or slices directory to make way for new
     links.
    :type clean_links: bool

    :param clean_spectra: whether or not to delete the existing spectra output directory to make way for new spectra.
    :type clean_spectra: bool

    :param ignore_missing_igms: if OPUS files listed in the run file are missing, the default behavior is to raise an
     error and stop executing. Set this parameter to ``True`` to ignore that error and continue linking other days.
     Any day that is missing files will not have any files linked. Currently, this has no effect on sites that provide
     slices.
    :type ignore_missing_igms: bool

    :return: none, links files at the paths specified in the config
    """
    cfg = load_config_file(cfg_file)
    # Create a directory structure: SiteName/xxYYYYMMDD/<igms or slices> and link the individual igms or slice YYMMDD.R
    # directories. If using igms, then the igm files go directly in the igms directory. If using slices, then the
    # structure under slices must be "YYMMDD.R/scan/b*".
    #
    # If the slices aren't already in this structure, then we need to create it. We'll have to parse the input file to
    # figure out which slice numbers go with with run.

    for sect in cfg['Sites'].sections:
        sect_cfg = cfg['Sites'][sect]
        logger.info('Linking files for {}'.format(sect))
        for datesect in sect_cfg.sections:
            uses_slices = get_date_cfg_option(sect_cfg, datestr=datesect, optname='slices')

            if not uses_slices:
                logger.debug('Linking full igrams for {}'.format(datesect))
                _link_igms(cfg=cfg, site=sect, datestr=datesect, i2s_opts=cfg['I2S'], overwrite=overwrite,
                           clean_links=clean_links, clean_spectra=clean_spectra, ignore_missing=ignore_missing_igms)
            else:
                logger.debug('Linking slices for {}'.format(datesect))
                _link_slices(cfg=cfg, site=sect, datestr=datesect, i2s_opts=cfg['I2S'], overwrite=overwrite,
                             clean_links=clean_links, clean_spectra=clean_spectra)


    if create_runscript:
        run_file = os.path.join(cfg['Run']['run_top_dir'], 'multii2s.sh')
        create_i2s_parallel_run_file(cfg_file, run_file)


def _link_igms(cfg, site, datestr, i2s_opts, overwrite, clean_links, clean_spectra, ignore_missing):
    """
    Link full interferogram files to the appropriate site/date run directory; set up the flimit and opus-i2s.in files

    :param cfg: the configuration object
    :type cfg: :class:`configobj.Section`

    :param site: the two-letter site abbreviation; i.e. the subsection in cfg['Sites'] representing this site
    :type site: str

    :param datestr: the subsection key for this particular date. Must be "xxYYYYMMDD", cannot just be the date.
    :type datestr: str

    :param i2s_opts: section with config-specified options for the i2s input files.
    :type i2s_opts: :class:`configobj.Section` or dict

    :param overwrite: whether or not to overwrite existing symbolic links
    :type overwrite: bool

    :param clean_links: whether or not to delete the existing interferograms or slices directory to make way for new
     links.
    :type clean_links: bool

    :param clean_spectra: whether or not to delete the existing spectra output directory to make way for new spectra.
    :type clean_spectra: bool

    :return: none
    """
    if not re.match(r'[a-z]{2}\d{8}', datestr):
        raise ValueError('datestr must have the format xxYYYYMMDD')

    igms_dir, i2s_input_file, src_igm_dir = _link_common(cfg=cfg, site=site, datestr=datestr, i2s_opts=i2s_opts,
                                                         link_subdir='igms', input_file_basename='opus-i2s.in',
                                                         clean_links=clean_links, clean_spectra=clean_spectra)

    # Read the input file and link all the listed files into the igms directory.  Delay linking until we're sure that
    # all required igrams are present.
    _, run_lines = runutils.read_i2s_input_params(i2s_input_file)
    link_dict = dict()
    files_missing = False
    for i, run_dict in enumerate(run_lines, start=1):
        runf = run_dict['opus_file']
        src_file = os.path.join(src_igm_dir, runf)
        if not os.path.isfile(src_file):
            files_missing = True
            msg = 'Expected source file {src} (run line #{lnum} in {infile}) does not exist'.format(
                src=src_file, lnum=i, infile=i2s_input_file
            )
            if ignore_missing:
                logger.info(msg)
            else:
                raise exceptions.I2SDataException(msg)
        else:
            _make_link(src_file, os.path.join(igms_dir, runf), overwrite=overwrite)

    if files_missing:
        logger.warning('{} had 1 or more igrams missing. It will probably not run for I2S.'.format(datestr))


def _link_slices(cfg, site, datestr, i2s_opts, overwrite=False, clean_links=False, clean_spectra=False):
    """
    Link interferogram slices into the appropriate site/date run directory, set up the flimit and slice-i2s.in files

    This function also handles the case where the slice interferograms are not properly organized into YYMMDD.R/scan
    folders and does that organization if needed.

    :param cfg: the configuration object
    :type cfg: :class:`configobj.Section`

    :param site: the two-letter site abbreviation; i.e. the subsection in cfg['Sites'] representing this site
    :type site: str

    :param datestr: the subsection key for this particular date. Must be "xxYYYYMMDD", cannot just be the date.
    :type datestr: str

    :param i2s_opts: section with config-specified options for the i2s input files.
    :type i2s_opts: :class:`configobj.Section` or dict

    :param overwrite: whether or not to overwrite existing symbolic links
    :type overwrite: bool

    :param clean_links: whether or not to delete the existing interferograms or slices directory to make way for new
     links.
    :type clean_links: bool

    :param clean_spectra: whether or not to delete the existing spectra output directory to make way for new spectra.
    :type clean_spectra: bool

    :return: none
    """
    site_cfg = cfg['Sites'][site]
    igms_dir, i2s_input_file, src_igm_dir = _link_common(cfg=cfg, site=site, datestr=datestr, i2s_opts=i2s_opts,
                                                         link_subdir='slices', input_file_basename='slice-i2s.in',
                                                         overwrite=overwrite,
                                                         clean_links=clean_links, clean_spectra=clean_spectra)

    slices_need_org = get_date_cfg_option(site_cfg, datestr, 'slices_in_subdir')
    if slices_need_org:
        slice_files = sorted(glob(os.path.join(src_igm_dir, 'b*')))

    # If the slices need organized, then we'll have to link individual slice files into the right directories. If not,
    # we can just link the preexisting directories
    _, run_lines = runutils.read_i2s_input_params(i2s_input_file)
    last_run_date = None
    last_run_num = None
    for idx, line in enumerate(run_lines):
        run_date = dt.datetime(int(line['year']), int(line['month']), int(line['day']))
        slice_run_dir = runutils.slice_date_subdir(run_date, line['run'])
        if not slices_need_org:
            if run_date != last_run_date or line['run'] != last_run_num:
                # This check avoids lots of debug messages about not overwriting an existing symlink because typically
                # all or most of the lines in a run file will be for the same day (in target obs) and even outside
                # target obs. there will be lots with the same date

                # not entirely sure the difference using target_is_directory
                _make_link(os.path.join(src_igm_dir, slice_run_dir), os.path.join(igms_dir, slice_run_dir),
                           overwrite=overwrite, target_is_directory=True)
        else:
            logger.debug('Setting up correct directory structure for linked slices')
            _link_slices_needs_org(slice_files=slice_files, run_lines=run_lines, run_lines_index=idx,
                                   dest_run_dir=os.path.join(igms_dir, slice_run_dir), overwrite=overwrite)

        last_run_date = run_date
        last_run_num = line['run']


def _link_slices_needs_org(slice_files, run_lines, run_lines_index, dest_run_dir, overwrite):
    """
    Helper function for :func:`_link_slices` that organizes slice links into the proper YYMMDD.R/scan directories

    :param slice_files: the list of slice files to organize. Note: MUST be sorted alphanumerically, so that slice
     numbers are in sequence.
    :type slice_files: list(str)

    :param run_lines: the list of run line dictionaries read in from the I2S input file.
    :type run_lines: list(dict)

    :param run_lines_index: the index of the current run line to be linked.
    :type run_lines_index: int

    :param dest_run_dir: the YYMMDD.R run directory that :file:`scan` and the links to the slice files should be created
     in.
    :type dest_run_dir: str

    :param overwrite: whether or not to overwrite existing symbolic links
    :type overwrite: bool

    :return: none
    """
    scans_dir = os.path.join(dest_run_dir, 'scan')
    start_slice_num = int(run_lines[run_lines_index]['slice'])
    try:
        end_slice_num = int(run_lines[run_lines_index+1]['slice'])
    except IndexError:
        # last line of run_lines - no next slice for that day
        end_slice_num = None

    if not os.path.exists(scans_dir):
        os.makedirs(scans_dir)

    for slicef in slice_files:
        slice_num = int(re.search(r'\d+', os.path.basename(slicef)).group())
        if slice_num < start_slice_num:
            continue
        elif end_slice_num is not None and slice_num >= end_slice_num:
            return
        else:
            _make_link(slicef, os.path.join(scans_dir, os.path.basename(slicef)), overwrite=overwrite)


def _link_common(cfg, site, datestr, i2s_opts, link_subdir, input_file_basename, overwrite=False,
                 clean_links=False, clean_spectra=False):
    """
    Helper function that handles the common steps for linking full interferograms or slices

    :param cfg: the configuration object
    :type cfg: :class:`configobj.Section`

    :param site: the two-letter site abbreviation; i.e. the subsection in cfg['Sites'] representing this site
    :type site: str

    :param datestr: the subsection key for this particular date. Must be "xxYYYYMMDD", cannot just be the date.
    :type datestr: str

    :param run_top_dir: the top directory that all the sites' run dirs should be written to
    :type run_top_dir: str

    :param i2s_opts: section with config-specified options for the i2s input files.
    :type i2s_opts: :class:`configobj.Section` or dict

    :param link_subdir: the subdirectory under the date directory where the interferograms or slice directories will
     go. Usually "igms" or "slices".
    :type link_subdir: str

    :param input_file_basename: the name to give the I2S .in file. Usually either "opus-i2s.in" or "slice-i2s.in".
    :type input_file_basename: str

    :param clean_links: whether or not to delete the existing interferograms or slices directory to make way for new
     links.
    :type clean_links: bool

    :param clean_spectra: whether or not to delete the existing spectra output directory to make way for new spectra.
    :type clean_spectra: bool

    :return: the directory where the interferograms/slice directories should be linked, the path to the I2S input file
     created in the run directory, and the directory where the interferograms/slice directories can be linked from.
    :rtype: str, str, str
    """
    site_cfg = cfg['Sites'][site]
    site_root_dir = get_date_cfg_option(site_cfg, datestr=datestr, optname='site_root_dir')
    no_date_dir = get_date_cfg_option(site_cfg, datestr=datestr, optname='no_date_dir')
    site_subdir = get_date_cfg_option(site_cfg, datestr=datestr, optname='subdir')
    i2s_input_file = get_date_cfg_option(site_cfg, datestr=datestr, optname='i2s_input_file')
    # convert configobj.Section to dictionary and make a copy at the same time
    i2s_opts = {int(k): v for k, v in i2s_opts.items()}

    if no_date_dir:
        src_igm_dir = os.path.abspath(os.path.join(site_root_dir, site_subdir))
    else:
        src_igm_dir = os.path.abspath(os.path.join(site_root_dir, datestr, site_subdir))
    date_dir = date_subdir(cfg, site, datestr)
    igms_dir = os.path.join(date_dir, link_subdir)
    if clean_links and os.path.exists(igms_dir):
        logger.info('Removing existing igrams directory: {}'.format(igms_dir))
        shutil.rmtree(igms_dir)
    if not os.path.exists(igms_dir):
        os.makedirs(igms_dir)

    # Link the flimit file into the date dir with a consistent name to make setting up the input file easier
    src_flimit = get_date_cfg_option(site_cfg, datestr=datestr, optname='flimit_file')
    src_flimit = os.path.abspath(src_flimit)
    _make_link(src_flimit, os.path.join(date_dir, 'flimit.i2s'), overwrite=overwrite)

    # Make an output spectra directory
    spectra_dir = os.path.join(date_dir, 'spectra')
    if os.path.exists(spectra_dir) and clean_spectra:
        logger.info('Removing existing spectra directory: {}'.format(spectra_dir))
        shutil.rmtree(spectra_dir)
    if not os.path.exists(spectra_dir):
        os.makedirs(spectra_dir)

    # Copy the i2s input file into the date dir, setting the input (#1), output (#2), and flimit (#8) parameters to the
    # directories/files we just linked. Also turn off saving separated interferograms (#3), unless overridded by the
    # config opts. Note that i2s will be run from the date dirs so these paths can be relative to that directory.
    std_i2s_opts = {1: './{}/'.format(link_subdir), 2: './spectra/', 3: '0', 8: './flimit.i2s'}
    std_i2s_opts.update(i2s_opts)
    new_i2s_input_file = os.path.join(date_dir, input_file_basename)
    runutils.modify_i2s_input_params(i2s_input_file, std_i2s_opts, new_file=new_i2s_input_file)

    return igms_dir, new_i2s_input_file, src_igm_dir


def _make_link(src, dst, overwrite=False, **kwargs):
    """
    Make a single symbolic link, with extra checks if the link already exists.

    :param src: the file that the link should point to.
    :type src: str

    :param dst: the link name
    :type dst: str

    :param overwrite: whether to replace an existing file to make the link. Use with care!
    :type overwrite: bool

    :param kwargs: additional keyword arguments to :func:`os.symlink`

    :return: none
    """
    # need to use lexists, not exists because the latter will return False if dst is a broken symlink
    if os.path.lexists(dst):
        if overwrite:
            logger.debug('Overwriting existing symlink: {}'.format(dst))
            os.remove(dst)
        else:
            logger.debug('Symlink exists, not overwriting: {}'.format(dst))
            return

    os.symlink(src, dst, **kwargs)


def _group_uses_slices(site_dict, relative_to=None):
    """
    Guess whether a site uses slices or full interferograms

    :param site_dict: a dictionary for one site that has all the input files for that site as values.
    :type site_dict: dict

    :return: the config options for "slices" and "subdir": "1" and "slices" if more than half of the days use slices or
     "0" and "igms" if not.
    :rtype: str, str
    """
    n_using_slices = 0
    for input_file in site_dict.values():
        if relative_to is not None:
            input_file = os.path.join(relative_to, input_file)
        n_using_slices += runutils.i2s_use_slices(input_file)

    if n_using_slices > 0.5 * len(site_dict):
        return '1', 'slices'
    else:
        return '0', 'igms'


def _igm_subdir(run_dir):
    return os.path.join(run_dir, 'igms')


def _slices_subdir(run_dir):
    return os.path.join(run_dir, 'slices')


def _group_i2s_input_files(input_files):
    """
    Group I2S input files by site

    :param input_files: a list of paths to all I2S input files
    :type input_files: list(str)

    :return: a two-level dictionary where the first level's keys are the site abbreviations and the second level's are
     the xxYYYY, xxYYYYMM, or xxYYYYMMDD date strings.
    :rtype: dict
    """
    group_dict = dict()
    for f in input_files:
        fname = os.path.basename(f)
        site_date = re.search(r'(?P<site>\w\w)(?P<date>\d{4,8})', fname)
        if site_date is None:
            raise ValueError('{} does not contain a site abbreviation + date string in its name'.format(f))

        site = site_date.group('site')
        site_date = site_date.group()

        if site not in group_dict:
            group_dict[site] = dict()
        group_dict[site][site_date] = f

    return group_dict


def check_i2s_links(cfg_file, dump_level=1):
    """
    Verify interferogram or slice links

    :param cfg_file: the path to the config file to use that specifies where the input data may be found
    :type cfg_file: str

    :param dump_level: how much information to print to the screen. 0 = nothing, only the return value will indicate if
     any links are missing (0 = no, >0 = yes). 1 = print number of dates that each site has missing igrams. 2 = print
     number of missing igrams for each date. 3 = print exactly which igrams are missing.
    :type dump_level: int

    :return: status code, 0 means all igrams/slices found, >0 means some are missing.
    :rtype: int
    """

    cfg = load_config_file(cfg_file)
    return_code = 0
    for site, site_sect in cfg['Sites'].items():
        n_days_missing = 0
        n_days = 0
        for datestr in site_sect.sections:
            n_days += 1
            run_dir = date_subdir(cfg, site, datestr)
            run_file = os.path.join(run_dir, 'slice-i2s.in')
            if not os.path.exists(run_file):
                run_file = os.path.join(run_dir, 'opus-i2s.in')
            if not os.path.exists(run_file):
                raise RuntimeError('No I2S run file found in {}'.format(run_dir))

            try:
                if runutils.i2s_use_slices(run_file):
                    missing = _check_slice_links(run_directory=run_dir, run_file=run_file)
                else:
                    missing = _check_opus_links(run_directory=run_dir, run_file=run_file)
            except exceptions.I2SFormatException as err:
                if 'cannot tell' in err.args[0]:
                    # Can't tell if a file uses slices or full igrams if there's no files listed,
                    # but if there's none listed, then there can't be any missing.
                    missing = []
                else:
                    # Other errors should be raised as normal
                    raise

            if len(missing) > 0:
                n_days_missing += 1
                return_code = 1
                if dump_level == 0:
                    return 1
            if dump_level >= 2:
                print('{}: {} missing'.format(datestr, len(missing)))
            if dump_level >= 3 and len(missing) > 0:
                print('  * ' + '\n  * '.join(missing))

        if dump_level >= 1:
            print('{}: {}/{} dates missing at least 1 igram/slice'.format(site, n_days_missing, n_days))
            if dump_level > 1:
                print('')

    return return_code


def _check_opus_links(run_directory, run_file):
    _, igram_files = runutils.read_i2s_input_params(run_file)
    missing_igms = []
    for igm in igram_files:
        igm_path = igm['opus_file']
        if not os.path.isabs(igm_path):
            igm_path = os.path.join(_igm_subdir(run_directory), igm_path)
        igm_basefile = os.path.basename(igm_path)

        # Read the link and check if the file pointed to exists - do this rather than rely on os.path.exists returning
        # False if the target of a link doesn't exist b/c the documentation of os.path.lexists suggests that exists()
        # might behave differently if os.lstat() is not available
        try:
            igm_link = os.readlink(igm_path)
        except FileNotFoundError:
            missing_igms.append(igm_basefile)
        else:
            if not os.path.exists(igm_link):
                missing_igms.append(igm_basefile)

    return missing_igms


def _check_slice_links(run_directory, run_file):
    _, slices = runutils.read_i2s_input_params(run_file)
    slice_dir = _slices_subdir(run_directory)
    missing_slices = []

    # We won't be able to check that every slice is present easily because I have found no guarantee that the number
    # of slices is always the same, nor can we rely on the last slice number of the current scan being the one before
    # the next scan b/c we might have to skip some scans. So for now, this will be a very naive check that just
    # verifies that the .info and slice file for the start of the run are present.

    for slc in slices:
        slice_date = runutils.slice_line_date(slc)
        date_subdir = runutils.slice_date_subdir(slice_date, slc['run'])
        full_slice_dir = os.path.join(slice_dir, date_subdir, 'scan')
        slice_num = slc['slice']
        for f in ('b{}.0', 'b{}.0.info', 'b{}.1.info'):
            f = f.format(slice_num)
            if not os.path.exists(os.path.join(full_slice_dir, f)):
                missing_slices.append(slice_num)
                break

    return missing_slices


def _find_input_file(run_dir):
    """
    Find the I2S input file in a directory

    :param run_dir: directory to find an I2S input file in. Must contain a single file matching the pattern
     ``*i2s*.in``.
    :type run_dir: path-like

    :return: basename of the input file
    :rtype: str
    """
    possible_input_files = [os.path.basename(f) for f in glob(os.path.join(run_dir, '*i2s*.in'))]
    if len(possible_input_files) > 1:
        raise RuntimeError('Multiple I2S input files found in {}: {}'.format(
            run_dir, ', '.join(possible_input_files))
        )
    elif len(possible_input_files) == 0:
        raise RuntimeError('No I2S input files found in {}'.format(run_dir))
    else:
        return possible_input_files[0]


def create_i2s_parallel_run_file(cfg_file, run_file, abspaths=False):
    """
    Create file that can be used to run I2S with the GNU parallel utility

    :param cfg_file: path to the i2srun configuration file that determines where to run I2S
    :type cfg_file: path-like

    :param run_file: path to write the run file for GNU parallel
    :type run_file: path-like

    :param abspaths: if ``False``, makes the paths in the run file relative to the run file's location. Otherwise, they
     are absolute.
    :type abspaths: bool
    """
    cfg = load_config_file(cfg_file)

    gggpath = os.getenv('GGGPATH')
    if gggpath is None:
        raise exceptions.GGGPathException('GGGPATH is not set. It must be set to use gggrun.')
    i2s_cmd = os.path.join(gggpath, 'bin', 'i2s')
    if not os.path.exists(i2s_cmd):
        raise exceptions.GGGPathException('{} is not valid path. Please confirm your GGGPATH variable points to a '
                                          'valid install of GGG.'.format(i2s_cmd))

    run_file_dir = os.path.dirname(run_file)
    with open(run_file, 'w') as wobj:
        for site in cfg['Sites'].sections:
            site_cfg = cfg['Sites'][site]
            for datestr in site_cfg.sections:
                this_run_dir = date_subdir(cfg, site, datestr)
                if abspaths:
                    par_run_dir = os.path.abspath(this_run_dir)
                else:
                    par_run_dir = os.path.relpath(this_run_dir, run_file_dir)
                input_file = _find_input_file(this_run_dir)
                wobj.write('cd {rundir} && {i2s} {infile} > i2s.log\n'.format(
                    rundir=par_run_dir, i2s=i2s_cmd, infile=input_file
                ))


def run_all_i2s(cfg_file, n_procs=1, dry_run=False):
    """
    Run I2S for all days specified in a config file

    :param cfg_file: the path to the config file
    :type cfg_file: str

    :param n_procs: the number of processors to use. If >1, a multiprocssing pool is spawned; if <= 1, execution occurs
     in serial.
    :type n_procs: int

    :return: none
    """
    cfg = load_config_file(cfg_file)
    # remove the halt file at the beginning rather than the end so that if this was launched twice the halt file is
    # kept until everything ends.
    remove_i2s_halt_file()

    gggpath = os.getenv('GGGPATH')
    if gggpath is None:
        raise exceptions.GGGPathException('GGGPATH is not set. It must be set to use gggrun.')
    i2s_cmd = os.path.join(gggpath, 'bin', 'i2s')
    if not os.path.exists(i2s_cmd):
        raise exceptions.GGGPathException('{} is not valid path. Please confirm your GGGPATH variable points to a '
                                          'valid install of GGG.'.format(i2s_cmd))
    logger.debug('Will run I2S from {}'.format(i2s_cmd))
    pool_args = []
    for site in cfg['Sites'].sections:
        site_cfg = cfg['Sites'][site]
        for datestr in site_cfg.sections:
            this_run_dir = date_subdir(cfg, site, datestr)
            if n_procs > 1:
                pool_args.append((this_run_dir, i2s_cmd, dry_run))
            else:
                # I like to avoid opening a pool if only running with one processor because it's easier to debug; you
                # can actually step into the run function.
                _run_one_i2s(this_run_dir, i2s_cmd, dry_run)

    if len(pool_args) > 0:
        with Pool(processes=n_procs) as pool:
            pool.starmap(_run_one_i2s, pool_args)


def _run_one_i2s(run_dir, i2s_cmd, dry_run):
    """
    Run I2S for one day's interferograms

    :param run_dir: the directory to execute I2S in
    :type run_dir: str

    :param i2s_cmd: the command to use to execute I2S
    :type i2s_cmd: str

    :return: none
    """
    if _should_i2s_stop():
        logger.debug('I2S halt file exists. Aborting run in {}'.format(run_dir))
        return

    old_spectra = _list_completed_spectra(run_dir)
    i2s_input_file = _find_input_file(run_dir)

    now = dt.datetime.now()
    log_file = os.path.join(run_dir, 'run_i2s_{}.log'.format(now.strftime('%Y%m%dT%H%M%S')))
    logger.info('Starting I2S ({cmd}) in {rundir} using {infile} as input file. I2S output piped to {log}.'
                .format(cmd=i2s_cmd, rundir=run_dir, infile=i2s_input_file, log=log_file))
    if dry_run:
        return

    with open(log_file, 'w') as log:
        p = Popen([i2s_cmd, i2s_input_file], stdout=log, stderr=log, cwd=run_dir)
        p.wait()

    current_spectra = _list_completed_spectra(run_dir)
    _compare_completed_spectra(old_spectra, current_spectra, run_dir)


def _list_completed_spectra(run_dir):
    """
    Create a dictionary listing the times each spectra file under a run directory was modified.

    :param run_dir: the top run directory (must contain subdirectory "spectra" that the spectra are actually written in)
    :type run_dir: str

    :return: a dictionary with the spectra file names (base names only) as keys and the modification times (Unix
     timestamps) as values.
    :rtype: dict
    """
    spectra = dict()
    spectra_files = glob(os.path.join(run_dir, 'spectra', '*'))
    for f in spectra_files:
        spectra[os.path.basename(f)] = os.path.getmtime(f)
    return spectra


def _compare_completed_spectra(old_dict, new_dict, run_dir):
    """
    Create a list of spectra files that have been modified or created.

    :param old_dict: dictionary returned by :func:`_list_completed_spectra` before running I2S.
    :type old_dict: dict

    :param new_dict: dictionary returned by :func:`_list_completed_spectra` after running I2S.
    :type new_dict: dict

    :param run_dir: run directory containing the "spectra" directory in question.
    :type run_dir: str
    :return:
    """
    new_files = []
    for fname, mtime in new_dict.items():
        if fname not in old_dict or mtime > old_dict[fname]:
            new_files.append(fname)
    logger.info('{} new spectra created in {}'.format(len(new_files), run_dir))
    logger.debug('New spectra are: {}'.format(', '.join(new_files)))
    return new_files


def plot_rough_spectra(cfg_file, save_dir, plots_per_page=4, overwrite=True):
    """
    Make a rough plot of all completed spectra

    :param cfg_file: I2S bulk run config file, will iterate through the run directories specified in it
    :type cfg_file: str

    :param save_dir: where to save the .pdfs of the spectra
    :type save_dir: str

    :param plots_per_page: number of plots to include on a single

    :param overwrite: whether or not to overwrite existing plot file.

    :return: none
    """
    from matplotlib import pyplot as plt
    from matplotlib.backends.backend_pdf import PdfPages

    cfg = load_config_file(cfg_file)
    for run_dir, datestr in iter_i2s_dirs(cfg, incl_datestr=True):
        pdf_name = '{}_spectra.pdf'.format(datestr)
        pdf_name = os.path.join(save_dir, pdf_name)
        if not overwrite and os.path.exists(pdf_name):
            logger.info('Skipping {} because file already exists'.format(datestr))
            continue
        with PdfPages(pdf_name) as pdf:
            logger.info('Working on site "{}"'.format(datestr))

            spectra_dir = os.path.join(run_dir, 'spectra')
            spectra_files = sorted(glob(os.path.join(spectra_dir, '*')))
            nspec = len(spectra_files)
            ispec = 0
            while ispec < nspec:
                logger.debug('Plotting {} ({}/{})'.format(spectra_files[ispec], ispec+1, nspec))
                iplot = ispec % plots_per_page
                if iplot == 0:
                    nplots = min(plots_per_page, nspec - ispec)
                    fig, ax = plt.subplots(nplots, 1, figsize=(16, 4*nplots))
                    if nplots == 1:
                        # if only only subplot then ax will not be an array so indexing below
                        # will not work
                        ax = [ax]
                spectra = igram_analysis.read_spectrum_raw(spectra_files[ispec])
                ax[iplot].plot(spectra[600:])
                ax[iplot].set_title(os.path.basename(spectra_files[ispec]))
                if iplot == (nplots - 1):
                    ax[iplot].set_xlabel('Arbitrary index')
                    fig.suptitle(datestr)
                    plt.tight_layout()
                    plt.subplots_adjust(top=0.85)
                    pdf.savefig(fig)
                    plt.close(fig)
                ispec += 1


def make_i2s_halt_file():
    """
    Create the file signaling batch I2S to stop
    :return: none
    """
    msg = 'Requested to abort further I2S runs at {}\n'.format(dt.datetime.now())
    # open in append as an extract guarantee that this file will always exist, so if it currently exists, it won't be
    # truncated
    with open(_i2s_halt_file, 'a') as wobj:
        wobj.write(msg)


def remove_i2s_halt_file():
    """
    Remove the file signaling batch I2S to stop
    :return: none
    """
    logger.debug('Removing I2S halt file ({}) if exists'.format(_i2s_halt_file))
    if os.path.isfile(_i2s_halt_file):
        os.remove(_i2s_halt_file)


def _should_i2s_stop():
    """
    Return ``True`` if the batch I2S run should stop gracefully
    :rtype: bool
    """
    return os.path.exists(_i2s_halt_file)


def _cl_mod_runfile_driver(parameters, run_files, save_dir=None, backup_orig=None, infile_action=tuple()):
    """
    Command line driver to modify a set of parameters in a bunch of I2S run files

    :param parameters: a list of the parameters to change, alternating number and value (both as strings)
    :type parameters: list(str)

    :param run_files: a list of the run files to alter
    :type run_files: list(str)

    :param save_dir: a directory to save the new files to. If omitted, then the original files are overwritten.
    :type save_dir: str

    :param backup_orig: make a backup of the original file. Default is to make a backup if overwriting the original file
     and not to if copying. Set to ``False`` to never backup and ``True`` to always backup. Backups will be in the same
     directory as the original with the ".orig" suffix. Old backups WILL be overwritten.
    :type backup_orig: bool

    :return:
    """

    if backup_orig is None:
        backup_orig = save_dir is None

    for idx, param in enumerate(parameters):
        if idx % 2 == 1:
            continue

        try:
            parameters[idx] = int(param)
        except ValueError:
            print('ERROR: The parameter number in position {pos} ({val}) cannot be interpreted as an integer.'
                  .format(pos=idx+1, val=param), file=sys.stderr)
            sys.exit(1)

    # Convert the list of input file actions into a dict that we can expand into keywords.
    infile_action_dict = dict()
    for action in infile_action:
        if ':' in action:
            key, value = action.split(':')
            infile_action_dict[key] = value
        else:
            infile_action_dict[action] = True

    for rfile in run_files:
        if backup_orig:
            shutil.copy2(rfile, rfile + '.orig')

        if save_dir is None:
            new_file = None
        else:
            basename = os.path.basename(rfile)
            new_file = os.path.join(save_dir, basename)

        runutils.modify_i2s_input_params(rfile, *parameters, new_file=new_file, **infile_action_dict)


def parse_mod_run_files_args(parser):
    """

    :param parser: :class:`argparse.ArgumentParser`
    :return:
    """
    parser.add_argument('-p', '--parameters', nargs='*', default=[],
                        help='Parameters to change in the I2S run files. Must have an even number of arguments, '
                             'alternating parameter number and new value. Example: "-p 1 ./igms 2 ./spectra" would '
                             'change parameter #1 to "./igms" and #2 to "./spectra".')
    parser.add_argument('-f', '--run-files', nargs='+',
                        help='Run files to change. At least one must be given.')
    parser.add_argument('-s', '--save-dir', help='Directory to save the modified run files to')
    parser.add_argument('-b', '--backup', action='store_true', dest='backup_orig', default=None,
                        help='Always make a backup of the original file. The standard behavior is to only back up if '
                             'the original would be overwritten (i.e. --save-dir not specified).')
    parser.add_argument('-n', '--no-backup', action='store_false', dest='backup_orig', default=None,
                        help='Never make a backup of the original file.')
    parser.add_argument('-i', '--infile-action', action='append', default=[],
                        help='Actions to take on the list of input files at the bottom of the run files. Values have '
                             'the form ACTION[:VALUE]. Allowed actions are: "chdir" change the directory of the opus '
                             'input files (has no effect on a slice run file). "chdir" by itself just removes the '
                             'leading directories; giving a value (e.g. "chdir:/home/tccon/") replaces the leading '
                             'directories with the given path.')
    parser.set_defaults(driver_fxn=_cl_mod_runfile_driver)


def parse_header_catalog_args(parser):
    parser.description = 'Create a header (general I2S options) and scan catalog file from multiple I2S input files'
    parser.add_argument('header_file', help='Path to write the header file to.')
    parser.add_argument('scan_list_file', help='Path to write the catalog of scans to.')
    parser.add_argument('i2s_files', nargs='+', help='The I2S input files to merge.')
    parser.set_defaults(driver_fxn=create_header_and_full_scan_list)


def parse_build_cfg_args(parser):
    parser.description = 'Construct the starting config file for running I2S in bulk'
    parser.add_argument('cfg_file', help='The name to give the new config file')
    parser.add_argument('i2s_input_files', nargs='+', help='All the I2S input files to create I2S runs for. Note that '
                                                           'these files MUST include xxYYYYMMDD in the name, where xx '
                                                           'is the site abbreviation and YYYYMMDD the year/month/day.')
    parser.add_argument('-c', '--old-cfg-file', default=None,
                        help='Previous configuration file to merge with the new one; any options in the old one will '
                             'have their values inserted in the new config file.')

    parser.set_defaults(driver_fxn=build_cfg_file)


def parse_build_cfg_many_args(parser):
    parser.description = 'Construct the starting config file for running I2S in parallel from multiple I2S input files'
    parser.add_argument('site_id', help='The two letter site ID of the site that these I2S input files are for')
    parser.add_argument('output_dir', help='Path to the directory to output the config file and generated I2S '
                                           'input files.')
    parser.add_argument('i2s_input_files', nargs='+', help='The original I2S input files to merge and re-split')
    parser.add_argument('-s', '--split-by', default='D', choices=('D', 'M', 'Y'),
                        help='How to split up the I2S runs for parallelization. D = daily, M = monthly, Y = yearly. '
                             'Default is %(default)s.')
    grp = parser.add_mutually_exclusive_group()
    grp.add_argument('--is-slices', action='store_const', const=True, default=None,
                     help='Indicates that the interferograms are slices. If not specified, then i2srun will '
                          'try to infer this from the first I2S input file.')
    grp.add_argument('--is-opus', action='store_const', const=False, dest='is_slices',
                     help='Indicates that the interferograms are full Opus interferograms. If not specified, then '
                          'i2srun will try to infer this from the first I2S input file.')
    parser.epilog = 'This will take 1 or more original I2S input files, extract the scan catalogs from them, and ' \
                    'create new I2S input files organized by day/month/year and a base config file to control ' \
                    'running them in parallel. The general I2S options (everything before the scan catalog) are ' \
                    'taken from the first input file.'

    parser.set_defaults(driver_fxn=build_cfg_from_many_inputs)


def parse_build_cfg_header_catalog_args(parser):
    parser.description = 'Build an i2srun config file from a header and catalog file'
    parser.add_argument('site_id', help='The two letter site ID of the site that these I2S input files are for')
    parser.add_argument('output_dir', help='Path to the directory to output the config file and generated I2S '
                                           'input files.')
    parser.add_argument('header_file', help='The path to the header file with the general I2S options (before the '
                                            'scan catalog).')
    parser.add_argument('scan_list', help='The path to the scan catalog file.')
    parser.add_argument('-s', '--split-by', default='D', choices=('D', 'M', 'Y'),
                        help='How to split up the I2S runs for parallelization. D = daily, M = monthly, Y = yearly. '
                             'Default is %(default)s')
    grp = parser.add_mutually_exclusive_group()
    grp.add_argument('--is-slices', action='store_const', const=True, default=None,
                     help='Indicates that the interferograms are slices. If not specified, then i2srun will '
                          'try to infer this from the first I2S input file.')
    grp.add_argument('--is-opus', action='store_const', const=False, dest='is_slices',
                     help='Indicates that the interferograms are full Opus interferograms. If not specified, then '
                          'i2srun will try to infer this from the first I2S input file.')
    parser.epilog = 'This will take a file that has the general I2S options and combine it with a second that has ' \
                    'a list (a.k.a. catalog) of all the scans to process and create I2S input files split up by ' \
                    'day/month/year for parallelization and a config file to control how i2srun sets them up.'

    parser.set_defaults(driver_fxn=build_cfg_from_header_scan_list)


def parse_update_cfg_args(parser):
    parser.description = 'Update an existing I2S bulk config file with new run file paths'
    parser.add_argument('cfg_file', help='The config file to update')
    parser.add_argument('i2s_run_files', nargs='+', help='All the I2S input files to create I2S runs for. Note that '
                                                           'these files MUST include xxYYYYMMDD in the name, where xx '
                                                           'is the site abbreviation and YYYYMMDD the year/month/day.')
    parser.add_argument('-c', '--new-cfg-file', default=None,
                        help='Path to save the updated config file as. If omitted, then the old one will be overwritten.')
    parser.add_argument('-k', '--keep-missing', action='store_true',
                        help='Keep site target dates in the config file even if they no longer have a run file in the '
                             'list of files given.')

    parser.set_defaults(driver_fxn=update_cfg_run_files)


def parse_make_i2s_runfile_args(parser):
    """

    :param parser: :class:`argparse.ArgumentParser`
    :return:
    """
    parser.description = 'Create I2S input files for missing days. If you want to copy existing run files from ' \
                         'scattered target directories into a single directory, see "cp-runs".'
    parser.add_argument('dirs_list', metavar='target_dirs_list',
                        help='File containing a list of target directories (directories with subdirectories named '
                             'xxYYYYMMDD), one per line.')
    parser.add_argument('run_files', nargs='+', default=None, help='Existing run files to use as templates.')
    parser.add_argument('-s', '--save-dir', dest='run_file_save_dir',
                        help='Directory to write the new run files to. If not given, new files are saved to the same '
                             'directory as the file they are a copy of.')
    parser.add_argument('-d', '--slice-dir',
                        help='Directory containing slice run directories. If not given, then the one specified by the '
                             'original run file is used. This is used to generate the list of runs at the bottom of a '
                             'slice run file. With opus files, this has no effect.')
    parser.add_argument('-e', '--exclude-dates', default='',
                        help='Comma-separate list of dates to exclude. Give full date strings (xxYYYYMMDD) and do '
                             'not put spaces in the list.')
    parser.add_argument('-o', '--overwrite', action='store_true',
                        help='Overwrite input files if the destination file already exists.')
    parser.set_defaults(driver_fxn=make_i2s_run_files)


def parse_make_one_i2s_runfile_args(parser):
    parser.add_argument('target_data_dir', help='Target directory (named following the xxYYYYMMDD pattern) to make '
                                                'a run file for.')
    parser.add_argument('run_files', nargs='+', default=None, help='Existing run files to use as templates.')
    parser.add_argument('-s', '--save-dir', dest='run_file_save_dir',
                        help='Directory to write the new run files to. If not given, new files are saved to the same '
                             'directory as the file they are a copy of.')
    parser.add_argument('-d', '--slice-dir', default='slices',
                        help='Directory containing slice run directories. If given as an absolute path or a relative '
                             'path starting with "." (e.g. "./slices"), it is interpreted normally. HOWEVER, if given '
                             'as a relative path NOT starting with ".", it is interpreted relative to TARGET_DATA_DIR.')
    parser.add_argument('-o', '--overwrite', action='store_true',
                        help='Overwrite input files if the destination file already exists.')
    parser.set_defaults(driver_fxn=make_one_i2s_run_file)


def parse_patch_i2s_runfiles(parser):
    # header_example_file, catalogue_files, save_dir, overwrite
    parser.description = 'Create new I2S run files by combining the header part from one file with the list of ' \
                         'slices/igrams from others.'
    parser.add_argument('header_example_file', help='File to copy the header parameters from')
    parser.add_argument('save_dir', help='Directory to save the new files to.')
    parser.add_argument('catalogue_files', nargs='+', help='Files to copy the lists of slices/igrams from')
    parser.add_argument('-c', '--catalogue-start', default=runutils._default_last_header_param,
                        help='Controls where to start copying the catalogue of slices/igrams from the catalogue files. '
                             'Given a number, e.g. "29", this indicates the parameter number to start copying the '
                             'catalogue from. Alternately, specify a number prefixed with "l", e.g. "l239" to start '
                             'copying from that line. The latter depends on all the catalogue files having the same '
                             'number of header lines, but is necessary if copying from older versions of the run files '
                             'that have a different number of lines for a given parameter (e.g. parameter 17 went from '
                             'being one line to two between GGG2014 and GGG2019).')
    parser.add_argument('-o', '--overwrite', action='store_true', help='Overwrite existing run files in the save '
                                                                       'directory.')
    parser.set_defaults(driver_fxn=patch_i2s_run_header)


def parse_copy_i2s_target_runfiles_args(parser):
    parser.description = 'Copy I2S input files from target directories to a single directory. If you need to create ' \
                         'run files for dates that do not have one from existing run files, see "make-runs".'
    parser.add_argument('dirs_list', metavar='target_dirs_list',
                        help='File containing a list of target directories (directories with subdirectories named '
                             'xxYYYYMMDD), one per line.')
    parser.add_argument('save_dir', help='Directory to save the copies to')
    parser.add_argument('-i', '--interactive', choices=('choice', 'all', 'none'),
                        help='Alter interactive behavior. "choice" will prompt the user to choose a file if multiple '
                             'files are found. "all" will always prompt, no matter how many are found, and "none" will '
                             'never prompt. Note that if multiple files are found, "none" will cause an error.')
    parser.add_argument('-o', '--overwrite', action='store_true', default=None,
                        help='Overwrite destination file if it exists. The default behavior is to ask.')
    parser.add_argument('-n', '--no-overwrite', action='store_false', default=None, dest='overwrite',
                        help='Never overwrite destination files if they exist. The default behavior is to ask.')
    parser.add_argument('-p', '--prefix', default=None,
                        help='The prefix to use for the copy made. If not specified, either "opus" or "slice" will be '
                             'used, as appropriate.')
    parser.set_defaults(driver_fxn=copy_i2s_run_files_from_target_dirs)


def parse_link_i2s_args(parser):
    parser.description = 'Link all the input files needed to run I2S in bulk'
    parser.add_argument('cfg_file', help='The config file to use to find the files to link')
    parser.add_argument('-o', '--overwrite', action='store_true', help='Overwrite existing symbolic links')
    parser.add_argument('-i', '--ignore_missing-igms', action='store_true',
                        help='Ignore errors caused when a day is missing one or more interferograms listed in the '
                             'I2S run file. Currently only has an effect when using OPUS-type files.')
    parser.add_argument('--clean-links', action='store_true', help='Clean up (delete) existing symbolic links')
    parser.add_argument('--clean-spectra', action='store_true', help='Clean up (delete) the existing spectra directory')
    parser.set_defaults(driver_fxn=link_i2s_input_files)


def parse_check_i2s_link_args(parser):
    parser.description = 'Check I2S input file links and report which ones are missing. Note that currently the ' \
                         'check for slices is rather naive, and only checks that the first slice in a scan is present.'
    parser.add_argument('cfg_file', help='The config file to use to find the files to check that they were linked')
    parser.add_argument('-d', '--dump-level', default=1, type=int,
                        help='The level of information to include in the print out. 0 = none, only the exit code will '
                             'indicate this (0 = none missing, >0 = some missing). 1 = print number of dates missing '
                             'at least one input file for each site. 2 = print number of input files missing for each '
                             'date. 3 = print list of input files missing.')
    parser.set_defaults(driver_fxn=check_i2s_links)


def parse_i2s_par_file_args(parser):
    parser.description = 'Create a file that can be used with GNU parallel to run I2S in parallel'
    parser.add_argument('cfg_file', help='Path to the i2srun config file to base the parallel file on.')
    parser.add_argument('run_file', help='Path to write the parallel run file to')
    parser.add_argument('-a', '--abspaths', action='store_true',
                        help='Make the paths in the run file absolute, rather than relative to the run file location.')
    parser.set_defaults(driver_fxn=create_i2s_parallel_run_file)


def parse_run_i2s_args(parser):
    parser.description = 'Run I2S in bulk for all target interferograms specified in a config file'
    parser.add_argument('cfg_file', help='The configuration file to use to drive the execution of I2S')
    parser.add_argument('-n', '--n-procs', default=1, type=int, help='Number of processors to use to run I2S')
    parser.add_argument('-d', '--dry-run', action='store_true', help='Print the actions that would normally be taken')
    parser.set_defaults(driver_fxn=run_all_i2s)


def parse_halt_i2s_args(parser):
    parser.description = 'Tell a currently running bulk I2S program to finish the current sites then stop'
    parser.set_defaults(driver_fxn=make_i2s_halt_file)


def parser_plot_rough_spec(parser):
    parser.description = 'Make .pdf file of rough spectra produced by I2S'
    parser.add_argument('cfg_file', help='The configuration file to use to drive the execution of I2S')
    parser.add_argument('save_dir', help='Where to save the .pdfs produced. Will be one per site')
    parser.add_argument('-n', '--plots-per-page', default=4, type=int,
                        help='Number of plots to put on a single pdf page')
    parser.add_argument('-k', '--no-overwrite', action='store_false', dest='overwrite',
                        help='Keep existing plot files, do not overwrite them')
    parser.set_defaults(driver_fxn=plot_rough_spectra)


def parse_i2s_args(parser):
    subp = parser.add_subparsers()

    create_head_cat = subp.add_parser('header-catalog', aliases=['hc'],
                                      help='Build a header and catalog file from many I2S input files')
    parse_header_catalog_args(create_head_cat)

    build_cfg = subp.add_parser('build-cfg', help='Build the config file to run I2S in bulk.')
    parse_build_cfg_args(build_cfg)

    build_cfg_many = subp.add_parser('build-cfg-many', aliases=['bcm'],
                                     help='Build the config file from multiple original I2S input files.')
    parse_build_cfg_many_args(build_cfg_many)

    build_cfg_head_cat = subp.add_parser('build-cfg-hc', aliases=['bchc'],
                                         help='Build the config file from header and catalog files')
    parse_build_cfg_header_catalog_args(build_cfg_head_cat)

    update_cfg = subp.add_parser('up-cfg', help='Update the config file with new run files.')
    parse_update_cfg_args(update_cfg)

    mod_runfiles = subp.add_parser('mod-runs', help='Modify a batch of run files')
    parse_mod_run_files_args(mod_runfiles)

    make_runfiles = subp.add_parser('make-runs', help='Make missing I2S run files')
    parse_make_i2s_runfile_args(make_runfiles)

    make_one_runfile = subp.add_parser('make-one-run', help='Make one I2S run file')
    parse_make_one_i2s_runfile_args(make_one_runfile)

    patch_runfiles = subp.add_parser('patch-runfiles', help='Patch header and slice/igram lists together')
    parse_patch_i2s_runfiles(patch_runfiles)

    cp_runfiles = subp.add_parser('cp-runs', help='Copy target I2S run files to a single directory')
    parse_copy_i2s_target_runfiles_args(cp_runfiles)

    link_i2s = subp.add_parser('link-inp', help='Link the input files to run I2S in bulk')
    parse_link_i2s_args(link_i2s)

    check_links = subp.add_parser('chk-links', help='Check the linked I2S input files')
    parse_check_i2s_link_args(check_links)

    make_par_file = subp.add_parser('par', help='Create run file for GNU parallel')
    parse_i2s_par_file_args(make_par_file)

    run_i2s = subp.add_parser('run', help='Run I2S in batch')
    parse_run_i2s_args(run_i2s)

    halt_i2s = subp.add_parser('halt', help='Gracefully halt an active batch I2S run')
    parse_halt_i2s_args(halt_i2s)

    plot_spec = subp.add_parser('plot-spec', help='Plot rough spectra')
    parser_plot_rough_spec(plot_spec)
