from collections import OrderedDict
from configobj import ConfigObj, flatten_errors
import datetime as dt
from glob import glob
from logging import getLogger
from multiprocessing import Pool
import os
import re
import shutil
from subprocess import Popen
import sys
from validate import Validator

from textui import uielements

from . import _etc_dir, _i2s_halt_file
from . import runutils, exceptions, target_utils


logger = getLogger('gggrun')


def build_cfg_file(cfg_file, i2s_input_files, old_cfg_file=None):
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

    for site, site_dict in grouped_files.items():
        slices, subdir = _group_uses_slices(site_dict)
        site_opts = {'slices': slices,
                     'site_root_dir': '',
                     'no_date_dir': '0',
                     'subdir': subdir,
                     'slices_in_subdir': '0',
                     'flimit_file': ''}
        for site_date, input_file in site_dict.items():
            site_opts[site_date] = {'i2s_input_file': os.path.abspath(input_file)}
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


def load_config_file(cfg_file):
    """
    Load an I2S run config file, validating options and normalizing paths

    Boolean values will be converted into actual booleans, and paths that are allowed to be relative to the config file
    will be converted into absolute paths if given as relative

    :param cfg_file: the path to the config file
    :type cfg_file: str

    :return: the configuration object
    :rtype: :class:`configobj.ConfigObj`
    """
    # paths that, if relative, should be interpreted as relative to the config file. We exclude "subdir" here because
    # it's relative to the source date directory
    cfg_file_dir = os.path.abspath(os.path.dirname(cfg_file))
    path_keys = ('site_root_dir', 'flimit_file', 'i2s_input_file')

    def make_paths_abs(section, key):
        if key in path_keys and not os.path.isabs(section[key]):
            section[key] = os.path.abspath(os.path.join(cfg_file_dir, section[key]))

    cfg = ConfigObj(cfg_file, configspec=os.path.join(_etc_dir, 'i2s_in_val.cfg'))
    validator = Validator()
    result = cfg.validate(validator, preserve_errors=True)

    if result != True:
        error_msgs = []
        for sects, key, msg in flatten_errors(cfg, result):
            error_msgs.append('{}/{}: {}'.format('/'.join(sects), key, msg))

        final_error_msg = 'There are problems with one or more options in {}:\n*  {}'.format(
            cfg_file, '\n*  '.join(error_msgs)
        )
        raise exceptions.ConfigException(final_error_msg)

    # Make relative paths relative to the config file
    cfg.walk(make_paths_abs)

    # In the I2S setting section, replace "\\n" and "\\r" with "\n" and "\r" - i.e. undo the
    # backslash escaping the configobj does. This is necessary because some of the i2s parameters
    # need to have two lines.
    for key, value in cfg['I2S'].items():
        cfg['I2S'][key] = value.replace('\\n', '\n').replace('\\r', '\r')

    return cfg


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
        run_file_dict = _list_existing_i2s_run_files(site_dict, run_files=run_files)

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


def _list_existing_i2s_run_files(target_date_dict, run_files):
    """
    Make a dictionary indicating which target days already have run files.

    :param target_date_dict: a dictionary with full date strings (xxYYYYMMDD) as keys. The values are not used.
    :type target_date_dict: dict

    :param run_files: the list of run files available to be matched with target days
    :type run_files: list(str)

    :return: a ordered dictionary with the same date strings as keys as ``target_date_dict`` and the corresponding run
     file as the value, or ``None`` if no run file is available.
    :rtype: :class:`collections.OrderedDict`
    """
    target_dates = runutils.sort_datestr(target_date_dict.keys())
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
        for run in range(start_run, end_run+1):
            curr_dir = os.path.join(slice_dir, '{}.{}'.format(curr_date.strftime('%y%m%d'), run))
            ifs_log_file = os.path.join(curr_dir, 'IFSretr.log')
            if not os.path.exists(ifs_log_file):
                logger.warning('Cannot add data from {}, no IFSretr.log file'.format(curr_dir))
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


def link_i2s_input_files(cfg_file, overwrite=False, clean_links=False, clean_spectra=False, ignore_missing_igms=False):
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
            uses_slices = _get_date_cfg_option(sect_cfg, datestr=datesect, optname='slices')

            if not uses_slices:
                logger.debug('Linking full igrams for {}'.format(datesect))
                _link_igms(cfg=cfg, site=sect, datestr=datesect, i2s_opts=cfg['I2S'], overwrite=overwrite,
                           clean_links=clean_links, clean_spectra=clean_spectra, ignore_missing=ignore_missing_igms)
            else:
                logger.debug('Linking slices for {}'.format(datesect))
                _link_slices(cfg=cfg, site=sect, datestr=datesect, i2s_opts=cfg['I2S'], overwrite=overwrite,
                             clean_links=clean_links, clean_spectra=clean_spectra)


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
                logger.warning(msg)
            else:
                raise exceptions.I2SDataException(msg)
        link_dict[src_file] = os.path.join(igms_dir, runf)

    if not files_missing:
        for src, dst in link_dict.items():
            _make_link(src, dst, overwrite=overwrite)
    else:
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

    slices_need_org = _get_date_cfg_option(site_cfg, datestr, 'slices_in_subdir')
    if slices_need_org:
        slice_files = sorted(glob(os.path.join(src_igm_dir, 'b*')))

    # If the slices need organized, then we'll have to link individual slice files into the right directories. If not,
    # we can just link the preexisting directories
    _, run_lines = runutils.read_i2s_input_params(i2s_input_file)
    last_run_date = None
    for idx, line in enumerate(run_lines):
        run_date = dt.datetime(int(line['year']), int(line['month']), int(line['day']))
        slice_run_dir = runutils.slice_date_subdir(run_date, line['run'])
        if not slices_need_org:
            if run_date != last_run_date:
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
    site_root_dir = _get_date_cfg_option(site_cfg, datestr=datestr, optname='site_root_dir')
    no_date_dir = _get_date_cfg_option(site_cfg, datestr=datestr, optname='no_date_dir')
    site_subdir = _get_date_cfg_option(site_cfg, datestr=datestr, optname='subdir')
    i2s_input_file = _get_date_cfg_option(site_cfg, datestr=datestr, optname='i2s_input_file')
    # convert configobj.Section to dictionary and make a copy at the same time
    i2s_opts = {int(k): v for k, v in i2s_opts.items()}

    if no_date_dir:
        src_igm_dir = os.path.abspath(os.path.join(site_root_dir, site_subdir))
    else:
        src_igm_dir = os.path.abspath(os.path.join(site_root_dir, datestr, site_subdir))
    date_dir = _date_subdir(cfg, site, datestr)
    igms_dir = os.path.join(date_dir, link_subdir)
    if clean_links and os.path.exists(igms_dir):
        logger.info('Removing existing igrams directory: {}'.format(igms_dir))
        shutil.rmtree(igms_dir)
    if not os.path.exists(igms_dir):
        os.makedirs(igms_dir)

    # Link the flimit file into the date dir with a consistent name to make setting up the input file easier
    src_flimit = _get_date_cfg_option(site_cfg, datestr=datestr, optname='flimit_file')
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


def _group_uses_slices(site_dict):
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
        n_using_slices += runutils.i2s_use_slices(input_file)

    if n_using_slices > 0.5 * len(site_dict):
        return '1', 'slices'
    else:
        return '0', 'igms'


def _date_subdir(cfg, site, datestr):
    """
    Return the path to the directory where a day's I2S will be run.

    :param cfg: the configuration object from reading the I2S bulk config file.
    :type cfg: :class:`configobj.ConfigObj`

    :param site: the site abbreviaton
    :type site: str

    :param datestr: the string defining which date the run directory is for.  May include the site abbreviation
     (xxYYYYMMDD) or not (YYYYMMDD).
    :type datestr: str

    :return: the path to the run directory
    :rtype: str
    """
    run_top_dir = cfg['Run']['run_top_dir']
    site_sect = cfg['Sites'][site]
    datestr = _find_site_datekey(site_sect, datestr)
    return os.path.join(run_top_dir, site_sect.name, datestr)


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
     the xxYYYYMMDD date strings.
    :rtype: dict
    """
    group_dict = dict()
    for f in input_files:
        fname = os.path.basename(f)
        site_date = re.search(r'(?P<site>\w\w)(?P<date>\d{8})', fname)
        if site_date is None:
            raise ValueError('{} does not contain a site abbreviation + date string in its name'.format(f))

        site = site_date.group('site')
        site_date = site_date.group()

        if site not in group_dict:
            group_dict[site] = dict()
        group_dict[site][site_date] = f

    return group_dict


def _find_site_datekey(site_cfg, datestr):
    """
    Find a site date key in a site config section

    :param site_cfg: the site config section
    :type site_cfg: :class:`configobj.Section`

    :param datestr: the date string to search for. May either include or exclude the site abbreviation; including the
     site abbreviation will be faster.
    :type datestr: str

    :return: the key in the ``site_cfg`` for the requested date
    :rtype: str
    """
    if datestr in site_cfg.keys():
        return datestr
    else:
        for k in site_cfg.keys():
            if k.endswith(datestr):
                return k
        raise exceptions.SiteDateException('No key matching "{}" found in site "{}"'.format(datestr, site_cfg.name))


def _get_date_cfg_option(site_cfg, datestr, optname):
    """
    Get a config option for a specific date, falling back on the general site option if not present

    :param site_cfg: the section of the config for that site that includes all the date-specific sections
    :type site_cfg: :class:`configobj.Section`

    :param datestr: the date string to search for. May either include or exclude the site abbreviation; including the
     site abbreviation will be faster.
    :type datestr: str

    :param optname: the option key to search for.
    :type optname: str

    :return: the option value, from the specific date if found there, from the site if not.
    :raises exceptions.ConfigExceptions: if the give option isn't found in either the site or date section
    """
    key = _find_site_datekey(site_cfg, datestr)
    if optname in site_cfg[key] and site_cfg[key][optname] is not None:
        return site_cfg[key][optname]
    elif optname in site_cfg:
        return site_cfg[optname]
    else:
        raise exceptions.ConfigException('The option "{}" was not found in the date-specific section ({}) nor the '
                                         'overall site exception'.format(optname, key))


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
            run_dir = _date_subdir(cfg, site, datestr)
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


def run_all_i2s(cfg_file, n_procs=1):
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
            this_run_dir = _date_subdir(cfg, site, datestr)
            if n_procs > 1:
                pool_args.append((this_run_dir, i2s_cmd))
            else:
                # I like to avoid opening a pool if only running with one processor because it's easier to debug; you
                # can actually step into the run function.
                _run_one_i2s(this_run_dir, i2s_cmd)

    if len(pool_args) > 0:
        with Pool(processes=n_procs) as pool:
            pool.starmap(_run_one_i2s, pool_args)


def _run_one_i2s(run_dir, i2s_cmd):
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
    possible_input_files = [os.path.basename(f) for f in glob(os.path.join(run_dir, '*i2s*.in'))]
    if len(possible_input_files) > 1:
        raise RuntimeError('Multiple I2S input files found in {}: {}'.format(
            run_dir, ', '.join(possible_input_files))
        )
    elif len(possible_input_files) == 0:
        raise RuntimeError('No I2S input files found in {}'.format(run_dir))
    else:
        i2s_input_file = possible_input_files[0]

    now = dt.datetime.now()
    log_file = os.path.join(run_dir, 'run_i2s_{}.log'.format(now.strftime('%Y%m%dT%H%M%S')))
    logger.info('Starting I2S in {rundir} using {infile} as input file. I2S output piped to {log}.'
                .format(rundir=run_dir, infile=i2s_input_file, log=log_file))
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
    parser.add_argument('run_files', nargs='*', default=None,
                        help='Existing run files to use as templates. If any are given, then only those are used as '
                             'templates rather than all .in files in the run_file_dir')
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


def parse_run_i2s_args(parser):
    parser.description = 'Run I2S in bulk for all target interferograms specified in a config file'
    parser.add_argument('cfg_file', help='The configuration file to use to drive the execution of I2S')
    parser.add_argument('-n', '--n-procs', default=1, help='Number of processors to use to run I2S')
    parser.set_defaults(driver_fxn=run_all_i2s)


def parse_halt_i2s_args(parser):
    parser.description = 'Tell a currently running bulk I2S program to finish the current sites then stop'
    parser.set_defaults(driver_fxn=make_i2s_halt_file)


def parse_i2s_args(parser):
    subp = parser.add_subparsers()

    build_cfg = subp.add_parser('build-cfg', help='Build the config file to run I2S in bulk.')
    parse_build_cfg_args(build_cfg)

    update_cfg = subp.add_parser('up-cfg', help='Update the config file with new run files.')
    parse_update_cfg_args(update_cfg)

    mod_runfiles = subp.add_parser('mod-runs', help='Modify a batch of run files')
    parse_mod_run_files_args(mod_runfiles)

    make_runfiles = subp.add_parser('make-runs', help='Make missing I2S run files')
    parse_make_i2s_runfile_args(make_runfiles)

    cp_runfiles = subp.add_parser('cp-runs', help='Copy target I2S run files to a single directory')
    parse_copy_i2s_target_runfiles_args(cp_runfiles)

    link_i2s = subp.add_parser('link-inp', help='Link the input files to run I2S in bulk')
    parse_link_i2s_args(link_i2s)

    check_links = subp.add_parser('chk-links', help='Check the linked I2S input files')
    parse_check_i2s_link_args(check_links)

    run_i2s = subp.add_parser('run', help='Run I2S in batch')
    parse_run_i2s_args(run_i2s)

    halt_i2s = subp.add_parser('halt', help='Gracefully halt an active batch I2S run')
    parse_halt_i2s_args(halt_i2s)
