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

    return cfg


def make_i2s_run_files(dirs_list, run_files, run_file_save_dir, overwrite=False, slice_dir=None):

    avail_target_dates = target_utils.build_target_dirs_dict(target_dirs=[], dirs_list=dirs_list,
                                                             flat=True, full_datestr=True)
    for site, site_dict in avail_target_dates.items():
        if len(site_dict) == 0:
            logger.info('No date folders found for {}'.format(site))
            continue
        run_file_dict = _list_existing_i2s_run_files(site_dict, run_files=run_files)

        # Find the first date string key that has a file associated with it. If there are any dates before that missing
        # a file, we'll use that file to fill in.
        key_with_file = None
        for datestr, runfile in run_file_dict.items():
            if runfile is not None:
                key_with_file = datestr
                break

        # Now for each target date, if it has a file, reset the last key found to have a file to that, so that we use
        # the most recent file before a missing date as the template if we need to make a new input file. If there's not
        # a file, then copy one to be that file.
        for datestr, runfile in run_file_dict.items():
            if runfile is None:
                _make_new_i2s_run_file(datestr=datestr, run_files=run_file_dict, last_key_with_file=key_with_file,
                                       save_dir=run_file_save_dir, overwrite=overwrite, slice_dir=slice_dir)
            else:
                key_with_file = datestr


def _list_existing_i2s_run_files(target_date_dict, run_files):
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


def _make_new_i2s_run_file(datestr, run_files, last_key_with_file, save_dir, overwrite=False, slice_dir=None,
                           file_type=None):
    if last_key_with_file is None:
        logger.warning('Cannot make a run file for {}: no existing files for that site'.format(datestr))
        return
    old_file = run_files[last_key_with_file]
    new_base_file = os.path.basename(old_file)
    new_base_file = re.sub(r'\w\w\d{8}', datestr, new_base_file)
    if save_dir is None:
        save_dir = os.path.dirname(run_files[last_key_with_file])
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

    if file_type is None:
        file_type = 'slice' if runutils.i2s_use_slices(old_file) else 'opus'

    if file_type == 'slice':
        add_slice_info_to_i2s_run_file(old_file, new_run_file=new_file, start_date=datestr, slice_dir=slice_dir)
    elif file_type == 'opus':
        # At the moment, I haven't implemented a way to figure out the input file list with opus format. So just copy
        # everything but the list of files.
        logger.warning('For {}, the list of input files will be blank because it is opus-format and auto-populating '
                       'a list of opus files is not implemented'.format(new_base_file))
        runutils.modify_i2s_input_params(old_file, new_file=new_file, include_input_files=False)
    else:
        raise ValueError('{} is not an allowed value for file_type. Must be "slice" or "opus"')


def copy_i2s_run_files_from_target_dirs(dirs_list, save_dir, interactive='choice', overwrite=None, prefix=None):
    avail_target_dates = target_utils.build_target_dirs_dict([], dirs_list=dirs_list, flat=True,
                                                             full_datestr=True, key_by_basename=False)
    for site, site_dict in avail_target_dates.items():
        for site_date, revision in site_dict.items():
            full_site_dir = os.path.join(site, site_date, revision)
            site_input_files = glob(os.path.join(full_site_dir, '*.in'))

            if len(site_input_files) == 0:
                if interactive == 'all':
                    print('\nNo input files found in {}. Press ENTER to continue.'.format(full_site_dir), end='')
                    input()
                else:
                    logger.warning('No input files found in {}'.format(full_site_dir))
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
        * If only a start date is given, all data from that day will be added. If start or ending run numbers are given,
          then only runs within that range are added. (Omitted one of the run numbers removes that limit, e.g. only
          specifying a start run will include all runs >= that number.)
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
        for date, run, slice_num in _iter_runs(slice_dir=slice_dir, start_date=start_date, end_date=end_date,
                                               start_run=start_run, end_run=end_run, scantype=scantype):
            year = date.strftime('%Y')
            month = date.strftime('%m')
            day = date.strftime('%d')
            fobj.write('{yr} {mo} {day} {run} {slice_num}\n'.format(yr=year, mo=month, day=day, run=run,
                                                                    slice_num=slice_num))


def _parse_add_slice_info_inputs(run_file, start_date, end_date, start_run, end_run, slice_dir):
    def parse_datestr(datestr, input_name):
        if len(datestr) == 8:
            return dt.datetime.strptime(datestr, '%Y%m%d')
        elif len(datestr) == 6:
            return dt.datetime.strptime(datestr, '%y%m%d')
        else:
            raise ValueError('{} must be in YYMMDD or YYYYMMDD format'.format(input_name))

    def parse_date_input(date_in, input_name):
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
        for fname in run_dirs:
            fname = os.path.basename(fname.rstrip(os.sep))
            date_str, run_str = fname.split('.')
            key = dt.datetime.strptime('%y%m%d', date_str)
            if key not in run_dates:
                run_dates[key] = []
            run_dates[key].append(int(run_str))

        if start_date is None:
            start_date = min(run_dates.keys())
        if end_date is None:
            end_date = max(run_dates.keys())
        if start_run is None:
            start_run = min(run_dates[start_date])
        if end_run is None:
            end_run = max(run_dates[end_date])

    return start_date, end_date, start_run, end_run, slice_dir


def _iter_runs(slice_dir, start_date, end_date, start_run, end_run, scantype='Solar'):
    curr_date = start_date
    while curr_date <= end_date:
        for run in range(start_run, end_run+1):
            curr_dir = os.path.join(slice_dir, '{}.{}'.format(curr_date.strftime('%y%m%d'), run))
            ifs_log_file = os.path.join(slice_dir, 'IFSretr.log')
            if not os.path.exists(ifs_log_file):
                logger.warning('Cannot add data from {}, no IFSretr.log file'.format(curr_dir))
            with open(ifs_log_file, 'r') as robj:
                in_scan = False
                for line in robj:
                    # each line will be date: status. "date" includes colons, so split on the last colon
                    status = line.split(':')[-1].strip()
                    if in_scan:
                        if status == 'Request Completed':
                            in_scan = False

                        slice_num = re.search(r'(?<=b)\d+(?=\.0)')
                        if slice_num is None:
                            raise NotImplementedError('Line inside a scan does not include a slice number! Line was: '
                                                      '{}'.format(line))
                        slice_num = slice_num.group()
                        yield curr_date, run, slice_num
        curr_date += dt.timedelta(days=1)


def link_i2s_input_files(cfg_file, overwrite=False, clean_links=False, clean_spectra=False):
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
                           clean_links=clean_links, clean_spectra=clean_spectra)
            else:
                logger.debug('Linking slices for {}'.format(datesect))
                _link_slices(cfg=cfg, site=sect, datestr=datesect, i2s_opts=cfg['I2S'], overwrite=overwrite,
                             clean_links=clean_links, clean_spectra=clean_spectra)


def _link_igms(cfg, site, datestr, i2s_opts, overwrite, clean_links, clean_spectra):
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

    # Read the input file and link all the listed files into the igms directory
    _, run_lines = runutils.read_i2s_input_params(i2s_input_file)
    for i, run_dict in enumerate(run_lines, start=1):
        runf = run_dict['opus_file']
        src_file = os.path.join(src_igm_dir, runf)
        if not os.path.isfile(src_file):
            raise exceptions.I2SDataException('Expected source file {src} (run line #{lnum} in {infile}) does not exist'
                                              .format(src=src_file, lnum=i, infile=i2s_input_file))
        _make_link(src_file, os.path.join(igms_dir, runf), overwrite=overwrite)


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
    for idx, line in enumerate(run_lines):
        run_date = dt.datetime(int(line['year']), int(line['month']), int(line['day'])).strftime('%y%m%d')
        slice_run_dir = '{}.{}'.format(run_date, line['run'])
        if not slices_need_org:
            _make_link(os.path.join(src_igm_dir, slice_run_dir), os.path.join(igms_dir, slice_run_dir),
                       overwrite=overwrite, target_is_directory=True)  # not entirely sure the difference using target_is_directory
        else:
            logger.debug('Setting up correct directory structure for linked slices')
            _link_slices_needs_org(slice_files=slice_files, run_lines=run_lines, run_lines_index=idx,
                                   dest_run_dir=os.path.join(igms_dir, slice_run_dir), overwrite=overwrite)


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
    if os.path.exists(dst):
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
    run_top_dir = cfg['Run']['run_top_dir']
    site_sect = cfg['Sites'][site]
    datestr = _find_site_datekey(site_sect, datestr)
    return os.path.join(run_top_dir, site_sect.name, datestr)


def _group_i2s_input_files(input_files):
    """
    Group I2S input files by site

    :param input_files: a list of paths to all I2S input files
    :type input_files: list(str)

    :return: a two-level dictionary where the first level's keys are the site abbreviations and the second level's are
     the YYYYMMDD date strings.
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
    spectra = dict()
    spectra_files = glob(os.path.join(run_dir, 'spectra', '*'))
    for f in spectra_files:
        spectra[os.path.basename(f)] = os.path.getmtime(f)
    return spectra


def _compare_completed_spectra(old_dict, new_dict, run_dir):
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
    parser.add_argument('--clean-links', action='store_true', help='Clean up (delete) existing symbolic links')
    parser.add_argument('--clean-spectra', action='store_true', help='Clean up (delete) the existing spectra directory')
    parser.set_defaults(driver_fxn=link_i2s_input_files)


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

    mod_runfiles = subp.add_parser('mod-runs', help='Modify a batch of run files')
    parse_mod_run_files_args(mod_runfiles)

    make_runfiles = subp.add_parser('make-runs', help='Make missing I2S run files')
    parse_make_i2s_runfile_args(make_runfiles)

    cp_runfiles = subp.add_parser('cp-runs', help='Copy target I2S run files to a single directory')
    parse_copy_i2s_target_runfiles_args(cp_runfiles)

    link_i2s = subp.add_parser('link-inp', help='Link the input files to run I2S in bulk')
    parse_link_i2s_args(link_i2s)

    run_i2s = subp.add_parser('run', help='Run I2S in batch')
    parse_run_i2s_args(run_i2s)

    halt_i2s = subp.add_parser('halt', help='Gracefully halt an active batch I2S run')
    parse_halt_i2s_args(halt_i2s)
