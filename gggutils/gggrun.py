from configobj import ConfigObj, flatten_errors
import datetime as dt
from glob import glob
import os
import re
from validate import Validator

from . import _etc_dir
from . import runutils, exceptions


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


def link_i2s_input_files(cfg_file):
    """
    Link all the input interferograms/slices and the required input files for I2S into the batch run directory

    :param cfg_file: the path to the config file to use that specifies where the input data may be found
    :type cfg_file: str

    :return: none, links files at the paths specified in the config
    """
    cfg = load_config_file(cfg_file)
    # Create a directory structure: SiteName/xxYYYYMMDD/<igms or slices> and link the individual igms or slice YYMMDD.R
    # directories. If using igms, then the igm files go directly in the igms directory. If using slices, then the
    # structure under slices must be "YYMMDD.R/scan/b*".
    #
    # If the slices aren't already in this structure, then we need to create it. We'll have to parse the input file to
    # figure out which slice numbers go with with run.

    run_top_dir = cfg['Run']['run_top_dir']

    for sect in cfg['Sites'].sections:
        sect_cfg = cfg['Sites'][sect]
        for datesect in sect_cfg.sections:
            uses_slices = _get_date_cfg_option(sect_cfg, datestr=datesect, optname='slices')

            if not uses_slices:
                _link_igms(cfg=cfg, site=sect, datestr=datesect, i2s_opts=cfg['I2S'])
            else:
                _link_slices(cfg=cfg, site=sect, datestr=datesect, i2s_opts=cfg['I2S'])


def _link_igms(cfg, site, datestr, i2s_opts):
    """
    Link full interferogram files to the appropriate site/date run directory; set up the flimit and opus-i2s.in files

    :param site_cfg: the config section for this site as a whole, including all dates
    :type site_cfg: :class:`configobj.Section`

    :param datestr: the subsection key for this particular date. Must be "xxYYYYMMDD", cannot just be the date.
    :type datestr: str

    :param run_top_dir: the top directory that all the sites' run dirs should be written to
    :type run_top_dir: str

    :param i2s_opts: section with config-specified options for the i2s input files.
    :type i2s_opts: :class:`configobj.Section` or dict

    :return: none
    """
    if not re.match(r'[a-z]{2}\d{8}', datestr):
        raise ValueError('datestr must have the format xxYYYYMMDD')

    site_cfg = cfg['Sites'][site]
    igms_dir, i2s_input_file, src_igm_dir = _link_common(cfg=cfg, site=site, datestr=datestr, i2s_opts=i2s_opts,
                                                         link_subdir='igms', input_file_basename='opus-i2s.in')

    # Read the input file and link all the listed files into the igms directory
    _, run_lines = runutils.read_i2s_input_params(i2s_input_file)
    for i, run_dict in enumerate(run_lines, start=1):
        runf = run_dict['opus_file']
        src_file = os.path.join(src_igm_dir, runf)
        if not os.path.isfile(src_file):
            raise exceptions.I2SDataException('Expected source file {src} (run line #{lnum} in {infile}) does not exist'
                                              .format(src=src_file, lnum=i, infile=i2s_input_file))
        os.symlink(src_file, os.path.join(igms_dir, runf))


def _link_slices(cfg, site, datestr, i2s_opts):
    """
    Link interferogram slices into the appropriate site/date run directory, set up the flimit and slice-i2s.in files

    This function also handles the case where the slice interferograms are not properly organized into YYMMDD.R/scan
    folders and does that organization if needed.

    :param site_cfg: the config section for this site as a whole, including all dates
    :type site_cfg: :class:`configobj.Section`

    :param datestr: the subsection key for this particular date. Must be "xxYYYYMMDD", cannot just be the date.
    :type datestr: str

    :param run_top_dir: the top directory that all the sites' run dirs should be written to
    :type run_top_dir: str

    :param i2s_opts: section with config-specified options for the i2s input files.
    :type i2s_opts: :class:`configobj.Section` or dict

    :return: none
    """
    site_cfg = cfg['Sites'][site]
    igms_dir, i2s_input_file, src_igm_dir = _link_common(cfg=cfg, site=site, datestr=datestr, i2s_opts=i2s_opts,
                                                         link_subdir='slices', input_file_basename='slice-i2s.in')

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
            os.symlink(os.path.join(src_igm_dir, slice_run_dir), os.path.join(igms_dir, slice_run_dir),
                       target_is_directory=True)  # not entirely sure the difference using target_is_directory
        else:
            _link_slices_needs_org(slice_files=slice_files, run_lines=run_lines, run_lines_index=idx,
                                   dest_run_dir=os.path.join(igms_dir, slice_run_dir))


def _link_slices_needs_org(slice_files, run_lines, run_lines_index, dest_run_dir):
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
            os.symlink(slicef, os.path.join(scans_dir, os.path.basename(slicef)))


def _link_common(cfg, site, datestr, i2s_opts, link_subdir, input_file_basename):
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

    :return: the directory where the interferograms/slice directories should be linked, the path to the I2S input file
     created in the run directory, and the directory where the interferograms/slice directories can be linked from.
    :rtype: str, str, str
    """
    site_cfg = cfg['Sites'][site]
    site_root_dir = _get_date_cfg_option(site_cfg, datestr=datestr, optname='site_root_dir')
    site_subdir = _get_date_cfg_option(site_cfg, datestr=datestr, optname='subdir')
    i2s_input_file = _get_date_cfg_option(site_cfg, datestr=datestr, optname='i2s_input_file')
    # convert configobj.Section to dictionary and make a copy at the same time
    i2s_opts = {int(k): v for k, v in i2s_opts.items()}

    src_igm_dir = os.path.abspath(os.path.join(site_root_dir, datestr, site_subdir))
    date_dir = _date_subdir()
    igms_dir = os.path.join(date_dir, link_subdir)
    if not os.path.exists(igms_dir):
        os.makedirs(igms_dir)

    # Link the flimit file into the date dir with a consistent name to make setting up the input file easier
    src_flimit = _get_date_cfg_option(site_cfg, datestr=datestr, optname='flimit_file')
    src_flimit = os.path.abspath(src_flimit)
    os.symlink(src_flimit, os.path.join(date_dir, 'flimit.i2s'))

    # Make an output spectra directory
    spectra_dir = os.path.join(date_dir, 'spectra')
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


def run_all_i2s(cfg_file):
    cfg = load_config_file(cfg_file)



def parse_build_cfg_args(parser):
    """

    :param parser:
    :type parser: :class:`argparse.ArgumentParser`
    :return:
    """
    parser.description = 'Construct the starting config file for running I2S in bulk'
    parser.add_argument('cfg_file', help='The name to give the new config file')
    parser.add_argument('i2s_input_files', nargs='+', help='All the I2S input files to create I2S runs for. Note that '
                                                           'these files MUST include xxYYYYMMDD in the name, where xx '
                                                           'is the site abbreviation and YYYYMMDD the year/month/day.')
    parser.add_argument('-c', '--old-cfg-file', default=None,
                        help='Previous configuration file to merge with the new one; any options in the old one will '
                             'have their values inserted in the new config file.')

    parser.set_defaults(driver_fxn=build_cfg_file)


def parse_link_i2s_args(parser):
    """

    :param parser:
    :type parser: :class:`argparse.ArgumentParser`
    :return:
    """
    parser.description = 'Link all the input files needed to run I2S in bulk'
    parser.add_argument('cfg_file', help='The config file to use to find the files to link')
    parser.set_defaults(driver_fxn=link_i2s_input_files)