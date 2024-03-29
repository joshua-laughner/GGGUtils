import datetime as dt
from glob import glob
from logging import getLogger
import ntpath
import os
import re
import shutil
import sys
import tempfile

from configobj import ConfigObj, flatten_errors
from validate import Validator

from . import _run_cols_for_full, _run_cols_for_slices
from . import exceptions, _etc_dir

_default_last_header_param = 28
logger = getLogger('runutils')


class ProgressBar(object):
    """
    Create a text-based progress bar

    An instance of this class can be used to print a text progress bar that does not need a new line for each progress
    step. It uses carriage returns to reset to the beginning of each line before printing the next. This therefore
    does not work well if other print statements occur in between calls to :meth:`print_bar`, the progress bar will
    either end up on a new line anyway or potentially overwrite previous print statements if they did not end with a
    newline.

    :param num_symbols: how many steps there should be in the progress bar. In other words, the progress bar will be
     complete when :meth:`print_bar` is called with ``num_symbols-1``.
    :type num_symbols: int

    :param prefix: a string to include before the beginning of each progress bar. The class will ensure that at least
     one space is present between the prefix and the progress bar, but will not add one if one is already present at
     the end of the prefix.
    :type prefix: str

    :param suffix: a string to include at the end of each progress bar. The class will ensure that at least one space
     is present between the progress bar and the suffix, but will not add one if one is already present at the beginning
     of the suffix.
    :type suffix: str

    :param add_one: if ``True``, the number of symbols printed in the progress bar is equal to ``i+1`` where ``i`` is
     the argument to :meth:`print_bar`. This works well with Python loops over ``i in range(n)``, since the last value
     of ``i`` will be ``n-1``, setting ``add_one`` to ``True`` ensures that a full progress bar is printed at the end.
    :type add_one: bool

    :param style: can be either '*' or 'counter'. The former prints a symbolic progress bar of the form:

        [*   ]
        [**  ]
        [*** ]
        [****]

     where the number of *'s is set by ``num_symbols``. The latter will instead print 'i/num_symbols' for each step.
    :type style: str
    """
    def __init__(self, num_symbols, prefix='', suffix='', add_one=True, style='*'):
        """
        See class help.
        """
        if len(prefix) > 0 and not prefix.endswith(' '):
            prefix += ' '
        if len(suffix) > 0 and not suffix.startswith(' '):
            suffix = ' ' + suffix

        if style == '*':
            self._fmt_str = '{pre}[{{pstr:<{n}}}]{suf}'.format(pre=prefix, n=num_symbols, suf=suffix)
        elif style == 'counter':
            self._fmt_str = '{pre}{{i:>{l}}}/{n}{suf}'.format(pre=prefix, n=num_symbols, suf=suffix, l=len(str(num_symbols)))
        else:
            raise ValueError('style "{}" not recognized'.format(style))
        self._add_one = add_one

    def print_bar(self, i):
        """
        Print the iteration of the progress bar corresponding to step ``i``.

        :param i: defines the progress step, either the number of *'s to print with ``style='*'`` or the counter number
         with ``style='counter'``.
        :type i: int
        :return: None, prints to screen.
        """
        if self._add_one:
            i += 1

        pstr = '*' * i
        pbar = self._fmt_str.format(pstr=pstr, i=i)
        sys.stdout.write('\r' + pbar)
        sys.stdout.flush()

    def finish(self):
        """
        Close the progress bar. By default, just prints a newline.
        :return: None
        """
        sys.stdout.write('\n')
        sys.stdout.flush()



def finalize_target_dirs(target_dirs, dirs_list=None):
    """
    Combine command line list of target directories with those in a file

    :param target_dirs: list of directories
    :type target_dirs: list(str)

    :param dirs_list: a path to a file containing one directory per line to add to the list of target directories
    :type dirs_list: str or None

    :return: the combined list of target directories
    :rtype: list(str)
    """
    if target_dirs is None:
        target_dirs = []

    if dirs_list is not None:
        with open(dirs_list, 'r') as robj:
            extra_dirs = [l.strip() for l in robj.readlines()]
        target_dirs.extend(extra_dirs)
    return target_dirs


def modify_i2s_input_params(filename, *args, new_file=None, last_header_param=_default_last_header_param,
                            include_input_files=True, **infile_actions):
    """
    Modify an I2S input file's common parameters. This cannot easily handle adding interferograms to process.

    This assumes that in an I2S input file, lines that contain any non-comment, non-whitespace characters are parameters
    and that they go in order. It will preserve comments.

    :param filename: the path to the I2S input file to modify
    :type filename: str

    :param args: the values to change the I2S parameters to. This may be specified in two ways. Either provide a single
     dictionary, where the keys are the parameter numbers (1-based, integers) to change and the values are the new
     values to give those parameters (as strings), or give the parameter numbers and values as alternating positional
     arguments. That is, ``modify_i2s_input_params('slice-i2s.in', {1: './igms/', 8: './flimit.i2s'})`` and
     ``modify_i2s_input_params('slice-i2s.in', 1, './igms/', 8, './flimit.i2s')`` are equivalent; both indicate to change
     parameter #1 to "./igms/" and #8 to "./flimit.i2s". Note: if a parameter requires multiple lines (e.g. parameter
     #17), all lines need to be given as a single string with the lines separated by line breaks. Any commonly
     recognized line break (\n, \r, \r\n) may be used.

    :param new_file: optional, if given, write the modified file to this path. If not given, overwrites the original
     files.
    :type new_file: str

    :param last_header_param: the number of parameters in the header (before the list of slices or opus files). This
     generally should not need to change unless using a non-standard version of I2S that expects more or fewer
     parameters.
    :type last_header_param: int

    :param include_input_files: whether to keep the list of opus interferograms or slices at the end of the new file.
    :type include_input_files: bool

    :param infile_actions: additional keyword arguments specifying changes to make to the existing runfiles. Allowed
     keywords are:

        * "chdir" - replace the leading directory of any opus files listed at the bottom of the run file. Has no effect
          on slice files. If the value is not a string, then the leading directories are just stripped.

    :return: None, writes new file.
    """
    if new_file is None:
        new_file = filename
    i2s_params = _mod_i2s_args_parsing(args)

    # We'll always write the changes to a temporary file first. That way we keep the code simple, and just which file
    # it gets copied to changes
    with tempfile.NamedTemporaryFile('w') as tfile:
        with open(filename, 'rb') as robj, open(tfile.name, 'w') as wobj:
            for param_num, subparam_num, value, comment, is_param in iter_i2s_input_params(robj, include_all_lines=True):
                curr_param_lines = _nlines_for_param(param_num)
                if is_param:
                    # Line has non-comment, non-whitespace characters. If it was one of the parameters to be changed,
                    # replace the value part. If not, just keep the value as-is.
                    if param_num in i2s_params:
                        if len(i2s_params[param_num]) != curr_param_lines:
                            raise ValueError('Parameter {param} requires {req} lines, only {n} given.'
                                             .format(param=param_num, req=curr_param_lines, n=len(i2s_params[param_num])))
                        # to keep things pretty, capture existing whitespace between the value and any trailing comments
                        trailing_space = re.search(r'\s*$', value).group()
                        value = i2s_params[param_num][subparam_num-1] + trailing_space
                    elif param_num > last_header_param:
                        if not include_input_files:
                            continue
                        elif 'chdir' in infile_actions:
                            value = re.split(r'\s+', value, maxsplit=1)
                            if re.match(r'\s*\d{4}', value[0]):
                                logger.info('Not removing opus file directory names in line "{}" because this looks '
                                            'like a slice file (no file paths)')
                            # os.path.basename will not split on backslashed on linux. ntpath.basename seems to split
                            # on forward or backslashes
                            value[0] = ntpath.basename(value[0])
                            if isinstance(infile_actions['chdir'], str):
                                value[0] = os.path.join(infile_actions['chdir'], value[0])
                            value = ' '.join(value)

                wobj.write(value)

                if len(comment) > 0:
                    wobj.write(':' + comment)

        shutil.copy(tfile.name, new_file)


# If a parameter has >1 line, specify the number of lines here
_params_with_extra_lines = {17: 2}


def _nlines_for_param(param_num):
    if param_num in _params_with_extra_lines:
        return _params_with_extra_lines[param_num]
    else:
        return 1


def _mod_i2s_args_parsing(args):
    def check_dict_fmt(dict_in):
        if any(not isinstance(k, int) for k in dict_in.keys()) or any(k < 1 for k in dict_in.keys()):
            raise TypeError('The parameter numbers to modify must be specified as positive integers')
        elif any(not isinstance(v, str) for v in dict_in.values()):
            raise TypeError('The values to assign to the parameters must be specified as string')

        for k, v in dict_in.items():
            dict_in[k] = v.splitlines()

        return dict_in

    if len(args) == 1:
        if isinstance(args[0], dict):
            return check_dict_fmt(args[0])
        else:
            raise TypeError('If giving a single argument to specify which i2s parameters to change, it must be a '
                            'dictionary with the keys as parameter numbers (1-based) and the values the new values '
                            'to assign the parameters (strings)')
    elif len(args) % 2 != 0:
        raise TypeError('If giving the parameter numbers and parameter values as positional arguments, there must be '
                        'an even number (i.e. a value for every number)')
    else:
        dict_out = {args[i]: args[i+1] for i in range(0, len(args), 2)}
        return check_dict_fmt(dict_out)


def parse_run_line(line, infile=None):
    line = line.split()
    if len(line) == _run_cols_for_slices:
        keys = ('year', 'month', 'day', 'run', 'slice')
    elif len(line) <= _run_cols_for_full:
        keys = ('opus_file', 'year', 'month', 'day', 'run', 'lat', 'lon', 'alt', 'Tins', 'Pins', 'Hins',
                'Tout', 'Pout', 'Hout', 'SIA', 'FVSI', 'WSPD', 'WDIR')
    elif infile is None:
        raise exceptions.I2SFormatException('The following line had {} columns for the igram list, expected '
                                            'no more than {}:\n{}'
                                            .format(len(line), _run_cols_for_full, line))
    else:
        raise exceptions.I2SFormatException('I2S input file ({}) had {} columns for the igram list, expected '
                                            'no more than {}'
                                            .format(infile, len(line), _run_cols_for_full))
    return {k: v for k, v in zip(keys, line)}


def read_i2s_input_params(infile, last_header=_default_last_header_param, verbatim_run_lines=False):
    """
    Read and parse an I2S input file

    :param infile: the path to the I2S file to parse
    :type infile: path-like

    :param last_header: the number of header parameters. Non-comment lines after this one are treated as run lines, i.e.
     lines that specify a scan to process.
    :type last_header: int

    :param verbatim_run_lines: controls how the run lines are returned. The default (``False``) is that they are parsed
     into dictionaries. Set this to ``True`` to keep them as plain strings, just with leading and trailing whitespace
     stripped.
    :type verbatim_run_lines: bool

    :return: two lists, one of header values and one of run lines. The latter will be dicts or strings, depending on
     ``verbatim_run_lines``.
    """


    header_params = []
    run_files = []
    with open(infile, 'rb') as robj:
        for paramnum, partnum, value, comment in iter_i2s_input_params(robj):
            value = value.strip()
            if paramnum <= last_header:
                if len(header_params) < paramnum:
                    header_params.append(value)
                else:
                    header_params[paramnum-1] += '\n'+value
            elif verbatim_run_lines:
                run_files.append(value)
            else:
                run_files.append(parse_run_line(value, infile))

    return header_params, run_files


def slice_line_date(slice_dict):
    """
    Create a datetime object corresponding to a line in a slice input file

    :param slice_dict: a dictionary for one scan from the bottom of a slice-i2s run file, returned by
     :func:`read_i2s_input_params`.
    :type slice_dict: dict

    :return: the datetime of that line
    :rtype: :class:`datetime.datetime`
    """
    return dt.datetime(int(slice_dict['year']), int(slice_dict['month']), int(slice_dict['day']))


def slice_date_subdir(date, run):
    return '{}.{}'.format(date.strftime('%y%m%d'), run)


def i2s_use_slices(infile, last_header=_default_last_header_param):
    _, igrams = read_i2s_input_params(infile, last_header=last_header)
    if len(igrams) == 0:
        raise exceptions.I2SFormatException('I2S intput file ({}) has no igrams listed, cannot tell if uses slices or full igrams'.format(infile))
    n = len(igrams[0])
    if n == _run_cols_for_slices:
        return True
    elif n <= _run_cols_for_full:
        return False
    else:
        raise exceptions.I2SFormatException('I2S input file ({}) had {} columns for the igram list, expected '
                                            'no more than {}'
                                            .format(infile, n, _run_cols_for_full))


def iter_i2s_input_params(fobj, include_all_lines=False):
    """
    Iterate over parameters in an I2S input file

    :param fobj: an open file handle to the input file

    :param include_all_lines: whether or not to return each line in the input file. Default is ``False``, which will
     only return lines that are input parameters. ``True``

    :return: iterator of I2S parameters. If ``include_all_lines`` is ``False``, then the returned values will be the
     parameter number, parameter subpart number, the value part of the line, and the comment part of the line. If
     ``include_all_lines`` is ``True`` then a boolean indicating whether the line is a parameter is returned as the
     fifth value.
    """
    param_num = 1
    subparam_num = 1
    curr_param_lines = _nlines_for_param(param_num)

    for line in fobj:
        if isinstance(line, bytes):
            line = line.decode('utf8', errors='replace')
        # Anything after a colon is a comment. Lines that contain nothing but white space and/or comments are
        # not parameters, so we split on the colon and check if the part before the colon has any non-whitespace
        # characters. Also do NOT split if the colon is immediately followed by a backslash - this indicates that
        # it is part of a Windows path (e.g. c:\tccon\documents).
        line = re.split(r':(?=[^\\])', line, maxsplit=1)
        value = line[0]
        if len(line) > 1:
            comment = ':'.join(line[1:])
        else:
            comment = ''

        is_param = len(value.strip()) > 0
        if include_all_lines:
            if is_param:
                pnum_to_yield = param_num
                subpnum_to_yield = subparam_num
            else:
                pnum_to_yield = -1
                subpnum_to_yield = -1
            yield pnum_to_yield, subpnum_to_yield, value, comment, is_param
        elif is_param:
            yield param_num, subparam_num, value, comment

        if is_param:
            # Check if we've completed all the parts of the parameter - some require multiple lines. This is
            # defined by the _params_with_extra_lines dictionary in this module. If we've gotten all the
            # required lines, advance the parameter number. Otherwise, advance the indicator of which part
            # of the parameter we're on.
            if subparam_num == curr_param_lines:
                param_num += 1
                subparam_num = 1
                curr_param_lines = _nlines_for_param(param_num)
            else:
                subparam_num += 1


def sort_datestr(date_strings):
    def keyfxn(dstr):
        dstr = re.search(r'\d{8}', dstr).group()
        return dt.datetime.strptime(dstr, '%Y%m%d')

    return sorted(date_strings, key=keyfxn)


def iter_target_dirs(cfg, incl_datestr=False):
    """
    Iterate over target data delivery directories.

    :param cfg: the configuration object that defines which target directories to use
    :type cfg: :class:`configobj.ConfigObj`

    :param incl_datestr: if ``False`` (default), only the path to the target directory is returned. If ``True``, then
     the site datestr (xxYYYYMMDD, e.g. ci20191008) is returned as the second value.
    :type incl_datestr: bool

    :return: iterable of target directory paths (as strings) and (if ``incl_datestr`` is ``True``) site date strings.
    """
    for site in cfg['Sites'].sections:
        for rvalues in iter_site_target_dirs(cfg['Sites'][site], incl_datestr=incl_datestr):
            yield rvalues


def iter_site_target_dirs(site_sect, incl_datestr=False, to_subdir=True):
    """
    Iterate over target data delivery directories for a single site

    :param site_sect: the section of the config that is for the desired site

    :param incl_datestr: if ``False`` (default), only the path to the target directory is returned. If ``True``, then
     the site datestr (xxYYYYMMDD, e.g. ci20191008) is returned as the second value.
    :type incl_datestr: bool

    :return: iterable of target directory paths (as strings) and (if ``incl_datestr`` is ``True``) site date strings.
    """
    for sitedate in site_sect.sections:
        root_dir = get_date_cfg_option(site_sect, sitedate, 'site_root_dir')
        if to_subdir:
            subdir = get_date_cfg_option(site_sect, sitedate, 'subdir')
            full_dir = os.path.join(root_dir, sitedate, subdir)
        else:
            full_dir = os.path.join(root_dir, sitedate)

        if incl_datestr:
            yield full_dir, sitedate
        else:
            yield full_dir


def iter_i2s_dirs(cfg, incl_datestr=False):
    """
    Iterate over batch I2S run directories

    :param cfg: the configuration object that defines which run directories to use
    :type cfg: :class:`configobj.ConfigObj`

    :param incl_datestr: if ``False`` (default), only the path to the run directory is returned. If ``True``, then the
     site datestr (xxYYYYMMDD, e.g. ci20191008) is returned as the second value.
    :type incl_datestr: bool

    :return: iterable of run directory paths (as strings) and (if ``incl_datestr`` is ``True``) site date strings.
    """
    for site in cfg['Sites'].sections:
        for rvalues in iter_site_i2s_dirs(site, cfg, incl_datestr=incl_datestr):
            yield rvalues


def iter_site_i2s_dirs(site, cfg, incl_datestr=False):
    """
    Iterate over batch I2S run directories for a specific site

    :param cfg: the configuration object that defines which run directories to use
    :type cfg: :class:`configobj.ConfigObj`

    :param incl_datestr: if ``False`` (default), only the path to the run directory is returned. If ``True``, then the
     site datestr (xxYYYYMMDD, e.g. ci20191008) is returned as the second value.
    :type incl_datestr: bool

    :return: iterable of run directory paths (as strings) and (if ``incl_datestr`` is ``True``) site date strings.
    """
    for sitedate in cfg['Sites'][site].sections:
        run_dir = date_subdir(cfg, site, sitedate)
        if incl_datestr:
            yield run_dir, sitedate
        else:
            yield run_dir


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
    path_keys = ('run_top_dir', 'site_root_dir', 'flimit_file', 'i2s_input_file')

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


def date_subdir(cfg, site, datestr):
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


def get_date_cfg_option(site_cfg, datestr, optname):
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


def get_ggg_subpath(*dir_parts, gggpath=None):
    if gggpath is None:
        gggpath = os.path.expandvars('$GGGPATH')
    return os.path.join(gggpath, *dir_parts)


def find_by_glob(pattern: str) -> str:
    """
    Find exactly one file matching a pattern.

    :param pattern: a glob-style pattern to match to find the desired file.
    :return: the path to the matching file, if found
    :raises IOError: if 0 or 2+ files found.
    """
    files = glob(pattern)
    if len(files) == 1:
        return files[0]
    else:
        raise IOError('{} files matching {} found'.format(len(files), pattern))


def change_ggg_file(gggfile, backup=False, aks='no', spts='no'):
    # Get the window from the file name, needed for the ak/spt subdirectories
    gggbname = os.path.basename(gggfile)
    window = re.search(r'^[a-z]+_\d+', gggbname).group()

    save_aks = aks != 'no'
    save_spts = spts != 'no'

    with open(gggfile, 'r') as robj:
        ggglines = robj.readlines()

    for iline, line in enumerate(ggglines):
        if '{sep}ak{sep}'.format(sep=os.sep) in line and aks != 'gggpath':
            end = '\n'  # TODO: figure out how to turn on saving AKs
            ggglines[iline] = os.path.join('.', 'ak', window, 'k') + end
        elif '{sep}spt{sep}'.format(sep=os.sep) in line and spts != 'gggpath':
            # putting a 0 at the end of the line tells it to save no spectral fits.
            end = '\n' if save_spts else ' 0 \n'
            ggglines[iline] = os.path.join('.', 'spt', window, 'z') + end

    if backup:
        shutil.copy2(gggfile, gggfile+'.orig')

    with open(gggfile, 'w') as wobj:
        wobj.writelines(ggglines)

    # Make the "ak" and "spt" subdirs just in case gfit will stop if they are missing
    run_dir = os.path.dirname(gggfile)
    ak_dir = os.path.join(run_dir, 'ak', window)
    if aks != 'gggpath' and not os.path.exists(ak_dir):
        os.makedirs(ak_dir)
    spt_dir = os.path.join(run_dir, 'spt', window)
    if spts != 'gggpath' and not os.path.exists(spt_dir):
        os.makedirs(spt_dir)


def get_num_header_lines(filename):
    """
    Get the number of header lines in a standard GGG file

    This assumes that the file specified begins with a line with two numbers: the number of header rows and the number
    of data columns.

    :param filename: the file to read
    :type filename: str

    :return: the number of header lines
    :rtype: int
    """
    with open(filename, 'r') as fobj:
        header_info = fobj.readline()

    if ',' in header_info:
        header = header_info.split(',')
    else:
        header = header_info.split()
    return int(header[0])
