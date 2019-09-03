import re
import shutil
import tempfile

from . import _run_cols_for_full, _run_cols_for_slices
from . import exceptions


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
    if dirs_list is not None:
        with open(dirs_list, 'r') as robj:
            extra_dirs = [l.strip() for l in robj.readlines()]
        target_dirs.extend(extra_dirs)
    return target_dirs


def modify_i2s_input_params(filename, *args, new_file=None):
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

    :return: None, writes new file.
    """
    if new_file is None:
        new_file = filename
    i2s_params = _mod_i2s_args_parsing(args)

    # We'll always write the changes to a temporary file first. That way we keep the code simple, and just which file
    # it gets copied to changes
    with tempfile.NamedTemporaryFile('w') as tfile:
        with open(filename, 'r') as robj, open(tfile.name, 'w') as wobj:
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
        # Anything after a colon is a comment. Lines that contain nothing but white space and/or comments are
        # not parameters, so we split on the colon and check if the part before the colon has any non-whitespace
        # characters.
        line = line.split(':')
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
