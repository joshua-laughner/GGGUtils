from glob import glob
import os
import re

from . import runutils


def tabulate_targets(out_file, target_dirs, dirs_list=None):
    """
    Create a .csv file indicating which data revisions for target data contain which dates

    :param out_file: the filename to give the .csv file
    :type out_file: str

    :param target_dirs: a list of target directories to search. These are directories that contain subdirectories named
     ``xxYYYYMMDD`` where "xx" is the site abbreviation and YYYYMMDD the year, month, and day.
    :type target_dirs: list(str)

    :param dirs_list: a path to a text file that lists one target directory per line. These will be added to the end of
     the ``target_dirs`` list. If ``None``, then no file is read.
    :type dirs_list: str or None

    :return: none, writes a .csv file
    """
    target_dirs = runutils.finalize_target_dirs(target_dirs, dirs_list=dirs_list)

    with open(out_file, 'w') as wobj:
        for tdir in target_dirs:
            tname = os.path.basename(tdir.rstrip(os.sep))
            date_dict = build_target_date_dict(tdir)
            rev_names = list_revisions(date_dict)

            wobj.write(tname + '\n')
            wobj.write('Date,top dir,' + ','.join(rev_names[1:]) + '\n')
            for dstr in sorted(date_dict.keys()):
                line = dstr + ',' + ','.join('x' if date_dict[dstr][r] else ' ' for r in rev_names)
                wobj.write(line + '\n')

            wobj.write('\n')


def build_target_dirs_dict(target_dirs, dirs_list=None, key_by_basename=True, **kwargs):
    """
    Build a dictionary listing the available revisions for each target directory specified.

    :param target_dirs: a list of target directories to search. These are directories that contain subdirectories named
     ``xxYYYYMMDD`` where "xx" is the site abbreviation and YYYYMMDD the year, month, and day.
    :type target_dirs: list(str)

    :param dirs_list: a path to a text file that lists one target directory per line. These will be added to the end of
     the ``target_dirs`` list. If ``None``, then no file is read.
    :type dirs_list: str or None

    :param key_by_basename: make the keys of the resulting dictionary just the bottom directory of the target dir (e.g.
     :file:`/data/tccon/Ascension` would be just "Ascension"). If ``False``, then the full directory path is used.
    :type key_by_basename: bool

    :param kwargs: additional keyword arguments, passed through to :func:`build_target_date_dict`

    :return: a dictionary of dictionaries where the top keys are the site name or path, and the next keys are the date
     strings (in YYYYMMDD format). If ``flat`` is ``False``, the final dictionary will have the revisions as keys and
     the values will be ``True`` if that date is available in that revision, ``False`` otherwise. If ``flat`` is
     ``True``, then the dictionary's values will just be strings indicating the most recent revision to include that
     date. This assumes that the top directory is always the most recent revision.
    :rtype: dict
    """
    if isinstance(target_dirs, str):
        raise TypeError('target_dirs must not be a single directory as a string, it must be a collection. If you want '
                        'to build a dictionary for a single directory, consider using build_target_date_dict.')
    target_dirs = runutils.finalize_target_dirs(target_dirs, dirs_list)
    target_dict = dict()
    for this_tdir in target_dirs:
        key = os.path.basename(this_tdir.rstrip(os.sep)) if key_by_basename else this_tdir
        target_dict[key] = build_target_date_dict(this_tdir, **kwargs)
    return target_dict


def build_target_date_dict(target_dir, flat=False, full_datestr=False):
    """
    Build a dictionary describing which dates are contained in which revisions for a given target.

    :param target_dir: the target directory (contains subdirectories named xxYYYYMMDD).
    :type target_dir: str

    :param flat: set to ``True`` to return a flat (one-layer) dictionary. See ``return`` for more details.
    :type flat: bool

    :param full_datestr: set to ``True`` to keep the full date string (including the site abbreviation) in the dict
     keys. ``False`` (default) only keeps the date part.
    :type full_datestr: bool

    :return: if ``flat`` is ``False``, the returned dictionary will be two levels: the first will have the date strings
     (in YYYYMMDD format) as keys, the second will have the revisions as keys and the values will be ``True`` if that
     date is available in that revision, ``False`` otherwise. If ``flat`` is ``True``, then the dictionary will have the
     same date strings as keys, but the values will just be strings indicating the most recent revision to include that
     date. This assumes that the top directory is always the most recent revision.
    :rtype: dict
    """
    rev_dirs = ['.'] + glob(os.path.join(target_dir, 'R?'))
    rev_names = [os.path.basename(r) for r in rev_dirs]
    date_re = re.compile(r'\w\w\d{8}') if full_datestr else re.compile(r'(?<=\w\w)\d{8}')
    date_dict = dict()
    for rdir, rname in zip(rev_dirs, rev_names):
        date_dirs = glob(os.path.join(target_dir, rname, '*'))
        for ddir in date_dirs:
            datestr = date_re.search(os.path.basename(ddir))
            if datestr is None:
                # not a date dir
                continue

            datestr = datestr.group()
            if datestr not in date_dict:
                date_dict[datestr] = {r: False for r in rev_names}
            date_dict[datestr][rname] = True

    if flat:
        date_dict = flatten_target_dir_dict(date_dict)
    return date_dict


def flatten_target_dir_dict(date_dict):
    """
    Flatten a dictionary from two levels (date, revision) to one (date with most recent revision).

    :param date_dict: the dictionary to flatten.
    :type date_dict: dict

    :return: the flattened dictionary.
    """
    flat_dict = dict()

    for datestr, rev_dict in date_dict.items():
        flat_dict[datestr] = None
        ordered_revisions = sort_rev_names(rev_dict.keys(), highest_first=True)
        for rev in ordered_revisions:
            if rev_dict[rev]:
                flat_dict[datestr] = rev
                break

    return flat_dict


def list_revisions(date_dict, sort_highest_first=True):
    """
    List the revisions present in the given dictionary.

    :param date_dict: dictionary with date strings as keys, and sub-dictionaries with revisions as keys.
    :type date_dict: dict

    :param sort_highest_first: by default, the revisions are returned listed most recent to oldest. Set this to
     ``False`` to reverse that order. Note that the top directory is *always* listed first if present.
    :type sort_highest_first: bool

    :return: list of revision names ('.' for top directory) sorted.
    :rtype: list(str)
    """
    revisions = set()
    for subdict in date_dict.values():
        revisions.add(subdict.keys())

    return sort_rev_names(list(revisions), highest_first=sort_highest_first)


def sort_rev_names(rev_names, highest_first=True):
    """
    Sort a collection of revision names.

    :param rev_names: the collection of revision names. Use '.' for the top directory.

    :param highest_first: by default, the revisions are returned listed most recent to oldest. Set this to
     ``False`` to reverse that order. Note that the top directory is *always* listed first if present.
    :type highest_first: bool

    :return: the sorted revision names.
    :rtype: list(str)
    """
    def rev_key(rev):
        if rev == '.':
            return -9999

        rind = int(re.search(r'\d+', rev).group())
        if not highest_first:
            rind *= -1

        return rind

    return sorted(rev_names, key=rev_key)


def parse_tab_args(parser):
    parser.description = 'Create a .csv file tabulating the available dates for targets in different revisions'
    parser.add_argument('out_file', help='Path to write the .csv file out to')
    parser.add_argument('target_dirs', nargs='*', help='The target directories to tabulate. Must contain xxYYYYMMDD '
                                                       'subdirectories, where xx is the site abbreviaton and YYYYMMDD '
                                                       'is the date of the target.')
    parser.add_argument('-f', '--dirs-list', help='File listing target directories to parse')
    parser.set_defaults(driver_fxn=tabulate_targets)
