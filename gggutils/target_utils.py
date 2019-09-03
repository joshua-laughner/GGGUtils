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
            rev_dirs = ['.'] + glob(os.path.join(tdir, 'R?'))
            rev_names = [os.path.basename(r) for r in rev_dirs]
            date_dict = dict()
            for rdir, rname in zip(rev_dirs, rev_names):
                date_dirs = glob(os.path.join(tdir, rdir, '*'))
                for ddir in date_dirs:
                    datestr = re.search(r'(?<=\w\w)\d{8}', os.path.basename(ddir))
                    if datestr is None:
                        # not a date dir
                        continue

                    datestr = datestr.group()
                    if datestr not in date_dict:
                        date_dict[datestr] = {r: False for r in rev_names}
                    date_dict[datestr][rname] = True

            wobj.write(tname + '\n')
            wobj.write('Date,top dir,' + ','.join(rev_names[1:]) + '\n')
            for dstr in sorted(date_dict.keys()):
                line = dstr + ',' + ','.join('x' if date_dict[dstr][r] else ' ' for r in rev_names)
                wobj.write(line + '\n')

            wobj.write('\n')


def parse_tab_args(parser):
    parser.description = 'Create a .csv file tabulating the available dates for targets in different revisions'
    parser.add_argument('out_file', help='Path to write the .csv file out to')
    parser.add_argument('target_dirs', nargs='*', help='The target directories to tabulate. Must contain xxYYYYMMDD '
                                                       'subdirectories, where xx is the site abbreviaton and YYYYMMDD '
                                                       'is the date of the target.')
    parser.add_argument('-f', '--dirs-list', help='File listing target directories to parse')
    parser.set_defaults(driver_fxn=tabulate_targets)
