from glob import glob
import os
import re


def tabulate_targets(out_file, target_dirs):
    with open(out_file, 'w') as wobj:
        for tdir in target_dirs:
            tname = os.path.basename(tdir.rstrip(os.sep))
            rev_dirs = ['.'] + glob(os.path.join(tdir, 'R?'))
            date_dict = dict()
            for rdir in rev_dirs:
                date_dirs = glob(os.path.join(tdir, rdir, '*'))
                for ddir in date_dirs:
                    datestr = re.search(r'(?<=\w\w)\d{8}', os.path.basename(ddir))
                    if datestr is None:
                        # not a date dir
                        continue

                    datestr = datestr.group()
                    if datestr not in date_dict:
                        date_dict[datestr] = {r: False for r in rev_dirs}
                    date_dict[datestr][rdir] = True

            wobj.write(tname + '\n')
            wobj.write('Date,top dir,' + ','.join(rev_dirs[1:]))
            for dstr in sorted(date_dict.keys()):
                line = dstr + ',' + ','.join('x' if date_dict[dstr][r] else ' ' for r in rev_dirs)
                wobj.write(line + '\n')

            wobj.write('\n')


def parse_tab_args(parser):
    parser.description = 'Create a .csv file tabulating the available dates for targets in different revisions'
    parser.add_argument('out_file', help='Path to write the .csv file out to')
    parser.add_argument('target_dirs', nargs='+', help='The target directories to tabulate. Must contain xxYYYYMMDD '
                                                       'subdirectories, where xx is the site abbreviaton and YYYYMMDD '
                                                       'is the date of the target.')
    parser.set_defaults(driver_fxn=tabulate_targets)
