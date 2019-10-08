from argparse import ArgumentParser
import datetime as dt
from logging import getLogger
import os

from ginput.mod_maker import tccon_sites

from . import runutils

logger = getLogger('gfitrun')


def make_automod_input_files(cfg_file, output_path, email, overwrite=True):
    """
    Creates input files suitable for PyAutoMod for the priors needed to run GFIT on the desired data

    :return:
    """
    if not os.path.isdir(output_path):
        raise IOError('output_path ({}) does not exist'.format(output_path))

    cfg = runutils.load_config_file(cfg_file)
    input_file_date_fmt = '%Y%m%d'
    for _, site_datestr in runutils.iter_i2s_dirs(cfg, incl_datestr=True):
        site_id, datestr = site_datestr[:2], site_datestr[2:]
        target_date = dt.datetime.strptime(datestr, '%Y%m%d')

        # We need to deal with the fact that sites east of the prime meridian need priors for the UTC date before the
        # actual target date and sites west of it need priors for the UTC date after the target date, both in addition
        # to the target date itself. For example, midnight at Caltech on 2019-10-08 is 8a 2019-10-08 UTC, so midnight to
        # midnight in LA requires profiles from 2019-10-08 and 2019-10-09 in UTC.
        site_info = tccon_sites.tccon_site_info_for_date(target_date, site_abbrv=site_id)
        if site_info['lon_180'] >= 0:
            start_date = target_date - dt.timedelta(days=1)
            end_date = target_date
        else:
            start_date = target_date
            end_date = target_date + dt.timedelta(days=1)

        start_date = start_date.strftime(input_file_date_fmt)
        # advance the end date by 1 since it is exclusive in PyAutoMod
        end_date = (end_date + dt.timedelta(days=1)).strftime(input_file_date_fmt)

        # Now we can just write the file
        input_filename = os.path.join(output_path, 'input_{}.txt'.format(site_datestr))
        if not overwrite and os.path.exists(input_filename):
            logger.warning('Not creating prior input file for {}, specified path ({}) already exists'
                           .format(site_datestr, input_filename))
        else:
            logger.info('Writing prior input file for {} at {}'.format(site_datestr, input_filename))
            with open(input_filename, 'w') as wobj:
                wobj.write(site_id + '\n')
                wobj.write(start_date + '\n')
                wobj.write(end_date + '\n')
                wobj.write(str(site_info['lat']) + '\n')
                wobj.write(str(site_info['lon']) + '\n')
                wobj.write(email)


def parse_prior_infile_args(parser: ArgumentParser):
    parser.description = 'Generate PyAutoMod input files for the desired target dates specified in a config file'
    parser.add_argument('cfg_file', help='The I2S/GFIT-run config file that specifies the target sites and dates to run')
    parser.add_argument('output_path', help='Path to write the new PyAutoMod input files to. By default, files will '
                                            'be overwritten.')
    parser.add_argument('email', help='Email to use for the contact email in the input file')
    parser.add_argument('-k', '--no-overwrite', dest='overwrite', action='store_false',
                        help='Do not overwrite existing input file.')
    parser.set_defaults(driver_fxn=make_automod_input_files)


def parse_gfit_args(parser: ArgumentParser):
    subp = parser.add_subparsers()
    make_infiles = subp.add_parser('make-request-files', aliases=['mrf'], help='Make PyAutoMod request files')
    parse_prior_infile_args(make_infiles)
