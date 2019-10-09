from argparse import ArgumentParser
import datetime as dt
from glob import glob
from logging import getLogger
import os
import re
import subprocess
import sys

from ginput.mod_maker import tccon_sites
from ginput.common_utils import mod_utils

from . import runutils
from .exceptions import GGGInputException, GGGLinkingException

logger = getLogger('gfitrun')

_sunrun_header_lines = 4


# Ensure that we do not use Python 2 style input
if sys.version_info.major < 3:
    input = raw_input


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


def create_slice_target_site_runlogs(cfg_file, clean_spectrum_links='ask'):
    # Step 1: link all site spectra files into one directory, create a single list of all of them. Add this directory
    # to both .lst files
    #
    # Step 2: Use that list to create a new sunrun for all the days
    #
    # Step 3: Use that sunrun to create a single runlog for all the days
    cfg = runutils.load_config_file(cfg_file)
    for site, site_cfg in cfg['Sites'].sections.items():
        if not site_cfg['slices']:
            logger.debug('{} does not use slices, skipping'.format(site))
            continue

        site_all_spectra_dir = _link_site_spectra(site, cfg, clean_links=clean_spectrum_links)
        _add_dir_to_data_part(site_all_spectra_dir, add_to_list_data_part=True)
        list_file = _make_spectra_list(site_all_spectra_dir, site)
        sunrun_file = _create_sunrun(list_file, site)
        runlog_file = _create_individual_date_runlogs([sunrun_file])[0]


def create_opus_target_site_runlogs(cfg_file, clean_spectrum_links='ask'):
    # Step 1: copy sunruns (with appropriate names) into $GGGPATH/sunruns/gnd. Remove spectra from the sunrun that we
    # didn't generate and note spectra that we generated that aren't in the sunrun
    #
    # Step 2: link each site's spectra into a single directory for all days and add that directory to
    # $GGGPATH/config/data_part.lst
    #
    # Step 3: Call create_runlog to make per-day runlogs, then concatenate each site's runlogs into a single runlog
    # (clean up the per-day runlogs). Add these runlogs to $GGGPATH/runlogs/gnd/runlogs.men file
    cfg = runutils.load_config_file(cfg_file)
    for site, site_cfg in cfg['Sites'].sections.items():
        if site_cfg['slices']:
            logger.debug('{} uses slices, skipping'.format(site))
            continue

        sunrun_files = _copy_delivered_sunruns(site_cfg)
        site_all_spectra_dir = _link_site_spectra(site, cfg, clean_links=clean_spectrum_links)
        _add_dir_to_data_part(site_all_spectra_dir)
        runlogs = _create_individual_date_runlogs(sunrun_files)
        _concate_runlogs(runlogs, site)


def _copy_delivered_sunruns(site_cfg):
    def find_sunrun(tar_dir):
        possible_files = glob(os.path.join(tar_dir, '*.gop'))
        if len(possible_files) == 1:
            return possible_files[0]
        elif len(possible_files) == 0:
            raise GGGInputException('No sunrun (.gop) file found in {}'.format(tar_dir))
        else:
            raise GGGInputException('Multiple sunrun (.gop) files found in {}'.format(tar_dir))

    sunrun_dir = runutils.get_ggg_subpath('sunruns', 'gnd')
    sunrun_files = []

    for target_dir, date_str in runutils.iter_site_target_dirs(site_cfg, incl_datestr=True):
        spectrum_dir = os.path.join(target_dir, 'spectra')
        spectra_files = set(os.listdir(spectrum_dir))
        spectra_missing = []

        delivered_sunrun = find_sunrun(target_dir)
        nheader = mod_utils.get_num_header_lines(delivered_sunrun)
        new_name = date_str + '.gop'
        sunrun_files.append(new_name)

        new_name = os.path.join(sunrun_dir, new_name)

        # Copy line by line, checking if the spectrum file listed is available in our new spectra directory
        with open(delivered_sunrun, 'r') as robj, open(new_name, 'w') as wobj:
            for i, line in enumerate(robj):
                if i < nheader:
                    wobj.write(line)
                else:
                    line_spec_file = line.split()[0]
                    if line_spec_file in spectra_files:
                        spectra_files.remove(line_spec_file)
                        wobj.write(line)
                    else:
                        spectra_missing.append(line_spec_file)

        if len(spectra_missing) > 0:
            msg = '{}: {} spectra included in the sunrun were missing from {}:\n  * {}'.format(
                date_str, len(spectra_missing), spectrum_dir, '\n  * '.join(spectra_missing)
            )
            logger.debug(msg)
        if len(spectra_files) > 0:
            msg = '{}: {} spectra present in {} were not listed in the sunrun:\n  * {}'.format(
                date_str, len(spectra_files), spectrum_dir, '\n  * '.join(spectra_missing)
            )
            logger.debug(msg)

    return sunrun_files


def _link_site_spectra(site, cfg, clean_links='ask'):
    site_top_dir = os.path.join(cfg['Run']['run_top_dir'], site)
    link_dir = os.path.join(site_top_dir, 'all-spectra')
    if not os.path.exists(link_dir):
        os.mkdir(link_dir)

    links_present = glob(os.path.join(link_dir, '*'))
    if len(links_present) > 0:
        if clean_links == 'ask':
            user_response = input('Links already exist in {}. Remove them [yN]: ')
            clean_links = user_response.lower() == 'y'

        if clean_links:
            for f in links_present:
                os.remove(f)
        else:
            raise GGGLinkingException('Links already exist in {} and clean_links was False'.format(link_dir))

    for site_dir in runutils.iter_site_i2s_dirs(site, cfg):
        site_spectrum_dir = os.path.join(site_dir, 'spectra')
        site_spectra_files = os.listdir(site_spectrum_dir)
        for spectrum in site_spectra_files:
            link_name = os.path.join(link_dir, spectrum)
            source_name = os.path.join(site_spectrum_dir, spectrum)
            logger.debug('Linking {} -> {}'.format(link_name, source_name))
            os.symlink(source_name, link_name)

    return link_dir


def _add_dir_to_data_part(new_dir, add_to_list_data_part=False):
    def add_line_to_file(filename):
        with open(data_part, 'r') as robj:
            for line in robj:
                if line.strip() == new_dir:
                    logger.debug('data_part.lst already contains {}, not adding'.format(new_dir))
                    return

        with open(filename, 'a') as wobj:
            if not re.search('[\\r\\n]$', line):
                # ensure there's a newline at the end of the file so that we don't add our new directory to an
                # existing line
                wobj.write('\n')
            logger.debug('Adding {} to data_part.lst'.format(new_dir))
            wobj.write(new_dir + '\n')

    data_part = runutils.get_ggg_subpath('config', 'data_part.lst')
    list_data_part = runutils.get_ggg_subpath('config', 'data_part_list_maker.lst')

    add_line_to_file(data_part)
    if add_to_list_data_part:
        add_line_to_file(list_data_part)


def _create_sunrun(spectrum_list_file, site):
    sunrun_dat_file = runutils.get_ggg_subpath('tccon', '{}_sunrun.dat'.format(site))
    if not os.path.exists(sunrun_dat_file):
        raise GGGInputException('{} is required. Please add it to the $GGGPATH/tccon directory'.format(sunrun_dat_file))
    list_dir = os.path.dirname(spectrum_list_file)
    list_file = os.path.basename(spectrum_list_file)
    sunrun_cmd = runutils.get_ggg_subpath('bin', 'create_sunrun')
    subprocess.check_call([sunrun_cmd, list_file], cwd=list_dir)
    return list_file.replace('.gnd', '.gop')


def _create_individual_date_runlogs(sunruns, delete_sunruns=False):
    runlogs = []
    create_runlog = runutils.get_ggg_subpath('bin', 'create_runlog')
    for this_sunrun in sunruns:
        logger.info('Creating runlog from {}'.format(this_sunrun))
        subprocess.check_call([create_runlog, this_sunrun])
        runlogs.append(this_sunrun.replace('.gop', '.grl'))
        if delete_sunruns:
            logger.info('Deleting {}'.format(this_sunrun))
            os.remove(this_sunrun)

    return runlogs


def _concate_runlogs(runlogs, site_id, delete_date_runlogs=False):
    combined_runlog = runutils.get_ggg_subpath('runlogs', 'gnd', '{}_targets.grl'.format(site_id))
    first_runlog = True
    with open(combined_runlog, 'w') as wobj:
        for this_runlog in runlogs:
            nheader = mod_utils.get_num_header_lines(this_runlog)
            with open(this_runlog, 'r') as robj:
                for line_num, line in enumerate(robj):
                    if line_num >= nheader or first_runlog:
                        wobj.write(line)
            first_runlog = False
            if delete_date_runlogs:
                logger.info('Deleting {}'.format(this_runlog))
                os.remove(this_runlog)


def _make_spectra_list(all_spectra_dir, site):
    list_dir = runutils.get_ggg_subpath('lists')
    if not os.path.exists(list_dir):
        os.mkdir(list_dir)
    list_file = os.path.join(list_dir, '{}_targets.gnd'.format(site))

    # the s means solar (avoids lamp runs), the a means the InGaAs detector
    ingaas_spectra = sorted(glob(os.path.join(all_spectra_dir, '??????????s????a.*')))
    spectrum_noext = ingaas_spectra[0].split('.')[0]
    first_spectrum = spectrum_noext + '.001'
    last_spectrum = spectrum_noext[:-1] + 'b.999'

    make_list_cmd = runutils.get_ggg_subpath('bin', 'list_maker')
    proc = subprocess.Popen([make_list_cmd], cwd=list_dir)
    proc.communicate('{}\n{}\n'.format(first_spectrum, last_spectrum))
    proc.wait()

    os.rename(os.path.join(list_dir, 'list_maker.out'), list_file)
    return list_file


def parse_prior_infile_args(parser: ArgumentParser):
    parser.description = 'Generate PyAutoMod input files for the desired target dates specified in a config file'
    parser.add_argument('cfg_file', help='The I2S/GFIT-run config file that specifies the target sites and dates to run')
    parser.add_argument('output_path', help='Path to write the new PyAutoMod input files to. By default, files will '
                                            'be overwritten.')
    parser.add_argument('email', help='Email to use for the contact email in the input file')
    parser.add_argument('-k', '--no-overwrite', dest='overwrite', action='store_false',
                        help='Do not overwrite existing input file.')
    parser.set_defaults(driver_fxn=make_automod_input_files)


def parse_slice_runlog_args(parser: ArgumentParser):
    parser.description = 'Generate runlogs for targets that use slices'
    parser.add_argument('cfg_file', help='Config file that specifies the sites to make runlogs for. May include sites '
                                         'that do not use slices, they will be skipped.')
    parser.add_argument('--clean-links', action='store_true', dest='clean_spectrum_links',
                        help='If the directory that all the site spectra are to be linked to exists and has links '
                             'already, this will cause the links to be deleted. The default behavior is to ask.')
    parser.add_argument('--no-clean-links', action='store_false', dest='clean_spectrum_links',
                        help='If the directory that all the site spectra are to be linked to exists and has links '
                             'already, this will cause the job to abort.')
    parser.set_defaults(driver_fxn=create_slice_target_site_runlogs, clean_spectrum_links='ask')


def parse_opus_runlog_args(parser: ArgumentParser):
    parser.description = 'Generate runlogs for targets that use opus igrams'
    parser.add_argument('cfg_file', help='Config file that specifies the sites to make runlogs for. May include sites '
                                         'that do use slices, they will be skipped.')
    parser.add_argument('--clean-links', action='store_true', dest='clean_spectrum_links',
                        help='If the directory that all the site spectra are to be linked to exists and has links '
                             'already, this will cause the links to be deleted. The default behavior is to ask.')
    parser.add_argument('--no-clean-links', action='store_false', dest='clean_spectrum_links',
                        help='If the directory that all the site spectra are to be linked to exists and has links '
                             'already, this will cause the job to abort.')
    parser.set_defaults(driver_fxn=create_opus_target_site_runlogs, clean_spectrum_links='ask')


def parse_gfit_args(parser: ArgumentParser):
    subp = parser.add_subparsers()
    make_infiles = subp.add_parser('make-request-files', aliases=['mrf'], help='Make PyAutoMod request files')
    parse_prior_infile_args(make_infiles)

    make_slice_runlogs = subp.add_parser('make-slice-runlogs', aliases=['msr'], help='Make runlogs for slice sites')
    parse_slice_runlog_args(make_slice_runlogs)

    make_opus_runlogs = subp.add_parser('make-opus-runlogs', aliases=['mor'], help='Make runlogs for igram sites')
    parse_slice_runlog_args(make_opus_runlogs)
