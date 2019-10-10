from argparse import ArgumentParser
import datetime as dt
from glob import glob
from logging import getLogger
import os
import re
import shutil
import subprocess
import sys

from ginput.mod_maker import tccon_sites
from ginput.common_utils import mod_utils

from . import runutils
from .exceptions import GGGInputException, GGGMenuError

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


def create_runlogs_from_scratch(cfg_file, clean_spectrum_links='ask', do_slice_sites=True, do_opus_sites=True):
    """
    Create runlogs and sunruns for sites from scratch.

    Requires that there be a ??_sunrun.dat file for the site in $GGGPATH/tccon.

    :param cfg_file:
    :param clean_spectrum_links:
    :param do_slice_sites:
    :param do_opus_sites:
    :return:
    """
    # Step 1: link all site spectra files into one directory, create a single list of all of them. Add this directory
    # to both .lst files
    #
    # Step 2: Use that list to create a new sunrun for all the days
    #
    # Step 3: Use that sunrun to create a single runlog for all the days
    cfg = runutils.load_config_file(cfg_file)
    for site in cfg['Sites'].sections:
        site_cfg = cfg['Sites'][site]
        uses_slices = site_cfg['slices']

        if not do_slice_sites and uses_slices:
            logger.debug('{} uses slices and do_slice_sites is False, skipping'.format(site))
            continue
        if not do_opus_sites and not uses_slices:
            logger.debug('{} does not use slices and do_opus_sites is False, skipping'.format(site))
            continue

        site_all_spectra_dir = _link_site_spectra(site, cfg, clean_links=clean_spectrum_links)
        _add_dir_to_data_part(site_all_spectra_dir, add_to_list_data_part=True)
        try:
            list_file = _make_spectra_list(site_all_spectra_dir, site)
            sunrun_file = _create_sunrun(list_file, site)
        except GGGInputException as err:
            logger.warning('Skipping {}: {}'.format(site, err))
            continue

        runlog_file = _create_individual_date_runlogs([sunrun_file])[0]


def create_runlogs_from_delivered_sunruns(cfg_file, clean_spectrum_links='ask'):
    """
    Create runlogs from sunrun files delivered as part of OCO2 targets.

    Can only potentially be used for sites that use OPUS igrams because the numbering of spectra derived from slices
    will likely be different between the delivered sunrun and our sunrun.

    :param cfg_file:
    :param clean_spectrum_links:
    :return:
    """
    # Step 1: copy sunruns (with appropriate names) into $GGGPATH/sunruns/gnd. Remove spectra from the sunrun that we
    # didn't generate and note spectra that we generated that aren't in the sunrun
    #
    # Step 2: link each site's spectra into a single directory for all days and add that directory to
    # $GGGPATH/config/data_part.lst
    #
    # Step 3: Call create_runlog to make per-day runlogs, then concatenate each site's runlogs into a single runlog
    # (clean up the per-day runlogs). Add these runlogs to $GGGPATH/runlogs/gnd/runlogs.men file
    cfg = runutils.load_config_file(cfg_file)
    for site in cfg['Sites'].sections:
        site_cfg = cfg['Sites'][site]
        if site_cfg['slices']:
            logger.debug('{} uses slices, skipping'.format(site))
            continue

        sunrun_files = _copy_delivered_sunruns(site, cfg)
        site_all_spectra_dir = _link_site_spectra(site, cfg, clean_links=clean_spectrum_links)
        _add_dir_to_data_part(site_all_spectra_dir)
        runlogs = _create_individual_date_runlogs(sunrun_files)
        _concate_runlogs(runlogs, site)


########################################
# Helper functions for runlog creation #
########################################

def _copy_delivered_sunruns(site, cfg):
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
    site_cfg = cfg['Sites'][site]

    for target_dir, date_str in runutils.iter_site_target_dirs(site_cfg, incl_datestr=True, to_subdir=False):
        run_dir = runutils.date_subdir(cfg, site, date_str)
        spectrum_dir = os.path.join(run_dir, 'spectra')
        spectra_files = set(os.listdir(spectrum_dir))
        nspectra = len(spectra_files)
        spectra_missing = []

        try:
            delivered_sunrun = find_sunrun(target_dir)
        except GGGInputException as err:
            logger.warning('Skipping {}: {}'.format(date_str, err))
            continue

        nheader = mod_utils.get_num_header_lines(delivered_sunrun)
        new_name = date_str + '.gop'
        sunrun_files.append(new_name)

        new_name = os.path.join(sunrun_dir, new_name)

        # Copy line by line, checking if the spectrum file listed is available in our new spectra directory
        nsunrun_spec = 0
        with open(delivered_sunrun, 'r') as robj, open(new_name, 'w') as wobj:
            for i, line in enumerate(robj):
                if i < nheader:
                    wobj.write(line)
                else:
                    line_spec_file = line.split()[0]
                    nsunrun_spec += 1
                    if line_spec_file in spectra_files:
                        spectra_files.remove(line_spec_file)
                        wobj.write(line)
                    else:
                        spectra_missing.append(line_spec_file)

        if len(spectra_missing) > 0:
            msg = '{}: {}/{} spectra included in the sunrun were missing from {}:\n  * {}'.format(
                date_str, len(spectra_missing), nsunrun_spec, spectrum_dir, '\n  * '.join(spectra_missing)
            )
            logger.debug(msg)
        if len(spectra_files) > 0:
            msg = '{}: {}/{} spectra present in {} were not listed in the sunrun:\n  * {}'.format(
                date_str, len(spectra_files), nspectra, spectrum_dir, '\n  * '.join(spectra_files)
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
            user_response = input('Links already exist in {}. Remove them [yN]: '.format(link_dir))
            clean_links = user_response.lower() == 'y'

        if clean_links:
            for f in links_present:
                os.remove(f)

    for site_dir in runutils.iter_site_i2s_dirs(site, cfg):
        site_spectrum_dir = os.path.join(site_dir, 'spectra')
        site_spectra_files = os.listdir(site_spectrum_dir)
        for spectrum in site_spectra_files:
            link_name = os.path.join(link_dir, spectrum)
            source_name = os.path.join(site_spectrum_dir, spectrum)
            #logger.debug('Linking {} -> {}'.format(link_name, source_name))
            n_not_linked = 0
            if not os.path.exists(link_name):
                os.symlink(source_name, link_name)
            else:
                n_not_linked += 1

        if n_not_linked > 0:
            logger.info('{} files not linked to {} because they already exist'.format(n_not_linked, link_dir))

    return link_dir


def _add_dir_to_data_part(new_dir, add_to_list_data_part=False):
    def add_line_to_file(filename):
        nonlocal new_dir
        if not new_dir.endswith(os.sep):
            new_dir += os.sep

        with open(filename, 'r') as robj:
            for line in robj:
                if line.strip() == new_dir:
                    logger.debug('{} already contains {}, not adding'.format(filename, new_dir))
                    return

        with open(filename, 'a') as wobj:
            if not re.search('[\\r\\n]$', line):
                # ensure there's a newline at the end of the file so that we don't add our new directory to an
                # existing line
                wobj.write('\n')
            logger.debug('Adding {} to {}'.format(new_dir, filename))
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
    runlog_dir = runutils.get_ggg_subpath('runlogs', 'gnd')
    combined_runlog = runutils.get_ggg_subpath('runlogs', 'gnd', '{}_targets.grl'.format(site_id))
    first_runlog = True
    with open(combined_runlog, 'w') as wobj:
        for this_runlog in runlogs:
            this_runlog = os.path.join(runlog_dir, this_runlog)
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

    if len(ingaas_spectra) == 0:
        raise GGGInputException('No spectra in {}'.format(all_spectra_dir))

    # These should already be in chronological order, so we just need to interleave the "b" detector files
    # The shell script on the wiki doesn't check that the b files exist so I'm going to assume that there should
    # always be a "b" file for every "a" file for now.
    with open(list_file, 'w') as wobj:
        for spectrum in ingaas_spectra:
            spectrum = os.path.basename(spectrum)
            wobj.write(spectrum + '\n')
            b_spectrum = re.sub(r'a(?=\.)', 'b', spectrum)
            if os.path.exists(os.path.join(all_spectra_dir, b_spectrum)):
                wobj.write(b_spectrum + '\n')

    return list_file


##################
# Running gsetup #
##################

def run_gsetup(cfg_file, overwrite=False):
    # 1. Loop through the sites (not the dates - we've made each site a single runlog)
    # 2. Make a gfit-exec directory in that site's run directory. Clear out if exists.
    # 3. Run gsetup in that directory, passing in the answers via Popen.communicate so that it executes automatically

    cfg = runutils.load_config_file(cfg_file)
    level_menu_number = _get_menu_number(runutils.get_ggg_subpath('levels', 'levels.men'), 'ap_51_level_0_to_70km.gnd')

    for site in cfg['Sites'].sections:
        run_top_dir = cfg['Run']['run_top_dir']
        site_top_dir = os.path.join(run_top_dir, site)
        gfit_exec_dir = os.path.join(site_top_dir, 'gfit-exec')
        if os.path.exists(gfit_exec_dir):
            if not overwrite:
                logger.warning('Not running for {} because gfit-exec already exists'.format(site))
                continue
            else:
                logger.warning('Deleting {}'.format(gfit_exec_dir))
                shutil.rmtree(gfit_exec_dir)

        os.mkdir(gfit_exec_dir)
        logger.info('Running gsetup in {}'.format(gfit_exec_dir))
        try:
            _run_one_gsetup(exec_dir=gfit_exec_dir, site=site, level_menu_number=level_menu_number)
        except GGGMenuError as err:
            logger.warning('Skipping {}: {}'.format(site, err))


def _run_one_gsetup(exec_dir, site, level_menu_number):
    gsetup_exec = runutils.get_ggg_subpath('bin', 'gsetup')
    runlog_name = '{}_targets.grl'.format(site)
    runlog_menu_number = _get_menu_number(runutils.get_ggg_subpath('runlogs', 'gnd', 'runlogs.men'), runlog_name)
    # The answers to gsetup's questions: geometry (g = ground), runlog, levels, windows (1 = tccon), and standard
    # TCCON processing (y = yes, use the FPIT .vmr/.mod files)
    gsetup_answers = 'g\n{rl}\n{lev}\n1\ny\n'.format(rl=runlog_menu_number, lev=level_menu_number).encode('ascii')
    proc = subprocess.Popen([gsetup_exec], cwd=exec_dir, stdin=subprocess.PIPE)
    proc.communicate(gsetup_answers)
    proc.wait()


def _get_menu_number(menu_file, menu_value):
    with open(menu_file, 'r') as mobj:
        for line_num, line in enumerate(mobj):
            if line_num == 0:
                # first line is always a header
                continue

            elif menu_value in line:
                return line_num

    raise GGGMenuError('Could not find a line matching "{}" in the {} menu'.format(
        menu_value, os.path.basename(menu_file)
    ))


########################
# Command-line parsing #
########################

def parse_prior_infile_args(parser: ArgumentParser):
    parser.description = 'Generate PyAutoMod input files for the desired target dates specified in a config file'
    parser.add_argument('cfg_file', help='The I2S/GFIT-run config file that specifies the target sites and dates to run')
    parser.add_argument('output_path', help='Path to write the new PyAutoMod input files to. By default, files will '
                                            'be overwritten.')
    parser.add_argument('email', help='Email to use for the contact email in the input file')
    parser.add_argument('-k', '--no-overwrite', dest='overwrite', action='store_false',
                        help='Do not overwrite existing input file.')
    parser.set_defaults(driver_fxn=make_automod_input_files)


def parse_scratch_runlog_args(parser: ArgumentParser):
    parser.description = 'Generate runlogs for targets that use slices'
    parser.add_argument('cfg_file', help='Config file that specifies the sites to make runlogs for. May include sites '
                                         'that do not use slices, they will be skipped.')
    parser.add_argument('--clean-links', action='store_true', dest='clean_spectrum_links',
                        help='If the directory that all the site spectra are to be linked to exists and has links '
                             'already, this will cause the links to be deleted. The default behavior is to ask.')
    parser.add_argument('--no-clean-links', action='store_false', dest='clean_spectrum_links',
                        help='If the directory that all the site spectra are to be linked to exists and has links '
                             'already, it will create any missing links.')
    parser.add_argument('--no-slice-sites', dest='do_slice_sites', action='store_false',
                        help='Do not run sites that use slices')
    parser.add_argument('--no-opus-sites', dest='do_opus_sites', action='store_false',
                        help='Do not run sites that use opus igrams')
    parser.set_defaults(driver_fxn=create_runlogs_from_scratch, clean_spectrum_links='ask')


def parse_delivered_runlog_args(parser: ArgumentParser):
    parser.description = 'Generate runlogs from delivered sunruns'
    parser.add_argument('cfg_file', help='Config file that specifies the sites to make runlogs for. May include sites '
                                         'that do use slices, they will be skipped.')
    parser.add_argument('--clean-links', action='store_true', dest='clean_spectrum_links',
                        help='If the directory that all the site spectra are to be linked to exists and has links '
                             'already, this will cause the links to be deleted. The default behavior is to ask.')
    parser.add_argument('--no-clean-links', action='store_false', dest='clean_spectrum_links',
                        help='If the directory that all the site spectra are to be linked to exists and has links '
                             'already, it will create any missing links.')
    parser.set_defaults(driver_fxn=create_runlogs_from_delivered_sunruns, clean_spectrum_links='ask')


def parse_gsetup_args(parser: ArgumentParser):
    parser.description = 'Run gsetup for all the target sites'
    parser.add_argument('cfg_file', help='Config file that specifies the sites to run gsetup for')
    parser.add_argument('-o', '--overwrite', action='store_true',
                        help='If the directory where gsetup needs to execute exists, overwrite it. Be careful, '
                             'since this will wipe out previous gfit runs done in that directory.')
    parser.set_defaults(driver_fxn=run_gsetup)


def parse_gfit_args(parser: ArgumentParser):
    subp = parser.add_subparsers()
    make_infiles = subp.add_parser('make-request-files', aliases=['mrf'], help='Make PyAutoMod request files')
    parse_prior_infile_args(make_infiles)

    make_slice_runlogs = subp.add_parser('make-runlogs', aliases=['makerun'], help='Make sunruns and runlogs from '
                                                                                   'scratch.')
    parse_scratch_runlog_args(make_slice_runlogs)

    make_opus_runlogs = subp.add_parser('make-runlogs-from-delivered', aliases=['makerundel'],
                                        help='Make runlogs from delivered sunruns.')
    parse_delivered_runlog_args(make_opus_runlogs)

    run_gsetup_p = subp.add_parser('run-gsetup', help='Run gsetup for target sites')
    parse_gsetup_args(run_gsetup_p)
