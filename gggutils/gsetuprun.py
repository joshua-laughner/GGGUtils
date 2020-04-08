from argparse import ArgumentParser
from logging import getLogger
import os
from . import runutils as utils

logger = getLogger('gsetuprun')

def change_many_ggg_files(gggfiles, pdb=False, **options):
    if pdb:
        import pdb
        pdb.set_trace()

    if isinstance(gggfiles, str):
        gggfiles = [gggfiles]

    for gf in gggfiles:
        logger.info('Modifying {}'.format(gf))
        utils.change_ggg_file(gf, **options)


def change_ggg_files_recursive(top_dir='.', windows=None, pdb=False,  **options):
    if pdb:
        import pdb
        pdb.set_trace()

    for dirname, _, files in os.walk(top_dir):
        for fname in files:
            if fname.endswith('.ggg'):
                if windows is None or any([fname.startswith(w) for w in windows]):
                    fullfile = os.path.join(dirname, fname)
                    logger.info('Modifying {}'.format(fullfile))
                    utils.change_ggg_file(fullfile, **options)


def _add_common_opts(p):
    ak_spt_choices = ('no', 'here', 'gggpath')
    p.add_argument('--aks', choices=ak_spt_choices, default='no',
                   help='Control where averaging kernel files are saved. "no" will not save, '
                        '"here" will create ./ak/<window> subdirectories and save there, "gggpath" '
                        'will leave the original value in the .ggg file.')
    p.add_argument('--spts', choices=ak_spt_choices, default='no',
                   help='Control where the spectra fit files are saved. Same options as --aks, '
                        'if "here" is used, then the subdirectories will be under ./spt')
    p.add_argument('--backup', action='store_true', help='Create backups of the original .ggg files')
    p.add_argument('-v', '--verbose', action='store_true', help='Print actions taken')
    p.add_argument('--pdb', action='store_true', help='Immediately start python debugger')


def parse_change_many_args(p):
    p.description = 'Change settings in multiple .ggg files'
    p.add_argument('gggfiles', nargs='+', help='One or more .ggg files to alter')
    _add_common_opts(p)
    p.set_defaults(driver_fxn=change_many_ggg_files)


def parse_change_recursive_args(p):
    def parse_comma_sep(val):
        return tuple(val.split(','))
    p.description = 'Recursively find and change .ggg files under a given directory'
    p.add_argument('top_dir', nargs='?', default='.', help='Directory to search for .ggg files under. '
                                                           'Default is "%(default)s".')
    p.add_argument('-w', '--windows', type=parse_comma_sep,
                   help='Comma separated list of which windows\' .ggg files to change. This matches at '
                        'the start of the filename, so both "-w co_" and "-w co_4233,co_4290" would '
                        'change both CO windows\' files.')
    _add_common_opts(p)
    p.set_defaults(driver_fxn=change_ggg_files_recursive)


def parse_all_gsetup_args(p):
    p.description = 'Function to help prepare to run GGG'
    subp = p.add_subparsers()

    chg_multi_p = subp.add_parser('change_ggg_files', aliases=['change'], help='Change .ggg file')
    parse_change_many_args(chg_multi_p)

    chg_recurse_p = subp.add_parser('change_ggg_recursive', aliases=['chng-rec'], help='Change .ggg files under a directory')
    parse_change_recursive_args(chg_recurse_p)
