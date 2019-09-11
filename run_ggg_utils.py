from argparse import ArgumentParser
import sys

from gggutils import target_utils, i2srun, ggglogging


def parse_args():
    p = ArgumentParser(description='Run parts of GGGUtils')
    ggglogging.add_logging_clargs(p)

    subp = p.add_subparsers()

    tabp = subp.add_parser('tab-tgts', help='Tabulate available target files')
    target_utils.parse_tab_args(tabp)

    i2sp = subp.add_parser('i2s', help='I2S related commands.')
    i2srun.parse_i2s_args(i2sp)

    args = vars(p.parse_args())
    return args


def main():
    args = parse_args()
    ggglogging.setup_logging_from_clargs(args)
    driver = args.pop('driver_fxn')
    return driver(**args)


if __name__ == '__main__':
    status = main()
    if not isinstance(status, int):
        status = 0
    sys.exit(status)
