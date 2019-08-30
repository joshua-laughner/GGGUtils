from argparse import ArgumentParser

from gggutils import target_utils


def parse_args():
    p = ArgumentParser(description='Run parts of GGGUtils')
    subp = p.add_subparsers()

    tabp = subp.add_parser('tab-tgts', help='Tabulate available target files')
    target_utils.parse_tab_args(tabp)

    args = vars(p.parse_args())
    return args


def main():
    args = parse_args()
    driver = args.pop('driver_fxn')
    driver(**args)


if __name__ == '__main__':
    main()
