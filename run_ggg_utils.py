from argparse import ArgumentParser

from gggutils import target_utils, gggrun, ggglogging


def parse_args():
    p = ArgumentParser(description='Run parts of GGGUtils')
    ggglogging.add_logging_clargs(p)

    subp = p.add_subparsers()

    tabp = subp.add_parser('tab-tgts', help='Tabulate available target files')
    target_utils.parse_tab_args(tabp)

    build_cfg_p = subp.add_parser('build-i2s-cfg', help='Build the config file to run I2S in bulk.')
    gggrun.parse_build_cfg_args(build_cfg_p)

    link_i2s_p = subp.add_parser('link-i2s-inputs', help='Link the input files to run I2S in bulk')
    gggrun.parse_link_i2s_args(link_i2s_p)

    run_i2s_p = subp.add_parser('run-i2s', help='Run I2S in batch')
    gggrun.parse_run_i2s_args(run_i2s_p)

    halt_i2s_p = subp.add_parser('halt-i2s', help='Halt an active batch I2S run')
    gggrun.parse_halt_i2s_args(halt_i2s_p)

    args = vars(p.parse_args())
    return args


def main():
    args = parse_args()
    ggglogging.setup_logging_from_clargs(args)
    driver = args.pop('driver_fxn')
    driver(**args)


if __name__ == '__main__':
    main()
