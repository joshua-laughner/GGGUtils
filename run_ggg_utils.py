from argparse import ArgumentParser
import sys

from gggutils.console_main import main


if __name__ == '__main__':
    status = main()
    if not isinstance(status, int):
        status = 0
    sys.exit(status)
