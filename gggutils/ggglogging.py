import logging


def add_logging_clargs(parser):
    """

    :param parser:
    :type parser: :class:`argparse.ArgumentParser`
    :return:
    """
    parser.add_argument('-v', '--verbose', action='count', default=0, help='Increase detail of logging messages')
    parser.add_argument('-q', '--quiet', action='store_const', const=-1, dest='verbose',
                        help='Suppress detail of logging messages')
    parser.add_argument('-f', '--log-file', dest='to_file', default=False,
                        help='File to divert the logging messages to')


def setup_logging_from_clargs(args):
    verbosity = args.pop('verbose', 0)
    to_file = args.pop(args, False)
    setup_logging(verbosity=verbosity, to_file=to_file)


def setup_logging(verbosity, to_file):
    verbosity_dict = {-1: 'ERROR', 0: 'WARNING', 1: 'INFO', 2: 'DEBUG'}
    if verbosity < -1:
        verbosity = -1
    elif verbosity > 2:
        verbosity = 2

    logger = logging.getLogger()
    formatter = logging.Formatter('%(levelname)s from %(name)s [%(asctime)s]: %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')
    if to_file:
        filename = to_file
        file_h = logging.FileHandler(filename=filename)
        file_h.setFormatter(formatter)
        logger.addHandler(file_h)

    logger.setLevel(verbosity_dict[verbosity])