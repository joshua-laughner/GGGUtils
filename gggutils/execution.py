import os
from subprocess import run, PIPE, DEVNULL
from unittest.mock import patch


class GGGError(Exception):
    pass


def gggpath_context(gggpath=None):
    if gggpath is None:
        gggpath = os.environ['GGGPATH']
    return patch.dict(os.environ, {'GGGPATH': gggpath, 'gggpath': gggpath})


def run_ggg_exec(exec_name, *args, gggpath=None, verbose=False, **run_kws):
    with gggpath_context(gggpath):
        gp = os.environ['GGGPATH']
        exec_name = os.path.join(gp, 'bin', exec_name)
        if not os.path.exists(exec_name):
            raise FileNotFoundError(f'No GGG executable "{exec_name}"')
        args = [exec_name] + list(args)
        if verbose:
            print('Executing {} with GGGPATH={}'.format(' '.join(args), gggpath))
        result = run(args, stderr=PIPE, **run_kws)
        err = result.stderr.decode('utf8')
        if len(result.stderr) > 0:
            raise GGGError(f'{exec_name} failed: {err}')
        return result


