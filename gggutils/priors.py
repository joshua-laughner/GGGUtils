from argparse import ArgumentParser
from datetime import datetime as dtime
from glob import glob
import numpy as np
import os
import pandas as pd
import re
import tarfile
import xarray as xr

from ginput.common_utils import readers
from jllutils import miscutils


def make_mod_combo_nc_from_dir(mod_dir, nc_savename):
    # always sorted by make_mod_combo_nc, no need to sort here
    mod_files = glob(os.path.join(mod_dir, '*.mod'))
    return make_mod_combo_nc(mod_files, nc_savename)

def make_mod_combo_nc(mod_files, nc_savename):
    # Define the units for the scalar variables because they aren't in the .mod files
    scalar_units = {'Pressure': 'hPa', 
                    'Temperature': 'K',
                    'Height': 'km',
                    'MMW': 'g/mole',
                    'H2O': 'mol/mol',
                    'RH': '%',
                    'SLP': 'hPa',
                    'TROPPB': 'hPa',
                    'TROPPT': 'hPa',
                    'TROPPV': 'hPa',
                    'TROPT': 'K',
                    'SZA': 'degrees'}

    mod_files = sorted(mod_files)

    # make the vector of datetimes
    file_dates = []
    pbar = miscutils.ProgressBar(len(mod_files), prefix='Parsing file dates', freq=96)
    i = 0 
    for f, fbase in miscutils.file_iter(mod_files):
        i += 1
        pbar.print_bar(i)
        datestr = re.search(r'\d{10}', fbase).group()
        file_dates.append(pd.Timestamp.strptime(datestr, '%Y%m%d%H'))
    
    pbar.finish()
    
    file_dates = pd.DatetimeIndex(file_dates)
    
    # read one file to get the number of levels and the names of the variables
    init_data = readers.read_mod_file(mod_files[0])
    init_profs = init_data['profile']
    init_scalars = init_data['scalar']
    units = readers.read_mod_file_units(mod_files[0])
    
    # construct a dictionary of DataArrays that are nprofs-by-nlevels. This will get
    # converted to a Dataset and saved as a netCDF file as the end
    mod_data = dict()
    levels = np.arange(init_profs['Height'].size)
    nlevs = levels.size
    nprofs = file_dates.size
    
    coords = {'time': file_dates, 'lev': levels}
    dims = ['time', 'lev']
    surf_key_mapping = dict()
    for col in init_profs.keys():
        mod_data[col] = xr.DataArray(np.full([nprofs, nlevs], np.nan), dims=dims, coords=coords)
        mod_data[col].attrs['units'] = units[col]
    for col in init_data['scalar'].keys():
        outkey = 'Surf' + col if col in mod_data else col
        surf_key_mapping[col] = outkey
        mod_data[outkey] = xr.DataArray(np.full([nprofs], np.nan), dims=['time'], coords={'time': file_dates})
        mod_data[outkey].attrs['units'] = scalar_units[col]

    
    pbar = miscutils.ProgressBar(len(mod_files), prefix='Reading mod files', freq=8)
    for ifile, f in enumerate(mod_files):
        pbar.print_bar(ifile)
        data = readers.read_mod_file(f)
        profiles = data['profile']
        for varname, profile in profiles.items():
            mod_data[varname][ifile, :] = profile
        scalars = data['scalar']
        for varname, scalar in scalars.items():
            key = surf_key_mapping[varname]
            mod_data[key][ifile] = scalar
    
    pbar.finish()
    ds = xr.Dataset(mod_data)
    if nc_savename is not None:
        ds.to_netcdf(nc_savename)
    
    return ds


def copy_and_untar_priors(tarball_dir, output_dir, site=None, start=None, stop=None, verbose=0):
    pattern = '??_ggg_inputs_????????.tgz' if site is None else '{}_ggg_inputs_????????.tgz'.format(site)
    tar_files = sorted(glob(os.path.join(tarball_dir, pattern)))
    
    def get_file_date(f):
        f = os.path.basename(f)
        return dtime.strptime(re.search(r'\d{8}', f).group(), '%Y%m%d')

    def debug(msg, dblevel=0):
        if verbose >= dblevel:
            print(msg)
    
    mod_dir = os.path.join(output_dir, 'mod')
    vmr_dir = os.path.join(output_dir, 'vmr')

    if not os.path.exists(mod_dir):
        os.mkdir(mod_dir)
        debug('Created {}'.format(mod_dir))
    if not os.path.exists(vmr_dir):
        os.mkdir(vmr_dir)
        debug('Created {}'.format(vmr_dir))
    
    for f in tar_files:
        d = get_file_date(f)
        if d < start or d > stop:
            debug('Skipping {}, out of date range'.format(f), 2)
            continue
        
        with tarfile.open(f) as tobj:
            debug('Extracting {}'.format(f), 1)
            for name in tobj.getnames():
                if name.endswith('.mod'):
                    tobj.extract(name, path=mod_dir)
                elif name.endswith('.vmr'):
                    tobj.extract(name, path=vmr_dir)
