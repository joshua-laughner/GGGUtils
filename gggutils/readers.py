import netCDF4 as ncdf
import numpy as np
import pandas as pd
from pathlib import Path
import re
from jllutils.fileops import ncio

from typing import Sequence, Union


class MavParsingError(Exception):
    pass


def ydh_to_timestamp(year: int, day: int, hour: Union[int, float], has_decimal=False) -> pd.Timestamp:
    """
    Convert a single year, day, and fractional hour into a Pandas timestamp.

    :param year: the year
    :param day: the day of year, 1-based.
    :param hour: the hour. May contain a fractional component and be negative.
    :return: the datetime
    """
    # There are two different formats of date in GGG files. Both give year, day of year,
    # and hour of day, but in one year and day are always integers, while in the second
    # year includes the days and hours as a decimal component and days includes the hours
    # in the decimal.
    #
    # The tricky part is, in certain circumstances, the day or year can wrap to the next
    # year. This happens if, for example, hour is > 24 on Dec 31. (Hour can be > 24 because
    # day is kept the same for all measurements during the same sunrise-sunset period. So an
    # instrument that measures across midnight UTC will have hour > 24.)
    if has_decimal:
        # We need to subtract off enough of the decimal part to ensure that we round correctly -
        # this doesn't need to be super precise, just enough to make sure round() will give the
        # right value.
        year = int(round(year - 0.99*(day / 366)))
        day = int(round(day - 0.99*(hour / 24)))
    else:
        year = int(year)
        day = int(day)
    return pd.Timestamp(year, 1, 1) + pd.Timedelta(days=day - 1, hours=hour)


def df_ydh_to_dtind(df: pd.DataFrame, has_decimal=False) -> pd.DatetimeIndex:
    """
    Create a DatetimeIndex from a .eof.csv dataframe
    :param df: a dataframe containing "year", "day" and "hour" columns that are the year, day-of-year (1-based), and
     fractional hour of their rows.
    :return: a DatetimeIndex with the corresponding datetimes.
    """
    return pd.DatetimeIndex([ydh_to_timestamp(y, d, h, has_decimal=has_decimal) for y, d, h in zip(df.year, df.day, df.hour)])


def _read_private_nc(ncfile: str, date_index: bool = True):
    df = ncio.ncdf_to_dataframe(ncfile, target_dim='time')
    df.rename(columns={'time': 'date'}, inplace=True)
    # By default the dataframe will have the time as the index since that was the index
    # dimension. Reset this if that is not desired. The 'time' (now 'date') column is
    # retained
    if not date_index:
        df.reset_index(drop=True, inplace=True)
    
    with ncdf.Dataset(ncfile) as ds:
        # Read the spectrum name
        if 'spectrum' in ds.variables:
            df['spectrum'] = ds.variables['spectrum'][:]
        
        # Find all variables with _Encoding as an attribute -
        # those are text variables
        for varname, var in ds.variables.items():
            if '_Encoding' in var.ncattrs():
                df[varname] = var[:]
                
    return df


def _read_eof_csv(eof_file: str, date_index: bool = True, compute_date: bool = True):
    with open(eof_file, 'r') as robj:
        line1 = robj.readline()
        if ',' in line1:
            line1 = line1.split(',')
        else:
            line1 = line1.split()
        nhead = int(line1[0])
    df = pd.read_csv(eof_file, header=nhead - 1, sep=',')


    if date_index:
        df.set_index(df_ydh_to_dtind(df), inplace=True, verify_integrity=True)
    elif compute_date:
        df['date'] = df_ydh_to_dtind(df)

    return df


def read_eng_file(private_file: str, date_index: bool = True, compute_date: bool = True, 
                  allowed_flags: Sequence[int] = (0,), dates: pd.DatetimeIndex = None) -> pd.DataFrame:
    """Read a .eof.csv (engineering output file, comma-separated value format) file

    Parameters
    ----------
    private_file: 
        the path to the private netCDF file or the .eof.csv file

    date_index:
        if `True` then the returned dataframe is indexed by date. 

    compute_date:
        if `True` and `date_index` is `False`, then a 'date' column is added to the dataframe containing the observation
        datetimes. Has no effect if reading a netCDF file.

    allowed_flags:
        which quality flags are kept in the dataframe. If `None` or the string `'all'` any flag is valid. 

    dates:
        a date array indicating the date range of data to retain. Data between the min and max of this array will be
        kept. If this is `None`, no date limiting is done. If this is given, `compute_date` is considered `True` regardless
        of its actual value.

    Returns
    -------
    pd.DataFrame:
        dataframe with all the information from the .eof.csv file
    """
    if private_file.endswith('.nc') or private_file.endswith('.nc4'):
        df = _read_private_nc(private_file, date_index=date_index)
    else:
        df = _read_eof_csv(private_file, date_index=date_index, compute_date=compute_date)
    
    if allowed_flags is None or allowed_flags == 'all':
        xx = df['flag'] > -99 
    else:
        xx = df['flag'].isin(allowed_flags)

    if dates is not None:
        if date_index:
            df_dates = this_df.index
        else:
            df_dates = this_df['date']
            
        xx &= (this_df['date'] >= dates.min()) & (this_df['date'] <= dates.max())
    
    return df[xx]

# may finish this in the future to avoid screen dumping hundreds of
# pandas tables when accidentally printing the mav dict. Also make it
# easier to access items by key or index
class MavContainer(object):
    def __init__(self, **mav_tables):
        raise NotImplementedError('MavContainer not finished')
        self._mav_blocks = []
        self._indices = []

        for k,v in mav_tables.items():
            self.add_block(k, v)

    def add_block(self, key, value):
        if isinstance(key, int):
            raise IndexError('Cannot add block by integer index')
        elif key not in self._indices:
            self._indices.append(key)
            self._mav_blocks.append(value)
        else:
            i = self._indices.index(key)
            self._mav_blocks[i] = value

    def keys(self):
        return self._indices

    def values(self):
        return self._mav_blocks

    def items(self):
        for k, v in zip(self._indices, self._mav_blocks):
            yield k, v
            


def read_mav_file(mav_file, indexing='spectrum'):
    def specname(l):
        return l.split(':')[1].strip()

    mav_dict = dict()

    with open(mav_file, 'r') as robj:
        # Find the first "Next Spectrum" line
        while True:
            address = robj.tell()
            line = robj.readline()
            if 'next spectrum' in line.lower():
                break

        # Rewind so that the file pointer is aimed at the
        # "next spectrum" line
        robj.seek(address)
        
        # Read mav blocks until we run out
        nread = 0
        while True:
            idx, table = _parse_mav_block(robj, indexing=indexing)
            nread += 1
            print('\rRead {} mav blocks'.format(nread), end='')
            if idx is None:
                print('')
                return mav_dict
            
            mav_dict[idx] = table




def _parse_mav_block(fh, exclude_cell=True, indexing='spectrum'):
    # The first line should have 'Next Spectrum:<specname>'. Get the spectrum name, or
    # raise an error if not

    line = fh.readline()
    if len(line) == 0:
        # End of file
        return None, None
    elif 'next spectrum' not in line.lower():
        raise MavParsingError('MAV block did not start with line containing "Next Spectrum"')
    else:
        specname = line.split(':')[1].strip()

    
    count_line = fh.readline()
    nhead, ncol, nrow = [int(x) for x in count_line.split()]

    # Advance to the second to last line of the header - the line we just read counts
    for i in range(nhead-2):
        line = fh.readline()

    # The second to last line should include the FPIT mod file name - get the date from that
    m = re.search(r'(?<=FPIT_)\d{10}(?=Z)', line)
    if m is None and indexing == 'datetime':
        raise MavParsingError('Could not find FPIT model file to get the datetime from')
    else:
        specdate = pd.to_datetime(m.group(), format='%Y%m%d%H')

    # Pandas does not count the header for nrows, neither does the .mav file. Also the C 
    # engine reads in chunks and so can go past the end of the mav block. The python 
    # engine is slower but behaves correctly.
    table = pd.read_csv(fh, sep='\s+', nrows=nrow, engine='python')

    # Cell concentrations are represented by negative altitudes (-9.9 and -8.8 km)
    # Unless told not to, remove those levels
    if exclude_cell:
        xx = table['Height'] > -2  # technically if we had a TCCON in Death Valley it should have a negative altitude...
        table = table[xx]

    if indexing == 'spectrum':
        return specname, table
    elif indexing == 'datetime':
        return specdate, table
    else:
        raise ValueError('Unknown indexing type: {}'.format(indexing))


def read_out_file(out_file, as_dataframes=True):
    """Read a standard GGG post processing out file (*not* one of the .csv files)

    Parameters
    ----------
    out_file : pathlike
        The path to the file to read.

    as_dataframes : bool
        If `True` (default), return the file data as a dataframe. If `False`, return it as a dictionary
        of arrays.

    Returns
    -------
    pandas.DataFrame or dict:
        The data from the file.
    """
    n_header_lines = _get_num_header_lines(out_file)
    df = pd.read_csv(out_file, header=n_header_lines-1, sep=r'\s+')
    if not as_dataframes:
        return df.to_dict()
    else:
        return df


def _get_num_header_lines(filename):
    """Get the number of header lines in a standard GGG file

    This assumes that the file specified begins with a line with two or more numbers: the number of header rows and the number
    of data columns.

    Parameters
    ----------

    filename: pathlike
        The file to read

    Returns
    -------
    int 
        The number of header lines
    """
    with open(filename, 'r') as fobj:
        header_info = fobj.readline()

    if ',' in header_info:
        header = header_info.split(',')
    else:
        header = header_info.split()
    return int(header[0])


def read_spt(spt_file, convert_transmittance=True):
    """Read a spectral fit file

    Parameters
    ----------
    spt_file: pathlike
        Path to the spectral fit text file to read

    convert_transmittance: bool
        The total transmittance columns, ``Tm`` and ``Tc``, are not
        comparable to the individual gases absorbance in the file.
        By default this function converts them to comparable units,
        set ``convert_transmittance = False`` to disable that.

    Returns
    -------
    pd.DataFrame
        A dataframe containing the spectral fit information.
    """
    with open(spt_file) as f:
        f.readline()
        lineparts = f.readline().split()
        sza = float(lineparts[4])
        xzo = float(lineparts[10])
        #print(f'xzo = {xzo}')
        
    df = pd.read_csv(spt_file, sep=r'\s+', header=2).set_index('Freq')
    if convert_transmittance:
        df['Tm'] = (df['Tm']/df['Cont'] - xzo)/(1-xzo)
        df['Tc'] = (df['Tc']/df['Cont'] - xzo)/(1-xzo)
    df['sza'] = sza
    return df


def read_multi_nc_dataframe(nc_files: Sequence[str], variables: Sequence[str], flag0_only: bool = False, quiet: bool = False):
    data = {v: [] for v in variables}
    data['file'] = []
    msg_width = 0
    nfile = len(nc_files)
    for ifile, nc_file in enumerate(nc_files):
        msg = f'\rReading {Path(nc_file).name} ({ifile+1} of {nfile})'
        if not quiet:
            msg_width = max(msg_width, len(msg))
            print(msg.ljust(msg_width), end='')
        with ncdf.Dataset(nc_file) as ds:
            if flag0_only and 'flag' in ds.variables.keys():
                qq = ds['flag'][:] == 0
            else:
                qq = np.ones(ds['time'].shape, dtype=bool)

            for v in variables:
                data[v].append(ds[v][:][qq])

        data['file'].append(np.full(qq.shape, str(nc_file)))

    # This could be more efficient if we queried all the files for their lengths first,
    # then preallocated numpy arrays and inserted the values, but that's also way more
    # complex.
    data = {k: np.concatenate(v) for k, v in data.items()}
    return pd.DataFrame(data)


def read_multi_nc_xarray(nc_files: Sequence[str], variables: Sequence[str], flag0_only: bool = False, quiet: bool = False):
    try:
        import xarray as xr
    except ImportError:
        raise ImportError('The read_multi_nc_xarray function requires the xarray package')
    
    data = {v: [] for v in variables}
    first_dims = dict()
    msg_width = 0
    nfile = len(nc_files)
    for ifile, nc_file in enumerate(nc_files):
        with xr.open_dataset(nc_file) as ds:
            msg = f'\rReading {Path(nc_file).name} ({ifile+1} of {nfile})'
            if not quiet:
                msg_width = max(msg_width, len(msg))
                print(msg.ljust(msg_width), end='')
            for v in variables:
                var_data = ds[v]
            
                if v not in first_dims:
                    first_dims[v] = var_data.dims[0]
                elif first_dims[v] != var_data.dims[0]:
                    raise ValueError(f'In file {nc_file}, the first dimension of {v} ({var_data.dims[0]}) differs from that of previous files ({first_dims[v]})')

                if not flag0_only:
                    data[v].append(var_data)
                elif var_data.dims[0] == 'time' and 'flag' in ds:
                    qq = ds['flag'] == 0
                    data[v].append(var_data.isel(time=qq))

            if 'time' in first_dims.values():
                data.setdefault('time_file', [])
                data['time_file'].append(
                    xr.DataArray(np.full(ds.time.shape, str(nc_file)), dims=['time'], coords={'time': ds.time})
                )
                if 'time_file' not in first_dims:
                    first_dims['time_file'] = 'time'
            if 'prior_time' in first_dims.values():
                data.setdefault('prior_time_file', [])
                data['prior_time_file'].append(
                    xr.DataArray(np.full(ds.prior_time.shape, str(nc_file)), dims=['prior_time'], coords={'prior_time': ds.prior_time})
                )
                if 'prior_time_file' not in first_dims:
                    first_dims['prior_time_file'] = 'prior_time'

    data = {k: xr.concat(v, dim=first_dims[k]) for k, v in data.items()}
    return xr.Dataset(data)