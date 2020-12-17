import netCDF4 as ncdf
import pandas as pd
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

