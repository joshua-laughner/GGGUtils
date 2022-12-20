import numpy as np
from . import constants as const


def effective_vertical_path(z, zmin, p=None, t=None, nair=None):
    """  
    Calculate the effective vertical path used by GFIT for a given z/P/T grid.

    :param z: altitudes of the vertical levels. May be any unit, but note that the effective paths will be returned in
     the same unit.
    :type z: array-like
    
    :param zmin: minimum altitude that the light ray reaches. This is given as ``zmin`` in the netCDF files and the .ray
     files. Must be in the same unit as ``z``.
    :type zmin: float

    :param p: pressures of the vertical levels. Must be in hPa.
    :type p: array-like

    :param t: temperatures of the vertical levels. Must be in K.
    :type t: array-like

    :return: effective vertical paths in the same units as ``z``
    :rtype: array-like
    """
    def integral(dz_in, lrp_in, sign):
        return dz_in * 0.5 * (1.0 + sign * lrp_in / 3 + lrp_in**2/12 + sign*lrp_in**3/60)

    if nair is not None:
        d = nair 
    elif p is not None and t is not None:
        d = number_density_air(p, t)
    else:
        raise TypeError('Either nair or p & t must be given')
    
    vpath = np.zeros_like(d)
    
    # From gfit/compute_vertical_paths.f, we need to find the first level above zmin
    # If there is no such level (which should not happen for TCCON), we treat the top
    # level this way
    try:
        klev = np.flatnonzero(z > zmin)[0]
    except IndexError:
        klev = np.size(z) - 1
        
    # from gfit/compute_vertical_paths.f, the calculation for level i is
    #   v_i = 0.5 * dz_{i+1} * (1 - l_{i+1}/3 + l_{i+1}**2/12 - l_{i+1}**3/60)
    #       + 0.5 * dz_i * (1 + l_i/3 + l_i**2/12 + l_i**3/60)
    # where
    #   dz_i = z_i - z_{i-1}
    #   l_i  = ln(d_{i-1}/d_i)
    # The top level has no i+1 term. This vector addition duplicates that calculation. The zeros padded to the beginning
    # and end of the difference vectors ensure that when there's no i+1 or i-1 term, it is given a value of 0.
    dz = np.concatenate([[0.0], np.diff(z[klev:]), [0.0]])
    log_rp = np.log(d[klev:-1] / d[klev+1:])
    log_rp = np.concatenate([[0.0], log_rp, [0.0]])
    
    # The indexing is complicated here, but with how dz and log_rp are constructed, this makes sure that, for vpath[klev],
    # the first integral(...) term uses dz = z[klev+1] - z[klev] and log_rp = ln(d[klev]/d[klev+1]) and the second integral
    # term is 0 (as vpath[klev] needs to account for the surface location below). For all other terms, this combines the
    # contributions from the weight above and below each level, with different integration signs to account for how the
    # weights increase from the level below to the current level and decrease from the current level to the level above.
    vpath[klev:] = integral(dz[1:], log_rp[1:], sign=-1) + integral(dz[:-1], log_rp[:-1], sign=1)
       
    # Now handle the surface - I don't fully understand how this is constructed mathematically, but the idea is that both
    # the levels in the prior above and below zmin need to contribute to the column, however that contribution needs to be
    # 0 below zmin. 
    
    dz = z[klev] - z[klev-1]
    xo = (zmin - z[klev-1])/dz
    log_rp = 0.0 if d[klev] <= 0 else np.log(d[klev-1]/d[klev])
    xl = log_rp * (1-xo)
    vpath[klev-1] += dz * (1-xo) * (1-xo-xl*(1+2*xo)/3 + (xl**2)*(1+3*xo)/12 + (xl**3)*(1+4*xo)/60)/2
    vpath[klev] += dz * (1-xo) * (1+xo+xl*(1+2*xo)/3 + (xl**2)*(1+3*xo)/12 - (xl**3)*(1+4*xo)/60)/2

    return vpath


def number_density_air(p, t):
    """
    Calculate the ideal dry number density of air in molec. cm^-3

    :param p: pressure in hPa
    :type p: float or :class:`numpy.ndarray`

    :param t: temperature in K
    :type t: float or :class:`numpy.ndarray`

    :return: ideal dry number density in molec. cm^-3
    :rtype: float or :class:`numpy.ndarray`
    """
    R = const.gas_const  # gas constant in cm^3 * hPa / (mol * K)
    return p / (R*t) * const.avogadro