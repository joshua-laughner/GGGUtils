import struct


def read_spectrum_raw(spec_file):
    with open(spec_file, 'rb') as f:
        raw = f.read()

    fmt = '<{}f'.format(len(raw)//4)
    spectra = struct.unpack(fmt, raw)
    return spectra
