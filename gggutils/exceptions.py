class I2SException(Exception):
    pass


class I2SFormatException(I2SException):
    pass


class I2SDataException(I2SException):
    pass


class SiteDateException(Exception):
    pass


class ConfigException(Exception):
    pass


class GGGException(Exception):
    pass


class GGGPathException(GGGException):
    pass
