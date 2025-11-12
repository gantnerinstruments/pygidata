from enum import Enum


class Resolution(Enum):
    MONTH = "MONTH"
    WEEK = "WEEK"
    DAY = "DAY"
    HOUR = "HOUR"
    QUARTER_HOUR = "QUARTER_HOUR"
    MINUTE = "MINUTE"
    SECOND = "SECOND"
    HZ10 = "HZ10"
    HZ100 = "HZ100"
    KHZ = "KHZ"
    KHZ10 = "KHZ10"
    NANOS = "nanos"


class DataFormat(Enum):
    COL = "col"
    ROW = "row"
    JSON = "json"
    CSV = "csv"
    UDBF = "udbf"
    FAMOS = "famos"
    MDF = "mdf"
    MAT = "mat"
    WAV = "wav"
    RPC = "rpc"


class DataType(Enum):
    EQUIDISTANT = "equidistant"
    ABSOLUTE = "absolute"
    AUTO = "auto"
    FFT = "fft"
