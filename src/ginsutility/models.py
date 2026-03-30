# -*- coding: utf-8 -*-
"""
Shared models and constants for GiUtility.
ref:GI.bench\api\bin\linux\include\GInsUtility\interface_public\eGateHighSpeedPort
"""

from enum import IntEnum
import logging

logger = logging.getLogger(__name__)


class HighSpeedPortError(Exception):
    """Exception for HighSpeedPort errors."""
    pass


class PostProcessBufferError(Exception):
    """Exception for PostProcessBuffer errors."""
    pass


class ConnectionState(IntEnum):
    """Connection states."""
    DISCONNECTED = 0
    CONNECTED = 1
    BUFFER_CONNECTION = 2
    ONLINE_CONNECTION = 3
    WEBSOCKET_CONNECTION = 4
    FILE_CONNECTION = 5


class BufferType(IntEnum):
    """Buffer type constants."""
    ONLINE = 1
    BUFFER = 2
    ARCHIVES = 4
    FILES = 5

    @classmethod
    def from_string(cls, buffer_type_str: str) -> int:
        """Convert string buffer type to integer."""
        if not buffer_type_str:
            return cls.BUFFER

        mapping = {
            "ONLINE": cls.ONLINE,
            "HSP_ONLINE": cls.ONLINE,
            "BUFFER": cls.BUFFER,
            "HSP_BUFFER": cls.BUFFER,
            "ARCHIVES": cls.ARCHIVES,
            "HSP_ARCHIVES": cls.ARCHIVES,
            "FILES": cls.FILES,
            "HSP_FILES": cls.FILES,
        }

        if buffer_type_str.isdigit():
            value = int(buffer_type_str)
            if value in cls._value2member_map_:
                return value

        return mapping.get(buffer_type_str.upper(), cls.BUFFER)


class DeviceInfoType(IntEnum):
    """Device information constants."""
    LOCATION = 10
    ADDRESS = 11
    SAMPLE_RATE = 16
    SERIAL_NUMBER = 15
    CHANNEL_COUNT = 18
    CHANNEL_INFO_NAME = 0
    CHANNEL_UNIT = 1


class DataType(IntEnum):
    """Data type constants."""
    DOUBLE = 8
    FLOAT = 4
    INT32 = 1
    INT64 = 2


class VariableKind(IntEnum):
    """Variable kind constants."""
    ANALOG_INPUT = 6
    DIGITAL_INPUT = 7
    ANALOG_OUTPUT = 8
    DIGITAL_OUTPUT = 9


class ChannelInfo:
    """Channel information container."""

    def __init__(self, index: int, name: str, unit: str = "", total_index: int = -1):
        self.index = index
        self.name = name
        self.unit = unit
        self.total_index = total_index

    def __str__(self) -> str:
        return f"Channel {self.index}: {self.name} [{self.unit}] (total: {self.total_index})"

    def __repr__(self) -> str:
        return f"ChannelInfo(index={self.index}, name='{self.name}', unit='{self.unit}')"


class BufferInfo:
    """Buffer information container."""

    def __init__(self, index: int, name: str, buffer_id: str):
        self.index = index
        self.name = name
        self.buffer_id = buffer_id

    def __str__(self) -> str:
        return f"Buffer {self.index}: {self.name} ({self.buffer_id})"

    def __repr__(self) -> str:
        return f"BufferInfo(index={self.index}, name='{self.name}', buffer_id='{self.buffer_id}')"
