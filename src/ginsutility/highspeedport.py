# -*- coding: utf-8 -*-
"""HighSpeedPort client for Gantner device communication."""
from __future__ import annotations

import logging
import json
import time

import numpy as np
from ctypes import (
    c_int,
    c_double,
    c_char_p,
    POINTER,
    byref,
    c_int32,
    c_int64,
    c_size_t,
    c_uint64,
    create_string_buffer,
)

from .loader import load_giutility
from .models import (
    HighSpeedPortError,
    ConnectionState,
    BufferType,
    DeviceInfoType,
    ChannelInfo,
    BufferInfo,
)
from .errors import HspErrorUtil

logger = logging.getLogger(__name__)


class HighSpeedPortClient:
    """Client for connecting to Gantner devices via HighSpeedPort."""

    def __init__(self, log_level: int = logging.INFO):
        self.HCONNECTION = None
        self.HCLIENT = None
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.logger.setLevel(log_level)

        self.GINSDll = load_giutility()
        self.logger.info("Loaded GiUtility from: %s", self.GINSDll._name)

        self._setup_function_prototypes()
        self._init_connection_params()

        self.connection_state = ConnectionState.DISCONNECTED

    def _setup_function_prototypes(self):
        try:
            self.GINSDll._CD_eGateHighSpeedPort_Init.argtypes = [
                c_char_p,
                c_int,
                c_int,
                c_int,
                POINTER(c_int),
                POINTER(c_int),
            ]
            self.GINSDll._CD_eGateHighSpeedPort_Init.restype = c_int

            self.GINSDll._CD_eGateHighSpeedPort_SetBackTime.argtypes = [c_int, c_double]
            self.GINSDll._CD_eGateHighSpeedPort_SetBackTime.restype = c_int

            self.GINSDll._CD_eGateHighSpeedPort_InitBuffer.argtypes = [c_int, c_int, c_int]
            self.GINSDll._CD_eGateHighSpeedPort_InitBuffer.restype = c_int

            self.GINSDll._CD_eGateHighSpeedPort_DecodeFile_Select.argtypes = [
                POINTER(c_int),
                POINTER(c_int),
                c_char_p,
            ]
            self.GINSDll._CD_eGateHighSpeedPort_DecodeFile_Select.restype = c_int

            self.GINSDll._CD_eGateHighSpeedPort_GetDeviceInfo.argtypes = [
                c_int,
                c_int,
                c_int,
                POINTER(c_double),
                c_char_p,
            ]
            self.GINSDll._CD_eGateHighSpeedPort_GetDeviceInfo.restype = c_int

            self.GINSDll._CD_eGateHighSpeedPort_GetChannelInfo_String.argtypes = [
                c_int,
                c_int,
                c_int,
                c_int,
                c_char_p,
            ]
            self.GINSDll._CD_eGateHighSpeedPort_GetChannelInfo_String.restype = c_int

            self.GINSDll._CD_eGateHighSpeedPort_GetChannelInfo_Int.argtypes = [
                c_int,
                c_int,
                c_int,
                c_int,
                POINTER(c_int),
            ]
            self.GINSDll._CD_eGateHighSpeedPort_GetChannelInfo_Int.restype = c_int

            self.GINSDll._CD_eGateHighSpeedPort_ReadBufferToDoubleArray.argtypes = [
                c_int,
                POINTER(c_double),
                c_int,
                c_int,
                POINTER(c_int),
                POINTER(c_int),
                POINTER(c_int),
            ]
            self.GINSDll._CD_eGateHighSpeedPort_ReadBufferToDoubleArray.restype = c_int

            self.GINSDll._CD_eGateHighSpeedPort_ReadBufferToDoubleArray_StraightTimestamp.argtypes = [
                c_int,
                POINTER(c_double),
                c_int,
                c_int,
                POINTER(c_int),
                POINTER(c_int),
                POINTER(c_int),
            ]
            self.GINSDll._CD_eGateHighSpeedPort_ReadBufferToDoubleArray_StraightTimestamp.restype = c_int

            self.GINSDll._CD_eGateHighSpeedPort_WriteOnline_Single_Immediate.argtypes = [
                c_int,
                c_int,
                c_double,
            ]
            self.GINSDll._CD_eGateHighSpeedPort_WriteOnline_Single_Immediate.restype = c_int

            self.GINSDll._CD_eGateHighSpeedPort_WriteOnline_Single.argtypes = [
                c_int,
                c_int,
                c_double,
            ]
            self.GINSDll._CD_eGateHighSpeedPort_WriteOnline_Single.restype = c_int

            self.GINSDll._CD_eGateHighSpeedPort_WriteOnline_ReleaseOutputData.argtypes = [c_int]
            self.GINSDll._CD_eGateHighSpeedPort_WriteOnline_ReleaseOutputData.restype = c_int

            self.GINSDll._CD_eGateHighSpeedPort_ReadOnline_Single.argtypes = [
                c_int,
                c_int,
                POINTER(c_double),
            ]
            self.GINSDll._CD_eGateHighSpeedPort_ReadOnline_Single.restype = c_int

            self.GINSDll._CD_eGateHighSpeedPort_Close.argtypes = [c_int, c_int]
            self.GINSDll._CD_eGateHighSpeedPort_Close.restype = c_int

            self.GINSDll._CD_eGateHighSpeedPort_GetPostProcessBufferInfo.argtypes = [
                c_int,
                c_char_p,
                c_size_t,
                c_char_p,
                c_size_t,
            ]
            self.GINSDll._CD_eGateHighSpeedPort_GetPostProcessBufferInfo.restype = c_int

            self.GINSDll._CD_eGateHighSpeedPort_Init_PostProcessBuffer.argtypes = [
                c_char_p,
                POINTER(c_int),
                POINTER(c_int),
            ]
            self.GINSDll._CD_eGateHighSpeedPort_Init_PostProcessBuffer.restype = c_int

            self.GINSDll._CD_eGateHighSpeedPort_Init_WebSocketStream.argtypes = [
                c_char_p,
                c_int32,
                c_char_p,
                c_char_p,
                c_char_p,
                c_char_p,
                c_int64,
                c_int64,
                c_double,
                c_int32,
                POINTER(c_int32),
                POINTER(c_int32),
            ]
            self.GINSDll._CD_eGateHighSpeedPort_Init_WebSocketStream.restype = c_int

            self.GINSDll._CD_eGateHighSpeedPort_Init_WebSocketStreamExt.argtypes = [
                c_char_p,
                c_int,
                c_char_p,
                c_char_p,
                c_char_p,
                c_char_p,
                c_int,
                c_int,
                c_double,
                c_int,
                c_char_p,
                POINTER(c_int),
                POINTER(c_int),
            ]
            self.GINSDll._CD_eGateHighSpeedPort_Init_WebSocketStreamExt.restype = c_int

            self.GINSDll._CD_eGateHighSpeedPort_InitWebSocket.argtypes = [
                c_char_p,  # url
                c_int32,  # port
                c_char_p,  # route
                c_char_p,  # username
                c_char_p,  # password
                c_double,  # timeoutSec
                c_char_p,  # addConfig (JSON)
                POINTER(c_int32),  # clientInstance
                POINTER(c_int32),  # connectionInstance
            ]
            self.GINSDll._CD_eGateHighSpeedPort_InitWebSocket.restype = c_int32

            self.GINSDll._CD_eGateHighSpeedPort_LogToUDBF_File.argtypes = [
                c_int32,
                c_uint64,
                c_char_p,
                c_char_p,
            ]
            self.GINSDll._CD_eGateHighSpeedPort_LogToUDBF_File.restype = c_int32

            if hasattr(self.GINSDll, "_CD_eGateHighSpeedPort_GetPostProcessBufferCount"):
                self.GINSDll._CD_eGateHighSpeedPort_GetPostProcessBufferCount.restype = c_int

        except AttributeError as e:
            self.logger.error("Failed to setup function prototypes: %s", e)
            raise HighSpeedPortError(f"Function prototype setup failed: {e}")

    def _init_connection_params(self):
        self.HCLIENT = c_int(-1)
        self.HCONNECTION = c_int(-1)

        self.timeout = 5
        self.sample_rate = 100
        self.buffer_index = 0
        self.auto_run = 0
        self.backtime = 0

        self._char_buffer = create_string_buffer(256)
        self._buffer_id = create_string_buffer(256)
        self._buffer_name = create_string_buffer(256)
        self._info_double = c_double(0)
        self._channel_info_int = c_int(-1)

        self.CHINFO_INDX = 7
        self.DADI_INOUT = 2

        self._last_error = 0

    def _check_connection(self) -> bool:
        if self.connection_state == ConnectionState.DISCONNECTED or self.HCONNECTION.value == -1:
            self.logger.warning("No active connection!")
            return False
        return True

    def _handle_error(self, operation: str, ret_code: int, raise_exception: bool = True) -> bool:
        self._last_error = int(ret_code)
        try:
            return HspErrorUtil.check_ret(
                self.logger,
                operation,
                ret_code,
                error_buffer=None,
                raise_exception=raise_exception,
            )
        except Exception as e:
            if raise_exception:
                raise HighSpeedPortError(str(e))
            return False

    def init_connection(self, controller_ip: str, buffer_type: str = "BUFFER") -> bool:
        try:
            if self.connection_state != ConnectionState.DISCONNECTED:
                self.close_connection()

            buffer_type_int = BufferType.from_string(buffer_type)

            ret = self.GINSDll._CD_eGateHighSpeedPort_Init(
                controller_ip.encode("utf-8"),
                c_int(self.timeout),
                c_int(buffer_type_int),
                c_int(self.sample_rate),
                byref(self.HCLIENT),
                byref(self.HCONNECTION),
            )
            if not self._handle_error("Init", ret, raise_exception=False):
                return False

            ret = self.GINSDll._CD_eGateHighSpeedPort_InitBuffer(
                self.HCONNECTION.value,
                self.buffer_index,
                self.auto_run,
            )
            if not self._handle_error("InitBuffer", ret, raise_exception=False):
                self.close_connection()
                return False

            if self.backtime > 0:
                ret = self.GINSDll._CD_eGateHighSpeedPort_SetBackTime(
                    self.HCONNECTION.value,
                    c_double(self.backtime),
                )
                self._handle_error("SetBackTime", ret, raise_exception=False)

            self.connection_state = (
                ConnectionState.ONLINE_CONNECTION
                if buffer_type_int == BufferType.ONLINE
                else ConnectionState.BUFFER_CONNECTION
            )
            self.logger.info("Connected to %s", controller_ip)
            return True

        except Exception as e:
            self.logger.error("Connection failed: %s", e)
            self.connection_state = ConnectionState.DISCONNECTED
            return False

    def init_online_connection(self, controller_ip: str) -> bool:
        return self.init_connection(controller_ip, "ONLINE")

    def init_websocket_stream(
            self,
            url: str,
            port: int,
            route: str,
            username: str,
            password: str,
            stream_id: str,
            start_time: int,
            end_time: int,
            timeout_sec: float,
            buffer_type: str,
            add_config: str = None,
    ) -> bool:
        try:
            if self.connection_state != ConnectionState.DISCONNECTED:
                self.close_connection()

            buffer_type_int = BufferType.from_string(buffer_type)
            if buffer_type_int == -1:
                self.logger.error("Invalid buffer type: %s", buffer_type)
                return False

            client_instance = c_int()
            connection_instance = c_int()

            ret = self.GINSDll._CD_eGateHighSpeedPort_Init_WebSocketStreamExt(
                url.encode("utf-8"),
                c_int(int(port)),
                route.encode("utf-8"),
                username.encode("utf-8") if username else b"",
                password.encode("utf-8") if password else b"",
                stream_id.encode("utf-8"),
                c_int(int(start_time)),
                c_int(int(end_time)),
                c_double(float(timeout_sec)),
                c_int(int(buffer_type_int)),
                add_config.encode("utf-8") if add_config else b"",
                byref(client_instance),
                byref(connection_instance),
            )

            if not self._handle_error("Init_WebSocketStreamExt", ret, raise_exception=False):
                return False

            self.HCLIENT = c_int(client_instance.value)
            self.HCONNECTION = c_int(connection_instance.value)
            self.connection_state = ConnectionState.WEBSOCKET_CONNECTION

            self.logger.info("WebSocket stream connected")
            return True

        except Exception as e:
            self.logger.error("WebSocket stream failed: %s", e)
            return False

    def init_websocket_online(
            self,
            url: str,
            port: int = 8090,
            route: str = "",
            username: str = "",
            password: str = "",
            timeout_sec: float = 10.0,
            interval_ms: int = 200,
            add_config: dict | None = None,
    ) -> bool:
        """
        Initialize ONLINE web-socket connection for reading/writing process image data.
        """
        try:
            if self.connection_state != ConnectionState.DISCONNECTED:
                self.close_connection()

            cfg = dict(add_config or {})
            cfg.setdefault("IntervalMs", int(interval_ms))
            cfg_json = json.dumps(cfg)

            client_instance = c_int32()
            connection_instance = c_int32()

            ret = self.GINSDll._CD_eGateHighSpeedPort_InitWebSocket(
                url.encode("utf-8"),
                c_int32(int(port)),
                route.encode("utf-8"),
                username.encode("utf-8") if username else b"",
                password.encode("utf-8") if password else b"",
                c_double(float(timeout_sec)),
                cfg_json.encode("utf-8"),
                byref(client_instance),
                byref(connection_instance),
            )
            if not self._handle_error("InitWebSocket", ret, raise_exception=False):
                return False

            self.HCLIENT = c_int(int(client_instance.value))
            self.HCONNECTION = c_int(int(connection_instance.value))
            self.connection_state = ConnectionState.WEBSOCKET_CONNECTION

            time.sleep(1)  # Wait a bit for init to fully complete
            self.logger.info("WebSocket ONLINE connected")
            return True

        except Exception as e:
            self.logger.error("WebSocket ONLINE failed: %s", e)
            return False

    def init_file(self, filepath: str) -> bool:
        try:
            if self.connection_state != ConnectionState.DISCONNECTED:
                self.close_connection()

            ret = self.GINSDll._CD_eGateHighSpeedPort_DecodeFile_Select(
                byref(self.HCLIENT),
                byref(self.HCONNECTION),
                filepath.encode("utf-8"),
            )
            if self._handle_error("DecodeFile_Select", ret, raise_exception=False):
                self.connection_state = ConnectionState.FILE_CONNECTION
                self.logger.info("File loaded: %s", filepath)
                return True
            return False

        except Exception as e:
            self.logger.error("File loading failed: %s", e)
            return False

    def get_device_info(self, info_type: DeviceInfoType, index: int = 0, as_string: bool = False):
        if not self._check_connection():
            return None

        try:
            # Always pass valid pointers for both outputs.
            self._char_buffer.value = b""
            self._info_double.value = 0.0

            ret = self.GINSDll._CD_eGateHighSpeedPort_GetDeviceInfo(
                self.HCONNECTION.value,
                int(info_type.value),
                int(index),
                byref(self._info_double),
                self._char_buffer,
            )

            if not self._handle_error(f"GetDeviceInfo({info_type})", ret, raise_exception=False):
                return None

            if as_string:
                return self._char_buffer.value.decode("utf-8", errors="replace")
            return self._info_double.value

        except Exception as e:
            self.logger.error("Device info failed: %s", e)
            return None

    def get_channel_info(self, channel_index: int, as_string: bool = False):
        if not self._check_connection():
            return None

        try:
            if as_string:
                ret = self.GINSDll._CD_eGateHighSpeedPort_GetChannelInfo_String(
                    self.HCONNECTION.value,
                    0,
                    self.DADI_INOUT,
                    int(channel_index),
                    self._char_buffer,
                )
                if self._handle_error(f"GetChannelInfo_String({channel_index})", ret, raise_exception=False):
                    return self._char_buffer.value.decode("utf-8", errors="replace")
            else:
                ret = self.GINSDll._CD_eGateHighSpeedPort_GetChannelInfo_Int(
                    self.HCONNECTION.value,
                    self.CHINFO_INDX,
                    self.DADI_INOUT,
                    int(channel_index),
                    byref(self._channel_info_int),
                )
                if self._handle_error(f"GetChannelInfo_Int({channel_index})", ret, raise_exception=False):
                    return self._channel_info_int.value

            return None

        except Exception as e:
            self.logger.error("Channel info failed: %s", e)
            return None

    def yield_buffer(self, frames_per_chunk: int = 100000, fill_array: int = 0, straight_timestamp: bool = False):
        if not self._check_connection():
            return

        channel_count = self.get_device_info(DeviceInfoType.CHANNEL_COUNT, as_string=False)
        channel_count = int(channel_count) if channel_count else 0
        if channel_count == 0:
            self.logger.error("No channels")
            return

        buf_size = int(frames_per_chunk) * int(channel_count)
        values_ptr = (c_double * buf_size)()

        received_frames = c_int(0)
        received_channels = c_int(0)
        received_complete = c_int(0)

        try:
            while True:
                if straight_timestamp:
                    ret = self.GINSDll._CD_eGateHighSpeedPort_ReadBufferToDoubleArray_StraightTimestamp(
                        self.HCONNECTION.value,
                        values_ptr,
                        buf_size,
                        int(fill_array),
                        byref(received_frames),
                        byref(received_channels),
                        byref(received_complete),
                    )
                else:
                    ret = self.GINSDll._CD_eGateHighSpeedPort_ReadBufferToDoubleArray(
                        self.HCONNECTION.value,
                        values_ptr,
                        buf_size,
                        int(fill_array),
                        byref(received_frames),
                        byref(received_channels),
                        byref(received_complete),
                    )

                if ret != 0:
                    self._handle_error("ReadBufferToDoubleArray", ret, raise_exception=False)
                    break

                frames = received_frames.value
                chans = received_channels.value
                if frames > 0 and chans > 0:
                    data = np.array(values_ptr[: frames * chans])
                    yield data.reshape((frames, chans))

        except Exception as e:
            self.logger.error("Buffer yield failed: %s", e)

    def close_connection(self):
        try:
            if self.HCONNECTION.value != -1 and self.HCLIENT.value != -1:
                ret = self.GINSDll._CD_eGateHighSpeedPort_Close(self.HCONNECTION.value, self.HCLIENT.value)
                if ret != 0:
                    self._handle_error("Close", ret, raise_exception=False)

            self.HCONNECTION.value = -1
            self.HCLIENT.value = -1
            self.connection_state = ConnectionState.DISCONNECTED
            self.logger.info("Connection closed")

        except Exception as e:
            self.logger.error("Close failed: %s", e)

    def init_post_process_buffer_conn(self, buffer_id: str) -> bool:
        try:
            if self.connection_state != ConnectionState.DISCONNECTED:
                self.close_connection()

            ret = self.GINSDll._CD_eGateHighSpeedPort_Init_PostProcessBuffer(
                buffer_id.encode("utf-8"),
                byref(self.HCLIENT),
                byref(self.HCONNECTION),
            )
            if not self._handle_error("Init_PostProcessBuffer", ret, raise_exception=False):
                return False

            self.connection_state = ConnectionState.BUFFER_CONNECTION
            self.logger.info("Connected to buffer: %s", buffer_id)
            return True

        except Exception as e:
            self.logger.error("Buffer connection failed: %s", e)
            return False

    def get_buffer_info(self, buffer_index: int) -> BufferInfo:
        try:
            ret = self.GINSDll._CD_eGateHighSpeedPort_GetPostProcessBufferInfo(
                int(buffer_index),
                self._buffer_id,
                len(self._buffer_id),
                self._buffer_name,
                len(self._buffer_name),
            )
            if self._handle_error(f"GetPostProcessBufferInfo({buffer_index})", ret, raise_exception=False):
                name = self._buffer_name.value.decode("utf-8", errors="replace")
                bid = self._buffer_id.value.decode("utf-8", errors="replace")
                return BufferInfo(int(buffer_index), name, bid)
            return None

        except Exception as e:
            self.logger.error("Buffer info failed: %s", e)
            return None

    def get_buffer_count(self) -> int:
        try:
            if hasattr(self.GINSDll, "_CD_eGateHighSpeedPort_GetPostProcessBufferCount"):
                return int(self.GINSDll._CD_eGateHighSpeedPort_GetPostProcessBufferCount())
            return -1
        except Exception as e:
            self.logger.error("Buffer count failed: %s", e)
            return -1

    def write_online_value(self, channel_index: int, value: float, immediate: bool = True) -> bool:
        """Write a value to an ONLINE channel."""
        if not self._check_connection():
            return False

        try:
            if immediate:
                ret = self.GINSDll._CD_eGateHighSpeedPort_WriteOnline_Single_Immediate(
                    self.HCONNECTION.value,
                    int(channel_index),
                    c_double(float(value)),
                )
                op = f"WriteOnline_Single_Immediate(ch={channel_index})"
            else:
                ret = self.GINSDll._CD_eGateHighSpeedPort_WriteOnline_Single(
                    self.HCONNECTION.value,
                    int(channel_index),
                    c_double(float(value)),
                )
                op = f"WriteOnline_Single(ch={channel_index})"

            return self._handle_error(op, ret, raise_exception=False)

        except Exception as e:
            self.logger.error("Write failed: %s", e)
            return False

    def read_online_single(self, channel_index: int):
        """Read a single ONLINE channel value."""
        if not self._check_connection():
            return None

        try:
            value_out = c_double()
            ret = self.GINSDll._CD_eGateHighSpeedPort_ReadOnline_Single(
                self.HCONNECTION.value,
                int(channel_index),
                byref(value_out),
            )

            if self._handle_error(f"ReadOnline_Single(ch={channel_index})", ret, raise_exception=False):
                return float(value_out.value)

            return None

        except Exception as e:
            self.logger.error("Read single failed: %s", e)
            return None

    def release_output(self) -> bool:
        """Release buffered outputs when using non-immediate writes."""
        if not self._check_connection():
            return False

        try:
            ret = self.GINSDll._CD_eGateHighSpeedPort_WriteOnline_ReleaseOutputData(
                self.HCONNECTION.value
            )
            return self._handle_error("WriteOnline_ReleaseOutputData", ret, raise_exception=False)

        except Exception as e:
            self.logger.error("Release failed: %s", e)
            return False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_connection()

    @property
    def is_connected(self) -> bool:
        return self.connection_state != ConnectionState.DISCONNECTED and self.HCONNECTION.value != -1

    @property
    def last_error(self) -> int:
        return self._last_error

    # --- Convenience wrappers ------------------------------------

    def read_serial_number(self) -> float:
        result = self.get_device_info(DeviceInfoType.SERIAL_NUMBER, as_string=False)
        return float(result) if result is not None else 0.0

    def read_sample_rate(self) -> float:
        result = self.get_device_info(DeviceInfoType.SAMPLE_RATE, as_string=False)
        return float(result) if result is not None else 0.0

    def read_channel_count(self) -> int:
        result = self.get_device_info(DeviceInfoType.CHANNEL_COUNT, as_string=False)
        return int(result) if result is not None else 0

    def read_controller_name(self) -> str:
        result = self.get_device_info(DeviceInfoType.LOCATION, as_string=True)
        return result or ""

    def read_controller_address(self) -> str:
        result = self.get_device_info(DeviceInfoType.ADDRESS, as_string=True)
        return result or ""

    def read_index_name(self, index: int) -> str:
        # channel info string uses "Info type 0" in your existing get_channel_info(as_string=True)
        # which corresponds to the name in the original code.
        result = self.get_channel_info(index, as_string=True)
        return result or ""

    def read_index_unit(self, index: int) -> str:
        # Unit is device info string per-channel (your original code used DeviceInfoType.CHANNEL_UNIT)
        result = self.get_device_info(DeviceInfoType.CHANNEL_UNIT, index, as_string=True)
        if not result:
            return ""

        # Preserve your special-char fixups
        if result == "\xb0C":
            return "°C"
        if result == "\xb5m/m":
            return "µm/m"
        return result

    def read_channel_names(self) -> dict[int, str]:
        count = self.read_channel_count()
        channels: dict[int, str] = {}
        for i in range(count):
            name = self.read_index_name(i)
            if name:
                channels[i] = name
        return channels


def read_gins_dat_file(connection: HighSpeedPortClient):
    if not connection.is_connected:
        raise HighSpeedPortError("Not connected")

    buffer_gen = connection.yield_buffer()
    try:
        data = next(buffer_gen)
        while True:
            try:
                new_data = next(buffer_gen)
                if len(new_data) > 0 and data[-1, 0] < new_data[0, 0]:
                    data = np.vstack((data, new_data))
                else:
                    break
            except StopIteration:
                break
        logger.info("Read %d rows", len(data))
        return data
    except Exception as e:
        logger.error("DAT read failed: %s", e)
        raise


def create_channel_list(connection: HighSpeedPortClient):
    count = connection.get_device_info(DeviceInfoType.CHANNEL_COUNT, as_string=False)
    count = int(count) if count else 0

    channels = []
    for i in range(count):
        name = connection.get_channel_info(i, as_string=True) or ""
        unit = connection.get_device_info(DeviceInfoType.CHANNEL_UNIT, i, as_string=True) or ""
        channels.append(ChannelInfo(i, name, unit))
    return channels
