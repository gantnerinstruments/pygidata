# -*- coding: utf-8 -*-
"""PostProcessBuffer manager for creating and writing buffers."""

import logging
from ctypes import (
    c_char_p,
    c_double,
    c_int,
    c_int32,
    c_uint16,
    c_uint32,
    c_uint64,
    POINTER,
    byref,
    create_string_buffer,
)

from .loader import load_giutility
from .models import PostProcessBufferError, DataType, VariableKind
from .errors import HspErrorUtil

logger = logging.getLogger(__name__)


class PostProcessBufferManager:
    """Manager for PostProcess buffer operations."""

    def __init__(self, log_level: int = logging.INFO):
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.logger.setLevel(log_level)

        self.GINSDll = load_giutility()
        self.logger.debug("Loaded GiUtility")

        self._setup_function_prototypes()
        self._init_parameters()

    def _setup_function_prototypes(self):
        try:
            self.GINSDll._CD_eGateHighSpeedPort_PostProcessBufferServer_Create.argtypes = [
                c_char_p,
                c_char_p,
                c_double,
                c_uint64,
                c_uint64,
                c_double,
                c_char_p,
                POINTER(c_int32),
                c_char_p,
                c_int,
            ]
            self.GINSDll._CD_eGateHighSpeedPort_PostProcessBufferServer_Create.restype = c_int32

            self.GINSDll._CD_eGateHighSpeedPort_PostProcessBufferServer_CreateUDBFFileBuffer.argtypes = [
                c_char_p,
                c_char_p,
                c_char_p,
                c_double,
                c_uint64,
                c_uint16,
                POINTER(c_int32),
                c_char_p,
                c_uint32,
            ]
            self.GINSDll._CD_eGateHighSpeedPort_PostProcessBufferServer_CreateUDBFFileBuffer.restype = c_int32

            self.GINSDll._CD_eGateHighSpeedPort_PostProcessBufferServer_AddVariableDefinition.argtypes = [
                c_int32,
                c_char_p,
                c_char_p,
                c_char_p,
                c_int32,
                c_int32,
                c_int32,
                c_int32,
                c_double,
                c_double,
                c_char_p,
                c_int,
            ]
            self.GINSDll._CD_eGateHighSpeedPort_PostProcessBufferServer_AddVariableDefinition.restype = c_int32

            self.GINSDll._CD_eGateHighSpeedPort_PostProcessBufferServer_Initialize.argtypes = [
                c_int32,
                c_int32,
                c_char_p,
                c_int,
            ]
            self.GINSDll._CD_eGateHighSpeedPort_PostProcessBufferServer_Initialize.restype = c_int32

            self.GINSDll._CD_eGateHighSpeedPort_PostProcessBufferServer_WriteDoubleToFrameBuffer.argtypes = [
                c_int32,
                c_int32,
                c_int32,
                c_double,
                c_char_p,
                c_int,
            ]
            self.GINSDll._CD_eGateHighSpeedPort_PostProcessBufferServer_WriteDoubleToFrameBuffer.restype = c_int32

            self.GINSDll._CD_eGateHighSpeedPort_PostProcessBufferServer_WriteTimestampToFrameBuffer.argtypes = [
                c_int32,
                c_int32,
                c_uint64,
                c_char_p,
                c_int,
            ]
            self.GINSDll._CD_eGateHighSpeedPort_PostProcessBufferServer_WriteTimestampToFrameBuffer.restype = c_int32

            self.GINSDll._CD_eGateHighSpeedPort_PostProcessBufferServer_AppendFrameBuffer.argtypes = [
                c_int32,
                c_char_p,
                c_int,
            ]
            self.GINSDll._CD_eGateHighSpeedPort_PostProcessBufferServer_AppendFrameBuffer.restype = c_int32

            self.GINSDll._CD_eGateHighSpeedPort_PostProcessBufferServer_AppendDataBuffer.argtypes = [
                c_int32,
                c_char_p,
                c_uint64,
                c_char_p,
                c_int,
            ]
            self.GINSDll._CD_eGateHighSpeedPort_PostProcessBufferServer_AppendDataBuffer.restype = c_int32

            self.GINSDll._CD_eGateHighSpeedPort_PostProcessBufferServer_Close.argtypes = [
                c_int32,
                c_char_p,
                c_int,
            ]
            self.GINSDll._CD_eGateHighSpeedPort_PostProcessBufferServer_Close.restype = c_int32

            self.GINSDll._CD_eGateHighSpeedPort_SleepMS.argtypes = [c_int32]
            self.GINSDll._CD_eGateHighSpeedPort_SleepMS.restype = None

        except AttributeError as e:
            self.logger.error("Prototype setup failed: %s", e)
            raise PostProcessBufferError(f"Prototype setup failed: {e}")

    def _init_parameters(self):
        self.buffer_handle = c_int32(-1)
        self._error_buffer = create_string_buffer(1024)
        self._error_len = len(self._error_buffer)

        self.buffer_size = 50_000_000
        self.segment_size = 50_000_000
        self.frame_buffer_length = 1
        self.data_type_ident = "raw"

        self.default_data_type = DataType.DOUBLE
        self.default_variable_kind = VariableKind.ANALOG_INPUT
        self.default_precision = 4
        self.default_field_length = 8
        self.default_range_min = -100.0
        self.default_range_max = 100.0

    def _check(self, operation: str, ret: int, raise_exception: bool = True) -> bool:
        try:
            return HspErrorUtil.check_ret(
                self.logger,
                operation,
                ret,
                error_buffer=self._error_buffer,
                raise_exception=raise_exception,
            )
        except Exception as e:
            if raise_exception:
                raise PostProcessBufferError(str(e))
            return False

    def create_buffer(self, buffer_id: str, buffer_name: str, sample_rate: float) -> bool:
        ret = self.GINSDll._CD_eGateHighSpeedPort_PostProcessBufferServer_Create(
            buffer_id.encode("utf-8"),
            buffer_name.encode("utf-8"),
            c_double(sample_rate),
            c_uint64(self.buffer_size),
            c_uint64(self.segment_size),
            c_double(0.0),
            self.data_type_ident.encode("utf-8"),
            byref(self.buffer_handle),
            self._error_buffer,
            self._error_len,
        )
        ok = self._check("PostProcessBufferServer_Create", ret, raise_exception=False)
        if ok:
            self.logger.info("Buffer created: %s", buffer_name)
        return ok

    def create_udbf_file_buffer(
            self,
            filepath: str,
            source_id: str,
            source_name: str,
            sample_rate: float,
            max_length_seconds: int,
            options: int = 0,
    ) -> bool:
        ret = self.GINSDll._CD_eGateHighSpeedPort_PostProcessBufferServer_CreateUDBFFileBuffer(
            filepath.encode("utf-8"),
            source_id.encode("utf-8"),
            source_name.encode("utf-8"),
            c_double(sample_rate),
            c_uint64(max_length_seconds),
            c_uint16(options),
            byref(self.buffer_handle),
            self._error_buffer,
            c_uint32(self._error_len),
        )
        ok = self._check("PostProcessBufferServer_CreateUDBFFileBuffer", ret, raise_exception=False)
        if ok:
            self.logger.info("UDBF buffer created: %s", filepath)
        return ok

    def add_channel(
            self,
            variable_id: str,
            variable_name: str,
            unit: str,
            data_type: int = None,
            variable_kind: int = None,
            precision: int = None,
            field_length: int = None,
            range_min: float = None,
            range_max: float = None,
    ) -> bool:
        if self.buffer_handle.value == -1:
            self.logger.error("No buffer")
            return False

        data_type = data_type if data_type is not None else self.default_data_type
        variable_kind = variable_kind if variable_kind is not None else self.default_variable_kind
        precision = precision if precision is not None else self.default_precision
        field_length = field_length if field_length is not None else self.default_field_length
        range_min = range_min if range_min is not None else self.default_range_min
        range_max = range_max if range_max is not None else self.default_range_max

        ret = self.GINSDll._CD_eGateHighSpeedPort_PostProcessBufferServer_AddVariableDefinition(
            self.buffer_handle,
            variable_id.encode("utf-8"),
            variable_name.encode("utf-8"),
            unit.encode("utf-8"),
            c_int32(int(data_type)),
            c_int32(int(variable_kind)),
            c_int32(int(precision)),
            c_int32(int(field_length)),
            c_double(float(range_min)),
            c_double(float(range_max)),
            self._error_buffer,
            self._error_len,
        )
        return self._check(f"AddVariableDefinition({variable_name})", ret, raise_exception=False)

    def initialize_buffer(self, frame_buffer_length: int = None) -> bool:
        if self.buffer_handle.value == -1:
            self.logger.error("No buffer")
            return False

        if frame_buffer_length is not None:
            self.frame_buffer_length = int(frame_buffer_length)

        ret = self.GINSDll._CD_eGateHighSpeedPort_PostProcessBufferServer_Initialize(
            self.buffer_handle,
            c_int32(self.frame_buffer_length),
            self._error_buffer,
            self._error_len,
        )
        ok = self._check("PostProcessBufferServer_Initialize", ret, raise_exception=False)
        if ok:
            self.logger.info("Buffer initialized: %d frames", self.frame_buffer_length)
        return ok

    def write_timestamp(self, frame_index: int, timestamp_ns: int) -> bool:
        if self.buffer_handle.value == -1:
            self.logger.error("Buffer not initialized")
            return False

        ret = self.GINSDll._CD_eGateHighSpeedPort_PostProcessBufferServer_WriteTimestampToFrameBuffer(
            self.buffer_handle,
            c_int32(int(frame_index)),
            c_uint64(int(timestamp_ns)),
            self._error_buffer,
            self._error_len,
        )
        return self._check(f"WriteTimestamp(frame={frame_index})", ret, raise_exception=False)

    def write_value(self, frame_index: int, variable_index: int, value: float) -> bool:
        if self.buffer_handle.value == -1:
            self.logger.error("Buffer not initialized")
            return False

        ret = self.GINSDll._CD_eGateHighSpeedPort_PostProcessBufferServer_WriteDoubleToFrameBuffer(
            self.buffer_handle,
            c_int32(int(frame_index)),
            c_int32(int(variable_index)),
            c_double(float(value)),
            self._error_buffer,
            self._error_len,
        )
        return self._check(f"WriteDouble(frame={frame_index}, var={variable_index})", ret, raise_exception=False)

    def append_frame_buffer(self) -> bool:
        if self.buffer_handle.value == -1:
            self.logger.error("Buffer not initialized")
            return False

        ret = self.GINSDll._CD_eGateHighSpeedPort_PostProcessBufferServer_AppendFrameBuffer(
            self.buffer_handle,
            self._error_buffer,
            self._error_len,
        )
        return self._check("AppendFrameBuffer", ret, raise_exception=False)

    def append_data_buffer(self, data: bytes, data_length: int = None) -> bool:
        if self.buffer_handle.value == -1:
            self.logger.error("Buffer not initialized")
            return False

        if not isinstance(data, (bytes, bytearray, memoryview)):
            self.logger.error("append_data_buffer expects bytes-like object")
            return False

        if isinstance(data, memoryview):
            data = data.tobytes()
        elif isinstance(data, bytearray):
            data = bytes(data)

        if data_length is None:
            data_length = len(data)

        self._clear_error()
        ret = self.GINSDll._CD_eGateHighSpeedPort_PostProcessBufferServer_AppendDataBuffer(
            self.buffer_handle,
            c_char_p(data),  # explicit
            c_uint64(int(data_length)),
            self._error_buffer,
            self._error_len,
        )
        return self._check("AppendDataBuffer", ret, raise_exception=False)

    def get_buffer_count(self) -> int:
        try:
            if hasattr(self.GINSDll, "_CD_eGateHighSpeedPort_GetPostProcessBufferCount"):
                return int(self.GINSDll._CD_eGateHighSpeedPort_GetPostProcessBufferCount())
            return -1
        except Exception as e:
            self.logger.error("Buffer count failed: %s", e)
            return -1

    def close_buffer(self):
        try:
            if self.buffer_handle.value != -1:
                self._clear_error()
                ret = self.GINSDll._CD_eGateHighSpeedPort_PostProcessBufferServer_Close(
                    self.buffer_handle,
                    self._error_buffer,
                    self._error_len,
                )
                if ret != 0:
                    self._check("CloseBuffer", ret, raise_exception=False)
                else:
                    self.logger.info("Buffer closed")
        finally:
            self.buffer_handle.value = -1

    def _handle_error(self, operation: str, ret_code: int, raise_exception: bool = True) -> bool:
        return self._check(operation, ret_code, raise_exception=raise_exception)

    def _clear_error(self) -> None:
        try:
            self._error_buffer.value = b""
        except Exception:
            pass

    def __del__(self):
        try:
            self.close_buffer()
        except Exception:
            pass

    def sleep_ms(self, milliseconds: int):
        self.GINSDll._CD_eGateHighSpeedPort_SleepMS(c_int32(int(milliseconds)))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_buffer()

    @property
    def is_initialized(self) -> bool:
        return self.buffer_handle.value != -1
