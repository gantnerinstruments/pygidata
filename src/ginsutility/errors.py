from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class HspStatus:
    code: int
    name: str
    meaning: str
    dll_detail: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.code == 0

    def format(self, operation: str) -> str:
        msg = f"{operation} failed ({self.name}={self.code}): {self.meaning}"
        if self.dll_detail:
            msg += f" | dll: {self.dll_detail}"
        return msg


class HspError(RuntimeError):
    def __init__(self, status: HspStatus, operation: str):
        self.status = status
        self.operation = operation
        super().__init__(status.format(operation))


class HspNotReady(HspError):
    pass


class HspNotConnected(HspError):
    pass


class HspInvalidIndex(HspError):
    pass


class HspInitError(HspError):
    pass


class HspFileError(HspError):
    pass


class HspCoreError(HspError):
    pass


class HspLimitError(HspError):
    pass


class HspPointerInvalid(HspError):
    pass


class HspNotImplemented(HspError):
    pass


class HspInvalidTimestamp(HspError):
    pass


class HspErrorUtil:
    # Return codes from your DLL header
    HSP_OK = 0
    HSP_ERROR = 1
    HSP_CONNECTION_ERROR = 2
    HSP_INIT_ERROR = 3
    HSP_LIMIT_ERROR = 4
    HSP_SYNC_CONF_ERROR = 5
    HSP_MULTYUSED_ERROR = 6
    HSP_INDEX_ERROR = 7
    HSP_FILE_ERROR = 8
    HSP_NOT_READY = 9
    HSP_EXLIB_MISSING = 10
    HSP_NOT_CONNECTED = 11
    HSP_NO_FILE = 12
    HSP_CORE_ERROR = 13
    HSP_POINTER_INVALID = 14
    HSP_NOT_IMPLEMENTED = 15
    HSP_INVALID_TIMESTAMP = 16
    HSP_COMPLETE = 17

    _CODE_INFO: dict[int, tuple[str, str]] = {
        HSP_OK: ("HSP_OK", "Success"),
        HSP_ERROR: ("HSP_ERROR", "Generic error"),
        HSP_CONNECTION_ERROR: ("HSP_CONNECTION_ERROR", "Connection error"),
        HSP_INIT_ERROR: ("HSP_INIT_ERROR", "Initialization error"),
        HSP_LIMIT_ERROR: ("HSP_LIMIT_ERROR", "Limit exceeded"),
        HSP_SYNC_CONF_ERROR: ("HSP_SYNC_CONF_ERROR", "Sync/config error"),
        HSP_MULTYUSED_ERROR: ("HSP_MULTYUSED_ERROR", "Resource already in use"),
        HSP_INDEX_ERROR: ("HSP_INDEX_ERROR", "Index invalid/out of range"),
        HSP_FILE_ERROR: ("HSP_FILE_ERROR", "File error"),
        HSP_NOT_READY: ("HSP_NOT_READY", "Not ready yet"),
        HSP_EXLIB_MISSING: ("HSP_EXLIB_MISSING", "External library missing"),
        HSP_NOT_CONNECTED: ("HSP_NOT_CONNECTED", "Not connected"),
        HSP_NO_FILE: ("HSP_NO_FILE", "No file selected/available"),
        HSP_CORE_ERROR: ("HSP_CORE_ERROR", "Core/internal error"),
        HSP_POINTER_INVALID: ("HSP_POINTER_INVALID", "Pointer invalid/null"),
        HSP_NOT_IMPLEMENTED: ("HSP_NOT_IMPLEMENTED", "Not implemented"),
        HSP_INVALID_TIMESTAMP: ("HSP_INVALID_TIMESTAMP", "Invalid timestamp"),
        HSP_COMPLETE: ("HSP_COMPLETE", "Operation complete"),
    }

    @staticmethod
    def status_from_ret(ret_code: int, error_buffer=None) -> HspStatus:
        code = int(ret_code)
        name, meaning = HspErrorUtil._CODE_INFO.get(code, (f"HSP_UNKNOWN_{code}", "Unknown return code"))

        detail: Optional[str] = None
        if error_buffer is not None:
            try:
                raw = getattr(error_buffer, "value", b"") or b""
                if raw:
                    decoded = raw.decode("utf-8", errors="replace").strip()
                    detail = decoded or None
            except Exception:
                detail = None

        return HspStatus(code=code, name=name, meaning=meaning, dll_detail=detail)

    @staticmethod
    def to_exception(status: HspStatus, operation: str) -> HspError:
        c = status.code
        if c == HspErrorUtil.HSP_NOT_READY:
            return HspNotReady(status, operation)
        if c in (HspErrorUtil.HSP_NOT_CONNECTED, HspErrorUtil.HSP_CONNECTION_ERROR):
            return HspNotConnected(status, operation)
        if c == HspErrorUtil.HSP_INDEX_ERROR:
            return HspInvalidIndex(status, operation)
        if c == HspErrorUtil.HSP_INIT_ERROR:
            return HspInitError(status, operation)
        if c in (HspErrorUtil.HSP_FILE_ERROR, HspErrorUtil.HSP_NO_FILE):
            return HspFileError(status, operation)
        if c == HspErrorUtil.HSP_CORE_ERROR:
            return HspCoreError(status, operation)
        if c == HspErrorUtil.HSP_LIMIT_ERROR:
            return HspLimitError(status, operation)
        if c == HspErrorUtil.HSP_POINTER_INVALID:
            return HspPointerInvalid(status, operation)
        if c == HspErrorUtil.HSP_NOT_IMPLEMENTED:
            return HspNotImplemented(status, operation)
        if c == HspErrorUtil.HSP_INVALID_TIMESTAMP:
            return HspInvalidTimestamp(status, operation)

        return HspError(status, operation)

    @staticmethod
    def check_ret(
            logger,
            operation: str,
            ret_code: int,
            *,
            error_buffer=None,
            raise_exception: bool = True,
            log_level_on_error: str = "error",
            treat_not_ready_as_ok: bool = False,
    ) -> bool:
        status = HspErrorUtil.status_from_ret(ret_code, error_buffer=error_buffer)
        if status.ok:
            return True

        if treat_not_ready_as_ok and status.code == HspErrorUtil.HSP_NOT_READY:
            logger.debug("%s", status.format(operation))
            return False

        log_fn = getattr(logger, log_level_on_error, None)
        if not callable(log_fn):
            log_fn = logger.error
        log_fn("%s", status.format(operation))

        if raise_exception:
            raise HspErrorUtil.to_exception(status, operation)

        return False
