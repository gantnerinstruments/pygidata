from __future__ import annotations

import abc
from typing import AsyncIterator, Dict, List, Literal, Optional
from uuid import UUID

import pandas as pd

from gi_data.mapping.models import LogSettings, CSVSettings, VarSelector


class BaseDriver(abc.ABC):
    """
    Abstract transport driver.

    Concrete subclasses implement only the subset of methods
    their protocol / product family supports.
    """

    priority: int = 10
    name: str = "base"

    def __init__(self, auth_manager, http_client, ws_client) -> None:
        self.auth = auth_manager
        self.http = http_client
        self.ws = ws_client

    # ----------------------------  ONLINE  --------------------------------

    async def list_variables(self) -> List["Variable"]:  # noqa: F821
        """Return metadata for every online variable."""
        raise NotImplementedError

    async def read(self, var_ids: List[UUID]) -> Dict[UUID, float]:
        """Read current online values for a list of UUIDs."""
        raise NotImplementedError

    async def write(self, mapping: Dict[UUID, float]) -> None:
        """Write values to online variables."""
        raise NotImplementedError

    # ----------------------------  BUFFER  --------------------------------

    async def list_buffer_sources(self) -> List["Source"]:  # noqa: F821
        """Return buffer-stream definitions."""
        raise NotImplementedError

    async def list_buffer_variables(self, source_id) -> List["GIStreamVariable"]:  # noqa: F821
        """Return buffer-stream variables."""
        raise NotImplementedError

    async def fetch_buffer(self, *args, **kwargs) -> "TimeSeriesFrame":  # noqa: F821
        """Fetch equidistant or absolute buffer data."""
        raise NotImplementedError

    # ---------------------------  HISTORY  --------------------------------

    async def list_measurements(self, *args, **kwargs) -> List["Measurement"]:  # noqa: F821
        """Return measurements inside a history source."""
        raise NotImplementedError

    async def fetch_history(self, *args, **kwargs) -> "TimeSeriesFrame":  # noqa: F821
        """Read historical data within a time window."""
        raise NotImplementedError

    # ---------------------------  STREAMING  ------------------------------

    def stream(
            self, worker: str, **cfg
    ) -> AsyncIterator[pd.DataFrame]:  # pragma: no cover
        """
        Subscribe to a WebSocket worker and yield DataFrame chunks.

        Implementation is optional; drivers that do not support WebSocket
        simply raise `NotImplementedError`.
        """
        raise NotImplementedError

    async def export_data(
            self,
            selectors: List["VarSelector"],
            *,
            start_ms: float,
            end_ms: float,
            format: Literal["csv", "udbf"],
            points: Optional[int] = None,
            timezone: str = "UTC",
            aggregation: Optional[str] = None,
            date_format: Optional[str] = None,
            filename: Optional[str] = None,
            precision: int = -1,
            csv_settings: Optional["CSVSettings"] = None,
            log_settings: Optional["LogSettings"] = None,
            target: Optional[str] = None,
    ) -> bytes:
        raise NotImplementedError

    def supported_exports(self) -> set[str]:
        return {"csv", "udbf"}

    def import_csv(self, source_id, source_name, file_bytes, target,
                   csv_settings, add_time_series, retention_time_sec,
                   time_offset_sec, sample_rate, auto_create_metadata, session_timeout_sec):
        pass

    def import_udbf(self, source_id, source_name, file_bytes,
                    target, add_time_series, sample_rate,
                    auto_create_metadata, session_timeout_sec):
        pass

    def export(self, selectors, start_ms, end_ms, format, points,
               timezone, resolution, data_type, aggregation,
               date_format, filename, precision, csv_settings, log_settings, target):
        pass
