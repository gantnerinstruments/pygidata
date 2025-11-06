from __future__ import annotations

import abc
from typing import AsyncIterator, Dict, List
from uuid import UUID

import pandas as pd


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

    async def list_sources(self) -> List["Source"]:  # noqa: F821
        """Return buffer-stream definitions."""
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