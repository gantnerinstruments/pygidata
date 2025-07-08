from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional, Tuple, Union
from uuid import UUID

import nest_asyncio
import pandas as pd

from gimodules.gi_data.drivers.local_http import LocalHTTPDriver
from gimodules.gi_data.drivers.ws_stream import WebSocketDriver
from gimodules.gi_data.infra.auth import AuthManager
from gimodules.gi_data.infra.http import AsyncHTTP
from gimodules.gi_data.mapping.models import GIStream

asyncio.set_event_loop(asyncio.new_event_loop())


def _to_task(future, as_task, loop):
    if not as_task or isinstance(future, asyncio.Task):
        return future
    return loop.create_task(future)


def _run(future, as_task=True):
    """
    A safer implementation of async call runner.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # No running loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(_to_task(future, as_task, loop))
    else:
        # Running loop: allow nested usage
        nest_asyncio.apply(loop)
        return loop.run_until_complete(_to_task(future, as_task, loop))


class GIDataClient:
    """
    High-level synchronous interface for GI Data-API.
    """

    def __init__(
            self,
            base_url: str,
            *,
            username: Optional[str] = None,
            password: Optional[str] = None,
    ) -> None:
        self._ws_driver: Optional[WebSocketDriver] = None
        self._auth = AuthManager(base_url, username, password)
        self._http = AsyncHTTP(base_url, self._auth)
        self._driver = LocalHTTPDriver(self._auth, self._http, None)

    # ------------------------------- Online ---------------------------- #

    def list_variables(self) -> List[dict]:
        """Return metadata dictionaries for every online variable."""
        return _run(self._driver.list_variables())

    def read_online(self, var_ids: List[UUID]) -> Dict[UUID, float]:
        """Read current values of the given variable UUIDs."""
        return _run(self._driver.read(var_ids))

    def write_online(self, mapping: Dict[UUID, float]) -> None:
        """Write new values to the given variable UUIDs."""
        _run(self._driver.write(mapping))

    # ------------------------------- Buffer ---------------------------- #

    def list_buffer_sources(self) -> List[GIStream]:
        """Return definitions of all buffer streams."""
        return _run(self._driver.list_sources())

    def list_stream_variables(self, source_id: Union[UUID, int]):
        """Return metadata dictionaries for every online variable."""
        return _run(self._driver.list_stream_variables(source_id))

    def fetch_buffer(
            self,
            selectors: List[Tuple[Union[UUID, int], UUID]],
            *,
            start_ms: int = -20_000,
            end_ms: int = 0,
            points: int = 2048,
    ) -> pd.DataFrame:
        return _run(
            self._driver.fetch_buffer(
                selectors, start_ms=start_ms, end_ms=end_ms, points=points
            )
        )

    # ------------------------------- History ---------------------------- #

    def list_history_measurements(self, source_id: UUID) -> List[Dict[str, Any]]:
        """Return measurement metadata for a history source."""
        return _run(self._driver.list_measurements(source_id))

    def fetch_history(
            self,
            source_id: UUID,
            measurement_id: UUID,
            var_ids: List[UUID],
            *,
            start_ms: int = 0,
            end_ms: int = 0,
            points: int = 2048,
    ) -> pd.DataFrame:
        return _run(
            self._driver.fetch_history(
                source_id,
                measurement_id,
                var_ids,
                start_ms=start_ms,
                end_ms=end_ms,
                points=points,
            )
        )

    # ------------------------------- Websocket ------------------------- #

    async def stream_online(
            self,
            var_ids: List[UUID],
            *,
            interval_ms: int = 1,
            extended: bool = True,
            on_change: bool = True,
            precision: int = -1,
    ):
        """
        Async generator that yields {uuid: value} ticks.

        Example:
            async for tick in client.stream_online([vid], interval_ms=10):
                print(tick)
        """
        driver = await self._ensure_ws_driver()
        async for update in driver.stream_online(
                var_ids,
                interval_ms=interval_ms,
                extended=extended,
                on_change=on_change,
                precision=precision,
        ):
            yield update

    async def publish_online(
            self,
            data: Dict[UUID, float] | List[tuple[UUID, float]],
            *,
            function: str = "write",
    ) -> None:
        """
        Push values (set-point variables).

        Example:
            await client.publish_online({vid: 42.0})
        """
        driver = await self._ensure_ws_driver()
        await driver.publish(data, function=function)

    async def _ensure_ws_driver(self):
        """Lazy-initialise the Web-Socket layer exactly once."""
        if self._ws_driver is None:
            from gimodules.gi_data.infra.ws import AsyncWS
            from gimodules.gi_data.drivers.ws_stream import WebSocketDriver

            ws = AsyncWS(self._http.base_url, self._auth)
            self._ws_driver = WebSocketDriver(self._auth, ws, self._http)
        return self._ws_driver

    def close(self) -> None:
        """Close all underlying network connections."""
        _run(self._driver.http.aclose())

    def __enter__(self) -> GIDataClient:
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.close()
        return False
