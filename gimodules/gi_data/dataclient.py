# gimodules/gi_data/dataclient.py
from __future__ import annotations

import asyncio
import inspect
import logging
from typing import Any, Dict, List, Optional, Tuple, Union, Type
from uuid import UUID

import nest_asyncio
import pandas as pd

from gimodules.gi_data.drivers.base import BaseDriver
from gimodules.gi_data.drivers.cloud_gql import CloudGQLDriver
from gimodules.gi_data.drivers.kafka_stream import KafkaStreamDriver
from gimodules.gi_data.drivers.local_http import HTTPTimeSeriesDriver
from gimodules.gi_data.drivers.ws_stream import WebSocketDriver
from gimodules.gi_data.infra.auth import AuthManager
from gimodules.gi_data.infra.http import AsyncHTTP
from gimodules.gi_data.mapping.enums import Resolution, DataType, DataFormat
from gimodules.gi_data.mapping.models import GIStream, GIStreamVariable, GIOnlineVariable, VarSelector, CSVSettings, \
    LogSettings, CSVImportSettings
from gimodules.gi_data.utils.logging import setup_module_logger

logger = setup_module_logger(__name__, level=logging.DEBUG)

# ------------------------------------------------------------------ #
# helpers                                                            #
# ------------------------------------------------------------------ #
asyncio.set_event_loop(asyncio.new_event_loop())


def _to_task(fut, as_task, loop):
    if not as_task or isinstance(fut, asyncio.Task):
        return fut
    return loop.create_task(fut)


def _run(fut, as_task=True):
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(_to_task(fut, as_task, loop))
    else:
        nest_asyncio.apply(loop)
        return loop.run_until_complete(_to_task(fut, as_task, loop))


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
            access_token: Optional[str] = None,
            driver_cls: Type = HTTPTimeSeriesDriver,
            driver_kwargs: Optional[dict] = None,
    ) -> None:
        self._kafka = None
        self._auth = AuthManager(base_url, username, password, access_token=access_token)
        self._http = AsyncHTTP(base_url, self._auth)

        driver_kwargs = driver_kwargs or {}

        # ------------------------------------------------------------------
        # driver factory that only passes supported ctor-arguments
        # ------------------------------------------------------------------
        def _build_driver(domain: str):
            sig = inspect.signature(driver_cls)  # ctor signature
            kw: Dict[str, Any] = {"client_id": None, **driver_kwargs}

            # only add "domain" if the driver accepts it
            if "domain" in sig.parameters:
                kw["domain"] = domain

            kw = {k: v for k, v in kw.items() if k in sig.parameters}
            return driver_cls(self._auth, self._http, **kw)

        # domain drivers
        cloud_env = self._auth.is_cloud_environment()

        buffer_driver = CloudGQLDriver(self._auth, self._http) if cloud_env \
            else HTTPTimeSeriesDriver(self._auth, self._http, None, "buffer")

        self._drivers: Dict[str, BaseDriver] = {
            "buffer": buffer_driver,  # ← cloud => GQL Raw
            "history": HTTPTimeSeriesDriver(self._auth, self._http, None, "history"),
        }

        self._ws_driver: Optional[WebSocketDriver] = None

    # --------------------------- online ------------------------------ #
    def list_variables(self) -> List[GIOnlineVariable]:
        return _run(self._drivers["buffer"].list_variables())

    def read_online(self, var_ids: List[UUID]) -> Dict[UUID, float]:
        return _run(self._drivers["buffer"].read(var_ids))

    def write_online(self, mapping: Dict[UUID, float]) -> None:
        _run(self._drivers["buffer"].write(mapping))

    # --------------------------- buffer ------------------------------ #
    def list_buffer_sources(self) -> List[GIStream]:
        return _run(self._drivers["buffer"].list_sources())

    def list_stream_variables(self, source_id: Union[UUID, int]) -> List[GIStreamVariable]:
        return _run(self._drivers["buffer"].list_stream_variables(source_id))

    def fetch_buffer(
            self,
            selectors: List[VarSelector],
            *,
            start_ms: float = -20_000,
            end_ms: float = 0,
            points: int = 2048,
    ) -> pd.DataFrame:
        return _run(
            self._drivers["buffer"].fetch_buffer(
                selectors, start_ms=start_ms, end_ms=end_ms, points=points
            )
        )

    # --------------------------- history ----------------------------- #

    def list_history_sources(self) -> List[GIStream]:
        return _run(self._drivers["history"].list_sources())

    def list_history_variables(self, source_id: Union[UUID, int]):
        return _run(self._drivers["history"].list_stream_variables(source_id))

    def list_history_measurements(self, source_id: UUID) -> List[dict]:
        return _run(self._drivers["history"].list_measurements(source_id))

    def fetch_history(
            self,
            source_id: UUID,
            measurement_id: UUID,
            var_ids: List[UUID],
            *,
            start_ms: float = 0,
            end_ms: float = 0,
            points: int = 2048,
    ) -> pd.DataFrame:
        sels = [(source_id, vid) for vid in var_ids]
        return _run(
            self._drivers["history"].fetch_history(
                sels, start_ms=start_ms, end_ms=end_ms, points=points
            )
        )

    # -------------------------- websocket ---------------------------- #
    async def stream_online(
            self,
            var_ids: List[UUID],
            *,
            interval_ms: int = 1,
            extended: bool = True,
            on_change: bool = True,
            precision: int = -1,
    ):
        driver = await self._ensure_ws_driver()
        async for tick in driver.stream_online(
                var_ids,
                interval_ms=interval_ms,
                extended=extended,
                on_change=on_change,
                precision=precision,
        ):
            yield tick

    async def publish_online(
            self,
            data: Dict[UUID, float] | List[Tuple[UUID, float]],
            *,
            function: str = "write",
    ) -> None:
        driver = await self._ensure_ws_driver()
        await driver.publish(data, function=function)

    async def _ensure_ws_driver(self) -> WebSocketDriver:
        if self._ws_driver is None:
            from gimodules.gi_data.infra.ws import AsyncWS
            ws = AsyncWS(self._http.base_url, self._auth)
            self._ws_driver = WebSocketDriver(self._auth, ws, self._http)
        return self._ws_driver

    # ---------------------------- kafka ------------------------------ #
    async def stream_kafka(
            self,
            var_ids: List[UUID],
            *,
            ssl: bool = False,
            group_id: str = "gi_data_client",
    ):
        driver = await self._ensure_kafka_driver()
        logger.debug(f"Kafka driver: {driver}")
        async for update in driver.stream(var_ids, ssl=ssl, group_id=group_id):
            logger.debug("Kafka update: %s", update)
            yield update

    async def _ensure_kafka_driver(self) -> KafkaStreamDriver:
        if self._kafka is None:
            from gimodules.gi_data.drivers.kafka_stream import KafkaStreamDriver
            self._kafka = KafkaStreamDriver(self._auth, self._http)
        return self._kafka

    # --------------------------- export ------------------------------- #
    def export_data(
            self,
            selectors: List[VarSelector],
            *,
            start_ms: float,
            end_ms: float,
            format: DataFormat,
            points: Optional[int] = None,
            timezone: str = "UTC",
            resolution: Optional[Resolution] = None,
            data_type: Optional[DataType] = None,
            aggregation: Optional[str] = None,
            date_format: Optional[str] = None,
            filename: Optional[str] = None,
            precision: int = -1,
            csv_settings: Optional[CSVSettings] = None,
            log_settings: Optional[LogSettings] = None,
            target: Optional[str] = None,
    ) -> bytes:
        drv = self._drivers["buffer"]

        if format.value not in drv.supported_exports():
            raise NotImplementedError(f"{drv.name} does not support {format.value}")

        return _run(
            drv.export(
                selectors,
                start_ms=start_ms,
                end_ms=end_ms,
                format=format.value,
                points=points,
                timezone=timezone,
                resolution=resolution.value if resolution else None,
                data_type=data_type.value if data_type else None,
                aggregation=aggregation,
                date_format=date_format,
                filename=filename,
                precision=precision,
                csv_settings=csv_settings,
                log_settings=log_settings,
                target=target,
            )
        )

    # convenience
    def export_csv(self, selectors, *, start_ms, end_ms, **kw) -> bytes:
        return self.export(selectors, start_ms=start_ms, end_ms=end_ms, format=DataFormat.CSV, **kw)

    def export_udbf(self, selectors, *, start_ms, end_ms, **kw) -> bytes:
        return self.export(selectors, start_ms=start_ms, end_ms=end_ms, format=DataFormat.UDBF, **kw)

    # --------------------------- import ------------------------------- #
    def import_data(
            self,
            source_id: str,
            source_name: str,
            file_bytes: bytes,
            *,
            format: DataFormat,
            target: str = "stream",  # "stream" | "record" - only stream on cloud
            csv_settings: Optional[CSVImportSettings] = None,
            add_time_series: bool = False,
            retention_time_sec: int = 0,
            time_offset_sec: int = 0,
            sample_rate: int = -1,
            auto_create_metadata: bool = True,
            session_timeout_sec: int = 300,
    ) -> str:
        drv = self._drivers["history"]

        if format == DataFormat.CSV:
            return _run(
                drv.import_csv(
                    source_id,
                    source_name,
                    file_bytes,
                    target=target,
                    csv_settings=csv_settings,
                    add_time_series=add_time_series,
                    retention_time_sec=retention_time_sec,
                    time_offset_sec=time_offset_sec,
                    sample_rate=sample_rate,
                    auto_create_metadata=auto_create_metadata,
                    session_timeout_sec=session_timeout_sec,
                )
            )

        if format == DataFormat.UDBF:
            return _run(
                drv.import_udbf(
                    source_id,
                    source_name,
                    file_bytes,
                    target=target,
                    add_time_series=add_time_series,
                    sample_rate=sample_rate,
                    auto_create_metadata=auto_create_metadata,
                    session_timeout_sec=session_timeout_sec,
                )
            )

        raise NotImplementedError(f"Import for format={format} not supported.")

    def import_csv(self, source_id, source_name, file_bytes, **kw) -> str:
        return self.import_(source_id, source_name, file_bytes, format=DataFormat.CSV, **kw)

    def import_udbf(self, source_id, source_name, file_bytes, **kw) -> str:
        return self.import_(source_id, source_name, file_bytes, format=DataFormat.UDBF, **kw)

    # ------------------------ housekeeping --------------------------- #
    def close(self) -> None:
        _run(self._http.aclose())

    def __enter__(self) -> "GIDataClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.close()
        return False
