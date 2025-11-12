from __future__ import annotations

import logging
from typing import Dict, List, Union, Optional, Literal, Iterable
from uuid import UUID

import pandas as pd

from gi_data.mapping.models import (
    BufferRequest,
    BufferSuccess,
    GIStream,
    GIStreamVariable,
    TimeSeries,
    VarSelector, HistorySuccess, GIHistoryMeasurement, GIOnlineVariable, CSVSettings, LogSettings, CSVImportSettings,
    HistoryRequest,
)
from .base import BaseDriver
from ..mapping.enums import Resolution, DataType
from ..utils.logging import setup_module_logger

logger = setup_module_logger(__name__, level=logging.DEBUG)


class HTTPTimeSeriesDriver(BaseDriver):
    """
    Data-API implementation for GI.bench / Q.core / Q.station.
    """

    name = "local_http"
    priority = 20

    def __init__(self, auth, http, ws, root: str) -> None:
        super().__init__(auth, http, ws)
        self._root = root.strip("/")  # “buffer”, ”history”, kafka

    # -------- Online -------------------------------------------------
    async def list_variables(self) -> List[GIOnlineVariable]:
        res = await self.http.get("/online/structure/variables")
        return [GIOnlineVariable.model_validate(d) for d in res.json()["Data"]]

    async def read(self, var_ids: List[UUID] | UUID) -> Dict[UUID, float]:
        # normalize to list
        if isinstance(var_ids, UUID):
            var_ids = [var_ids]

        payload = {"Variables": [str(v) for v in var_ids], "Function": "read"}
        res = await self.http.post("/online/data", json=payload)
        vals = res.json()["Data"]["Values"]
        return dict(zip(var_ids, vals))

    async def write(self, mapping: Dict[UUID, float]) -> None:
        payload = {
            "Variables": [str(v) for v in mapping],
            "Values": list(mapping.values()),
            "Function": "write",
        }
        await self.http.post("/online/data", json=payload)

    # -------- Structure ---------------------------------------------
    async def list_buffer_sources(self) -> List[GIStream]:
        res = await self.http.get(f"/{self._root}/structure/sources")
        return [GIStream.model_validate(d) for d in res.json()["Data"]]

    async def list_buffer_variables(
            self, sid: Union[str, int, UUID]
    ) -> List[GIStreamVariable]:
        res = await self.http.get(f"/{self._root}/structure/sources/{sid}/variables")
        if not res.json().get("Data"):
            logger.warning(f"Source {sid} has no variables")
            return []
        raw = res.json()["Data"]
        return [GIStreamVariable.model_validate(r | {"sid": sid}) for r in raw]

    async def list_measurements(
            self,
            sid: Union[str, int, UUID],
            *,
            start: Optional[int] = None,
            end: Optional[int] = None,
            order: str = "DESC",
            limit: Optional[int] = None,
            measurements: Optional[Iterable[Union[str, UUID]]] = None,
            add_var_mapping: bool = True,
            add_meas_metadata: bool = False,
            meas_metadata_filter: Optional[List[dict]] = None,
    ) -> List[GIHistoryMeasurement]:

        if self._root != "history":
            raise RuntimeError("measurements only exist on /history")

        payload = {}

        if start is not None:
            payload["Start"] = int(start)
        if end is not None:
            payload["End"] = int(end)
        if order:
            payload["Order"] = order
        if limit is not None:
            payload["Limit"] = int(limit)
        if measurements:
            payload["Measurements"] = [str(m) for m in measurements]

        # optional flags
        payload["AddVarMapping"] = bool(add_var_mapping)
        payload["AddMeasMetaData"] = bool(add_meas_metadata)

        if meas_metadata_filter:
            payload["MeasMetaDataFilter"] = meas_metadata_filter

        res = await self.http.post(
            f"/{self._root}/structure/sources/{sid}/measurements",
            json=payload,
        )

        return [GIHistoryMeasurement.model_validate(d) for d in res.json()["Data"]]

    # -------- Data ---------------------------------------------------
    async def fetch_buffer(
            self,
            selectors: List[VarSelector],
            *,
            start_ms: float,
            end_ms: float,
            points: int = 2048,
    ) -> pd.DataFrame:
        vars_ = [s for s in selectors]
        req = BufferRequest(Start=start_ms, End=end_ms, Points=points, Variables=vars_)

        res = await self.http.post(f"/{self._root}/data",
                                   json=req.model_dump(by_alias=True, mode="json"))

        if self._root == "history":
            ts = HistorySuccess.model_validate(res.json()).first_timeseries()
        else:
            ts = BufferSuccess.model_validate(res.json()).first_timeseries()

        return _to_frame(ts, [UUID(str(v.VID)) for v in vars_])

    async def fetch_history(
            self,
            selectors: List[VarSelector],
            *,
            measurement_id: UUID,
            start_ms: float = 0,
            end_ms: float = 0,
            points: int = 2048,
    ) -> pd.DataFrame:
        # Apply measurement selection to each selector
        vars_ = [
            VarSelector(
                SID=s.SID,
                VID=s.VID,
                Selector=f"mid:{measurement_id}"
            )
            for s in selectors
        ]

        req = HistoryRequest(
            Start=start_ms,
            End=end_ms,
            Variables=vars_,
            Points=points,
        )

        source_id = getattr(self, "_source_id", None)
        if source_id:
            url = f"/history/data/{source_id}"
        else:
            url = "/history/data"

        res = await self.http.post(
            url,
            json=req.model_dump(by_alias=True, mode="json"),
        )

        ts = HistorySuccess.model_validate(res.json()).first_timeseries()
        return _to_frame(ts, [UUID(str(v.VID)) for v in vars_])

    async def export(  # maps to /{root}/data
            self, selectors: List[VarSelector], *,
            start_ms: float, end_ms: float,
            format: Literal["csv", "udbf"],
            points: Optional[int] = 2048,
            timezone: str = "UTC",
            resolution: Optional[Resolution] = None,  # ignored
            data_type: Optional[DataType] = None,  # ignored
            aggregation: Optional[str] = None,  # ignored
            date_format: Optional[str] = None,  # ignored
            filename: Optional[str] = None,  # ignored
            precision: int = -1,
            csv_settings: Optional[CSVSettings] = None,
            log_settings: Optional[LogSettings] = None,
            target: Optional[str] = None,  # local-only for csv -> "stream"/"record"
    ) -> bytes:
        fmt = "csv" if format == "csv" else "udbf"
        req = BufferRequest(
            Start=start_ms, End=end_ms, Variables=selectors, Points=points or 2048,
            Type="equidistant", Format=fmt, Precision=precision, TimeZone=timezone, TimeOffset=0
        ).model_dump(by_alias=True, mode="json")
        if fmt == "csv":
            settings = csv_settings or CSVSettings()  # default settings
            req["CSVSettings"] = settings.model_dump(exclude_none=True)
        if fmt == "udbf" and log_settings:
            req["LogSettings"] = log_settings.model_dump(exclude_none=True)
        if fmt == "udbf" and target:
            req["Target"] = target
        r = await self.http.post(f"/{self._root}/data", json=req)
        return r.content

    async def import_csv(
            self,
            source_id: str,
            source_name: str,
            file_bytes: bytes,
            *,
            target: str = "stream",
            csv_settings: Optional[CSVImportSettings] = None,
            add_time_series: bool = False,
            retention_time_sec: int = 0,
            time_offset_sec: int = 0,
            sample_rate: int = -1,
            auto_create_metadata: bool = True,
            session_timeout_sec: int = 300,
    ) -> str:
        param = {
            "Type": "csv",
            "SourceID": source_id,
            "SourceName": source_name,
            "SessionTimeoutSec": str(session_timeout_sec),
            "SampleRate": str(sample_rate),
            "AutoCreateMetaData": str(auto_create_metadata).lower(),
            "CSVSettings": (csv_settings or CSVImportSettings()).model_dump(exclude_none=True),
            "RetentionTimeSec": retention_time_sec,
            "Target": target,
            "TimeOffsetSec": time_offset_sec,
            "AddTimeSeries": add_time_series,
        }
        res = await self.http.post("/history/data/import", json=param)
        sid = res.json()["Data"]["SessionID"]
        await self.http.post(f"/history/data/import/{sid}", content=file_bytes,
                             headers={"Content-Type": "text/csv"})
        await self.http.delete(f"/history/data/import/{sid}")
        return str(sid)

    async def import_udbf(
            self,
            source_id: str,
            source_name: str,
            file_bytes: bytes,
            *,
            target: str = "stream",
            add_time_series: bool = False,
            sample_rate: int = -1,
            auto_create_metadata: bool = True,
            session_timeout_sec: int = 300,
    ) -> str:
        param = {
            "Type": "udbf",
            "Target": target,
            "SourceID": source_id,
            "SourceName": source_name,
            "MeasID": "",
            "SessionTimeoutSec": int(session_timeout_sec),
            "AddTimeSeries": str(add_time_series).lower(),
            "SampleRate": str(sample_rate),
            "AutoCreateMetaData": str(auto_create_metadata).lower(),
        }
        res = await self.http.post("/history/data/import", json=param)
        sid = res.json()["Data"]["SessionID"]
        await self.http.post(f"/history/data/import/{sid}", content=file_bytes,
                             headers={"Content-Type": "application/octet-stream"})
        await self.http.delete(f"/history/data/import/{sid}")
        return str(sid)


def _to_frame(ts: TimeSeries, order: List[UUID]) -> pd.DataFrame:
    start_ns = int(ts.Start * 1_000_000)
    dt_ns = int(ts.Delta * 1_000_000)
    idx_ns = [start_ns + i * dt_ns for i in range(len(ts.Values[0]))]

    data = {str(uid): ts.Values[i] for i, uid in enumerate(order)}
    return pd.DataFrame(data, index=idx_ns).rename_axis("timestamp_ns")
