# src/src/drivers/cloud_gql.py  (no CloudRequest dependency)

from __future__ import annotations

import math
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Sequence, Union, Optional, Literal, Iterable
from uuid import UUID

import pandas as pd

from gi_data.mapping.enums import Resolution, DataType
from gi_data.mapping.models import (
    GIStream, GIStreamVariable, TimeSeries, VarSelector, BufferRequest, LogSettings, CSVSettings,
    CSVImportSettings, GIHistoryMeasurement, GIOnlineVariable, CSVImportSettingsDefaultCloud
)
from .base import BaseDriver


def _now_ms() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)


def _window(start_ms: float, end_ms: float) -> tuple[int, int]:
    to_ms = _now_ms() if end_ms == 0 else (_now_ms() + int(end_ms) if end_ms < 0 else int(end_ms))
    frm = to_ms + int(start_ms) if start_ms <= 0 else int(start_ms)
    if frm >= to_ms: frm = to_ms - 1
    return frm, to_ms


def _to_frame_from_raw(rows: List[List[Any]], order_vids: Sequence[UUID]) -> pd.DataFrame:
    ts_ms = pd.Series([int(r[0]) for r in rows], dtype="int64")
    nanos = pd.Series([int(r[1]) for r in rows], dtype="int64")
    idx = pd.to_datetime(ts_ms, unit="ms", utc=True) + pd.to_timedelta(nanos, unit="ns")
    idx.name = "time"
    vals = {str(vid): [r[i + 2] for r in rows] for i, vid in enumerate(order_vids)}
    return pd.DataFrame(vals, index=idx)


def _to_frame_from_ts(ts: TimeSeries, order: Sequence[UUID]) -> pd.DataFrame:
    start_s = ts.AbsoluteStart / 1_000.0
    delta_s = ts.Delta / 1_000.0
    idx = pd.to_datetime([start_s + i * delta_s for i in range(len(ts.Values[0]))], unit="s", utc=True)
    idx.name = "time"
    return pd.DataFrame({str(uid): ts.Values[i] for i, uid in enumerate(order)}, index=idx)


class CloudGQLDriver(BaseDriver):
    """Data-API implementation for GI.cloud."""

    name = "cloud_gql"
    priority = 10

    def __init__(self, auth, http, client_id=None, **kwargs) -> None:
        super().__init__(auth, http, client_id)
        self._vm_cache: Dict[str, Dict[str, str]] = {}  # sid -> {vid -> field_name}

    # --- infra ---------------------------------------------------------

    async def _bearer(self) -> str:
        return await self.auth.bearer()

    async def _gql(self, query: str) -> Dict[str, Any]:
        # Auth handled by AsyncHTTP; bearer ensures freshness
        await self._bearer()
        res = await self.http.post("/__api__/gql", json={"query": query})
        j = res.json()
        if "errors" in j:
            raise RuntimeError(j["errors"])
        return j.get("data", j)

    async def _vid_to_fieldnames(self, sid: Union[str, UUID, int], vids: List[UUID]) -> List[str]:
        s = str(sid)
        if s not in self._vm_cache:
            q = f'''
            {{
              variableMapping(sid: "{s}") {{
                columns {{ name variables {{ id }} }}
              }}
            }}'''
            data = await self._gql(q)
            idx: Dict[str, str] = {}
            for col in data["variableMapping"]["columns"]:
                for v in col.get("variables", []):
                    idx[str(v["id"])] = col["name"]
            self._vm_cache[s] = idx
        out = []
        for vid in vids:
            k = str(vid)
            if k not in self._vm_cache[s]:
                # fallback to REST variable structure with AddVarMapping
                body = {"AddVarMapping": True, "Sources": [s]}
                r = await self.http.post("/kafka/structure/sources", json=body)
                for src in r.json().get("Data", []):
                    for v in src.get("Variables", []):
                        if v.get("Id") and v.get("GQLId"):
                            self._vm_cache[s][str(v["Id"])] = v["GQLId"]
            if k not in self._vm_cache[s]:
                raise KeyError(f"{vid} not found in mapping for {s}")
            out.append(self._vm_cache[s][k])
        return out

    # --- structure -----------------------------------------------------

    async def list_buffer_sources(self) -> List[GIStream]:
        await self._bearer()
        r = await self.http.get("/kafka/structure/sources")
        data = r.json().get("Data", [])
        return [GIStream.model_validate(s) for s in data]

    async def list_buffer_variables(self, source_id: Union[str, int, UUID]) -> List[GIStreamVariable]:
        await self._bearer()
        body = {"AddVarMapping": True, "Sources": [str(source_id)]}
        r = await self.http.post("/kafka/structure/sources", json=body)
        out: List[GIStreamVariable] = []
        for src in r.json().get("Data", []):
            sid = src["Id"]
            for v in src.get("Variables", []):
                out.append(GIStreamVariable.model_validate({
                    "Id": v["Id"], "Name": v["Name"], "Index": v["Index"], "GQLId": v.get("GQLId"),
                    "Unit": v.get("Unit", ""), "DataFormat": v.get("DataFormat", ""), "sid": sid
                }))
        return out

    async def list_measurements(
            self,
            source_id: Union[str, int, UUID],
            *,
            start: Optional[int] = None,
            end: Optional[int] = None,
            order: str = "DESC",
            limit: Optional[int] = None,
            measurements: Optional[Iterable[Union[str, UUID]]] = None,
            add_var_mapping: bool = True,  # ignored on cloud
            add_meas_metadata: bool = False,  # ignored on cloud
            meas_metadata_filter: Optional[List[dict]] = None,  # ignored on cloud
    ) -> List[GIHistoryMeasurement]:

        sid = str(source_id)

        frm = 0 if start is None else int(start)
        to = 9999999999999 if end is None else int(end)
        lim = limit if limit is not None else 50
        sort = order if order in ("ASC", "DESC") else "DESC"

        mid_filter = ""
        if measurements:
            mids = [f'"{str(m)}"' for m in measurements]
            mid_filter = f"mid: [{', '.join(mids)}],"

        q = f"""
        {{
          measurementPeriods(
            sid: "{sid}",
            {mid_filter}
            from: {frm},
            to: {to},
            sort: {sort},
            limit: {lim}
          ) {{
            mid
            minTs
            maxTs
            sampleRate
          }}
        }}
        """

        data = await self._gql(q)
        rows = data.get("measurementPeriods", [])

        return [
            GIHistoryMeasurement(
                Id=r["mid"],
                AbsoluteStart=r["minTs"],
                LastTimeStamp=r["maxTs"],
                SampleRateHz=r.get("sampleRate", 0),
                SourceId=sid,
            )
            for r in rows
        ]

    async def list_variables(self) -> list[GIOnlineVariable]:
        await self._bearer()
        res = await self.http.get("/online/structure/variables")
        return [GIOnlineVariable.model_validate(d) for d in res.json()["Data"]]

    # --- online --------------------------------------------------------

    async def read(self, var_ids: List[UUID]) -> Dict[UUID, float]:
        await self._bearer()
        r = await self.http.post("/online/data", json={"Variables": [str(v) for v in var_ids], "Function": "read"})
        j = r.json()
        if "Data" not in j: return {}
        return {vid: val for vid, val in zip(var_ids, j["Data"]["Values"])}

    async def write(self, mapping: Dict[UUID, float]) -> None:
        await self._bearer()
        await self.http.post("/online/data", json={
            "Variables": [str(v) for v in mapping.keys()],
            "Values": list(mapping.values()),
            "Function": "write",
        })

    # --- buffer (cloud => GraphQL Raw) --------------------------------

    async def fetch_buffer(
            self,
            selectors: List[VarSelector],
            *,
            start_ms: float = -20_000,
            end_ms: float = 0,
            points: int = 2048,
    ) -> pd.DataFrame:
        frm, to = _window(start_ms, end_ms)
        by_sid: Dict[str, List[UUID]] = defaultdict(list)
        for selector in selectors:
            by_sid[str(selector.SID)].append(selector.VID)

        frames: List[pd.DataFrame] = []
        for sid, vids in by_sid.items():
            fields = await self._vid_to_fieldnames(sid, vids)
            cols = '", "'.join(fields)
            q = f'''
            {{
              Raw(columns: ["ts", "nanos", "{cols}"], sid: "{sid}", from: {frm}, to: {to}) {{
                data
              }}
            }}'''
            data = await self._gql(q)
            rows = data.get("Raw", {}).get("data", [])
            if not rows:
                continue
            df = _to_frame_from_raw(rows, vids)
            if points and len(df) > points:
                step = max(1, math.ceil(len(df) / points))
                df = df.iloc[::step]
            frames.append(df)

        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, axis=1).sort_index()

    # --- history (unchanged REST) -------------------------------------

    # async def fetch_history(
    #         self,
    #         selectors: List[VarSelector],
    #         *,
    #         measurement_id: UUID,
    #         start_ms: float = 0,
    #         end_ms: float = 0,
    #         points: int = 2048,
    # ) -> pd.DataFrame:
    #     variables = selectors
    #     var_ids = [s.VID for s in selectors]
    #     req = BufferRequest(Start=start_ms, End=end_ms, Points=points, Variables=variables)
    #     r = await self.http.post("/kafka/data", json=req.model_dump(by_alias=True, mode="json"))
    #     ts = BufferSuccess.model_validate(r.json()).first_timeseries()
    #     return _to_frame_from_ts(ts, var_ids)

    async def fetch_history(
            self,
            selectors: List[VarSelector],
            *,
            measurement_id: UUID,
            start_ms: float = 0,
            end_ms: float = 0,
            points: int = 2048,
    ) -> pd.DataFrame:
        return await self.fetch_buffer(
            selectors,
            start_ms=start_ms,
            end_ms=end_ms,
            points=points,
        )

    async def _stream_name(self, sid: Union[str, UUID, int]) -> str:
        s = str(sid)
        r = await self.http.get("/kafka/structure/sources")
        for src in r.json().get("Data", []):
            if str(src.get("Id")) == s:
                return src.get("Name", s)
        return s

    async def _var_meta(self, sid: Union[str, UUID, int]) -> Dict[str, Dict[str, Any]]:
        body = {"AddVarMapping": True, "Sources": [str(sid)]}
        r = await self.http.post("/kafka/structure/sources", json=body)
        meta: Dict[str, Dict[str, Any]] = {}
        for src in r.json().get("Data", []):
            for v in src.get("Variables", []):
                vid = str(v["Id"])
                meta[vid] = {
                    "name": v.get("Name", vid),
                    "unit": v.get("Unit", ""),
                    "gql": v.get("GQLId"),
                }
        return meta

    async def export(  # csv via GQL exportCSV, udbf via /history/data
            self, selectors: List[VarSelector], *,
            start_ms: float, end_ms: float,
            format: Literal["csv", "udbf"],
            points: Optional[int] = 2048,
            timezone: str = "UTC",
            resolution: Optional[Resolution] = None,
            data_type: Optional[DataType] = None,  # ignored on cloud
            aggregation: Optional[str] = "avg",
            date_format: Optional[str] = "%Y-%m-%dT%H:%M:%S",
            filename: Optional[str] = None,
            precision: int = -1,
            csv_settings: Optional[CSVSettings] = None,  # ignored on cloud
            log_settings: Optional[LogSettings] = None,
            target: Optional[str] = None,  # ignored on cloud
    ) -> bytes:
        frm, to = _window(start_ms, end_ms)
        if format == "csv":
            by_sid: Dict[str, List[UUID]] = {}
            for s in selectors:
                by_sid.setdefault(str(s.SID), []).append(UUID(str(s.VID)))
            chunks = []
            for sid, vids in by_sid.items():
                fields = await self._vid_to_fieldnames(sid, vids)
                meta = await self._var_meta(sid)
                sname = await self._stream_name(sid)
                for vid, f in zip(vids, fields):
                    m = meta.get(str(vid), {"name": str(vid), "unit": ""})
                    hdr = '", "'.join([m["name"], sname, aggregation or "avg", m["unit"]])
                    chunks.append('{ field: "%s:%s.%s", headers: ["%s"] }' % (sid, f, aggregation or "avg", hdr))
            cols = ",\n          ".join([
                '{ field: "ts", headers: ["datetime"], dateFormat: "%s" }' % (date_format or "%Y-%m-%dT%H:%M:%S"),
                '{ field: "ts", headers: ["time", "", "", "[s since 01.01.1970]"] }',
                *chunks,
            ])
            fname = filename or f"export_{frm}_{to}.csv"
            q = f"""{{ exportCSV(from:{frm}, to:{to}, resolution: {resolution}, timezone:"{timezone}",
                        filename:"{fname}", columns:[ {cols} ]) {{ file }} }}"""
            await self._bearer()
            res = await self.http.post("/__api__/gql", json={"query": q})
            return res.content
        # udbf
        raise NotImplementedError
        req = BufferRequest(
            Start=frm, End=to, Variables=selectors, Points=points or 2048,
            Type="equidistant", Format="udbf", Precision=precision,
            TimeZone=timezone, TimeOffset=0
        ).model_dump(by_alias=True, mode="json")
        if log_settings:
            req["LogSettings"] = log_settings.model_dump(exclude_none=True)
        await self._bearer()
        r = await self.http.post("/history/data", json=req)
        return r.content

    async def export_udbf(
            self,
            selectors: List[VarSelector],
            *,
            start_ms: float,
            end_ms: float,
            points: int = 0,
            log_settings: Optional[LogSettings] = None,
            target: Optional[str] = None,
            timezone: str = "UTC",
            precision: int = -1,
    ) -> bytes:
        frm, to = _window(start_ms, end_ms)
        req = BufferRequest(
            Start=frm, End=to, Variables=selectors, Points=points or 2048,
            Type="equidistant", Format="udbf", Precision=precision,
            TimeZone=timezone, TimeOffset=0
        ).model_dump(by_alias=True, mode="json")

        if log_settings:
            req["LogSettings"] = log_settings.model_dump(exclude_none=True)
        if target:
            req["Target"] = target

        await self._bearer()
        r = await self.http.post("/buffer/data", json=req)
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
        if csv_settings is None:
            csv_settings = CSVImportSettingsDefaultCloud()

        param = {
            "Type": "csv",
            "SourceID": source_id,
            "SourceName": source_name,
            "SessionTimeoutSec": str(session_timeout_sec),
            "SampleRate": str(sample_rate),
            "AutoCreateMetaData": str(auto_create_metadata).lower(),
            "CSVSettings": (csv_settings.model_dump(exclude_none=True) if csv_settings else {}),
            "RetentionTimeSec": retention_time_sec,
            "Target": target,
            "TimeOffsetSec": time_offset_sec,
            "AddTimeSeries": add_time_series,
        }
        await self._bearer()
        res = await self.http.post("/history/data/import", json=param)
        sid = res.json()["Data"]["SessionID"]

        hdrs = {"Content-Type": "text/csv"}
        await self.http.post(f"/history/data/import/{sid}", content=file_bytes, headers=hdrs)
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
            "SourceID": source_id,
            "SourceName": source_name,
            "MeasID": "",
            "SessionTimeoutSec": str(session_timeout_sec),
            "AddTimeSeries": str(add_time_series).lower(),
            "SampleRate": str(sample_rate),
            "AutoCreateMetaData": str(auto_create_metadata).lower(),
            "Target": target,
        }
        await self._bearer()
        res = await self.http.post("/history/data/import", json=param)
        sid = res.json()["Data"]["SessionID"]

        hdrs = {"Content-Type": "application/octet-stream"}
        await self.http.post(f"/history/data/import/{sid}", content=file_bytes, headers=hdrs)
        await self.http.delete(f"/history/data/import/{sid}")
        return str(sid)
