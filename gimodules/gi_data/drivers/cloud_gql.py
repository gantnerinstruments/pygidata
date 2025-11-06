# gimodules/gi_data/drivers/cloud_gql.py  (no CloudRequest dependency)

from __future__ import annotations
import asyncio, math
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Sequence, Tuple, Union
from uuid import UUID
import pandas as pd

from gimodules.gi_data.mapping.models import (
    GIStream, GIStreamVariable, TimeSeries, VarSelector, BufferRequest, BufferSuccess
)
from .base import BaseDriver

def _now_ms() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)

def _window(start_ms: float, end_ms: float) -> tuple[float, float]:
    return start_ms, end_ms

def _to_frame_from_raw(rows: List[List[Any]], order_vids: Sequence[UUID]) -> pd.DataFrame:
    ts_ms  = pd.Series([int(r[0]) for r in rows], dtype="int64")
    nanos  = pd.Series([int(r[1]) for r in rows], dtype="int64")
    idx    = pd.to_datetime(ts_ms, unit="ms", utc=True) + pd.to_timedelta(nanos, unit="ns")
    idx.name = "time"
    vals = {str(vid): [r[i+2] for r in rows] for i, vid in enumerate(order_vids)}
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

    async def list_sources(self) -> List[GIStream]:
        await self._bearer()
        r = await self.http.get("/kafka/structure/sources")
        data = r.json().get("Data", [])
        return [GIStream.model_validate(s) for s in data]

    async def list_stream_variables(self, source_id: Union[str, int, UUID]) -> List[GIStreamVariable]:
        await self._bearer()
        body = {"AddVarMapping": True, "Sources": [str(source_id)]}
        r = await self.http.post("/kafka/structure/sources", json=body)
        out: List[GIStreamVariable] = []
        for src in r.json().get("Data", []):
            sid = src["Id"]
            for v in src.get("Variables", []):
                out.append(GIStreamVariable.model_validate({
                    "Id": v["Id"], "Name": v["Name"], "Index": v["Index"], "GQLId": v.get("GQLId"),
                    "Unit": v.get("Unit",""), "DataFormat": v.get("DataFormat",""), "sid": sid
                }))
        return out

    async def list_measurements(self, source_id: Union[str, int, UUID]) -> List[Dict[str, Any]]:
        await self._bearer()
        r = await self.http.get(f"/history/structure/sources/{source_id}/measurements")
        return r.json().get("Data", [])

    async def list_variables(self) -> List[Dict[str, Any]]:
        await self._bearer()
        r = await self.http.get("/online/structure/variables")
        return r.json().get("Data", [])

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
        selectors: List[Tuple[Union[UUID, str, int], UUID]],
        *,
        start_ms: float = -20_000,
        end_ms: float = 0,
        points: int = 2048,
    ) -> pd.DataFrame:
        frm, to = _window(start_ms, end_ms)
        by_sid: Dict[str, List[UUID]] = defaultdict(list)
        for sid, vid in selectors:
            by_sid[str(sid)].append(UUID(str(vid)))

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

    async def fetch_history(
        self,
        source_id: Union[str, int, UUID],
        measurement_id: Union[str, int, UUID],
        var_ids: List[UUID],
        *,
        start_ms: float = 0,
        end_ms: float = 0,
        points: int = 2048,
    ) -> pd.DataFrame:
        variables = [VarSelector(SID=source_id, VID=v) for v in var_ids]
        req = BufferRequest(Start=start_ms, End=end_ms, Points=points, Variables=variables)
        r = await self.http.post("/history/data", json=req.model_dump(by_alias=True, mode="json"))
        ts = BufferSuccess.model_validate(r.json()).first_timeseries()
        return _to_frame_from_ts(ts, var_ids)
