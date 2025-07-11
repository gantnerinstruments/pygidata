from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple, Union
from uuid import UUID

import pandas as pd

from .base import BaseDriver
from gimodules.gi_data.mapping.models import (
    BufferRequest,
    BufferSuccess,
    GIStream,
    GIStreamVariable,
    TimeSeries,
    VarSelector, HistorySuccess, HistoryRequest, GIHistoryMeasurement,
)


class HTTPTimeSeriesDriver(BaseDriver):
    """
    Data-API implementation for GI.bench / Q.core / Q.station.
    """

    name = "local_http"
    priority = 20

    def __init__(self, auth, http, ws, root: str) -> None:
        super().__init__(auth, http, ws)
        self._root = root.strip("/")  # “buffer”, ”history”, …

    # -------- Online -------------------------------------------------
    async def list_variables(self) -> List[Dict[str, Any]]:
        res = await self.http.get("/online/structure/variables")
        return res.json().get("Data", [])

    async def read(self, var_ids: List[UUID]) -> Dict[UUID, float]:
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
    async def list_sources(self) -> List[GIStream]:
        res = await self.http.get(f"/{self._root}/structure/sources")
        return [GIStream.model_validate(d) for d in res.json()["Data"]]

    async def list_stream_variables(
            self, sid: Union[str, int, UUID]
    ) -> List[GIStreamVariable]:
        res = await self.http.get(f"/{self._root}/structure/sources/{sid}/variables")
        raw = res.json()["Data"]
        return [GIStreamVariable.model_validate(r | {"sid": sid}) for r in raw]

    async def list_measurements(  # only for history
            self, sid: Union[str, int, UUID]
    ) -> List[GIHistoryMeasurement]:
        if self._root != "history":
            raise RuntimeError("measurements only exist on /history")
        res = await self.http.get(f"/history/structure/sources/{sid}/measurements")
        return [GIHistoryMeasurement.model_validate(d) for d in res.json()["Data"]]

    # -------- Data ---------------------------------------------------
    async def fetch(
            self,
            selectors: List[Tuple[Union[str, int, UUID], UUID]],
            *,
            start_ms: float,
            end_ms: float,
            points: int = 2048,
    ) -> pd.DataFrame:
        vars_ = [VarSelector(SID=s, VID=v) for s, v in selectors]
        req = BufferRequest(Start=start_ms, End=end_ms, Points=points, Variables=vars_)

        res = await self.http.post(f"/{self._root}/data",
                                   json=req.model_dump(by_alias=True, mode="json"))

        if self._root == "history":
            ts = HistorySuccess.model_validate(res.json()).first_timeseries()
        else:
            ts = BufferSuccess.model_validate(res.json()).first_timeseries()

        return _to_frame(ts, [UUID(str(v.VID)) for v in vars_])


def _to_frame(ts: TimeSeries, order: List[UUID]) -> pd.DataFrame:
    start_s = ts.AbsoluteStart / 1_000
    dt_s = ts.Delta / 1_000
    idx = pd.to_datetime(
        [start_s + i * dt_s for i in range(len(ts.Values[0]))],
        unit="s", utc=True
    ).tz_convert(timezone.utc)

    data = {str(uid): ts.Values[i] for i, uid in enumerate(order)}
    return pd.DataFrame(data, index=idx).rename_axis("time")