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
    VarSelector,
)


class LocalHTTPDriver(BaseDriver):
    """
    Data-API implementation for GI.bench / Q.core / Q.station.
    """

    name = "local_http"
    priority = 20

    # ------------------------------- Online --------------------------- #

    async def list_variables(self) -> List[Dict[str, Any]]:
        res = await self.http.get("/online/structure/variables")
        return res.json().get("Data", [])

    async def read(self, var_ids: List[UUID]) -> Dict[UUID, float]:
        payload = {"Variables": [str(v) for v in var_ids], "Function": "read"}
        res = await self.http.post("/online/data", json=payload)
        values = res.json()["Data"]["Values"]
        return {vid: val for vid, val in zip(var_ids, values)}

    async def write(self, mapping: Dict[UUID, float]) -> None:
        payload = {
            "Variables": [str(v) for v in mapping.keys()],
            "Values": list(mapping.values()),
            "Function": "write",
        }
        await self.http.post("/online/data", json=payload)

    # ------------------------------- Buffer --------------------------- #

    async def list_sources(self) -> List[GIStream]:
        res = await self.http.get("/buffer/structure/sources")
        return [GIStream.model_validate(d) for d in res.json()["Data"]]

    async def list_stream_variables(
        self,
        source_id: Union[str, int, UUID],
    ) -> List[GIStreamVariable]:
        path = f"/buffer/structure/sources/{source_id}/variables"
        res = await self.http.get(path)
        raw = res.json()["Data"]
        return [
            GIStreamVariable.model_validate(r | {"sid": source_id})
            for r in raw
        ]

    async def fetch_buffer(
            self,
            selectors: List[Tuple[Union[UUID, str, int], UUID]],
            *,
            start_ms: int = -20_000,
            end_ms: int = 0,
            points: int = 2048,
    ) -> pd.DataFrame:
        # Build validated VarSelector objects
        variables: List[VarSelector] = [
            VarSelector(SID=s, VID=v) for s, v in selectors
        ]
        req = BufferRequest(
            Start=start_ms,
            End=end_ms,
            Points=points,
            Variables=variables,
        )

        res = await self.http.post(
            "/buffer/data",
            json=req.model_dump(by_alias=True, mode="json"),
        )
        ts = BufferSuccess.model_validate(res.json()).first_timeseries()
        return _timeseries_to_frame(
            ts, [UUID(str(v.VID)) for v in variables]
        )

    # ------------------------------- History -------------------------- #
    # (unchanged – omitted for brevity)                                  #


# ------------------------------------------------------------------#
# helpers                                                            #
# ------------------------------------------------------------------#


def _timeseries_to_frame(ts: TimeSeries, order: List[UUID]) -> pd.DataFrame:
    start_s = ts.AbsoluteStart / 1_000.0
    delta_s = ts.Delta / 1_000.0
    count = len(ts.Values[0])
    idx = pd.to_datetime(
        [start_s + i * delta_s for i in range(count)], unit="s", utc=True
    ).tz_convert(timezone.utc)

    data = {str(uid): ts.Values[i] for i, uid in enumerate(order)}
    frame = pd.DataFrame(data, index=idx)
    frame.index.name = "time"
    return frame
