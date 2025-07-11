# gimodules/gi_data/drivers/cloud_gql.py
from __future__ import annotations

import asyncio
from datetime import timezone
from typing import Any, Dict, List, Sequence, Tuple, Union
from uuid import UUID

import pandas as pd

from gimodules.cloudconnect.cloud_request import CloudRequest      # ← your big helper
from gimodules.gi_data.mapping.models import (
    BufferRequest,
    BufferSuccess,
    GIStream,
    GIStreamVariable,
    TimeSeries,
    VarSelector,
)
from .base import BaseDriver


# ─────────────────────────────── helpers ────────────────────────────── #


def _to_frame(ts: TimeSeries, order: Sequence[UUID]) -> pd.DataFrame:
    start_s = ts.AbsoluteStart / 1_000.0
    delta_s = ts.Delta / 1_000.0
    idx = pd.to_datetime(
        [start_s + i * delta_s for i in range(len(ts.Values[0]))],
        unit="s",
        utc=True,
    ).tz_convert(timezone.utc)
    data = {str(uid): ts.Values[i] for i, uid in enumerate(order)}
    df = pd.DataFrame(data, index=idx)
    df.index.name = "time"
    return df


def _async(fn, *args, **kw):
    """Run the blocking CloudRequest call in a worker thread."""
    return asyncio.get_running_loop().run_in_executor(None, lambda: fn(*args, **kw))


# ─────────────────────────────── driver ─────────────────────────────── #


class CloudGQLDriver(BaseDriver):
    """
    Strategy implementation for GI.cloud.

    Internally re-uses the **blocking** `CloudRequest` helper and therefore
    ships all public methods as `async` wrappers that off-load the heavy
    lifting to a thread pool.
    """

    name = "cloud_gql"
    priority = 10

    # the REST parts of CloudRequest still need the tenant URL
    _TENANT_REST_PREFIX = ""

    def __init__(self, auth, http, client_id=None, **kwargs) -> None:
        super().__init__(auth, http, client_id)
        self._cr = CloudRequest()
        # we will lazy-login on first use

    # ------------------------------------------------------------------#
    # login                                                             #
    # ------------------------------------------------------------------#

    async def _ensure_login(self) -> None:
        if self._cr.login_token:
            return
        token = await self.auth.bearer()           # <- already cached / refreshed
        await _async(
            self._cr.login,
            url=self.http.base_url,
            access_token=token,
        )

    # ------------------------------------------------------------------#
    # online                                                            #
    # ------------------------------------------------------------------#

    async def list_variables(self) -> List[Dict[str, Any]]:
        await self._ensure_login()
        return await _async(self._cr.variable_info)

    async def read(self, var_ids: List[UUID]) -> Dict[UUID, float]:
        await self._ensure_login()
        res = await _async(self._cr.read_value, [str(v) for v in var_ids])
        if res is None or "Data" not in res:
            return {}
        return {vid: val for vid, val in zip(var_ids, res["Data"]["Values"])}

    async def write(self, mapping: Dict[UUID, float]) -> None:
        await self._ensure_login()
        await _async(
            self._cr.write_value_on_channel,
            [str(v) for v in mapping.keys()],
            list(mapping.values()),
        )

    # ------------------------------------------------------------------#
    # buffer (Kafka live streams)                                       #
    # ------------------------------------------------------------------#

    async def list_sources(self) -> List[GIStream]:
        await self._ensure_login()
        raw = await _async(self._cr.get_all_stream_metadata)
        if not raw:
            return []
        return [GIStream.model_validate(s.__dict__) for s in raw.values()]

    async def list_stream_variables(
        self, source_id: Union[str, int, UUID]
    ) -> List[GIStreamVariable]:
        await self._ensure_login()
        raw = await _async(self._cr.get_all_vars_of_stream, str(source_id))
        if not raw:
            return []
        return [
            GIStreamVariable.model_validate(
                {
                    "Id": v.id,
                    "Name": v.name,
                    "Index": v.index,
                    "Unit": v.unit,
                    "DataFormat": v.data_type,
                    "sid": v.sid,
                }
            )
            for v in raw
        ]

    async def fetch_buffer(
        self,
        selectors: List[Tuple[Union[UUID, str, int], UUID]],
        *,
        start_ms: float = -20_000,
        end_ms: float = 0,
        points: int = 2048,
    ) -> pd.DataFrame:
        # Cloud backend still exposes the classic /buffer/data – we keep the
        # proven converter from LocalHTTPDriver.
        variables = [VarSelector(SID=s, VID=v) for s, v in selectors]
        req = BufferRequest(Start=start_ms, End=end_ms, Points=points, Variables=variables)

        res = await self.http.post("/buffer/data", json=req.model_dump(by_alias=True, mode="json"))
        ts = BufferSuccess.model_validate(res.json()).first_timeseries()
        return _to_frame(ts, [UUID(str(v.VID)) for v in variables])

    # ------------------------------------------------------------------#
    # history                                                           #
    # ------------------------------------------------------------------#

    async def list_measurements(self, source_id: Union[str, int, UUID]) -> List[Dict[str, Any]]:
        await self._ensure_login()
        res = await self.http.get(f"/history/structure/sources/{source_id}/measurements")
        return res.json().get("Data", [])

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
        req = BufferRequest(
            Start=start_ms,
            End=end_ms,
            Points=points,
            Variables=variables,
        )
        res = await self.http.post("/history/data", json=req.model_dump(by_alias=True, mode="json"))
        ts = BufferSuccess.model_validate(res.json()).first_timeseries()
        return _to_frame(ts, var_ids)
