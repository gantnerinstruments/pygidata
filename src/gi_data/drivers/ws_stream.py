from __future__ import annotations

import logging
from typing import (
    AsyncGenerator,
    Iterable,
    Mapping,
    MutableMapping,
    Sequence, Any,
)
from uuid import UUID

from gi_data.infra.auth import AuthManager
from gi_data.infra.http import AsyncHTTP
from gi_data.infra.ws import AsyncWS
from gi_data.utils.logging import setup_module_logger
from gi_data.ws.enums import (
    GInsWSMessageTypes as MT,
    GInsWSWorkerTypes as WT,
)

logger = setup_module_logger(__name__, level=logging.DEBUG)


class WebSocketDriver:
    """
    Subscribe to / publish online data frames.
    """

    _WORKER_ID = 1  # one OnlineData worker per connection

    def __init__(self, auth: AuthManager, ws: AsyncWS, http: AsyncHTTP):
        self._auth = auth
        self._ws = ws
        self._http = http

    async def stream_online(
            self,
            var_ids: Sequence[UUID],
            *,
            interval_ms: int = 1,
            extended: bool = True,
            on_change: bool = False,
            precision: int = -1,
    ) -> AsyncGenerator[dict[Any, Any] | dict[str, UUID | Any], None]:
        """
        Yield a dict {uuid: value} for every OnlineData publish frame.
        """
        await self._subscribe_online(
            var_ids,
            interval_ms=interval_ms,
            extended=extended,
            on_change=on_change,
            precision=precision,
        )

        while True:
            hdr, payload = await self._ws.recv()
            if hdr[1] != MT.WSMsgType_Publish:
                continue
            logger.debug("Received Payload [WS]: %s", payload)
            if not payload:
                yield {}
                continue

            """
            d = payload[0]
            data = {
                "_id": d["_id"],
                "Id": UUID(d["Id"]),
                "State": d["State"],
                "Value": d["Values"],
            }"""
            yield payload

    async def _subscribe_online(
            self,
            var_ids: Sequence[UUID],
            *,
            interval_ms: int,
            extended: bool,
            on_change: bool,
            precision: int,
    ) -> None:
        cfg = {
            "IntervalMs": interval_ms,
            "VIDs": [str(v) for v in var_ids],
            "ExtendedAnswer": extended,
            "OnValueChanged": on_change,
            "Precision": precision,
        }
        header = [
            "",  # route
            MT.WSMsgType_Subscribe.value,
            WT.WSWorkerType_OnlineData.value,
            self._WORKER_ID,
            "",
        ]
        await self._ws.send(header, cfg)
        logger.debug("Subscribed OnlineData %s (cfg=%s)", var_ids, cfg)

    async def publish(
            self,
            data: Mapping[UUID, float] | MutableMapping[str, float] | Iterable[tuple[UUID, float]],
            *,
            function: str = "write",
    ) -> None:
        """
        Send a *publish* frame.

        `data` may be:
        * ``{uuid: value, …}``
        * a sequence of ``(uuid, value)`` pairs
        * a dict that is already str-keyed (then left untouched)
        """
        if isinstance(data, Mapping):
            mapping = {str(k): v for k, v in data.items()}
        else:
            mapping = {str(k): v for k, v in data}

        payload = {
            "Variables": list(mapping.keys()),
            "Values": list(mapping.values()),
            "Function": function,
        }
        header = [
            "",  # route
            MT.WSMsgType_Publish.value,
            WT.WSWorkerType_OnlineData.value,
            self._WORKER_ID,
            "",
        ]
        await self._ws.send(header, payload)
        logger.debug("Published [WS] %s", payload)

    async def close(self) -> None:
        await self._ws.close()
