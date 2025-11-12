from __future__ import annotations

import json
import logging
from asyncio import Queue, create_task
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Dict, List, Tuple
from uuid import UUID

from aiokafka import AIOKafkaConsumer
from aiokafka.helpers import create_ssl_context

from gi_data.infra.auth import AuthManager
from gi_data.infra.http import AsyncHTTP
from gi_data.utils.logging import setup_module_logger

logger = setup_module_logger(__name__, level=logging.INFO)


class KafkaStreamDriver:
    """
    High-rate “fire-hose” driver for GI devices exposing the /kafka/info endpoint.
    """

    def __init__(self, auth: AuthManager, http: AsyncHTTP) -> None:
        self._auth = auth
        self._http = http
        self._consumer: AIOKafkaConsumer | None = None
        self._channel: Queue[Tuple[int, Dict[UUID, float]]] | None = None
        self._task = None

    async def stream(
        self,
        var_ids: List[UUID],
        *,
        group_id: str = "gi_data_client",
        ssl: bool = False,
    ) -> AsyncGenerator[Dict[UUID, float], None]:
        """
        Async generator yielding {uuid: value} dicts in *publish* order.
        """
        async with self._init_consumer(var_ids, group_id, ssl) as channel:
            while True:
                ts, payload = await channel.get()
                yield payload

    async def aclose(self) -> None:
        if self._task:
            self._task.cancel()
        if self._consumer:
            await self._consumer.stop()

    @asynccontextmanager
    async def _init_consumer(
        self,
        var_ids: List[UUID],
        group_id: str,
        ssl: bool,
    ):
        if self._consumer is None:
            info = await self._discover()
            broker = f"{info['Host']}:{info['Port']}"
            topic = info["Topic"]
            ssl_ctx = create_ssl_context() if ssl else None

            self._consumer = AIOKafkaConsumer(
                topic,
                bootstrap_servers=broker,
                group_id=group_id,
                security_protocol="SSL" if ssl else "PLAINTEXT",
                ssl_context=ssl_ctx,
                enable_auto_commit=False,
                value_deserializer=lambda x: json.loads(x.decode()),
            )
            await self._consumer.start()

            self._channel = Queue(maxsize=10_000)
            self._task = create_task(self._pump(var_ids))

        try:
            yield self._channel
        finally:
            await self.aclose()

    async def _discover(self) -> Dict[str, str | int]:
        res = await self._http.get("/kafka/info")
        return res.json()["Data"]

    async def _pump(self, var_ids: List[UUID]) -> None:
        wanted = {str(u) for u in var_ids}
        async for msg in self._consumer:
            data = msg.value  # {'Time':..., 'Values':{uuid: val, …}}
            values = {UUID(k): v for k, v in data["Values"].items() if k in wanted}
            if values:
                await self._channel.put((data["Time"], values))
