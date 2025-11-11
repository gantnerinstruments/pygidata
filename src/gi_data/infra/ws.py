from __future__ import annotations

import json
import logging
import ssl
from typing import Any, List, Tuple

import websockets
import certifi

from gi_data.infra.auth import AuthManager

log = logging.getLogger(__name__)


class AsyncWS:
    """
    Async wrapper around websockets with framing required by GI devices (version-byte + length + json+json).
    Specification under: https://git.gantner-instruments.com/gins/gisource/-/blob/main/doc/GInsWebSocket.md
    """

    VERSION_BYTE: bytes = b"\x00"

    def __init__(self, base_url: str, auth: AuthManager) -> None:
        base = base_url.rstrip("/")
        if base.startswith("https://"):
            base = "wss://" + base[len("https://"):]
        elif base.startswith("http://"):
            base = "ws://" + base[len("http://"):]
        self._url = f"{base}/ws"
        self._auth = auth
        self._ws: websockets.WebSocketClientProtocol | None = None

    @property
    def connected(self) -> bool:
        return self._ws is not None

    async def connect(self) -> None:
        token = await self._auth.bearer()
        uri = f"{self._url}?apitoken={token}"

        ssl_ctx = ssl.create_default_context(cafile=certifi.where()) if uri.startswith("wss://") else None

        try:
            self._ws = await websockets.connect(uri, ssl=ssl_ctx, ping_interval=None, ping_timeout=None)
            log.debug("WebSocket connected to %s", uri)
        except Exception as e:
            log.error("Failed to connect WebSocket: %s", e)
            raise

    async def close(self) -> None:
        if self.connected:
            await self._ws.close()
            log.debug("WebSocket closed")

    async def send(self, header: List[Any], payload: dict | List | None = None) -> None:
        if not self.connected:
            await self.connect()

        header_json = json.dumps(header, separators=(",", ":")).encode()
        payload_json = json.dumps(payload or {}, separators=(",", ":")).encode()

        frame = (
            self.VERSION_BYTE
            + len(header_json).to_bytes(2, "little")
            + header_json
            + payload_json
        )

        try:
            await self._ws.send(frame)
        except Exception as e:
            log.error("Failed to send WebSocket message: %s", e)
            raise

    async def recv(self) -> Tuple[List[Any], Any]:
        if not self.connected:
            await self.connect()

        try:
            raw: bytes = await self._ws.recv()
            if not raw or raw[0:1] != self.VERSION_BYTE:
                raise RuntimeError("Invalid frame received")

            header_len = int.from_bytes(raw[1:3], "little")
            header_json = raw[3:3 + header_len].decode()
            payload_json = raw[3 + header_len:].decode()

            return json.loads(header_json), json.loads(payload_json)
        except Exception as e:
            log.error("Failed to receive WebSocket message: %s", e)
            raise
