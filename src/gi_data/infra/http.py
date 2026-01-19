from __future__ import annotations

import logging
import time
from typing import Any, Mapping, MutableMapping, Optional

import httpx

from gi_data.infra.auth import AuthManager
from gi_data.utils.logging import setup_module_logger

logger = setup_module_logger(__name__, level=logging.DEBUG)


class AsyncHTTP:
    """
    Tiny facade over ``httpx.AsyncClient`` that transparently appends the current
    bearer token obtained from a shared ``AuthManager``.
    """

    def __init__(
            self,
            base_url: str,
            auth_manager: AuthManager,  # forward reference
            timeout: float = 160.0,
    ) -> None:
        self._base = base_url.rstrip("/")
        self._auth = auth_manager
        self._client = httpx.AsyncClient(base_url=self._base, timeout=timeout)

    @property
    def base_url(self) -> str:
        return self._base

    async def get(
            self,
            url: str,
            params: Optional[Mapping[str, Any]] = None,
            *,
            json: Any | None = None,
    ) -> httpx.Response:
        return await self._request("GET", url, params=params, json=json)

    async def post(
            self,
            url: str,
            *,
            params: Optional[Mapping[str, Any]] = None,
            json: Any | None = None,
            content: bytes | None = None,
            headers: Optional[Mapping[str, str]] = None,
    ) -> httpx.Response:
        return await self._request(
            "POST", url, params=params, json=json, content=content, headers=headers
        )

    async def delete(
            self,
            url: str,
            *,
            params: Optional[Mapping[str, Any]] = None,
            json: Any | None = None,
    ) -> httpx.Response:
        return await self._request("DELETE", url, params=params, json=json)

    async def _request(
            self,
            method: str,
            url: str,
            *,
            params: Optional[Mapping[str, Any]],
            json: Any | None,
            content: bytes | None = None,
            headers: Optional[MutableMapping[str, str]] = None,
    ) -> httpx.Response:
        final_headers: MutableMapping[str, str] = {}
        token = await self._auth.bearer()
        final_headers["authorization"] = f"Bearer {token}"

        if content is not None:
            final_headers["content-type"] = "application/octet-stream"
        else:
            final_headers["content-type"] = "application/json"

        if headers:
            final_headers.update(headers)

        logger.debug(
            "Request: %s %s%s | Params: %s | Payload: %s",
            method,
            self._base,
            url,
            params if params else "None",
            "(bytes)" if content is not None else (json if json else "None"),
        )

        CHUNK = 65536
        LOG_EVERY_SEC = 15.0

        async with self._client.stream(
                method,
                url,
                headers=final_headers,
                params=params,
                json=json,
                content=content,
        ) as resp:
            resp.raise_for_status()

            total = int(resp.headers.get("content-length") or 0)

            downloaded = 0
            last_log = time.monotonic()

            buf = bytearray()

            async for chunk in resp.aiter_bytes(CHUNK):
                downloaded += len(chunk)
                buf.extend(chunk)

                now = time.monotonic()
                if now - last_log >= LOG_EVERY_SEC:
                    if total:
                        pct = downloaded * 100.0 / total
                        logger.info(
                            "Download progress: %.1f%% (%d/%d)",
                            pct,
                            downloaded,
                            total,
                        )
                    else:
                        logger.info("Download progress: %d bytes", downloaded)
                    last_log = now

            if total:
                pct = downloaded * 100.0 / total
                logger.info("Download completed: %.1f%% (%d/%d)", pct, downloaded, total)
            else:
                logger.info("Download completed: %d bytes", downloaded)

            # Return an in-memory response (large files => large RAM usage)
            return httpx.Response(
                status_code=resp.status_code,
                headers=resp.headers,
                content=bytes(buf),
                request=resp.request,
            )

    async def _read_with_progress(self, method, url, headers, params, content, json):
        CHUNK = 65536  # 64 KB
        downloaded = 0

        async with self._client.stream(
                method,
                url,
                headers=headers,
                params=params,
                json=json,
                content=content
        ) as res:

            res.raise_for_status()

            total = int(res.headers.get("content-length") or 0)
            logger.info("Response started (total=%s)", total or "unknown")

            data = bytearray()
            async for chunk in res.aiter_bytes(CHUNK):
                downloaded += len(chunk)
                data.extend(chunk)

                if total:
                    pct = (downloaded / total) * 100
                    logger.info("Download progress: %.1f%% (%d/%d)", pct, downloaded, total)
                else:
                    logger.info("Download progress: %d bytes", downloaded)

            logger.info("Download completed (%d bytes)", downloaded)

        return httpx.Response(
            status_code=res.status_code,
            headers=res.headers,
            content=bytes(data),
            request=res.request
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> "AsyncHTTP":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: D401
        await self.aclose()
        return False
