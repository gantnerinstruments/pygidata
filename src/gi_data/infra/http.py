from __future__ import annotations

import logging
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

        # base headers
        final_headers: MutableMapping[str, str] = {}
        token = await self._auth.bearer()
        final_headers["authorization"] = f"Bearer {token}"

        if content is not None:
            final_headers["content-type"] = "application/octet-stream"
        else:
            final_headers["content-type"] = "application/json"

        # allow caller overrides
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

        res = await self._client.request(
            method,
            url,
            headers=final_headers,
            params=params,
            json=json,
            content=content,
        )

        logger.info(
            "Response: %s | Content-Length: %d",
            res.status_code,
            len(res.content),
        )
        logger.debug("Response Content: %s", res.content)

        if res.status_code == 404:
            logger.warning("WARNING!: Response Body (Error, first 500 chars): %s", res.text[:500])
            return res

        res.raise_for_status()
        return res

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> "AsyncHTTP":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: D401
        await self.aclose()
        return False
