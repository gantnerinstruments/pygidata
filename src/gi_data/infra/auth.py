from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, MutableMapping

import httpx

from gi_data.utils.logging import setup_module_logger

logger = setup_module_logger(__name__, level=logging.DEBUG)


class AuthError(RuntimeError):
    """Raised if login or refresh cannot obtain a valid access token."""


class AuthManager:
    """
    Centralised bearer-token handler.

    One instance is shared by all drivers so that login / refresh
    happens exactly once per process.
    """

    _LOCK = asyncio.Lock()

    def __init__(
            self,
            base_url: str,
            username: Optional[str] = None,
            password: Optional[str] = None,
            client_id: str = "gibench",
            access_token: Optional[str] = None,
    ) -> None:
        self._base = base_url.rstrip("/")
        self._user = username or os.getenv("GI_USER")
        self._pw = password or os.getenv("GI_PASSWORD")
        self._client_id = client_id
        self._access_token = access_token or os.getenv("GI_TOKEN")
        self._token: Optional[str] = None
        self._refresh: Optional[str] = None
        self._expires: datetime = datetime.min.replace(tzinfo=timezone.utc)
        self._sync_client = httpx.Client(base_url=f"{self._base}/rpc/", timeout=20.0)

        if not access_token and self._login_required():
            self._login()

        if access_token:
            self._token = access_token
            self._refresh = access_token
            self._expires = datetime.max.replace(tzinfo=timezone.utc)  # unlimited

    async def bearer(self) -> str | None:
        """
        Return a valid `AccessToken`, refreshing or logging-in if necessary.
        Thread-safe for concurrent coroutines.
        """
        async with self._LOCK:
            if self._token and datetime.now(tz=timezone.utc) < self._expires:
                return self._token
            if await asyncio.to_thread(self._login_required):
                await asyncio.to_thread(self._login)
                if not self._token:
                    raise AuthError("Unable to obtain access token")
                return self._token
            # No login required; no token needed
            return None

    def bearer_sync(self) -> Optional[str]:
        if self._token and datetime.now(tz=timezone.utc) < self._expires:
            return self._token
        if self._refresh:
            try:
                self._refresh_token()
                return self._token
            except AuthError("Unable to refresh token"):
                return None
        return None

    def _rpc_post(self, method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        headers: MutableMapping[str, str] = {"content-type": "application/json"}
        token = self.bearer_sync()
        headers["authorization"] = f"Bearer {token}"

        res = self._sync_client.post(f"{method}", json=payload, headers=headers)
        res.raise_for_status()
        return res.json()

    def _login_required(self) -> bool:
        body = self._rpc_post("AdminAPI.LoginRequired", {})
        return bool(body.get("LoginRequired", False))

    def _login(self) -> None:
        payload = {
            "ClientID": self._client_id,
            "Username": self._user,
            "Password": self._pw,
        }
        body = self._rpc_post("AdminAPI.Login", payload)
        self._store(body)

    def _refresh_token(self) -> None:
        if not self._refresh:
            self._login()
            return
        payload = {"ClientID": self._client_id, "RefreshToken": self._refresh}
        body = self._rpc_post("AdminAPI.RefreshToken", payload)
        self._store(body)

    def _store(self, body: Dict[str, Any]) -> None:
        self._token = body.get("AccessToken")
        self._refresh = body.get("RefreshToken")
        lifetime = int(body.get("ExpiresIn", 0))
        self._expires = datetime.now(tz=timezone.utc) + timedelta(seconds=lifetime)

    def is_cloud_environment(self) -> bool:
        # cloud
        try:
            body = self._rpc_post("ConfigAPI.GetGlobalSettings", {})
            return body["Config"].get("CloudEnvironment", False)
        except httpx.HTTPStatusError:
            pass  # fallback

        # controller fallback
        try:
            body = self._rpc_post("AdminAPI.GetGlobalSettings", {})
            return body["Config"].get("CloudEnvironment", False)
        except httpx.HTTPStatusError as e:
            raise AuthError("Unable to determine environment: both RPC endpoints failed") from e
