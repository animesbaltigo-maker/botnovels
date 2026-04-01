from __future__ import annotations

import asyncio
import importlib.util

import httpx

from config import CATALOG_SITE_BASE, HTTP_TIMEOUT

_CLIENT: httpx.AsyncClient | None = None
_CLIENT_LOCK = asyncio.Lock()
_HTTP2_ENABLED = importlib.util.find_spec("h2") is not None

_BASE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "application/json,text/plain,*/*;q=0.8"
    ),
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Referer": f"{CATALOG_SITE_BASE}/",
    "Origin": CATALOG_SITE_BASE,
}

_TIMEOUT = httpx.Timeout(
    float(HTTP_TIMEOUT),
    connect=10.0,
    read=float(HTTP_TIMEOUT),
    write=float(HTTP_TIMEOUT),
    pool=45.0,
)

_LIMITS = httpx.Limits(
    max_connections=180,
    max_keepalive_connections=80,
    keepalive_expiry=45.0,
)


async def get_http_client() -> httpx.AsyncClient:
    global _CLIENT
    if _CLIENT is not None:
        return _CLIENT

    async with _CLIENT_LOCK:
        if _CLIENT is None:
            _CLIENT = httpx.AsyncClient(
                headers=_BASE_HEADERS,
                follow_redirects=True,
                timeout=_TIMEOUT,
                limits=_LIMITS,
                http2=_HTTP2_ENABLED,
            )
    return _CLIENT


async def close_http_client() -> None:
    global _CLIENT
    async with _CLIENT_LOCK:
        if _CLIENT is not None:
            await _CLIENT.aclose()
            _CLIENT = None
