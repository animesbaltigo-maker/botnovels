from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path
from threading import Lock

from telegraph import Telegraph

from config import BOT_BRAND, DATA_DIR, DISTRIBUTION_TAG, PROMO_BANNER_URL, TELEGRAPH_AUTHOR

TELEGRAPH_CACHE_PATH = Path(DATA_DIR) / "telegraph_pages.json"
TELEGRAPH_PAGE_CACHE_VERSION = "v2"

_telegraph: Telegraph | None = None
_telegraph_lock = Lock()
_telegraph_cache: dict[str, str] | None = None
_telegraph_cache_lock = Lock()
_telegraph_inflight: dict[str, asyncio.Task] = {}


def _load_cache() -> dict[str, str]:
    global _telegraph_cache
    if _telegraph_cache is not None:
        return _telegraph_cache

    with _telegraph_cache_lock:
        if _telegraph_cache is not None:
            return _telegraph_cache
        if TELEGRAPH_CACHE_PATH.exists():
            try:
                _telegraph_cache = json.loads(TELEGRAPH_CACHE_PATH.read_text(encoding="utf-8"))
            except Exception:
                _telegraph_cache = {}
        else:
            _telegraph_cache = {}
        return _telegraph_cache


def _save_cache() -> None:
    cache = _load_cache()
    TELEGRAPH_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    TELEGRAPH_CACHE_PATH.write_text(
        json.dumps(cache, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _page_cache_key(chapter_id: str, title: str, paragraphs: list[str]) -> str:
    normalized = f"{chapter_id}|{title}|{'|'.join(paragraphs[:40])}|{len(paragraphs)}"
    digest = hashlib.sha1(normalized.encode("utf-8")).hexdigest()[:14]
    return f"{TELEGRAPH_PAGE_CACHE_VERSION}:{chapter_id}:{digest}"


def get_cached_chapter_page_url(chapter_id: str, title: str, paragraphs: list[str]) -> str:
    cache_key = _page_cache_key(chapter_id, title, paragraphs)
    return str(_load_cache().get(cache_key) or "").strip()


def _get_client() -> Telegraph:
    global _telegraph
    if _telegraph is not None:
        return _telegraph

    with _telegraph_lock:
        if _telegraph is None:
            client = Telegraph()
            client.create_account(short_name=BOT_BRAND[:32] or "NovelsBaltigo")
            _telegraph = client
    return _telegraph


def _normalize_title(title: str) -> str:
    raw = (title or "").strip() or "Leitura"
    if DISTRIBUTION_TAG.lower() not in raw.lower():
        raw = f"{raw} | {DISTRIBUTION_TAG}"
    return raw[:256]


def _normalize_paragraphs(paragraphs: list[str]) -> list[str]:
    cleaned: list[str] = []
    for paragraph in paragraphs or []:
        text = " ".join(str(paragraph or "").strip().split())
        if not text:
            continue
        cleaned.append(text)
    return cleaned


def _build_nodes(title: str, paragraphs: list[str], footer_text: str | None = None) -> list[dict]:
    nodes: list[dict] = []
    if PROMO_BANNER_URL:
        nodes.append({"tag": "img", "attrs": {"src": PROMO_BANNER_URL}})
    nodes.append({"tag": "h3", "children": [title]})
    if footer_text:
        nodes.append({"tag": "p", "children": [footer_text]})
    for paragraph in paragraphs:
        nodes.append({"tag": "p", "children": [paragraph]})
    return nodes


async def get_or_create_chapter_page(
    chapter_id: str,
    title: str,
    paragraphs: list[str],
    footer_text: str | None = None,
) -> str:
    normalized_paragraphs = _normalize_paragraphs(paragraphs)
    if not normalized_paragraphs:
        raise RuntimeError("Nenhum texto encontrado para criar a pagina do Telegraph.")

    cache = _load_cache()
    cache_key = _page_cache_key(chapter_id, title, normalized_paragraphs)
    cached = cache.get(cache_key)
    if cached:
        return cached

    task = _telegraph_inflight.get(cache_key)
    if task:
        return await task

    async def _runner() -> str:
        page_title = _normalize_title(title)
        footer = footer_text or f"Leitura via {TELEGRAPH_AUTHOR} | {DISTRIBUTION_TAG}"
        nodes = _build_nodes(page_title, normalized_paragraphs, footer)

        def _create_page() -> str:
            client = _get_client()
            response = client.create_page(
                title=page_title,
                content=nodes,
                author_name=(f"{TELEGRAPH_AUTHOR} {DISTRIBUTION_TAG}").strip()[:128],
            )
            return "https://telegra.ph/" + response["path"]

        url = await asyncio.to_thread(_create_page)
        cache[cache_key] = url
        await asyncio.to_thread(_save_cache)
        return url

    task = asyncio.create_task(_runner())
    _telegraph_inflight[cache_key] = task
    try:
        return await task
    finally:
        _telegraph_inflight.pop(cache_key, None)
