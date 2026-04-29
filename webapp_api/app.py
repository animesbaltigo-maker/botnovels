from __future__ import annotations

import asyncio
import hashlib
import html
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from config import (
    BASE_DIR,
    BOT_BRAND,
    BOT_TOKEN,
    CAKTO_NOTIFY_USERS,
    CAKTO_WEBHOOK_SECRET,
    DATA_DIR,
    HOME_SECTION_LIMIT,
)
from services.cakto_gateway import extract_webhook_secret_values, process_cakto_webhook
from services.centralnovel_client import (
    get_blog_posts,
    get_cached_chapter_payload,
    get_cached_novel_bundle,
    get_chapter_payload,
    get_novel_bundle,
    get_recent_updated_novels,
    get_series_catalog,
    prefetch_chapter_payloads,
    search_novels,
)
from services.metrics import get_last_read_entry, get_recently_read, mark_chapter_read
from services.offline_access import get_offline_access, init_offline_access_db
from services.profile_store import (
    list_user_favorites,
    merge_user_favorites,
    remove_user_favorite,
    set_user_favorite,
)

MINIAPP_DIR = BASE_DIR / "miniapp"
PROGRESS_PATH = Path(DATA_DIR) / "novel_miniapp_progress.json"

app = FastAPI(
    title="Novels Baltigo API",
    description="API do miniapp de leitura de novels",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,
)

init_offline_access_db()


class ProgressPayload(BaseModel):
    user_id: str = Field(min_length=1)
    title_id: str = Field(min_length=1)
    title_name: str = ""
    chapter_id: str = Field(min_length=1)
    chapter_number: str = ""
    chapter_url: str = ""
    page_index: int = 1
    total_pages: int = 1
    scroll_percent: float = 0
    paragraph_count: int = 0
    cover_url: str = ""
    updated_at: int | float | str | None = None


class ProgressSyncPayload(BaseModel):
    user_id: str = Field(min_length=1)
    progress: list[dict[str, Any]] = Field(default_factory=list)


class FavoritePayload(BaseModel):
    user_id: str = Field(min_length=1)
    title_id: str = Field(min_length=1)
    title: str = ""
    display_title: str = ""
    cover_url: str = ""
    background_url: str = ""
    latest_chapter: Any = ""
    latest_chapter_id: Any = ""
    chapter_id: Any = ""
    chapter_number: Any = ""
    status: Any = ""
    rating: Any = ""
    added_at: int | float | None = None
    updated_at: int | float | None = None
    favorite: bool = True


class FavoritesSyncPayload(BaseModel):
    user_id: str = Field(min_length=1)
    favorites: list[dict[str, Any]] = Field(default_factory=list)


_CACHE: dict[str, dict[str, Any]] = {}
_CACHE_LOCK = asyncio.Lock()
_HOME_TTL = 40
_TITLE_TTL = 120
_CHAPTER_TTL = 180
_SEARCH_TTL = 30


def _now() -> float:
    return time.time()


def _model_dump(payload: BaseModel) -> dict[str, Any]:
    if hasattr(payload, "model_dump"):
        return payload.model_dump()
    return payload.dict()


def _cache_key(namespace: str, **kwargs: Any) -> str:
    raw = json.dumps({"ns": namespace, **kwargs}, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


async def _cache_get(namespace: str, ttl: int, **kwargs: Any) -> Any | None:
    key = _cache_key(namespace, **kwargs)
    async with _CACHE_LOCK:
        entry = _CACHE.get(key)
        if not entry:
            return None
        if entry["expires_at"] < _now():
            _CACHE.pop(key, None)
            return None
        return entry["value"]


async def _cache_set(namespace: str, value: Any, ttl: int, **kwargs: Any) -> Any:
    key = _cache_key(namespace, **kwargs)
    async with _CACHE_LOCK:
        _CACHE[key] = {"value": value, "expires_at": _now() + ttl}
    return value


async def _cached(namespace: str, ttl: int, producer, **kwargs: Any) -> Any:
    cached = await _cache_get(namespace, ttl, **kwargs)
    if cached is not None:
        return cached
    value = await producer()
    return await _cache_set(namespace, value, ttl, **kwargs)


async def _clear_cache() -> None:
    async with _CACHE_LOCK:
        _CACHE.clear()


def _load_progress() -> dict[str, dict[str, Any]]:
    if not PROGRESS_PATH.exists():
        return {}
    try:
        data = json.loads(PROGRESS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _save_progress(data: dict[str, dict[str, Any]]) -> None:
    PROGRESS_PATH.parent.mkdir(parents=True, exist_ok=True)
    PROGRESS_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _progress_key(user_id: str, title_id: str) -> str:
    return f"{str(user_id).strip()}:{str(title_id).strip()}"


def _updated_at_ms(value: Any) -> int:
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value or "").strip()
    if not text:
        return int(time.time() * 1000)
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return int(datetime.strptime(text[:19], fmt).replace(tzinfo=timezone.utc).timestamp() * 1000)
        except ValueError:
            pass
    try:
        return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp() * 1000)
    except ValueError:
        return int(time.time() * 1000)


def _public_chapter(item: dict[str, Any] | None) -> dict[str, Any] | None:
    if not item:
        return None
    return {
        "chapter_id": item.get("chapter_id") or "",
        "chapter_number": str(item.get("chapter_number") or ""),
        "chapter_title": item.get("chapter_title") or item.get("title") or "",
        "chapter_url": item.get("chapter_url") or "",
    }


def _latest_chapter_value(item: dict[str, Any]) -> str:
    latest = item.get("latest_chapter")
    if isinstance(latest, dict):
        return str(latest.get("chapter_number") or latest.get("chapter_id") or "")
    return str(latest or item.get("chapter_number") or "")


def _public_title_item(item: dict[str, Any]) -> dict[str, Any]:
    latest = item.get("latest_chapter")
    latest_id = ""
    if isinstance(latest, dict):
        latest_id = str(latest.get("chapter_id") or "")

    title = item.get("display_title") or item.get("title") or "Novel"
    return {
        "title_id": item.get("title_id") or "",
        "title": title,
        "display_title": title,
        "description": item.get("description") or "",
        "cover_url": item.get("cover_url") or item.get("banner_url") or "",
        "background_url": item.get("banner_url") or item.get("cover_url") or "",
        "status": item.get("status") or "",
        "type": item.get("type") or "Novel",
        "author": item.get("author") or "",
        "genres": item.get("genres") or [],
        "total_chapters": item.get("total_chapters") or "",
        "latest_chapter": _latest_chapter_value(item),
        "latest_chapter_id": latest_id or str(item.get("latest_chapter_id") or item.get("chapter_id") or ""),
    }


def _public_title_bundle(bundle: dict[str, Any], user_id: str = "") -> dict[str, Any]:
    chapters = [_public_chapter(item) for item in (bundle.get("chapters") or []) if item.get("chapter_id")]
    latest = _public_chapter(bundle.get("latest_chapter"))
    first = _public_chapter(bundle.get("first_chapter"))
    title_id = bundle.get("title_id") or ""

    payload = {
        **_public_title_item(bundle),
        "title_id": title_id,
        "description": bundle.get("description") or "",
        "banner_url": bundle.get("banner_url") or bundle.get("cover_url") or "",
        "chapters": chapters,
        "latest_chapter": latest,
        "first_chapter": first,
    }
    if user_id:
        payload["last_read"] = _public_last_read(get_last_read_entry(user_id, title_id))
    return payload


def _public_reader_payload(chapter: dict[str, Any]) -> dict[str, Any]:
    paragraphs = [str(item).strip() for item in (chapter.get("paragraphs") or []) if str(item or "").strip()]
    return {
        "title_id": chapter.get("title_id") or "",
        "title": chapter.get("title") or "Novel",
        "chapter_id": chapter.get("chapter_id") or "",
        "chapter_number": str(chapter.get("chapter_number") or ""),
        "chapter_title": chapter.get("chapter_title") or "",
        "chapter_url": chapter.get("chapter_url") or "",
        "cover_url": chapter.get("cover_url") or "",
        "paragraph_count": len(paragraphs),
        "paragraphs": paragraphs,
        "previous_chapter": _public_chapter(chapter.get("previous_chapter")),
        "next_chapter": _public_chapter(chapter.get("next_chapter")),
    }


def _public_last_read(entry: dict[str, Any] | None) -> dict[str, Any] | None:
    if not entry:
        return None
    return {
        "title_id": entry.get("title_id") or "",
        "title_name": entry.get("title_name") or "",
        "chapter_id": entry.get("chapter_id") or "",
        "chapter_number": entry.get("chapter_number") or "",
        "chapter_url": entry.get("chapter_url") or "",
        "updated_at": entry.get("updated_at") or "",
    }


def _public_history_item(user_id: str, item: dict[str, Any], progress: dict[str, dict[str, Any]]) -> dict[str, Any]:
    title_id = item.get("title_id") or ""
    stored = progress.get(_progress_key(user_id, title_id)) or {}
    return {
        "title_id": title_id,
        "title_name": item.get("title_name") or stored.get("title_name") or "",
        "chapter_id": item.get("chapter_id") or stored.get("chapter_id") or "",
        "chapter_number": item.get("chapter_number") or stored.get("chapter_number") or "",
        "chapter_url": item.get("chapter_url") or stored.get("chapter_url") or "",
        "page_index": int(stored.get("page_index") or 1),
        "total_pages": int(stored.get("total_pages") or 1),
        "cover_url": stored.get("cover_url") or "",
        "updated_at": _updated_at_ms(stored.get("updated_at") or item.get("updated_at")),
    }


async def _home_payload(limit: int) -> dict[str, Any]:
    async def producer() -> dict[str, Any]:
        fetch_limit = max(limit, HOME_SECTION_LIMIT, 120)
        catalog, recent = await asyncio.gather(
            get_series_catalog(limit=fetch_limit),
            get_recent_updated_novels(limit=max(limit, HOME_SECTION_LIMIT, 40)),
        )
        catalog_items = [_public_title_item(item) for item in catalog if item.get("title_id")]
        recent_items = [_public_title_item(item) for item in recent if item.get("title_id")]
        return {
            "featured": catalog_items[:limit],
            "popular": catalog_items[:limit],
            "catalog": catalog_items,
            "catalog_count": len(catalog_items),
            "recent_titles": recent_items[:limit],
            "latest_titles": recent_items[:limit],
        }

    return await _cached("home", _HOME_TTL, producer, limit=limit)


@app.get("/api/ping")
async def ping() -> dict[str, bool]:
    return {"ok": True}


@app.get("/api/home")
async def api_home(limit: int = Query(max(HOME_SECTION_LIMIT, 60), ge=4, le=240)):
    return await _home_payload(limit)


@app.get("/api/news")
async def api_news(limit: int = Query(12, ge=1, le=40)):
    async def producer() -> dict[str, Any]:
        return {"items": await get_blog_posts(limit=limit)}

    return await _cached("news", _HOME_TTL, producer, limit=limit)


@app.get("/api/search")
async def api_search(q: str = Query("", min_length=1), limit: int = Query(12, ge=1, le=30)):
    async def producer() -> dict[str, Any]:
        results = await search_novels(q, limit=limit)
        return {"query": q, "results": [_public_title_item(item) for item in results if item.get("title_id")]}

    return await _cached("search", _SEARCH_TTL, producer, q=q, limit=limit)


@app.get("/api/title/{title_id}")
async def api_title(title_id: str, user_id: str = Query("")):
    try:
        bundle = get_cached_novel_bundle(title_id) or await get_novel_bundle(title_id)
        return _public_title_bundle(bundle, user_id=user_id)
    except Exception as error:
        raise HTTPException(status_code=404, detail=str(error)) from error


@app.get("/api/title/{title_id}/chapters")
async def api_title_chapters(title_id: str):
    bundle = get_cached_novel_bundle(title_id) or await get_novel_bundle(title_id)
    return {
        "title_id": bundle.get("title_id") or title_id,
        "title": bundle.get("title") or "",
        "chapters": [_public_chapter(item) for item in (bundle.get("chapters") or []) if item.get("chapter_id")],
    }


@app.get("/api/chapter/{chapter_id}")
async def api_chapter(chapter_id: str):
    try:
        chapter = get_cached_chapter_payload(chapter_id) or await get_chapter_payload(chapter_id)
        refs = [
            (chapter.get("previous_chapter") or {}).get("chapter_id") or "",
            (chapter.get("next_chapter") or {}).get("chapter_id") or "",
        ]
        prefetch_chapter_payloads(refs, limit=2)
        return _public_reader_payload(chapter)
    except Exception as error:
        raise HTTPException(status_code=404, detail=str(error)) from error


@app.get("/api/progress")
async def api_get_progress(user_id: str = Query(...), title_id: str = Query(...)):
    data = _load_progress()
    return data.get(_progress_key(user_id, title_id)) or {}


@app.post("/api/progress")
async def api_save_progress(payload: ProgressPayload):
    data = _load_progress()
    stored = _model_dump(payload)
    if not stored.get("updated_at"):
        stored["updated_at"] = int(time.time() * 1000)
    data[_progress_key(payload.user_id, payload.title_id)] = stored
    _save_progress(data)

    mark_chapter_read(
        user_id=payload.user_id,
        title_id=payload.title_id,
        chapter_id=payload.chapter_id,
        chapter_number=payload.chapter_number,
        title_name=payload.title_name,
        chapter_url=payload.chapter_url,
    )
    return {"ok": True}


@app.post("/api/progress/sync")
async def api_sync_progress(payload: ProgressSyncPayload):
    data = _load_progress()
    now_ms = int(time.time() * 1000)

    for raw_item in (payload.progress or [])[:300]:
        if not isinstance(raw_item, dict):
            continue
        title_id = str(raw_item.get("title_id") or "").strip()
        chapter_id = str(raw_item.get("chapter_id") or "").strip()
        if not title_id or not chapter_id:
            continue

        key = _progress_key(payload.user_id, title_id)
        current = data.get(key) or {}
        incoming_updated = _updated_at_ms(raw_item.get("updated_at") or now_ms)
        current_updated = _updated_at_ms(current.get("updated_at")) if current else 0
        if current and incoming_updated < current_updated:
            continue

        record = {
            "user_id": payload.user_id,
            "title_id": title_id,
            "title_name": str(raw_item.get("title_name") or raw_item.get("title") or "").strip(),
            "chapter_id": chapter_id,
            "chapter_number": str(raw_item.get("chapter_number") or "").strip(),
            "chapter_url": str(raw_item.get("chapter_url") or "").strip(),
            "page_index": int(raw_item.get("page_index") or 1),
            "total_pages": int(raw_item.get("total_pages") or 1),
            "cover_url": str(raw_item.get("cover_url") or "").strip(),
            "updated_at": incoming_updated,
        }
        data[key] = {**current, **record}
        try:
            mark_chapter_read(
                user_id=payload.user_id,
                title_id=title_id,
                chapter_id=chapter_id,
                chapter_number=record["chapter_number"],
                title_name=record["title_name"],
                chapter_url=record["chapter_url"],
            )
        except Exception:
            pass

    _save_progress(data)
    return {"ok": True, "items": _history_items(payload.user_id, data, limit=200)}


def _history_items(user_id: str, progress: dict[str, dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    return [
        _public_history_item(user_id, item, progress)
        for item in get_recently_read(user_id, limit=limit)
        if item.get("title_id") and item.get("chapter_id")
    ]


@app.get("/api/history")
async def api_get_history(user_id: str = Query(...), limit: int = Query(80, ge=1, le=300)):
    progress = _load_progress()
    return {"items": _history_items(user_id, progress, limit)}


@app.get("/api/favorites")
async def api_get_favorites(user_id: str = Query(...)):
    return {"items": list_user_favorites(user_id, limit=300)}


@app.post("/api/favorites")
async def api_save_favorite(payload: FavoritePayload):
    if not payload.favorite:
        remove_user_favorite(payload.user_id, payload.title_id)
        return {"ok": True, "items": list_user_favorites(payload.user_id, limit=300)}

    favorite = _model_dump(payload)
    favorite.pop("favorite", None)
    favorite.pop("user_id", None)
    set_user_favorite(payload.user_id, favorite)
    return {"ok": True, "items": list_user_favorites(payload.user_id, limit=300)}


@app.post("/api/favorites/sync")
async def api_sync_favorites(payload: FavoritesSyncPayload):
    return {"ok": True, "items": merge_user_favorites(payload.user_id, payload.favorites)}


@app.get("/api/profile")
async def api_profile(user_id: str = Query(...)):
    progress = _load_progress()
    favorites = list_user_favorites(user_id, limit=500)
    history = _history_items(user_id, progress, limit=500)
    opened_titles = {item["title_id"] for item in history if item.get("title_id")}
    chapters = {(item["title_id"], item["chapter_id"]) for item in history if item.get("chapter_id")}
    pages_read = 0
    for item in progress.values():
        if str(item.get("user_id") or "") != str(user_id):
            continue
        pages_read += max(1, int(item.get("page_index") or 1))
    return {
        "user_id": str(user_id),
        "favorites_count": len(favorites),
        "chapters_read": len(chapters),
        "titles_opened": len(opened_titles),
        "pages_read": pages_read,
        "last_read": history[0] if history else None,
        "offline_access": get_offline_access(user_id) or {},
    }


@app.get("/api/offline/access")
async def api_offline_access(user_id: str = Query(...)):
    return get_offline_access(user_id) or {"is_active": False, "status": "none"}


@app.post("/api/refresh")
async def api_refresh():
    await _clear_cache()
    return {"ok": True}


def _cakto_secret_candidates(request: Request, payload: dict[str, Any]) -> list[str]:
    candidates: list[str] = []
    for key in ("secret", "token"):
        value = request.query_params.get(key)
        if value:
            candidates.append(value.strip())

    for header_name in ("x-cakto-secret", "x-webhook-secret", "x-secret", "x-cakto-token"):
        value = request.headers.get(header_name)
        if value:
            candidates.append(value.strip())

    authorization = request.headers.get("authorization", "").strip()
    if authorization.lower().startswith("bearer "):
        candidates.append(authorization.split(" ", 1)[1].strip())
    elif authorization:
        candidates.append(authorization)

    candidates.extend(extract_webhook_secret_values(payload))
    return [item for item in candidates if item]


def _cakto_secret_is_valid(request: Request, payload: dict[str, Any]) -> bool:
    expected = (CAKTO_WEBHOOK_SECRET or "").strip()
    if not expected:
        return True
    return expected in _cakto_secret_candidates(request, payload)


def _log_cakto_webhook_payload(payload: dict[str, Any], result: dict[str, Any] | None = None) -> None:
    path = Path(DATA_DIR) / "cakto_webhooks.jsonl"
    record = {
        "received_at": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
        "result": result or {},
        "payload": payload,
    }
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as file:
            file.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
    except Exception:
        pass


async def _notify_cakto_user(result: dict[str, Any]) -> None:
    if not CAKTO_NOTIFY_USERS or not BOT_TOKEN:
        return
    access = result.get("access") or {}
    if access.get("duplicate_event"):
        return

    user_id = result.get("user_id")
    if not user_id:
        return

    action = result.get("action")
    brand = html.escape(BOT_BRAND or "Novels Baltigo")
    if action == "granted":
        plan = html.escape(result.get("plan_label") or access.get("plan_label") or "plano")
        expires_at = access.get("expires_at") or "vitalicio"
        text = (
            "<b>Leitura offline liberada!</b>\n\n"
            f"» <b>Plano:</b> <i>{plan}</i>\n"
            f"» <b>Validade:</b> <i>{html.escape(str(expires_at))}</i>\n\n"
            f"Agora PDF e EPUB estao ativos no <b>{brand}</b>."
        )
    elif action == "revoked":
        text = (
            "<b>Leitura offline bloqueada</b>\n\n"
            "A Cakto avisou cancelamento, reembolso ou chargeback dessa assinatura."
        )
    else:
        return

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(5.0, connect=3.0)) as client:
            await client.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={
                    "chat_id": int(user_id),
                    "text": text,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
            )
    except Exception:
        pass


@app.post("/api/webhooks/cakto")
async def api_cakto_webhook(request: Request):
    try:
        payload = await request.json()
    except Exception as error:
        raise HTTPException(status_code=400, detail="JSON invalido.") from error
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Payload precisa ser um objeto JSON.")

    if not _cakto_secret_is_valid(request, payload):
        _log_cakto_webhook_payload(payload, {"action": "unauthorized"})
        raise HTTPException(status_code=401, detail="Webhook Cakto nao autorizado.")

    result = process_cakto_webhook(payload)
    _log_cakto_webhook_payload(payload, result)
    if result.get("action") in {"granted", "revoked"}:
        asyncio.create_task(_notify_cakto_user(result))
    return result


@app.get("/")
async def root():
    return FileResponse(MINIAPP_DIR / "index.html")


@app.middleware("http")
async def add_headers(request: Request, call_next):
    start = time.perf_counter()
    no_cache_index = request.url.path in {"/", "/miniapp", "/miniapp/", "/miniapp/index.html"}
    if no_cache_index:
        request.scope["headers"] = [
            (key, value)
            for key, value in request.scope.get("headers", [])
            if key.lower() not in {b"if-none-match", b"if-modified-since"}
        ]
    response: Response = await call_next(request)
    response.headers["X-Response-Time"] = f"{round((time.perf_counter() - start) * 1000, 2)}ms"
    if no_cache_index:
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail, "path": str(request.url.path)},
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={"detail": "Erro interno no miniapp.", "path": str(request.url.path), "error": str(exc)},
    )


if MINIAPP_DIR.exists():
    app.mount("/miniapp", StaticFiles(directory=MINIAPP_DIR, html=True), name="miniapp")
