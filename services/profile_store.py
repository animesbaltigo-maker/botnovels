from __future__ import annotations

import json
import time
from threading import Lock
from typing import Any

from config import DATA_DIR

FAVORITES_PATH = DATA_DIR / "profile_favorites.json"

_LOCK = Lock()


def _now_ms() -> int:
    return int(time.time() * 1000)


def _load_data() -> dict[str, Any]:
    if not FAVORITES_PATH.exists():
        return {"users": {}}
    try:
        data = json.loads(FAVORITES_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"users": {}}
    if not isinstance(data, dict):
        return {"users": {}}
    users = data.get("users")
    if not isinstance(users, dict):
        data["users"] = {}
    return data


def _save_data(data: dict[str, Any]) -> None:
    FAVORITES_PATH.parent.mkdir(parents=True, exist_ok=True)
    FAVORITES_PATH.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _user_bucket(data: dict[str, Any], user_id: int | str) -> dict[str, Any]:
    users = data.setdefault("users", {})
    key = str(user_id).strip()
    bucket = users.setdefault(key, {})
    if not isinstance(bucket.get("favorites"), dict):
        bucket["favorites"] = {}
    return bucket


def _text(value: Any) -> str:
    return str(value or "").strip()


def _number(value: Any, fallback: int | None = None) -> int | None:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return fallback


def normalize_favorite(item: dict[str, Any]) -> dict[str, Any] | None:
    title_id = _text(item.get("title_id") or item.get("id") or item.get("novel_id") or item.get("manga_id"))
    if not title_id:
        return None

    title = _text(
        item.get("title")
        or item.get("display_title")
        or item.get("preferred_title")
        or item.get("title_name")
    )
    now = _now_ms()
    added_at = _number(item.get("added_at"), now) or now
    updated_at = _number(item.get("updated_at"), added_at) or added_at

    latest_chapter = item.get("latest_chapter")
    if isinstance(latest_chapter, dict):
        latest_chapter_id = _text(
            item.get("latest_chapter_id")
            or latest_chapter.get("chapter_id")
            or latest_chapter.get("id")
        )
        latest_chapter = _text(
            latest_chapter.get("chapter_number")
            or latest_chapter.get("number")
            or latest_chapter.get("chapter_id")
        )
    else:
        latest_chapter_id = _text(item.get("latest_chapter_id") or item.get("chapter_id"))
        latest_chapter = _text(latest_chapter or item.get("chapter_number"))

    return {
        "title_id": title_id,
        "title": title or "Novel",
        "display_title": _text(item.get("display_title")) or title or "Novel",
        "cover_url": _text(item.get("cover_url") or item.get("background_url")),
        "background_url": _text(item.get("background_url") or item.get("cover_url")),
        "latest_chapter": latest_chapter,
        "latest_chapter_id": latest_chapter_id,
        "status": _text(item.get("status")),
        "anilist_score": _text(item.get("anilist_score") or item.get("rating")),
        "added_at": added_at,
        "updated_at": updated_at,
    }


def list_user_favorites(user_id: int | str, limit: int | None = None) -> list[dict[str, Any]]:
    key = str(user_id).strip()
    if not key:
        return []

    with _LOCK:
        data = _load_data()
        bucket = (data.get("users") or {}).get(key) or {}
        favorites = bucket.get("favorites") or {}
        if not isinstance(favorites, dict):
            return []
        items = [item for item in favorites.values() if isinstance(item, dict)]

    items.sort(key=lambda item: int(item.get("added_at") or 0), reverse=True)
    if limit is not None:
        return items[: max(0, int(limit))]
    return items


def count_user_favorites(user_id: int | str) -> int:
    return len(list_user_favorites(user_id))


def set_user_favorite(user_id: int | str, item: dict[str, Any]) -> dict[str, Any] | None:
    favorite = normalize_favorite(item)
    if not favorite:
        return None

    with _LOCK:
        data = _load_data()
        bucket = _user_bucket(data, user_id)
        existing = bucket["favorites"].get(favorite["title_id"]) or {}
        if existing:
            favorite["added_at"] = int(existing.get("added_at") or favorite["added_at"])
        favorite["updated_at"] = _now_ms()
        bucket["favorites"][favorite["title_id"]] = favorite
        bucket["updated_at"] = favorite["updated_at"]
        _save_data(data)

    return favorite


def remove_user_favorite(user_id: int | str, title_id: str) -> bool:
    title_id = _text(title_id)
    if not title_id:
        return False

    with _LOCK:
        data = _load_data()
        bucket = _user_bucket(data, user_id)
        existed = title_id in bucket["favorites"]
        bucket["favorites"].pop(title_id, None)
        bucket["updated_at"] = _now_ms()
        _save_data(data)
    return existed


def merge_user_favorites(user_id: int | str, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    with _LOCK:
        data = _load_data()
        bucket = _user_bucket(data, user_id)
        now = _now_ms()

        for item in items or []:
            if not isinstance(item, dict):
                continue
            favorite = normalize_favorite(item)
            if not favorite:
                continue
            existing = bucket["favorites"].get(favorite["title_id"]) or {}
            if existing:
                favorite["added_at"] = int(existing.get("added_at") or favorite["added_at"])
            bucket["favorites"][favorite["title_id"]] = {**existing, **favorite}

        bucket["updated_at"] = now
        _save_data(data)
        favorites = [item for item in bucket["favorites"].values() if isinstance(item, dict)]

    favorites.sort(key=lambda item: int(item.get("added_at") or 0), reverse=True)
    return favorites
