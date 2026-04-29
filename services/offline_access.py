from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import unicodedata
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any

from config import DATA_DIR

DB_PATH = DATA_DIR / "offline_access.sqlite3"

PLAN_DAYS: dict[str, int | None] = {
    "bronze": 7,
    "ouro": 30,
    "diamante": 365,
    "rubi": None,
    "1m": 30,
    "3m": 90,
    "6m": 180,
    "lifetime": None,
}

PLAN_LABELS = {
    "bronze": "Plano Bronze (semanal)",
    "ouro": "Plano Ouro (mensal)",
    "diamante": "Plano Diamante (anual)",
    "rubi": "Plano Rubi (vitalício)",
    "1m": "1 mês",
    "3m": "3 meses",
    "6m": "6 meses",
    "lifetime": "vitalício",
}


def _utc_now_dt() -> datetime:
    return datetime.now(timezone.utc)


def _utc_now_str() -> str:
    return _format_dt(_utc_now_dt())


def _format_dt(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")


def _parse_dt(value: str | None) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None

    candidates = [
        raw,
        raw.replace("T", " ").replace("Z", "+00:00"),
        raw.replace("Z", "+00:00"),
    ]

    for candidate in candidates:
        try:
            parsed = datetime.fromisoformat(candidate)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            pass

    try:
        return datetime.strptime(raw[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _plain(value: str) -> str:
    text = unicodedata.normalize("NFKD", str(value or "").strip().lower())
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]+", "_", text).strip("_")


def normalize_plan(value: str | None) -> str:
    text = _plain(value or "")
    if not text:
        return ""

    aliases = {
        "bronze": "bronze",
        "plano_bronze": "bronze",
        "semanal": "bronze",
        "semana": "bronze",
        "weekly": "bronze",
        "week": "bronze",
        "7d": "bronze",
        "7_dias": "bronze",
        "wyd3e3i": "bronze",
        "ouro": "ouro",
        "plano_ouro": "ouro",
        "oferta_principal": "ouro",
        "1": "1m",
        "1m": "ouro",
        "1_mes": "ouro",
        "um_mes": "ouro",
        "mensal": "ouro",
        "monthly": "ouro",
        "30d": "ouro",
        "30_dias": "ouro",
        "38kt683_866815": "ouro",
        "diamante": "diamante",
        "plano_diamante": "diamante",
        "anual": "diamante",
        "ano": "diamante",
        "annual": "diamante",
        "yearly": "diamante",
        "12m": "diamante",
        "12_meses": "diamante",
        "365d": "diamante",
        "365_dias": "diamante",
        "33mfwfe": "diamante",
        "rubi": "rubi",
        "plano_rubi": "rubi",
        "3": "3m",
        "3m": "3m",
        "3_meses": "3m",
        "trimestral": "3m",
        "90d": "3m",
        "90_dias": "3m",
        "6": "6m",
        "6m": "6m",
        "6_meses": "6m",
        "semestral": "6m",
        "180d": "6m",
        "180_dias": "6m",
        "vitalicio": "rubi",
        "vitalicia": "rubi",
        "vital": "rubi",
        "lifetime": "rubi",
        "perpetuo": "rubi",
        "57t5ieq": "rubi",
    }
    if text in aliases:
        return aliases[text]

    if "wyd3e3i" in text or "plano_bronze" in text or "semanal" in text:
        return "bronze"
    if "38kt683_866815" in text or "plano_ouro" in text or "oferta_principal" in text:
        return "ouro"
    if "33mfwfe" in text or "plano_diamante" in text or "anual" in text:
        return "diamante"
    if "57t5ieq" in text or "plano_rubi" in text:
        return "rubi"
    if re.search(r"(^|_)1(_)?m(es)?($|_)", text) or "30_dias" in text:
        return "ouro"
    if re.search(r"(^|_)3(_)?m(eses)?($|_)", text) or "90_dias" in text:
        return "3m"
    if re.search(r"(^|_)6(_)?m(eses)?($|_)", text) or "180_dias" in text:
        return "6m"
    if any(item in text for item in ("vitalicio", "vitalicia", "lifetime", "perpetuo")):
        return "rubi"

    return ""


def plan_label(plan: str | None) -> str:
    return PLAN_LABELS.get(normalize_plan(plan), str(plan or "").strip() or "plano")


@contextmanager
def _get_conn():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA busy_timeout=30000;")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_offline_access_db() -> None:
    with _get_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS offline_access (
                user_id INTEGER PRIMARY KEY,
                plan TEXT NOT NULL,
                status TEXT NOT NULL,
                expires_at TEXT,
                source TEXT,
                last_event_id TEXT,
                last_event_type TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_offline_access_status
            ON offline_access(status)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_offline_access_expires
            ON offline_access(expires_at)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS cakto_events (
                event_id TEXT PRIMARY KEY,
                user_id INTEGER,
                plan TEXT,
                event_type TEXT,
                action TEXT,
                payload_json TEXT,
                created_at TEXT NOT NULL
            )
            """
        )


def _payload_json(payload: dict[str, Any] | None) -> str:
    try:
        return json.dumps(payload or {}, ensure_ascii=False, sort_keys=True, default=str)
    except Exception:
        return "{}"


def _event_key(event_id: str | None, payload: dict[str, Any] | None) -> str:
    raw = str(event_id or "").strip()
    if raw:
        return raw
    return "payload:" + hashlib.sha256(_payload_json(payload).encode("utf-8")).hexdigest()


def _record_event(
    conn: sqlite3.Connection,
    *,
    event_id: str | None,
    user_id: int | None,
    plan: str,
    event_type: str,
    action: str,
    payload: dict[str, Any] | None,
) -> tuple[str, bool]:
    key = _event_key(event_id, payload)
    cursor = conn.execute(
        """
        INSERT OR IGNORE INTO cakto_events (
            event_id,
            user_id,
            plan,
            event_type,
            action,
            payload_json,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            key,
            int(user_id) if user_id is not None else None,
            plan,
            event_type,
            action,
            _payload_json(payload),
            _utc_now_str(),
        ),
    )
    return key, cursor.rowcount > 0


def _row_to_access(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if not row:
        return None

    expires_at = row["expires_at"] or ""
    active = row["status"] == "active"
    expires_dt = _parse_dt(expires_at)
    if active and expires_dt is not None and expires_dt <= _utc_now_dt():
        active = False

    return {
        "user_id": int(row["user_id"]),
        "plan": row["plan"],
        "plan_label": plan_label(row["plan"]),
        "status": row["status"],
        "expires_at": expires_at,
        "is_lifetime": not bool(expires_at),
        "is_active": active,
        "source": row["source"] or "",
        "last_event_id": row["last_event_id"] or "",
        "last_event_type": row["last_event_type"] or "",
        "updated_at": row["updated_at"] or "",
        "created_at": row["created_at"] or "",
    }


def get_offline_access(user_id: int | str | None) -> dict[str, Any] | None:
    if user_id is None:
        return None
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return None

    init_offline_access_db()
    with _get_conn() as conn:
        row = conn.execute(
            """
            SELECT *
            FROM offline_access
            WHERE user_id = ?
            LIMIT 1
            """,
            (uid,),
        ).fetchone()

        access = _row_to_access(row)
        if access and access["status"] == "active" and not access["is_active"]:
            conn.execute(
                """
                UPDATE offline_access
                SET status = 'expired', updated_at = ?
                WHERE user_id = ? AND status = 'active'
                """,
                (_utc_now_str(), uid),
            )
            access["status"] = "expired"

    return access


def is_offline_user_allowed(user_id: int | str | None) -> bool:
    access = get_offline_access(user_id)
    return bool(access and access.get("is_active"))


def grant_offline_access(
    user_id: int | str,
    plan: str,
    *,
    event_id: str | None = None,
    event_type: str = "purchase_approved",
    source: str = "cakto",
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    uid = int(user_id)
    normalized_plan = normalize_plan(plan)
    if normalized_plan not in PLAN_DAYS:
        raise ValueError("Plano offline inválido.")

    init_offline_access_db()
    with _get_conn() as conn:
        stored_event_id, is_new_event = _record_event(
            conn,
            event_id=event_id,
            user_id=uid,
            plan=normalized_plan,
            event_type=event_type,
            action="grant",
            payload=payload,
        )

        if not is_new_event:
            row = conn.execute(
                """
                SELECT *
                FROM offline_access
                WHERE user_id = ?
                LIMIT 1
                """,
                (uid,),
            ).fetchone()
            access = _row_to_access(row) or {}
            access["duplicate_event"] = True
            return access

        now = _utc_now_dt()
        now_text = _format_dt(now)
        current = conn.execute(
            """
            SELECT *
            FROM offline_access
            WHERE user_id = ?
            LIMIT 1
            """,
            (uid,),
        ).fetchone()

        created_at = current["created_at"] if current else now_text
        current_active = _row_to_access(current)
        current_expires = _parse_dt(current["expires_at"]) if current else None

        if current_active and current_active.get("is_active") and current_active.get("is_lifetime"):
            final_plan = "rubi"
            expires_at = None
        elif PLAN_DAYS[normalized_plan] is None:
            final_plan = normalized_plan
            expires_at = None
        else:
            start = current_expires if current_expires and current_expires > now else now
            final_plan = normalized_plan
            expires_at = _format_dt(start + timedelta(days=int(PLAN_DAYS[normalized_plan] or 0)))

        conn.execute(
            """
            INSERT INTO offline_access (
                user_id,
                plan,
                status,
                expires_at,
                source,
                last_event_id,
                last_event_type,
                created_at,
                updated_at
            )
            VALUES (?, ?, 'active', ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id)
            DO UPDATE SET
                plan = excluded.plan,
                status = excluded.status,
                expires_at = excluded.expires_at,
                source = excluded.source,
                last_event_id = excluded.last_event_id,
                last_event_type = excluded.last_event_type,
                updated_at = excluded.updated_at
            """,
            (
                uid,
                final_plan,
                expires_at,
                source,
                stored_event_id,
                event_type,
                created_at,
                now_text,
            ),
        )

    access = get_offline_access(uid) or {}
    access["duplicate_event"] = False
    return access


def revoke_offline_access(
    user_id: int | str,
    *,
    event_id: str | None = None,
    event_type: str = "refund",
    reason: str = "cakto",
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    uid = int(user_id)
    init_offline_access_db()
    with _get_conn() as conn:
        stored_event_id, is_new_event = _record_event(
            conn,
            event_id=event_id,
            user_id=uid,
            plan="",
            event_type=event_type,
            action="revoke",
            payload=payload,
        )

        if not is_new_event:
            row = conn.execute(
                """
                SELECT *
                FROM offline_access
                WHERE user_id = ?
                LIMIT 1
                """,
                (uid,),
            ).fetchone()
            access = _row_to_access(row) or {}
            access["duplicate_event"] = True
            return access

        now_text = _utc_now_str()
        current = conn.execute(
            """
            SELECT *
            FROM offline_access
            WHERE user_id = ?
            LIMIT 1
            """,
            (uid,),
        ).fetchone()
        created_at = current["created_at"] if current else now_text
        plan = current["plan"] if current else "ouro"

        conn.execute(
            """
            INSERT INTO offline_access (
                user_id,
                plan,
                status,
                expires_at,
                source,
                last_event_id,
                last_event_type,
                created_at,
                updated_at
            )
            VALUES (?, ?, 'revoked', ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id)
            DO UPDATE SET
                status = excluded.status,
                expires_at = excluded.expires_at,
                source = excluded.source,
                last_event_id = excluded.last_event_id,
                last_event_type = excluded.last_event_type,
                updated_at = excluded.updated_at
            """,
            (
                uid,
                plan,
                now_text,
                reason,
                stored_event_id,
                event_type,
                created_at,
                now_text,
            ),
        )

    access = get_offline_access(uid) or {}
    access["duplicate_event"] = False
    return access
