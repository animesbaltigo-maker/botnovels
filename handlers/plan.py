from __future__ import annotations

import html
from datetime import datetime, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from config import AI_TIMEZONE, BOT_BRAND
from services.cakto_gateway import get_checkout_options
from services.offline_access import PLAN_DAYS, get_offline_access, normalize_plan, plan_label
from utils.gatekeeper import ensure_channel_membership

SUPPORT_BOT_URL = "https://t.me/QGSuporteBot"

PLAN_SHORT_LABELS = {
    "bronze": "Bronze",
    "ouro": "Ouro",
    "diamante": "Diamante",
    "rubi": "Rubi",
    "1m": "Ouro",
    "3m": "3 meses",
    "6m": "6 meses",
    "lifetime": "Rubi",
}


def _timezone() -> ZoneInfo:
    try:
        return ZoneInfo(AI_TIMEZONE or "America/Cuiaba")
    except ZoneInfoNotFoundError:
        return ZoneInfo("UTC")


def _parse_utc_datetime(value: str | None) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    candidates = [raw, raw.replace("T", " ").replace("Z", "+00:00"), raw.replace("Z", "+00:00")]
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


def _format_local_datetime(value: str | None) -> str:
    parsed = _parse_utc_datetime(value)
    if parsed is None:
        return "nao definido"
    return parsed.astimezone(_timezone()).strftime("%d/%m/%Y as %H:%M")


def _duration_label(plan: str | None) -> str:
    plan_key = normalize_plan(plan)
    days = PLAN_DAYS.get(plan_key)
    if days is None:
        return "vitalicio"
    if int(days) == 1:
        return "1 dia"
    return f"{int(days)} dias"


def _remaining_label(expires_at: str | None) -> str:
    expires = _parse_utc_datetime(expires_at)
    if expires is None:
        return "nao expira"
    delta = expires - datetime.now(timezone.utc)
    total_seconds = int(delta.total_seconds())
    if total_seconds <= 0:
        return "expirado"
    days, remainder = divmod(total_seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes = max(1, remainder // 60)
    if days:
        return f"{days} dia{'s' if days != 1 else ''} e {hours}h"
    if hours:
        return f"{hours}h e {minutes}min"
    return f"{minutes}min"


def _status_label(access: dict | None) -> str:
    if not access:
        return "sem plano"
    if access.get("is_active"):
        return "ativo"
    status = str(access.get("status") or "").strip()
    if status == "expired":
        return "expirado"
    if status == "revoked":
        return "bloqueado"
    return status or "inativo"


def _renew_option(user_id: int, plan: str | None) -> dict[str, str] | None:
    plan_key = normalize_plan(plan)
    if not plan_key:
        return None
    for option in get_checkout_options(user_id):
        if option.get("plan") == plan_key:
            return option
    return None


def _plan_keyboard(user_id: int, access: dict | None) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    plan_key = normalize_plan((access or {}).get("plan") or "")
    renew = _renew_option(user_id, plan_key)
    if renew:
        short_label = PLAN_SHORT_LABELS.get(plan_key, plan_label(plan_key))
        rows.append([InlineKeyboardButton(f"Renovar {short_label}", url=renew["url"])])
    else:
        for option in get_checkout_options(user_id):
            rows.append([InlineKeyboardButton(option["label"], url=option["url"])])
    rows.append([InlineKeyboardButton("Suporte", url=SUPPORT_BOT_URL)])
    return InlineKeyboardMarkup(rows)


def _plan_text(user_id: int, access: dict | None) -> str:
    brand = html.escape(BOT_BRAND or "Novels Baltigo")
    status = _status_label(access)
    if not access:
        return (
            "<b>Meu plano offline</b>\n\n"
            f"» <b>Status:</b> <i>{status}</i>\n"
            f"» <b>ID:</b> <code>{user_id}</code>\n\n"
            f"Voce ainda nao tem um plano ativo no <b>{brand}</b>.\n"
            "Escolha um plano abaixo para liberar PDF e EPUB."
        )

    plan = access.get("plan") or ""
    expires_at = access.get("expires_at") or ""
    expires_text = "nao expira" if access.get("is_lifetime") else _format_local_datetime(expires_at)
    remaining = "nao expira" if access.get("is_lifetime") else _remaining_label(expires_at)
    return (
        "<b>Meu plano offline</b>\n\n"
        f"» <b>Status:</b> <i>{html.escape(status)}</i>\n"
        f"» <b>Plano:</b> <i>{html.escape(plan_label(plan))}</i>\n"
        f"» <b>Duracao:</b> <i>{html.escape(_duration_label(plan))}</i>\n"
        f"» <b>Valido ate:</b> <i>{html.escape(expires_text)}</i>\n"
        f"» <b>Tempo restante:</b> <i>{html.escape(remaining)}</i>\n"
        f"» <b>ID:</b> <code>{user_id}</code>\n\n"
        "Use os botoes abaixo para renovar ou falar com o suporte."
    )


async def plano(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await ensure_channel_membership(update, context):
        return

    user = update.effective_user
    message = update.effective_message
    if not user or not message:
        return

    access = get_offline_access(user.id)
    await message.reply_text(
        _plan_text(user.id, access),
        parse_mode="HTML",
        reply_markup=_plan_keyboard(user.id, access),
        disable_web_page_preview=True,
    )
