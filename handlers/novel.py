import asyncio
import html
import re
import secrets
import time

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import TelegramError
from telegram.ext import ContextTypes

from config import PROMO_BANNER_URL, SEARCH_LIMIT
from core.background import fire_and_forget_sync
from services.centralnovel_client import (
    get_cached_search_novels,
    get_search_fallback_novels,
    prefetch_novel_bundles,
    schedule_warm_catalog_cache,
    search_novels,
)
from services.metrics import log_event, mark_user_seen
from utils.gatekeeper import ensure_channel_membership

RESULTS_PER_PAGE = 8
SEARCH_SESSION_TTL = 2 * 60 * 60
SEARCH_COOLDOWN = 1.2
SEARCH_INFLIGHT_TTL = 12.0
SEARCH_TIMEOUT = 12.0

_SEARCH_USER_LOCKS: dict[int, asyncio.Lock] = {}
_SEARCH_INFLIGHT: dict[str, float] = {}


def _now() -> float:
    return time.monotonic()


def _normalize_query(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "").strip())


def _user_lock(user_id: int) -> asyncio.Lock:
    lock = _SEARCH_USER_LOCKS.get(user_id)
    if lock is None:
        lock = asyncio.Lock()
        _SEARCH_USER_LOCKS[user_id] = lock
    return lock


def _search_last_key(user_id: int) -> str:
    return f"novel_search_last:{user_id}"


def _search_last_query_key(user_id: int) -> str:
    return f"novel_search_last_query:{user_id}"


def _search_inflight_key(user_id: int, query: str) -> str:
    return f"{user_id}:{query.lower()}"


def _is_search_cooldown(context: ContextTypes.DEFAULT_TYPE, user_id: int, query: str) -> bool:
    now = _now()
    last_ts = context.user_data.get(_search_last_key(user_id), 0.0)
    last_query = context.user_data.get(_search_last_query_key(user_id), "")

    if query and last_query == query and (now - last_ts) < SEARCH_COOLDOWN:
        return True

    context.user_data[_search_last_key(user_id)] = now
    context.user_data[_search_last_query_key(user_id)] = query
    return False


def _is_inflight(user_id: int, query: str) -> bool:
    key = _search_inflight_key(user_id, query)
    item = _SEARCH_INFLIGHT.get(key)
    if not item:
        return False
    if _now() - item > SEARCH_INFLIGHT_TTL:
        _SEARCH_INFLIGHT.pop(key, None)
        return False
    return True


def _set_inflight(user_id: int, query: str) -> None:
    _SEARCH_INFLIGHT[_search_inflight_key(user_id, query)] = _now()


def _clear_inflight(user_id: int, query: str) -> None:
    _SEARCH_INFLIGHT.pop(_search_inflight_key(user_id, query), None)


def _search_session_key(token: str) -> str:
    return f"novel_search_session:{token}"


def _clean_button_title(title: str) -> str:
    title = _normalize_query(title)
    title = re.sub(r"\(\s*\)", "", title)
    title = re.sub(r"\s{2,}", " ", title).strip(" -|")
    if len(title) <= 42:
        return title or "Sem titulo"
    return title[:39].rstrip() + "..."


def _item_button_title(item: dict, duplicate_counts: dict[str, int]) -> str:
    base_title = _normalize_query(item.get("display_title") or item.get("title") or "Novel")
    normalized_base = base_title.lower()

    if duplicate_counts.get(normalized_base, 0) > 1:
        latest = _normalize_query(str(item.get("latest_chapter") or ""))
        status = _normalize_query(str(item.get("status") or ""))
        if latest:
            base_title = f"{base_title} · Cap. {latest}"
        elif status:
            base_title = f"{base_title} · {status}"
        else:
            title_id = str(item.get("title_id") or "").strip()
            if title_id:
                base_title = f"{base_title} · {title_id[-4:].upper()}"

    return _clean_button_title(base_title)


def _build_search_text(query: str, page: int, total: int, *, partial: bool = False) -> str:
    total_pages = max(1, ((total - 1) // RESULTS_PER_PAGE) + 1)
    prefix = ""
    if partial:
        prefix = "⚠️ <b>Fonte lenta no momento.</b>\nMostrando resultados do cache local.\n\n"
    return (
        f"{prefix}"
        "📖 <b>Resultado da busca</b>\n\n"
        f"📚 <b>Pesquisa:</b> {html.escape(query)}\n"
        f"📄 <b>Pagina:</b> {page}/{total_pages}\n"
        f"📦 <b>Resultados:</b> {total}\n\n"
        "Toque em uma obra abaixo para abrir."
    )


def store_search_session(
    context: ContextTypes.DEFAULT_TYPE,
    query: str,
    results: list[dict],
    *,
    partial: bool = False,
) -> str:
    token = secrets.token_hex(4)
    context.user_data[_search_session_key(token)] = {
        "query": query,
        "results": results,
        "partial": partial,
        "created_at": time.time(),
    }
    return token


def get_search_session(context: ContextTypes.DEFAULT_TYPE, token: str) -> dict | None:
    payload = context.user_data.get(_search_session_key(token))
    if not isinstance(payload, dict):
        return None
    if time.time() - float(payload.get("created_at", 0.0)) > SEARCH_SESSION_TTL:
        context.user_data.pop(_search_session_key(token), None)
        return None
    return payload


def build_search_keyboard(results: list[dict], page: int, token: str) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    total = len(results)
    start = (page - 1) * RESULTS_PER_PAGE
    end = min(start + RESULTS_PER_PAGE, total)
    duplicate_counts: dict[str, int] = {}

    for item in results:
        key = _normalize_query(item.get("display_title") or item.get("title") or "Novel").lower()
        duplicate_counts[key] = duplicate_counts.get(key, 0) + 1

    for index, item in enumerate(results[start:end], start=start + 1):
        title = _item_button_title(item, duplicate_counts)
        title_id = item.get("title_id") or ""
        if not title_id:
            continue
        rows.append([InlineKeyboardButton(f"📘 {index}. {title}", callback_data=f"nv|title|{title_id}")])

    nav = []
    total_pages = max(1, ((total - 1) // RESULTS_PER_PAGE) + 1)
    if page > 1:
        nav.append(InlineKeyboardButton("⬅️ Anterior", callback_data=f"nv|sp|{token}|{page - 1}"))
    if page < total_pages:
        nav.append(InlineKeyboardButton("Proxima ➡️", callback_data=f"nv|sp|{token}|{page + 1}"))
    if nav:
        rows.append(nav)

    return InlineKeyboardMarkup(rows)


def render_search_page(context: ContextTypes.DEFAULT_TYPE, token: str, page: int) -> dict | None:
    session = get_search_session(context, token)
    if not session:
        return None

    results = session.get("results") or []
    query = session.get("query") or ""
    partial = bool(session.get("partial"))
    total = len(results)
    if total <= 0:
        return None

    total_pages = max(1, ((total - 1) // RESULTS_PER_PAGE) + 1)
    page = max(1, min(int(page), total_pages))
    return {
        "photo": PROMO_BANNER_URL,
        "text": _build_search_text(query, page, total, partial=partial),
        "keyboard": build_search_keyboard(results, page, token),
    }


async def _safe_delete_message(message) -> None:
    if not message:
        return
    try:
        await message.delete()
    except TelegramError:
        pass
    except Exception:
        pass


async def _safe_edit_loading(message, text: str) -> bool:
    if not message:
        return False
    try:
        await message.edit_text(text, parse_mode="HTML")
        return True
    except Exception:
        return False


async def send_search_page(message, rendered: dict) -> None:
    try:
        await message.reply_photo(
            photo=rendered["photo"],
            caption=rendered["text"],
            parse_mode="HTML",
            reply_markup=rendered["keyboard"],
        )
    except Exception:
        await message.reply_text(
            rendered["text"],
            parse_mode="HTML",
            reply_markup=rendered["keyboard"],
            disable_web_page_preview=True,
        )


async def edit_search_page(query, rendered: dict) -> None:
    try:
        await query.edit_message_caption(
            caption=rendered["text"],
            parse_mode="HTML",
            reply_markup=rendered["keyboard"],
        )
        return
    except Exception:
        pass

    try:
        await query.edit_message_text(
            rendered["text"],
            parse_mode="HTML",
            reply_markup=rendered["keyboard"],
            disable_web_page_preview=True,
        )
    except Exception:
        await query.message.reply_text(
            rendered["text"],
            parse_mode="HTML",
            reply_markup=rendered["keyboard"],
            disable_web_page_preview=True,
        )


async def novel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await ensure_channel_membership(update, context):
        return

    message = update.effective_message
    chat = update.effective_chat
    user = update.effective_user

    if not message or not chat or not user:
        return

    fire_and_forget_sync(mark_user_seen, user.id, user.username or user.first_name or "")

    if chat.type != "private":
        await message.reply_text(
            "🔒 <b>Esse comando so funciona no privado.</b>\n\n"
            "Me chama no PV e envie:\n"
            "<code>/novel nome da obra</code>",
            parse_mode="HTML",
        )
        return

    query = _normalize_query(" ".join(context.args or []))
    if not query:
        await message.reply_text(
            "📖 <b>Como buscar uma novel</b>\n\n"
            "Envie no formato:\n"
            "<code>/novel nome da obra</code>\n\n"
            "Exemplos:\n"
            "• <code>/novel shadow slave</code>\n"
            "• <code>/novel supreme magus</code>\n"
            "• <code>/novel lord of mysteries</code>",
            parse_mode="HTML",
        )
        return

    if len(query) < 2:
        await message.reply_text(
            "⚠️ <b>Digite pelo menos 2 caracteres para buscar.</b>",
            parse_mode="HTML",
        )
        return

    if _is_search_cooldown(context, user.id, query):
        await message.reply_text(
            "⏳ <b>Aguarde um instante antes de repetir essa busca.</b>",
            parse_mode="HTML",
        )
        return

    if _is_inflight(user.id, query):
        await message.reply_text(
            "⏳ <b>Essa busca ja esta sendo processada.</b>",
            parse_mode="HTML",
        )
        return

    lock = _user_lock(user.id)
    async with lock:
        if _is_inflight(user.id, query):
            await message.reply_text(
                "⏳ <b>Essa busca ja esta sendo processada.</b>",
                parse_mode="HTML",
            )
            return

        _set_inflight(user.id, query)
        schedule_warm_catalog_cache()
        cached_results = get_cached_search_novels(query, limit=SEARCH_LIMIT)
        loading = None

        try:
            partial = False

            if cached_results is not None:
                results = cached_results
            else:
                loading = await message.reply_text(
                    "📖 <b>Buscando novels...</b>\nAguarde um instante.",
                    parse_mode="HTML",
                )
                try:
                    results = await asyncio.wait_for(search_novels(query, limit=SEARCH_LIMIT), timeout=SEARCH_TIMEOUT)
                except asyncio.TimeoutError:
                    results = get_search_fallback_novels(query, limit=SEARCH_LIMIT)
                    partial = bool(results)
                    if not results:
                        raise

            fire_and_forget_sync(
                log_event,
                event_type="search",
                user_id=user.id,
                username=user.username or user.first_name or "",
                query_text=query,
                result_count=len(results),
            )

            if not results:
                fire_and_forget_sync(
                    log_event,
                    event_type="search_no_result",
                    user_id=user.id,
                    username=user.username or user.first_name or "",
                    query_text=query,
                    result_count=0,
                )
                edited = await _safe_edit_loading(
                    loading,
                    "❌ <b>Nenhuma novel encontrada.</b>\n\nTente outro nome ou uma variacao do titulo.",
                )
                if not edited:
                    await message.reply_text(
                        "❌ <b>Nenhuma novel encontrada.</b>\n\nTente outro nome ou uma variacao do titulo.",
                        parse_mode="HTML",
                    )
                return

            token = store_search_session(context, query, results, partial=partial)
            rendered = render_search_page(context, token, 1)
            if not rendered:
                edited = await _safe_edit_loading(
                    loading,
                    "❌ <b>Essa busca expirou.</b>\n\nFaz outra busca pra continuar.",
                )
                if not edited:
                    await message.reply_text(
                        "❌ <b>Essa busca expirou.</b>\n\nFaz outra busca pra continuar.",
                        parse_mode="HTML",
                    )
                return

            prefetch_novel_bundles([item.get("title_id") or "" for item in results[:3]], limit=3)
            await _safe_delete_message(loading)
            await send_search_page(message, rendered)
        except asyncio.TimeoutError:
            fallback_results = get_search_fallback_novels(query, limit=SEARCH_LIMIT)
            if fallback_results:
                token = store_search_session(context, query, fallback_results, partial=True)
                rendered = render_search_page(context, token, 1)
                if rendered:
                    await _safe_delete_message(loading)
                    await send_search_page(message, rendered)
                    return

            edited = await _safe_edit_loading(
                loading,
                "⏳ <b>A busca demorou demais.</b>\n\nTente novamente em instantes.",
            )
            if not edited:
                await message.reply_text(
                    "⏳ <b>A busca demorou demais.</b>\n\nTente novamente em instantes.",
                    parse_mode="HTML",
                )
        except Exception as error:
            print("ERRO BUSCA NOVEL:", repr(error))
            edited = await _safe_edit_loading(
                loading,
                "❌ <b>Nao consegui concluir a busca agora.</b>\n\nTente novamente em instantes.",
            )
            if not edited:
                await message.reply_text(
                    "❌ <b>Nao consegui concluir a busca agora.</b>\n\nTente novamente em instantes.",
                    parse_mode="HTML",
                )
        finally:
            _clear_inflight(user.id, query)
