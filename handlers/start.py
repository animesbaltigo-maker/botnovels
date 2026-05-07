import asyncio
import html
import re
import time

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, WebAppInfo
from telegram.error import TelegramError
from telegram.ext import ContextTypes

from config import BOT_BRAND, BOT_USERNAME, PROMO_BANNER_URL, WEBAPP_BASE_URL
from core.background import fire_and_forget_sync, run_sync
from handlers.novel_callbacks import send_chapter_panel, send_novel_panel
from services.centralnovel_client import get_cached_home_snapshot, schedule_warm_catalog_cache
from services.metrics import mark_user_seen
from services.referral_db import (
    create_referral,
    register_interaction,
    register_referral_click,
    upsert_user,
)
from services.user_registry import register_user
from utils.gatekeeper import ensure_channel_membership

START_COOLDOWN = 1.0
START_DEEP_LINK_TTL = 8.0
START_OPEN_TIMEOUT = 28.0

_START_USER_LOCKS: dict[int, asyncio.Lock] = {}
_START_INFLIGHT: dict[str, float] = {}


def _extract_title_id(arg: str) -> str:
    for prefix in ("novel_", "title_"):
        if arg.startswith(prefix):
            return arg[len(prefix) :]
    return ""


def _extract_chapter_id(arg: str) -> str:
    for prefix in ("ch_", "cap_", "read_"):
        if arg.startswith(prefix):
            payload = arg[len(prefix) :]
            match = re.match(r"^([a-f0-9]{8,40})(?:_([a-f0-9]{8,40}))?$", payload, flags=re.IGNORECASE)
            if match:
                return match.group(1)
            return payload
    return ""


def _extract_chapter_title_hint(arg: str) -> str:
    for prefix in ("ch_", "cap_", "read_"):
        if arg.startswith(prefix):
            payload = arg[len(prefix) :]
            match = re.match(r"^([a-f0-9]{8,40})_([a-f0-9]{8,40})$", payload, flags=re.IGNORECASE)
            if match:
                return match.group(2)
    return ""


def _referral_feedback(reason: str) -> str:
    if reason == "self":
        return "Seu proprio link de convite nao conta."
    if reason == "already_same":
        return "Esse convite ja estava associado ao mesmo link."
    if reason == "exists":
        return "Voce ja entrou no bot por outro convite."
    return "Convite registrado com sucesso."


def _safe_user_lock(user_id: int) -> asyncio.Lock:
    lock = _START_USER_LOCKS.get(user_id)
    if lock is None:
        lock = asyncio.Lock()
        _START_USER_LOCKS[user_id] = lock
    return lock


def _now() -> float:
    return time.monotonic()


def _deep_link_key(user_id: int, payload: str) -> str:
    return f"{user_id}:{payload}"


def _is_inflight(user_id: int, payload: str) -> bool:
    key = _deep_link_key(user_id, payload)
    item = _START_INFLIGHT.get(key)
    if not item:
        return False
    if _now() - item > START_DEEP_LINK_TTL:
        _START_INFLIGHT.pop(key, None)
        return False
    return True


def _set_inflight(user_id: int, payload: str) -> None:
    _START_INFLIGHT[_deep_link_key(user_id, payload)] = _now()


def _clear_inflight(user_id: int, payload: str) -> None:
    _START_INFLIGHT.pop(_deep_link_key(user_id, payload), None)


def _start_last_key(user_id: int) -> str:
    return f"novel_start_last:{user_id}"


def _start_last_payload_key(user_id: int) -> str:
    return f"novel_start_last_payload:{user_id}"


def _is_start_cooldown(context: ContextTypes.DEFAULT_TYPE, user_id: int, payload: str) -> bool:
    now = _now()
    last_ts = context.user_data.get(_start_last_key(user_id), 0.0)
    last_payload = context.user_data.get(_start_last_payload_key(user_id), "")

    if payload and payload == last_payload and (now - last_ts) < START_COOLDOWN:
        return True

    context.user_data[_start_last_key(user_id)] = now
    context.user_data[_start_last_payload_key(user_id)] = payload
    return False


def _queue_user_touch(user) -> None:
    username = user.username or ""
    first_name = user.first_name or ""

    def _runner():
        upsert_user(user.id, username, first_name)
        register_user(user.id)
        register_interaction(user.id)
        mark_user_seen(user.id, user.username or user.first_name or "")

    fire_and_forget_sync(_runner)


async def _safe_delete_message(message) -> None:
    if not message:
        return
    try:
        await message.delete()
    except TelegramError:
        pass
    except Exception:
        pass


async def _safe_edit_message(message, text: str) -> None:
    if not message:
        return
    try:
        await message.edit_text(text, parse_mode="HTML")
    except TelegramError:
        pass
    except Exception:
        pass


async def _send_start_loading(message, *, kind: str):
    label = "capitulo" if kind == "chapter" else "obra"
    try:
        return await message.reply_text(
            f"<b>Abrindo {label}...</b>\n\nUm instante, estou preparando a leitura.",
            parse_mode="HTML",
        )
    except Exception:
        return None


def _miniapp_url(*, title_id: str = "", chapter_id: str = "", route: str = "home", user_id: int | None = None) -> str:
    base = (WEBAPP_BASE_URL or "").rstrip("/")
    if not base:
        return ""
    params = [f"route={route or 'home'}", "source=start"]
    if title_id:
        params.append(f"title_id={title_id}")
    if chapter_id:
        params.append(f"chapter_id={chapter_id}")
    if user_id:
        params.append(f"user_id={int(user_id)}")
    return f"{base}/miniapp/index.html?{'&'.join(params)}"


async def _handle_referral(arg: str, user, message) -> None:
    try:
        referrer_id = int(arg.split("_", 1)[1])
    except Exception:
        return

    await run_sync(register_referral_click, referrer_id, user.id)
    ok, reason = await run_sync(create_referral, referrer_id, user.id)

    text = _referral_feedback(reason)
    if ok:
        text = "Seu convite foi registrado. Continue usando o bot que a indicacao entra em analise."

    await message.reply_text(text)


async def _send_welcome(message, first_name: str) -> None:
    snapshot = get_cached_home_snapshot(limit=4)
    featured = snapshot.get("featured") or []
    schedule_warm_catalog_cache()

    user_id = getattr(getattr(message, "from_user", None), "id", None)
    keyboard_rows: list[list[InlineKeyboardButton]] = []
    webapp_home = _miniapp_url(route="home", user_id=user_id)
    if webapp_home:
        keyboard_rows.append([InlineKeyboardButton("Abrir biblioteca", web_app=WebAppInfo(url=webapp_home))])

    keyboard_rows.append([InlineKeyboardButton("Buscar novel aqui", switch_inline_query_current_chat="")])
    if BOT_USERNAME:
        keyboard_rows.append(
            [
                InlineKeyboardButton("➕ Adicionar ao grupo", url=f"https://t.me/{BOT_USERNAME}?startgroup=true"),
            ]
        )


    for item in featured[:4]:
        title_id = str(item.get("title_id") or "").strip()
        title = str(item.get("title") or item.get("display_title") or "Novel").strip()
        url = _miniapp_url(title_id=title_id, route="detail", user_id=user_id)
        if title_id and url:
            keyboard_rows.append([InlineKeyboardButton(f"Ler {title[:34]}", web_app=WebAppInfo(url=url))])

    text = (
        f"📖 <b>Bem-vindo ao {BOT_BRAND}!</b>\n\n"
        f"{html.escape(first_name)}, aqui voce encontra web novels e light novels direto no Telegram.\n\n"
        "✨ <b>Como funciona:</b>\n"
        "• Use <code>/novel nome da obra</code>\n"
        "• Abra a obra\n"
        "• Escolha um capitulo\n"
        "• Leia pelo WebApp\n"
        "• Use /plano para liberar PDF e EPUB\n\n"
    )

    text = (
        f"<b>{html.escape(BOT_BRAND)}</b>\n\n"
        f"{html.escape(first_name or 'leitor')}, escolha uma novel, continue seu progresso e leia pelo WebApp "
        "com fonte ajustavel, tema claro/escuro e historico.\n\n"
        "<b>Como usar</b>\n"
        "- Abra a biblioteca pelo botao abaixo\n"
        "- Ou envie <code>/novel nome da obra</code>\n"
        "- Toque em uma obra e escolha o capitulo\n"
        "- Use <code>/plano</code> para liberar PDF e EPUB\n"
    )

    if keyboard_rows:
        keyboard = InlineKeyboardMarkup(keyboard_rows)
    else:
        keyboard = None

    try:
        await message.reply_photo(
            photo=PROMO_BANNER_URL,
            caption=text,
            parse_mode="HTML",
            reply_markup=keyboard,
        )
    except Exception:
        await message.reply_text(
            text,
            parse_mode="HTML",
            reply_markup=keyboard,
            disable_web_page_preview=True,
        )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.effective_message

    if not user or not message:
        return

    _queue_user_touch(user)

    if not await ensure_channel_membership(update, context):
        return

    arg = context.args[0].strip() if context.args else ""

    if arg.startswith("ref_"):
        await _handle_referral(arg, user, message)
        arg = ""

    if arg and _is_start_cooldown(context, user.id, arg):
        await message.reply_text("⏳ Aguarde um instante antes de repetir essa acao.")
        return

    if arg and _is_inflight(user.id, arg):
        await message.reply_text("⏳ Essa solicitacao ja esta sendo processada.")
        return

    user_lock = _safe_user_lock(user.id)
    async with user_lock:
        if arg and _is_inflight(user.id, arg):
            await message.reply_text("⏳ Essa solicitacao ja esta sendo processada.")
            return

        if arg:
            _set_inflight(user.id, arg)

        try:
            title_id = _extract_title_id(arg)
            if title_id:
                loading_msg = await _send_start_loading(message, kind="title")
                try:
                    await asyncio.wait_for(
                        send_novel_panel(message, context, title_id, user.id, edit=False),
                        timeout=START_OPEN_TIMEOUT,
                    )
                    await _safe_delete_message(loading_msg)
                except asyncio.TimeoutError:
                    await _safe_edit_message(loading_msg, "Demorou mais que o normal. Tente abrir de novo em alguns segundos.")
                except Exception as error:
                    print("ERRO START NOVEL:", repr(error))
                    await _safe_edit_message(loading_msg, "Nao consegui abrir essa obra agora.")
                return

            chapter_id = _extract_chapter_id(arg)
            if chapter_id:
                loading_msg = await _send_start_loading(message, kind="chapter")
                try:
                    await asyncio.wait_for(
                        send_chapter_panel(
                            message,
                            context,
                            chapter_id,
                            user.id,
                            edit=False,
                            title_hint=_extract_chapter_title_hint(arg),
                        ),
                        timeout=START_OPEN_TIMEOUT,
                    )
                    await _safe_delete_message(loading_msg)
                except asyncio.TimeoutError:
                    await _safe_edit_message(loading_msg, "Demorou mais que o normal. Tente abrir de novo em alguns segundos.")
                except Exception as error:
                    print("ERRO START CAPITULO:", repr(error))
                    await _safe_edit_message(loading_msg, "Nao consegui abrir esse capitulo agora.")
                return

            await _send_welcome(message, user.first_name or "leitor")
        finally:
            if arg:
                _clear_inflight(user.id, arg)
            await _safe_delete_message(message)
