import asyncio
import html
import time
from urllib.parse import urlencode

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto, Update, WebAppInfo
from telegram.ext import ContextTypes

from config import (
    BOT_BRAND,
    CHAPTERS_PER_PAGE,
    DISTRIBUTION_TAG,
    PDF_BULK_ALLOWED_IDS,
    PDF_BULK_SUBSCRIBE_URL,
    PROMO_BANNER_URL,
    WEBAPP_BASE_URL,
)
from core.background import fire_and_forget, fire_and_forget_sync, run_sync
from core.pdf_queue import EpubJob, PdfJob, enqueue_epub_job, enqueue_pdf_job
from handlers.novel import edit_search_page, render_search_page
from services.centralnovel_client import (
    get_cached_chapter_payload,
    get_cached_novel_bundle,
    get_chapter_payload,
    get_novel_bundle,
    prefetch_chapter_payloads,
    prefetch_novel_bundles,
)
from services.metrics import (
    get_last_read_entry,
    get_read_chapter_ids,
    log_event,
    mark_chapter_read,
)
from services.cakto_gateway import get_checkout_options
from services.offline_access import is_offline_user_allowed
from services.telegraph_service import get_cached_chapter_page_url, get_or_create_chapter_page

CALLBACK_COOLDOWN = 0.8
TELEGRAPH_INLINE_WAIT = 1.4
SUPPORT_BOT_URL = "https://t.me/QGSuporteBot"
_OFFLINE_ALLOWED_USERS = set(PDF_BULK_ALLOWED_IDS)

_USER_CALLBACK_LOCKS: dict[int, asyncio.Lock] = {}
_MESSAGE_EDIT_LOCKS: dict[str, asyncio.Lock] = {}
_MESSAGE_INFLIGHT_ACTIONS: dict[str, str] = {}
_MESSAGE_PANEL_STATE: dict[str, tuple[str, str]] = {}


def _miniapp_url(
    *,
    title_id: str = "",
    chapter_id: str = "",
    route: str = "",
    page: str = "",
    source: str = "bot",
) -> str:
    if not WEBAPP_BASE_URL:
        return ""

    params: dict[str, str] = {"source": source}
    if title_id:
        tid = str(title_id).strip()
        params["title_id"] = tid
        params["novel_id"] = tid
        params["id"] = tid
    if chapter_id:
        cid = str(chapter_id).strip()
        params["chapter_id"] = cid
        params["read"] = cid
        params["cap"] = cid
    if page:
        params["page"] = str(page).strip()

    resolved_route = route.strip() if route else ""
    if not resolved_route:
        if chapter_id:
            resolved_route = "reader"
        elif title_id and page == "chapters":
            resolved_route = "chapters"
        elif title_id:
            resolved_route = "detail"
        else:
            resolved_route = "home"
    params["route"] = resolved_route
    return f"{WEBAPP_BASE_URL}/miniapp/index.html?{urlencode(params)}"


def _reader_button(label: str, *, title_id: str = "", chapter_id: str = "", route: str = "", page: str = "", fallback: str = ""):
    url = _miniapp_url(title_id=title_id, chapter_id=chapter_id, route=route, page=page)
    if url:
        return InlineKeyboardButton(label, web_app=WebAppInfo(url=url))
    return InlineKeyboardButton(label, callback_data=fallback or "nv|noop")


def can_use_offline(user_id: int | str | None) -> bool:
    if user_id is None:
        return False
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return False
    return uid in _OFFLINE_ALLOWED_USERS or is_offline_user_allowed(uid)


def _now() -> float:
    return time.monotonic()


def _callback_last_key(user_id: int) -> str:
    return f"novel_callback_last:{user_id}"


def _callback_data_last_key(user_id: int) -> str:
    return f"novel_callback_data_last:{user_id}"


def _user_lock(user_id: int) -> asyncio.Lock:
    lock = _USER_CALLBACK_LOCKS.get(user_id)
    if lock is None:
        lock = asyncio.Lock()
        _USER_CALLBACK_LOCKS[user_id] = lock
    return lock


def _message_lock(chat_id: int, message_id: int) -> asyncio.Lock:
    key = f"{chat_id}:{message_id}"
    lock = _MESSAGE_EDIT_LOCKS.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _MESSAGE_EDIT_LOCKS[key] = lock
    return lock


def _action_signature(data: str) -> str:
    parts = (data or "").split("|")
    if len(parts) >= 3:
        return "|".join(parts[:3])
    return data or ""


def _message_action_key(chat_id: int, message_id: int) -> str:
    return f"{chat_id}:{message_id}"


def _get_inflight_action(chat_id: int, message_id: int) -> str:
    return _MESSAGE_INFLIGHT_ACTIONS.get(_message_action_key(chat_id, message_id), "")


def _set_inflight_action(chat_id: int, message_id: int, action: str) -> None:
    _MESSAGE_INFLIGHT_ACTIONS[_message_action_key(chat_id, message_id)] = action


def _clear_inflight_action(chat_id: int, message_id: int) -> None:
    _MESSAGE_INFLIGHT_ACTIONS.pop(_message_action_key(chat_id, message_id), None)


def _set_panel_state(chat_id: int, message_id: int, kind: str, ref: str) -> None:
    _MESSAGE_PANEL_STATE[_message_action_key(chat_id, message_id)] = (kind, ref)


def _get_panel_state(chat_id: int, message_id: int) -> tuple[str, str]:
    return _MESSAGE_PANEL_STATE.get(_message_action_key(chat_id, message_id), ("", ""))


def _is_callback_cooldown(context: ContextTypes.DEFAULT_TYPE, user_id: int, data: str) -> bool:
    last_ts = context.user_data.get(_callback_last_key(user_id), 0.0)
    last_data = context.user_data.get(_callback_data_last_key(user_id), "")
    now = _now()

    if data and last_data == data and (now - last_ts) < CALLBACK_COOLDOWN:
        return True

    context.user_data[_callback_last_key(user_id)] = now
    context.user_data[_callback_data_last_key(user_id)] = data
    return False


async def _safe_answer_query(query, text: str | None = None, show_alert: bool = False) -> None:
    try:
        if text is None:
            await query.answer()
        else:
            await query.answer(text, show_alert=show_alert)
    except Exception:
        pass


def _pick_bundle_image(bundle: dict | None) -> str:
    if bundle:
        image = (
            bundle.get("cover_url")
            or bundle.get("banner_url")
            or bundle.get("background_url")
            or ""
        ).strip()
        if image:
            return image
    return PROMO_BANNER_URL


def _truncate(text: str, limit: int = 360) -> str:
    value = (text or "").strip()
    if len(value) <= limit:
        return value
    return value[: limit - 3].rstrip() + "..."


def _ordered_chapters(bundle: dict) -> list[dict]:
    chapters = list(bundle.get("chapters") or [])
    chapters.reverse()
    return chapters


def _title_text(bundle: dict, last_read: dict | None = None) -> str:
    title = html.escape(bundle.get("title") or "Novel")
    status = html.escape(bundle.get("status") or "N/A")
    work_type = html.escape(bundle.get("type") or "Novel")
    author = html.escape(bundle.get("author") or "N/A")
    chapters = html.escape(str(bundle.get("total_chapters") or "?"))
    genres = bundle.get("genres") or []
    genres_text = html.escape(", ".join(str(item) for item in genres[:5])) if genres else "N/A"
    description = html.escape(
        _truncate(bundle.get("description") or "Sem sinopse disponivel no momento.", 420)
    )

    lines = [
        f"📖 <b>{title}</b>",
        "",
        f"» <b>Status:</b> <i>{status}</i>",
        f"» <b>Tipo:</b> <i>{work_type}</i>",
        f"» <b>Autor:</b> <i>{author}</i>",
        f"» <b>Capitulos:</b> <i>{chapters}</i>",
        f"» <b>Generos:</b> <i>{genres_text}</i>",
    ]
    if last_read and last_read.get("chapter_number"):
        lines.append(f"» <b>Continuar de:</b> <i>Capitulo {html.escape(last_read['chapter_number'])}</i>")

    lines.extend(["", f"💬 <i>{description}</i>", "", "✨ <i>Escolha abaixo como quer continuar.</i>"])
    return "\n".join(lines)


def _title_keyboard(bundle: dict, last_read: dict | None = None) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    title_id = bundle.get("title_id") or ""
    first_chapter = bundle.get("first_chapter") or {}
    latest_chapter = bundle.get("latest_chapter") or {}

    primary_row: list[InlineKeyboardButton] = []
    if last_read and last_read.get("chapter_id"):
        primary_row.append(
            _reader_button(
                "⏱ Continuar",
                title_id=title_id,
                chapter_id=last_read["chapter_id"],
                route="reader",
                fallback=f"nv|read|{last_read['chapter_id']}|{title_id}",
            )
        )
    if first_chapter.get("chapter_id"):
        primary_row.append(
            _reader_button(
                "📖 Comecar",
                title_id=title_id,
                chapter_id=first_chapter["chapter_id"],
                route="reader",
                fallback=f"nv|read|{first_chapter['chapter_id']}|{title_id}",
            )
        )
    if primary_row:
        rows.append(primary_row[:2])

    if latest_chapter.get("chapter_id") and latest_chapter.get("chapter_id") != first_chapter.get("chapter_id"):
        rows.append(
            [
                _reader_button(
                    "🆕 Ultimo capitulo",
                    title_id=title_id,
                    chapter_id=latest_chapter["chapter_id"],
                    route="reader",
                    fallback=f"nv|read|{latest_chapter['chapter_id']}|{title_id}",
                )
            ]
        )

    rows.append(
        [
            _reader_button(
                "📚 Lista de capitulos",
                title_id=title_id,
                route="detail",
                page="chapters",
                fallback=f"nv|chap|{title_id}|1",
            )
        ]
    )
    rows.append([InlineKeyboardButton("📥 PDF / EPUB offline", callback_data=f"nv|offline|{title_id}")])
    return InlineKeyboardMarkup(rows)


def _chapter_list_text(bundle: dict, page: int, total_items: int) -> str:
    total_pages = max(1, ((total_items - 1) // CHAPTERS_PER_PAGE) + 1)
    return (
        f"📚 <b>{html.escape(bundle.get('title') or 'Novel')}</b>\n\n"
        f"» <b>Pagina:</b> <i>{page}/{total_pages}</i>\n"
        f"» <b>Capitulos disponiveis:</b> <i>{total_items}</i>\n\n"
        "Toque em um capitulo abaixo para abrir a leitura.\n"
        "✅ = capitulo ja aberto por voce"
    )


def _chapter_button_label(item: dict, read_ids: set[str]) -> str:
    number = str(item.get("chapter_number") or "").strip()
    if not number:
        title = str(item.get("title") or "Capitulo").strip()
        return title[:14]
    prefix = "✅ " if item.get("chapter_id") in read_ids else ""
    return f"{prefix}{number}"


def _chapter_list_keyboard(bundle: dict, chapters: list[dict], page: int, read_ids: set[str]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    total_items = len(chapters)
    total_pages = max(1, ((total_items - 1) // CHAPTERS_PER_PAGE) + 1)
    start = (page - 1) * CHAPTERS_PER_PAGE
    end = min(start + CHAPTERS_PER_PAGE, total_items)
    page_items = chapters[start:end]

    line: list[InlineKeyboardButton] = []
    for item in page_items:
        line.append(
            InlineKeyboardButton(
                _chapter_button_label(item, read_ids),
                callback_data=f"nv|read|{item['chapter_id']}|{bundle['title_id']}",
            )
        )
        if len(line) == 3:
            rows.append(line)
            line = []
    if line:
        rows.append(line)

    nav: list[InlineKeyboardButton] = []
    if page > 1:
        nav.append(InlineKeyboardButton("⏪", callback_data=f"nv|chap|{bundle['title_id']}|1"))
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"nv|chap|{bundle['title_id']}|{page - 1}"))
    nav.append(InlineKeyboardButton(f"{page}/{total_pages}", callback_data="nv|noop"))
    if page < total_pages:
        nav.append(InlineKeyboardButton("➡️", callback_data=f"nv|chap|{bundle['title_id']}|{page + 1}"))
        nav.append(InlineKeyboardButton("⏩", callback_data=f"nv|chap|{bundle['title_id']}|{total_pages}"))
    rows.append(nav)

    rows.append([InlineKeyboardButton("🔙 Voltar para a obra", callback_data=f"nv|title|{bundle['title_id']}")])
    return InlineKeyboardMarkup(rows)


def _chapter_text(chapter: dict, bundle: dict | None = None) -> str:
    title = html.escape((bundle or {}).get("title") or chapter.get("title") or "Novel")
    chapter_number = html.escape(chapter.get("chapter_number") or "?")
    chapter_title = html.escape(chapter.get("chapter_title") or "")
    paragraph_count = len(chapter.get("paragraphs") or [])

    lines = [
        f"📖 <b>{title}</b>",
        "",
        f"» <b>Capitulo:</b> <i>{chapter_number}</i>",
        f"» <b>Blocos de texto:</b> <i>{paragraph_count}</i>",
    ]
    if chapter_title and chapter_title.lower() != title.lower():
        lines.append(f"» <b>Titulo:</b> <i>{chapter_title}</i>")

    if WEBAPP_BASE_URL:
        lines.extend(["", "✨ <i>Abra o WebApp para ler com progresso sincronizado.</i>"])
    else:
        lines.extend(["", "✨ <i>O Telegraph ja esta sendo preparado para leitura rapida.</i>"])
    return "\n".join(lines)


def _chapter_keyboard(chapter: dict, telegraph_url: str = "", *, telegraph_pending: bool = False) -> InlineKeyboardMarkup:
    title_id = chapter.get("title_id") or ""
    rows: list[list[InlineKeyboardButton]] = []

    if WEBAPP_BASE_URL:
        rows.append(
            [
                _reader_button(
                    "📖 Ler no WebApp",
                    title_id=title_id,
                    chapter_id=chapter["chapter_id"],
                    route="reader",
                    fallback=f"nv|read|{chapter['chapter_id']}|{title_id}",
                )
            ]
        )
    elif telegraph_url:
        rows.append([InlineKeyboardButton("📖 Abrir no Telegraph", url=telegraph_url)])
    elif telegraph_pending:
        rows.append([InlineKeyboardButton("⏳ Preparando Telegraph", callback_data="nv|noop")])
    else:
        rows.append([InlineKeyboardButton("📖 Gerar Telegraph", callback_data=f"nv|tg|{chapter['chapter_id']}|{title_id}")])

    rows.append(
        [
            InlineKeyboardButton("📄 PDF pago", callback_data=f"nv|pdf|{chapter['chapter_id']}|{title_id}"),
            InlineKeyboardButton("📚 EPUB pago", callback_data=f"nv|epub|{chapter['chapter_id']}|{title_id}"),
        ]
    )

    nav: list[InlineKeyboardButton] = []
    if chapter.get("previous_chapter"):
        nav.append(
            _reader_button(
                "⬅️ Anterior",
                title_id=title_id,
                chapter_id=chapter["previous_chapter"]["chapter_id"],
                route="reader",
                fallback=f"nv|read|{chapter['previous_chapter']['chapter_id']}|{title_id}",
            )
        )
    if chapter.get("next_chapter"):
        nav.append(
            _reader_button(
                "Proximo ➡️",
                title_id=title_id,
                chapter_id=chapter["next_chapter"]["chapter_id"],
                route="reader",
                fallback=f"nv|read|{chapter['next_chapter']['chapter_id']}|{title_id}",
            )
        )
    if nav:
        rows.append(nav)

    rows.append(
        [
            _reader_button(
                "📚 Ver capitulos",
                title_id=title_id,
                route="detail",
                page="chapters",
                fallback=f"nv|chap|{title_id}|1",
            )
        ]
    )
    return InlineKeyboardMarkup(rows)


def _normalize_url(url: str) -> str:
    url = str(url or "").strip()
    if not url:
        return ""
    if url.startswith(("http://", "https://", "tg://")):
        return url
    return f"https://{url}"


def _download_label(item: dict) -> str:
    number = str(item.get("chapter_number") or "?").strip()
    lowered = number.lower()
    for prefix in ("capitulo", "capítulo", "cap.", "ch.", "ch"):
        if lowered.startswith(prefix):
            number = number[len(prefix):].strip(" .:-")
            break
    return number or "?"


def _offline_locked_text(bundle: dict) -> str:
    title = html.escape(bundle.get("title") or "Novel")
    brand = html.escape(BOT_BRAND or "Novels Baltigo")
    return (
        f"🔒 <b>Conteudo exclusivo para assinantes do {brand}</b>\n\n"
        f"» <b>Obra:</b> <i>{title}</i>\n\n"
        "A leitura offline em PDF e EPUB esta bloqueada aqui no bot.\n\n"
        "Escolha um plano abaixo. Assim que a Cakto aprovar o pagamento, "
        "o bot libera seu ID automaticamente."
    )


def _offline_locked_keyboard(bundle: dict, user_id: int | None) -> InlineKeyboardMarkup | None:
    options = get_checkout_options(user_id)
    title_id = str(bundle.get("title_id") or "").strip()
    if options:
        rows = [[InlineKeyboardButton(option["label"], url=option["url"])] for option in options]
        if title_id:
            rows.append([InlineKeyboardButton("🔄 Ja paguei / verificar", callback_data=f"nv|paycheck|{title_id}")])
        rows.append([InlineKeyboardButton("🛟 Suporte", url=SUPPORT_BOT_URL)])
        return InlineKeyboardMarkup(rows)

    subscribe_url = _normalize_url(PDF_BULK_SUBSCRIBE_URL)
    if not subscribe_url:
        if title_id:
            return InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("🔄 Ja paguei / verificar", callback_data=f"nv|paycheck|{title_id}")],
                    [InlineKeyboardButton("🛟 Suporte", url=SUPPORT_BOT_URL)],
                ]
            )
        return None

    rows = [
        [InlineKeyboardButton(f"✨ Assinar {BOT_BRAND or 'Novels Baltigo'}", url=subscribe_url)],
        [InlineKeyboardButton("🛟 Suporte", url=SUPPORT_BOT_URL)],
    ]
    return InlineKeyboardMarkup(rows)


async def _send_offline_locked(target, bundle: dict, user_id: int | None) -> None:
    text = _offline_locked_text(bundle)
    keyboard = _offline_locked_keyboard(bundle, user_id)
    message = getattr(target, "message", None)
    if message:
        try:
            await message.reply_text(
                text,
                parse_mode="HTML",
                reply_markup=keyboard,
                disable_web_page_preview=True,
            )
            return
        except Exception:
            pass

    try:
        await target.reply_text(
            text,
            parse_mode="HTML",
            reply_markup=keyboard,
            disable_web_page_preview=True,
        )
    except Exception:
        pass


def _offline_chapters_text(bundle: dict, page: int, total_items: int) -> str:
    total_pages = max(1, ((total_items - 1) // CHAPTERS_PER_PAGE) + 1)
    return (
        f"📥 <b>PDF / EPUB offline</b>\n\n"
        f"» <b>Obra:</b> <i>{html.escape(bundle.get('title') or 'Novel')}</i>\n"
        f"» <b>Pagina:</b> <i>{page}/{total_pages}</i>\n"
        f"» <b>Capitulos:</b> <i>{total_items}</i>\n\n"
        "Escolha um capitulo para baixar."
    )


def _offline_chapters_keyboard(bundle: dict, chapters: list[dict], page: int) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    title_id = str(bundle.get("title_id") or "").strip()
    total_items = len(chapters)
    total_pages = max(1, ((total_items - 1) // CHAPTERS_PER_PAGE) + 1)
    page = max(1, min(page, total_pages))
    start = (page - 1) * CHAPTERS_PER_PAGE
    end = min(start + CHAPTERS_PER_PAGE, total_items)

    line: list[InlineKeyboardButton] = []
    for item in chapters[start:end]:
        chapter_id = str(item.get("chapter_id") or "").strip()
        if not chapter_id:
            continue
        line.append(
            InlineKeyboardButton(
                _download_label(item),
                callback_data=f"nv|offread|{chapter_id}|{title_id}|{page}",
            )
        )
        if len(line) == 3:
            rows.append(line)
            line = []
    if line:
        rows.append(line)

    nav: list[InlineKeyboardButton] = []
    if page > 1:
        nav.append(InlineKeyboardButton("⏪", callback_data=f"nv|offchap|{title_id}|1"))
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"nv|offchap|{title_id}|{page - 1}"))
    nav.append(InlineKeyboardButton(f"{page}/{total_pages}", callback_data="nv|noop"))
    if page < total_pages:
        nav.append(InlineKeyboardButton("➡️", callback_data=f"nv|offchap|{title_id}|{page + 1}"))
        nav.append(InlineKeyboardButton("⏩", callback_data=f"nv|offchap|{title_id}|{total_pages}"))
    rows.append(nav)

    rows.append([InlineKeyboardButton("🔙 Voltar para a obra", callback_data=f"nv|title|{title_id}")])
    return InlineKeyboardMarkup(rows)


def _offline_chapter_text(chapter: dict, bundle: dict | None = None) -> str:
    title = html.escape((bundle or {}).get("title") or chapter.get("title") or "Novel")
    number = html.escape(chapter.get("chapter_number") or "?")
    paragraphs = html.escape(str(len(chapter.get("paragraphs") or [])))
    return (
        f"📥 <b>PDF / EPUB offline</b>\n\n"
        f"» <b>Obra:</b> <i>{title}</i>\n"
        f"» <b>Capitulo:</b> <i>{number}</i>\n"
        f"» <b>Blocos de texto:</b> <i>{paragraphs}</i>\n\n"
        "Escolha o formato para baixar."
    )


def _offline_chapter_keyboard(chapter: dict, page: int = 1) -> InlineKeyboardMarkup:
    title_id = str(chapter.get("title_id") or "").strip()
    chapter_id = str(chapter.get("chapter_id") or "").strip()
    rows: list[list[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton("📄 Baixar PDF", callback_data=f"nv|offpdf|{chapter_id}|{title_id}|{page}"),
            InlineKeyboardButton("📚 Baixar EPUB", callback_data=f"nv|offepub|{chapter_id}|{title_id}|{page}"),
        ]
    ]

    nav: list[InlineKeyboardButton] = []
    if chapter.get("previous_chapter"):
        nav.append(
            InlineKeyboardButton(
                "⬅️ Anterior",
                callback_data=f"nv|offread|{chapter['previous_chapter']['chapter_id']}|{title_id}|{page}",
            )
        )
    if chapter.get("next_chapter"):
        nav.append(
            InlineKeyboardButton(
                "Proximo ➡️",
                callback_data=f"nv|offread|{chapter['next_chapter']['chapter_id']}|{title_id}|{page}",
            )
        )
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton("📚 Ver capitulos", callback_data=f"nv|offchap|{title_id}|{page}")])
    return InlineKeyboardMarkup(rows)


def _loading_keyboard(label: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton(label, callback_data="nv|noop")]])


async def _show_loading_markup(query, label: str) -> None:
    try:
        await query.edit_message_reply_markup(reply_markup=_loading_keyboard(label))
    except Exception:
        pass


async def _restore_reply_markup(query, reply_markup) -> None:
    if reply_markup is None:
        return
    try:
        await query.edit_message_reply_markup(reply_markup=reply_markup)
    except Exception:
        pass


async def _render_panel(target, text: str, keyboard: InlineKeyboardMarkup, photo: str = "", *, edit: bool):
    if edit:
        if photo:
            media = InputMediaPhoto(media=photo, caption=text, parse_mode="HTML")
            try:
                await target.edit_message_media(media=media, reply_markup=keyboard)
                return target.message
            except Exception:
                pass
        try:
            await target.edit_message_caption(caption=text, parse_mode="HTML", reply_markup=keyboard)
            return target.message
        except Exception:
            pass
        try:
            await target.edit_message_text(
                text,
                parse_mode="HTML",
                reply_markup=keyboard,
                disable_web_page_preview=True,
            )
            return target.message
        except Exception:
            pass
        if photo:
            return await target.message.reply_photo(photo=photo, caption=text, parse_mode="HTML", reply_markup=keyboard)
        return await target.message.reply_text(text, parse_mode="HTML", reply_markup=keyboard, disable_web_page_preview=True)

    if photo:
        return await target.reply_photo(photo=photo, caption=text, parse_mode="HTML", reply_markup=keyboard)
    return await target.reply_text(text, parse_mode="HTML", reply_markup=keyboard, disable_web_page_preview=True)


async def _render_panel_to_message(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    chat_id: int,
    message_id: int,
    text: str,
    keyboard: InlineKeyboardMarkup,
    photo: str = "",
) -> None:
    bot = context.bot

    if photo:
        media = InputMediaPhoto(media=photo, caption=text, parse_mode="HTML")
        try:
            await bot.edit_message_media(chat_id=chat_id, message_id=message_id, media=media, reply_markup=keyboard)
            return
        except Exception:
            pass

    try:
        await bot.edit_message_caption(
            chat_id=chat_id,
            message_id=message_id,
            caption=text,
            parse_mode="HTML",
            reply_markup=keyboard,
        )
        return
    except Exception:
        pass

    try:
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=text,
            parse_mode="HTML",
            reply_markup=keyboard,
            disable_web_page_preview=True,
        )
    except Exception:
        pass


def _chapter_telegraph_title(chapter: dict, bundle: dict | None = None) -> str:
    base_title = (bundle or {}).get("title") or chapter.get("title") or "Novel"
    number = chapter.get("chapter_number") or "?"
    return f"{base_title} - Capitulo {number}"


async def _auto_finalize_telegraph_panel(
    context: ContextTypes.DEFAULT_TYPE,
    panel_message,
    chapter: dict,
    bundle: dict | None,
    telegraph_task: asyncio.Task,
) -> None:
    if not panel_message:
        return

    try:
        url = await telegraph_task
    except Exception:
        return

    chat_id = getattr(getattr(panel_message, "chat", None), "id", None)
    message_id = getattr(panel_message, "message_id", None)
    if chat_id is None or message_id is None:
        return

    async with _message_lock(chat_id, message_id):
        kind, ref = _get_panel_state(chat_id, message_id)
        if kind != "chapter" or ref != (chapter.get("chapter_id") or ""):
            return

        await _render_panel_to_message(
            context,
            chat_id=chat_id,
            message_id=message_id,
            text=_chapter_text(chapter, bundle),
            keyboard=_chapter_keyboard(chapter, telegraph_url=url),
            photo=_pick_bundle_image(bundle),
        )


async def _prewarm_next_chapter_telegraph(chapter: dict, telegraph_task: asyncio.Task | None = None) -> None:
    next_ref = chapter.get("next_chapter") or {}
    next_chapter_id = str(next_ref.get("chapter_id") or "").strip()
    if not next_chapter_id:
        return

    try:
        if telegraph_task is not None:
            await asyncio.shield(telegraph_task)

        next_chapter = get_cached_chapter_payload(next_chapter_id) or await get_chapter_payload(next_chapter_id)
        title = _chapter_telegraph_title(next_chapter)
        paragraphs = next_chapter.get("paragraphs") or []
        if not paragraphs:
            return

        if get_cached_chapter_page_url(next_chapter["chapter_id"], title, paragraphs):
            return

        await get_or_create_chapter_page(
            chapter_id=next_chapter["chapter_id"],
            title=title,
            paragraphs=paragraphs,
        )
    except Exception:
        return


async def send_novel_panel(target, context: ContextTypes.DEFAULT_TYPE, title_id: str, user_id: int | None, *, edit: bool):
    bundle = get_cached_novel_bundle(title_id) or await get_novel_bundle(title_id)
    last_read = await run_sync(get_last_read_entry, user_id, bundle["title_id"]) if user_id else None

    warm_chapters: list[str] = []
    if last_read and last_read.get("chapter_id"):
        warm_chapters.append(last_read["chapter_id"])
    if (bundle.get("first_chapter") or {}).get("chapter_id"):
        warm_chapters.append(bundle["first_chapter"]["chapter_id"])
    if (bundle.get("latest_chapter") or {}).get("chapter_id"):
        warm_chapters.append(bundle["latest_chapter"]["chapter_id"])
    prefetch_chapter_payloads(warm_chapters, limit=3)

    if user_id:
        fire_and_forget_sync(
            log_event,
            event_type="title_open",
            user_id=user_id,
            title_id=bundle["title_id"],
            title_name=bundle.get("title") or "",
        )

    panel_message = await _render_panel(
        target,
        _title_text(bundle, last_read),
        _title_keyboard(bundle, last_read),
        _pick_bundle_image(bundle),
        edit=edit,
    )
    if panel_message:
        _set_panel_state(panel_message.chat.id, panel_message.message_id, "title", bundle["title_id"])


async def send_chapters_page(target, context: ContextTypes.DEFAULT_TYPE, title_id: str, page: int, user_id: int | None, *, edit: bool):
    bundle = get_cached_novel_bundle(title_id) or await get_novel_bundle(title_id)
    chapters = _ordered_chapters(bundle)
    read_ids = set(await run_sync(get_read_chapter_ids, user_id, bundle["title_id"])) if user_id else set()

    total_pages = max(1, ((len(chapters) - 1) // CHAPTERS_PER_PAGE) + 1)
    page = max(1, min(page, total_pages))

    panel_message = await _render_panel(
        target,
        _chapter_list_text(bundle, page, len(chapters)),
        _chapter_list_keyboard(bundle, chapters, page, read_ids),
        _pick_bundle_image(bundle),
        edit=edit,
    )
    if panel_message:
        _set_panel_state(panel_message.chat.id, panel_message.message_id, "chapters", bundle["title_id"])


async def send_offline_panel(target, context: ContextTypes.DEFAULT_TYPE, title_id: str, user_id: int | None, *, edit: bool):
    bundle = get_cached_novel_bundle(title_id) or await get_novel_bundle(title_id)
    if not can_use_offline(user_id):
        await _send_offline_locked(target, bundle, user_id)
        return

    await send_offline_chapters_page(target, context, title_id, 1, user_id, edit=edit)


async def send_offline_chapters_page(
    target,
    context: ContextTypes.DEFAULT_TYPE,
    title_id: str,
    page: int,
    user_id: int | None,
    *,
    edit: bool,
):
    bundle = get_cached_novel_bundle(title_id) or await get_novel_bundle(title_id)
    if not can_use_offline(user_id):
        await _send_offline_locked(target, bundle, user_id)
        return

    chapters = _ordered_chapters(bundle)
    total_pages = max(1, ((len(chapters) - 1) // CHAPTERS_PER_PAGE) + 1)
    page = max(1, min(page, total_pages))

    panel_message = await _render_panel(
        target,
        _offline_chapters_text(bundle, page, len(chapters)),
        _offline_chapters_keyboard(bundle, chapters, page),
        _pick_bundle_image(bundle),
        edit=edit,
    )
    if panel_message:
        _set_panel_state(panel_message.chat.id, panel_message.message_id, "offline_chapters", bundle["title_id"])


async def send_offline_chapter_panel(
    target,
    context: ContextTypes.DEFAULT_TYPE,
    chapter_id: str,
    user_id: int | None,
    *,
    title_hint: str = "",
    page: int = 1,
    edit: bool,
):
    chapter = get_cached_chapter_payload(chapter_id) or await get_chapter_payload(chapter_id)
    if not chapter.get("title_id") and title_hint:
        chapter["title_id"] = title_hint

    bundle = None
    if chapter.get("title_id"):
        bundle = get_cached_novel_bundle(chapter["title_id"]) or await get_novel_bundle(chapter["title_id"])

    if not can_use_offline(user_id):
        await _send_offline_locked(target, bundle or chapter, user_id)
        return

    adjacent_refs = [
        (chapter.get("previous_chapter") or {}).get("chapter_id") or "",
        (chapter.get("next_chapter") or {}).get("chapter_id") or "",
    ]
    prefetch_chapter_payloads(adjacent_refs, limit=2)

    panel_message = await _render_panel(
        target,
        _offline_chapter_text(chapter, bundle),
        _offline_chapter_keyboard(chapter, page),
        _pick_bundle_image(bundle),
        edit=edit,
    )
    if panel_message:
        _set_panel_state(panel_message.chat.id, panel_message.message_id, "offline_chapter", chapter["chapter_id"])


async def send_chapter_panel(
    target,
    context: ContextTypes.DEFAULT_TYPE,
    chapter_id: str,
    user_id: int | None,
    *,
    edit: bool,
    title_hint: str = "",
):
    chapter = get_cached_chapter_payload(chapter_id) or await get_chapter_payload(chapter_id)
    if not chapter.get("title_id") and title_hint:
        chapter["title_id"] = title_hint

    bundle = None
    if chapter.get("title_id"):
        bundle = get_cached_novel_bundle(chapter["title_id"])
        if bundle is None:
            prefetch_novel_bundles([chapter["title_id"]], limit=1)

    adjacent_refs = [
        (chapter.get("previous_chapter") or {}).get("chapter_id") or "",
        (chapter.get("next_chapter") or {}).get("chapter_id") or "",
    ]
    prefetch_chapter_payloads(adjacent_refs, limit=2)

    if user_id:
        fire_and_forget_sync(
            mark_chapter_read,
            user_id=user_id,
            title_id=chapter.get("title_id") or "",
            chapter_id=chapter["chapter_id"],
            chapter_number=chapter.get("chapter_number") or "",
            title_name=(bundle or {}).get("title") or chapter.get("title") or "",
            chapter_url=chapter.get("chapter_url") or "",
        )
        fire_and_forget_sync(
            log_event,
            event_type="chapter_open",
            user_id=user_id,
            title_id=chapter.get("title_id") or "",
            title_name=(bundle or {}).get("title") or chapter.get("title") or "",
            chapter_id=chapter["chapter_id"],
            chapter_number=chapter.get("chapter_number") or "",
        )

    telegraph_title = _chapter_telegraph_title(chapter, bundle)
    paragraphs = chapter.get("paragraphs") or []
    telegraph_url = "" if WEBAPP_BASE_URL else get_cached_chapter_page_url(chapter["chapter_id"], telegraph_title, paragraphs)
    telegraph_task: asyncio.Task | None = None

    if not WEBAPP_BASE_URL and not telegraph_url and paragraphs:
        telegraph_task = asyncio.create_task(
            get_or_create_chapter_page(
                chapter_id=chapter["chapter_id"],
                title=telegraph_title,
                paragraphs=paragraphs,
            )
        )
        try:
            telegraph_url = await asyncio.wait_for(asyncio.shield(telegraph_task), timeout=TELEGRAPH_INLINE_WAIT)
        except asyncio.TimeoutError:
            telegraph_url = ""
        except Exception:
            telegraph_task = None

    panel_message = await _render_panel(
        target,
        _chapter_text(chapter, bundle),
        _chapter_keyboard(chapter, telegraph_url=telegraph_url, telegraph_pending=not bool(telegraph_url)),
        _pick_bundle_image(bundle),
        edit=edit,
    )
    if panel_message:
        _set_panel_state(panel_message.chat.id, panel_message.message_id, "chapter", chapter["chapter_id"])

    if panel_message and not telegraph_url and telegraph_task is not None:
        fire_and_forget(_auto_finalize_telegraph_panel(context, panel_message, chapter, bundle, telegraph_task))

    if not WEBAPP_BASE_URL:
        fire_and_forget(_prewarm_next_chapter_telegraph(chapter, telegraph_task))


async def _send_telegraph(query, chapter_id: str, title_hint: str = ""):
    chapter = get_cached_chapter_payload(chapter_id) or await get_chapter_payload(chapter_id)
    if not chapter.get("title_id") and title_hint:
        chapter["title_id"] = title_hint

    bundle = None
    if chapter.get("title_id"):
        bundle = get_cached_novel_bundle(chapter["title_id"])

    url = await get_or_create_chapter_page(
        chapter_id=chapter["chapter_id"],
        title=_chapter_telegraph_title(chapter, bundle),
        paragraphs=chapter.get("paragraphs") or [],
    )
    panel_message = await _render_panel(
        query,
        _chapter_text(chapter, bundle),
        _chapter_keyboard(chapter, telegraph_url=url),
        _pick_bundle_image(bundle),
        edit=True,
    )
    if panel_message:
        _set_panel_state(panel_message.chat.id, panel_message.message_id, "chapter", chapter["chapter_id"])


async def _enqueue_pdf(query, context: ContextTypes.DEFAULT_TYPE, chapter_id: str, title_hint: str = ""):
    chapter = get_cached_chapter_payload(chapter_id) or await get_chapter_payload(chapter_id)
    if not chapter.get("title_id") and title_hint:
        chapter["title_id"] = title_hint

    bundle = None
    if chapter.get("title_id"):
        bundle = get_cached_novel_bundle(chapter["title_id"])

    title_name = (bundle or {}).get("title") or chapter.get("title") or "Novel"
    chapter_number = chapter.get("chapter_number") or "?"

    tag = html.escape(DISTRIBUTION_TAG or "")
    await enqueue_pdf_job(
        context.application,
        PdfJob(
            chat_id=query.message.chat_id,
            chapter_id=chapter["chapter_id"],
            chapter_number=chapter_number,
            title_name=title_name,
            paragraphs=chapter.get("paragraphs") or [],
            caption=(
                f"📄 <b>{html.escape(title_name)}</b>\n"
                f"Capitulo <code>{html.escape(str(chapter_number))}</code>\n"
                f"{tag}"
            ).strip(),
        ),
    )
    return chapter, bundle


async def _enqueue_epub(query, context: ContextTypes.DEFAULT_TYPE, chapter_id: str, title_hint: str = ""):
    chapter = get_cached_chapter_payload(chapter_id) or await get_chapter_payload(chapter_id)
    if not chapter.get("title_id") and title_hint:
        chapter["title_id"] = title_hint

    bundle = None
    if chapter.get("title_id"):
        bundle = get_cached_novel_bundle(chapter["title_id"])

    title_name = (bundle or {}).get("title") or chapter.get("title") or "Novel"
    chapter_number = chapter.get("chapter_number") or "?"

    tag = html.escape(DISTRIBUTION_TAG or "")
    await enqueue_epub_job(
        context.application,
        EpubJob(
            chat_id=query.message.chat_id,
            chapter_id=chapter["chapter_id"],
            chapter_number=chapter_number,
            title_name=title_name,
            paragraphs=chapter.get("paragraphs") or [],
            caption=(
                f"📚 <b>{html.escape(title_name)}</b>\n"
                f"Capitulo <code>{html.escape(str(chapter_number))}</code>\n"
                f"{tag}"
            ).strip(),
        ),
    )
    return chapter, bundle


async def callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = update.effective_user

    if not query or not query.data:
        return
    if not query.data.startswith("nv|"):
        return

    if query.data == "nv|noop":
        await _safe_answer_query(query)
        return

    if not user:
        await _safe_answer_query(query, "Nao consegui identificar seu usuario agora.", show_alert=True)
        return

    if _is_callback_cooldown(context, user.id, query.data):
        await _safe_answer_query(query, "⏳ Aguarde um instante antes de apertar de novo.", show_alert=False)
        return

    message = query.message
    user_lock = _user_lock(user.id)
    msg_lock = _message_lock(message.chat.id, message.message_id) if message else asyncio.Lock()

    current_action = _action_signature(query.data)
    if message and _get_inflight_action(message.chat.id, message.message_id) == current_action:
        await _safe_answer_query(query, "⏳ Essa acao ja esta sendo processada...", show_alert=False)
        return

    parts = query.data.split("|")
    action = parts[1] if len(parts) > 1 else ""
    user_id = user.id
    original_reply_markup = getattr(message, "reply_markup", None)

    async with user_lock:
        async with msg_lock:
            if message:
                if _get_inflight_action(message.chat.id, message.message_id) == current_action:
                    await _safe_answer_query(query, "⏳ Essa acao ja esta sendo processada...", show_alert=False)
                    return
                _set_inflight_action(message.chat.id, message.message_id, current_action)

            try:
                if action == "title" and len(parts) >= 3:
                    await _safe_answer_query(query)
                    await _show_loading_markup(query, "⏳ Abrindo obra")
                    await send_novel_panel(query, context, parts[2], user_id, edit=True)
                    return

                if action == "chap" and len(parts) >= 4:
                    await _safe_answer_query(query)
                    await _show_loading_markup(query, "⏳ Carregando capitulos")
                    await send_chapters_page(query, context, parts[2], int(parts[3]), user_id, edit=True)
                    return

                if action == "offline" and len(parts) >= 3:
                    await _safe_answer_query(query)
                    if can_use_offline(user_id):
                        await _show_loading_markup(query, "⏳ Carregando offline")
                    await send_offline_panel(query, context, parts[2], user_id, edit=True)
                    return

                if action == "offchap" and len(parts) >= 4:
                    await _safe_answer_query(query)
                    if can_use_offline(user_id):
                        await _show_loading_markup(query, "⏳ Carregando capitulos")
                    await send_offline_chapters_page(query, context, parts[2], int(parts[3]), user_id, edit=True)
                    return

                if action == "offread" and len(parts) >= 3:
                    await _safe_answer_query(query)
                    if can_use_offline(user_id):
                        await _show_loading_markup(query, "⏳ Abrindo capitulo offline")
                    await send_offline_chapter_panel(
                        query,
                        context,
                        parts[2],
                        user_id,
                        title_hint=parts[3] if len(parts) >= 4 else "",
                        page=int(parts[4]) if len(parts) >= 5 else 1,
                        edit=True,
                    )
                    return

                if action == "paycheck" and len(parts) >= 3:
                    if can_use_offline(user_id):
                        await _safe_answer_query(query, "Plano encontrado. Liberando offline.", show_alert=False)
                        await _show_loading_markup(query, "⏳ Carregando offline")
                        await send_offline_panel(query, context, parts[2], user_id, edit=True)
                        return
                    await _safe_answer_query(
                        query,
                        "Ainda nao encontrei uma compra aprovada vinculada ao seu ID.",
                        show_alert=True,
                    )
                    return

                if action == "read" and len(parts) >= 3:
                    await _safe_answer_query(query)
                    await _show_loading_markup(query, "⏳ Abrindo capitulo")
                    await send_chapter_panel(
                        query,
                        context,
                        parts[2],
                        user_id,
                        edit=True,
                        title_hint=parts[3] if len(parts) >= 4 else "",
                    )
                    return

                if action == "sp" and len(parts) >= 4:
                    await _safe_answer_query(query)
                    await _show_loading_markup(query, "⏳ Carregando pagina")
                    rendered = render_search_page(context, parts[2], int(parts[3]))
                    if not rendered:
                        await _safe_answer_query(query, "Essa busca expirou. Faz outra busca pra continuar.", show_alert=True)
                        return
                    await edit_search_page(query, rendered)
                    return

                if action == "tg" and len(parts) >= 3:
                    await _safe_answer_query(query)
                    await _show_loading_markup(query, "⏳ Preparando Telegraph")
                    await _send_telegraph(query, parts[2], parts[3] if len(parts) >= 4 else "")
                    return

                if action == "pdf" and len(parts) >= 3:
                    if not can_use_offline(user_id):
                        await _safe_answer_query(query, "PDF e EPUB sao liberados para assinantes.", show_alert=True)
                        await send_offline_chapter_panel(
                            query,
                            context,
                            parts[2],
                            user_id,
                            title_hint=parts[3] if len(parts) >= 4 else "",
                            edit=True,
                        )
                        return
                    await _safe_answer_query(query)
                    await _show_loading_markup(query, "⏳ Preparando PDF")
                    chapter, bundle = await _enqueue_pdf(query, context, parts[2], parts[3] if len(parts) >= 4 else "")
                    telegraph_title = _chapter_telegraph_title(chapter, bundle)
                    telegraph_url = get_cached_chapter_page_url(
                        chapter["chapter_id"],
                        telegraph_title,
                        chapter.get("paragraphs") or [],
                    )
                    await _render_panel(
                        query,
                        _chapter_text(chapter, bundle),
                        _chapter_keyboard(
                            chapter,
                            telegraph_url=telegraph_url,
                            telegraph_pending=not bool(telegraph_url),
                        ),
                        _pick_bundle_image(bundle),
                        edit=True,
                    )
                    return

                if action == "epub" and len(parts) >= 3:
                    if not can_use_offline(user_id):
                        await _safe_answer_query(query, "PDF e EPUB sao liberados para assinantes.", show_alert=True)
                        await send_offline_chapter_panel(
                            query,
                            context,
                            parts[2],
                            user_id,
                            title_hint=parts[3] if len(parts) >= 4 else "",
                            edit=True,
                        )
                        return
                    await _safe_answer_query(query)
                    await _show_loading_markup(query, "⏳ Preparando EPUB")
                    chapter, bundle = await _enqueue_epub(query, context, parts[2], parts[3] if len(parts) >= 4 else "")
                    telegraph_title = _chapter_telegraph_title(chapter, bundle)
                    telegraph_url = get_cached_chapter_page_url(
                        chapter["chapter_id"],
                        telegraph_title,
                        chapter.get("paragraphs") or [],
                    )
                    await _render_panel(
                        query,
                        _chapter_text(chapter, bundle),
                        _chapter_keyboard(
                            chapter,
                            telegraph_url=telegraph_url,
                            telegraph_pending=not bool(telegraph_url),
                        ),
                        _pick_bundle_image(bundle),
                        edit=True,
                    )
                    return

                if action == "offpdf" and len(parts) >= 3:
                    if not can_use_offline(user_id):
                        await _safe_answer_query(query, "Funcao liberada para assinantes.", show_alert=True)
                        await send_offline_chapter_panel(
                            query,
                            context,
                            parts[2],
                            user_id,
                            title_hint=parts[3] if len(parts) >= 4 else "",
                            page=int(parts[4]) if len(parts) >= 5 else 1,
                            edit=True,
                        )
                        return
                    await _safe_answer_query(query, "PDF enviado para a fila.", show_alert=False)
                    await _show_loading_markup(query, "⏳ Preparando PDF")
                    chapter, bundle = await _enqueue_pdf(query, context, parts[2], parts[3] if len(parts) >= 4 else "")
                    await _render_panel(
                        query,
                        _offline_chapter_text(chapter, bundle),
                        _offline_chapter_keyboard(chapter, int(parts[4]) if len(parts) >= 5 else 1),
                        _pick_bundle_image(bundle),
                        edit=True,
                    )
                    return

                if action == "offepub" and len(parts) >= 3:
                    if not can_use_offline(user_id):
                        await _safe_answer_query(query, "Funcao liberada para assinantes.", show_alert=True)
                        await send_offline_chapter_panel(
                            query,
                            context,
                            parts[2],
                            user_id,
                            title_hint=parts[3] if len(parts) >= 4 else "",
                            page=int(parts[4]) if len(parts) >= 5 else 1,
                            edit=True,
                        )
                        return
                    await _safe_answer_query(query, "EPUB enviado para a fila.", show_alert=False)
                    await _show_loading_markup(query, "⏳ Preparando EPUB")
                    chapter, bundle = await _enqueue_epub(query, context, parts[2], parts[3] if len(parts) >= 4 else "")
                    await _render_panel(
                        query,
                        _offline_chapter_text(chapter, bundle),
                        _offline_chapter_keyboard(chapter, int(parts[4]) if len(parts) >= 5 else 1),
                        _pick_bundle_image(bundle),
                        edit=True,
                    )
                    return

                await _safe_answer_query(query, "Acao desconhecida.", show_alert=True)
            except asyncio.TimeoutError:
                await _restore_reply_markup(query, original_reply_markup)
                await _safe_answer_query(query, "⏳ Demorou demais para carregar. Tente de novo.", show_alert=True)
            except Exception as error:
                print("ERRO CALLBACK NOVEL:", repr(error))
                await _restore_reply_markup(query, original_reply_markup)
                await _safe_answer_query(query, "Nao consegui concluir essa acao agora.", show_alert=True)
                try:
                    await query.message.reply_text(
                        "❌ Nao consegui concluir essa acao agora.\n\nTente novamente em instantes.",
                        parse_mode="HTML",
                    )
                except Exception:
                    pass
            finally:
                if message:
                    _clear_inflight_action(message.chat.id, message.message_id)
