import asyncio
import html
import json
import unicodedata
from pathlib import Path

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from config import ADMIN_IDS, BOT_USERNAME, CANAL_POSTAGEM_NOVELS, DATA_DIR
from core.channel_target import ensure_channel_target
from services.centralnovel_client import (
    get_cached_novel_bundle,
    get_novel_bundle,
    get_series_catalog,
    search_novels,
)

POSTED_JSON_PATH = Path(DATA_DIR) / "novels_postadas.json"
BULK_POST_DELAY_SECONDS = 30.0
GLOBAL_BULK_RUNNING_KEY = "novel_bulk_post_running"
GLOBAL_BULK_TASK_KEY = "novel_bulk_post_task"

NOVEL_STICKER_DIVISOR = "CAACAgQAAx0CbKkU-AACFJtps_kRLpeUt2Gvd7mT4d0gS1vyCgACOhUAAqDAiFJSU5pkUMltvzoE"
DIVIDER_FALLBACK_TEXT = "━━━━━━━━━━━━━━"


def _truncate_text(text: str, limit: int = 320) -> str:
    text = (text or "").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _is_admin(user_id: int | None) -> bool:
    return user_id is not None and user_id in ADMIN_IDS


def _normalize_text(value: str) -> str:
    value = unicodedata.normalize("NFKD", (value or "").strip().lower())
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    return " ".join(value.split())


def _pick_best_candidate(query: str, results: list[dict]) -> dict | None:
    if not results:
        return None

    normalized_query = _normalize_text(query)

    def _score(item: dict) -> tuple[int, int]:
        display_title = _normalize_text(item.get("display_title") or item.get("title") or "")
        if not display_title:
            return (-1, 0)
        if display_title == normalized_query:
            return (500, -len(display_title))
        if display_title.startswith(normalized_query):
            return (400, -len(display_title))
        if normalized_query in display_title:
            return (300, -len(display_title))
        overlap = len(set(normalized_query.split()) & set(display_title.split()))
        return (100 + overlap, -len(display_title))

    return max(results, key=_score)


def _build_caption(novel: dict) -> str:
    full_title = html.escape((novel.get("display_title") or novel.get("title") or "Sem título").upper())
    genres = novel.get("genres") or []
    genres_text = ", ".join(f"#{genre}" for genre in genres[:4]) if genres else "N/A"
    genres_text = html.escape(genres_text)
    chapters = html.escape(str(novel.get("total_chapters") or "?"))
    status = html.escape(str(novel.get("status") or "N/A"))
    description = html.escape(_truncate_text(novel.get("description") or "", 320))

    return (
        f"📚 <b>{full_title}</b>\n\n"
        f"<b>Gêneros:</b> <i>{genres_text}</i>\n"
        f"<b>Capítulos:</b> <i>{chapters}</i>\n"
        f"<b>Status:</b> <i>{status}</i>\n\n"
        f"💬 {description or 'Sem descrição disponível.'}"
    )


def _build_keyboard(novel: dict) -> InlineKeyboardMarkup:
    title_id = novel.get("title_id") or ""
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("📚 Ler obra", url=f"https://t.me/{BOT_USERNAME}?start=title_{title_id}")]]
    )


def _load_posted() -> list[str]:
    if not POSTED_JSON_PATH.exists():
        return []
    try:
        data = json.loads(POSTED_JSON_PATH.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return [str(item).strip() for item in data if str(item).strip()]
    except Exception:
        return []
    return []


def _save_posted(items: list[str]) -> None:
    POSTED_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
    POSTED_JSON_PATH.write_text(
        json.dumps(items, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _bulk_running(context: ContextTypes.DEFAULT_TYPE) -> bool:
    return bool(context.application.bot_data.get(GLOBAL_BULK_RUNNING_KEY, False))


def _set_bulk_running(context: ContextTypes.DEFAULT_TYPE, value: bool) -> None:
    context.application.bot_data[GLOBAL_BULK_RUNNING_KEY] = value


def _set_bulk_task(context: ContextTypes.DEFAULT_TYPE, task) -> None:
    context.application.bot_data[GLOBAL_BULK_TASK_KEY] = task


async def _safe_edit(message, text: str) -> None:
    try:
        await message.edit_text(text, parse_mode="HTML")
    except Exception:
        pass


async def _resolve_novel_payload(novel_ref: dict) -> dict | None:
    title_id = str(novel_ref.get("title_id") or "").strip()
    if not title_id:
        return None

    bundle = get_cached_novel_bundle(title_id)
    if bundle is None:
        bundle = await asyncio.wait_for(get_novel_bundle(title_id), timeout=18.0)

    return dict(bundle) if bundle else None


async def _send_divider(bot, destination) -> None:
    sticker = NOVEL_STICKER_DIVISOR.strip()
    sticker_error = None

    if sticker:
        for _ in range(3):
            try:
                await bot.send_sticker(chat_id=destination, sticker=sticker)
                return
            except Exception as error:
                sticker_error = error
                await asyncio.sleep(0.8)

        print("ERRO STICKER DIVISOR NOVEL:", repr(sticker_error), sticker)

    try:
        await bot.send_message(chat_id=destination, text=DIVIDER_FALLBACK_TEXT)
    except Exception as fallback_error:
        print("ERRO DIVISOR FALLBACK NOVEL:", repr(fallback_error))
        if sticker_error:
            raise sticker_error
        raise


async def _send_novel_post(bot, destination, novel: dict) -> None:
    photo = novel.get("banner_url") or novel.get("cover_url") or None
    caption = _build_caption(novel)
    keyboard = _build_keyboard(novel)

    if photo:
        try:
            await bot.send_photo(
                chat_id=destination,
                photo=photo,
                caption=caption,
                parse_mode="HTML",
                reply_markup=keyboard,
            )
        except Exception as photo_error:
            print("ERRO POSTNOVEL FOTO:", repr(photo_error))
            await bot.send_message(
                chat_id=destination,
                text=caption,
                parse_mode="HTML",
                reply_markup=keyboard,
                disable_web_page_preview=True,
            )
    else:
        await bot.send_message(
            chat_id=destination,
            text=caption,
            parse_mode="HTML",
            reply_markup=keyboard,
            disable_web_page_preview=True,
        )

    await _send_divider(bot, destination)


async def postnovel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else None
    message = update.effective_message

    if not message:
        return

    if not _is_admin(user_id):
        await message.reply_text(
            "❌ <b>Você não tem permissão para usar este comando.</b>",
            parse_mode="HTML",
        )
        return

    if not context.args:
        await message.reply_text(
            "❌ <b>Faltou o nome da novel.</b>\n\n"
            "Use assim:\n"
            "<code>/postnovel nome da obra</code>\n\n"
            "📌 <b>Exemplo:</b>\n"
            "<code>/postnovel shadow slave</code>",
            parse_mode="HTML",
        )
        return

    query = " ".join(context.args).strip()
    status_message = await message.reply_text(
        "📤 <b>Montando postagem...</b>\nAguarde um instante.",
        parse_mode="HTML",
    )

    try:
        results = await search_novels(query, limit=8)
        if not results:
            await status_message.edit_text(
                "❌ <b>Não encontrei essa novel.</b>",
                parse_mode="HTML",
            )
            return

        search_item = _pick_best_candidate(query, results)
        if not search_item or not search_item.get("title_id"):
            await status_message.edit_text(
                "❌ <b>Não consegui identificar a obra certa.</b>",
                parse_mode="HTML",
            )
            return

        novel = await _resolve_novel_payload(search_item)
        if not novel:
            await status_message.edit_text(
                "❌ <b>Não consegui montar os dados dessa novel.</b>",
                parse_mode="HTML",
            )
            return

        destination = await ensure_channel_target(context.bot, CANAL_POSTAGEM_NOVELS or message.chat_id)
        await _send_novel_post(context.bot, destination, novel)

        await status_message.edit_text(
            f"✅ <b>Postagem enviada com sucesso.</b>\n\n<code>{html.escape(novel.get('title') or query)}</code>",
            parse_mode="HTML",
        )
    except Exception as error:
        print("ERRO POSTNOVEL:", repr(error))
        await status_message.edit_text(
            f"❌ <b>Não consegui postar essa novel.</b>\n\n{html.escape(str(error) or 'Tente novamente em instantes.')}",
            parse_mode="HTML",
        )


async def _run_bulk_post_novels(
    context: ContextTypes.DEFAULT_TYPE,
    admin_chat_id: int,
    reply_to_message_id: int | None,
):
    _set_bulk_running(context, True)
    try:
        destination = await ensure_channel_target(context.bot, CANAL_POSTAGEM_NOVELS or admin_chat_id)
        catalog = await get_series_catalog()
        posted = _load_posted()
        posted_set = set(posted)

        pending = [
            item
            for item in catalog
            if str(item.get("title_id") or "").strip()
            and str(item.get("title_id")) not in posted_set
        ]

        if not pending:
            await context.bot.send_message(
                chat_id=admin_chat_id,
                text="✅ Nenhuma novel pendente para postar agora.",
                reply_to_message_id=reply_to_message_id,
            )
            return

        status_message = await context.bot.send_message(
            chat_id=admin_chat_id,
            text=(
                "🚀 <b>Postagem em lote iniciada.</b>\n\n"
                f"<b>Total pendente:</b> <code>{len(pending)}</code>\n"
                f"<b>Intervalo:</b> <code>{int(BULK_POST_DELAY_SECONDS)}s</code>"
            ),
            parse_mode="HTML",
            reply_to_message_id=reply_to_message_id,
        )

        sent = 0
        failed = 0
        total = len(pending)

        for index, item in enumerate(pending, start=1):
            title_id = str(item.get("title_id") or "").strip()
            title = str(item.get("title") or "Novel").strip()

            try:
                novel = await _resolve_novel_payload(item)
                if not novel:
                    raise RuntimeError("Não consegui montar a obra.")

                await _send_novel_post(context.bot, destination, novel)

                posted.append(title_id)
                posted_set.add(title_id)
                _save_posted(posted[-5000:])
                sent += 1

            except Exception as error:
                failed += 1
                print("ERRO POSTNOVEL BULK:", repr(error), title_id, title)

            await _safe_edit(
                status_message,
                (
                    "🚀 <b>Postagem em lote em andamento.</b>\n\n"
                    f"<b>Enviadas:</b> <code>{sent}</code>\n"
                    f"<b>Falhas:</b> <code>{failed}</code>\n"
                    f"<b>Processadas:</b> <code>{index}/{total}</code>\n"
                    f"<b>Atual:</b> <code>{html.escape(title)}</code>"
                ),
            )

            if index < total:
                await asyncio.sleep(BULK_POST_DELAY_SECONDS)

        await _safe_edit(
            status_message,
            (
                "✅ <b>Postagem em lote finalizada.</b>\n\n"
                f"<b>Enviadas:</b> <code>{sent}</code>\n"
                f"<b>Falhas:</b> <code>{failed}</code>\n"
                f"<b>Total analisado:</b> <code>{total}</code>"
            ),
        )
    finally:
        _set_bulk_running(context, False)
        context.application.bot_data.pop(GLOBAL_BULK_TASK_KEY, None)


async def posttodasnovels(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else None
    message = update.effective_message

    if not message:
        return

    if not _is_admin(user_id):
        await message.reply_text(
            "❌ <b>Você não tem permissão para usar este comando.</b>",
            parse_mode="HTML",
        )
        return

    if _bulk_running(context):
        await message.reply_text(
            "⏳ <b>Já existe uma postagem em lote rodando.</b>",
            parse_mode="HTML",
        )
        return

    task = context.application.create_task(
        _run_bulk_post_novels(
            context=context,
            admin_chat_id=message.chat_id,
            reply_to_message_id=message.message_id,
        )
    )
    _set_bulk_task(context, task)

    await message.reply_text(
        "🚀 <b>Fila de postagem em lote iniciada.</b>\n\n"
        "Vou enviar uma novel, depois um sticker divisor, e seguir nesse ritmo com 30 segundos entre uma postagem e outra.",
        parse_mode="HTML",
    )
