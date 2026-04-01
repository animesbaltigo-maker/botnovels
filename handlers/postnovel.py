import asyncio
import html
import unicodedata

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from config import ADMIN_IDS, BOT_USERNAME, CANAL_POSTAGEM_NOVELS, STICKER_DIVISOR
from core.channel_target import ensure_channel_target
from services.centralnovel_client import get_cached_novel_bundle, get_novel_bundle, search_novels


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
    full_title = html.escape((novel.get("display_title") or novel.get("title") or "Sem titulo").upper())
    genres = novel.get("genres") or []
    genres_text = ", ".join(f"#{genre}" for genre in genres[:4]) if genres else "N/A"
    genres_text = html.escape(genres_text)
    chapters = html.escape(str(novel.get("total_chapters") or "?"))
    status = html.escape(str(novel.get("status") or "N/A"))
    description = html.escape(_truncate_text(novel.get("description") or "", 320))

    return (
        f"📚 <b>{full_title}</b>\n\n"
        f"<b>Generos:</b> <i>{genres_text}</i>\n"
        f"<b>Capitulos:</b> <i>{chapters}</i>\n"
        f"<b>Status:</b> <i>{status}</i>\n\n"
        f"💬 {description or 'Sem descricao disponivel.'}"
    )


def _build_keyboard(novel: dict) -> InlineKeyboardMarkup:
    title_id = novel.get("title_id") or ""
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("📚 Ler obra", url=f"https://t.me/{BOT_USERNAME}?start=title_{title_id}")]]
    )


async def postnovel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else None
    message = update.effective_message

    if not message:
        return

    if not _is_admin(user_id):
        await message.reply_text(
            "❌ <b>Voce nao tem permissao para usar este comando.</b>",
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
            await status_message.edit_text("❌ <b>Nao encontrei essa novel.</b>", parse_mode="HTML")
            return

        search_item = _pick_best_candidate(query, results)
        if not search_item or not search_item.get("title_id"):
            await status_message.edit_text("❌ <b>Nao consegui identificar a obra certa.</b>", parse_mode="HTML")
            return

        title_id = search_item["title_id"]
        bundle = get_cached_novel_bundle(title_id)
        if bundle is None:
            bundle = await asyncio.wait_for(get_novel_bundle(title_id), timeout=15.0)

        novel = dict(bundle)
        photo = novel.get("banner_url") or novel.get("cover_url") or None
        caption = _build_caption(novel)
        keyboard = _build_keyboard(novel)
        destination = await ensure_channel_target(context.bot, CANAL_POSTAGEM_NOVELS or message.chat_id)

        if photo:
            try:
                await context.bot.send_photo(
                    chat_id=destination,
                    photo=photo,
                    caption=caption,
                    parse_mode="HTML",
                    reply_markup=keyboard,
                )
            except Exception as photo_error:
                print("ERRO POSTNOVEL FOTO:", repr(photo_error))
                await context.bot.send_message(
                    chat_id=destination,
                    text=caption,
                    parse_mode="HTML",
                    reply_markup=keyboard,
                    disable_web_page_preview=True,
                )
        else:
            await context.bot.send_message(
                chat_id=destination,
                text=caption,
                parse_mode="HTML",
                reply_markup=keyboard,
                disable_web_page_preview=True,
            )

        if STICKER_DIVISOR:
            await context.bot.send_sticker(chat_id=destination, sticker=STICKER_DIVISOR)

        await status_message.edit_text(
            f"✅ <b>Postagem enviada com sucesso.</b>\n\n<code>{html.escape(novel.get('title') or query)}</code>",
            parse_mode="HTML",
        )
    except Exception as error:
        print("ERRO POSTNOVEL:", repr(error))
        await status_message.edit_text(
            f"❌ <b>Nao consegui postar essa novel.</b>\n\n{html.escape(str(error) or 'Tente novamente em instantes.')}",
            parse_mode="HTML",
        )
