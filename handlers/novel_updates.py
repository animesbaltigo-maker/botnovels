import asyncio
import html
import json
from pathlib import Path

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from config import (
    ADMIN_IDS,
    AUTO_POST_LIMIT,
    BOT_BRAND,
    BOT_USERNAME,
    CANAL_POSTAGEM_NOVEL_CAPITULOS,
    DATA_DIR,
)
from core.channel_target import ensure_channel_target
from services.centralnovel_client import (
    get_cached_novel_bundle,
    get_novel_bundle,
    get_recent_updated_novels,
)

POSTED_JSON_PATH = Path(DATA_DIR) / "novel_capitulos_postados.json"


def _is_admin(user_id: int | None) -> bool:
    return user_id is not None and user_id in ADMIN_IDS


def _load_posted() -> list[str]:
    if not POSTED_JSON_PATH.exists():
        return []
    try:
        return json.loads(POSTED_JSON_PATH.read_text(encoding="utf-8"))
    except Exception:
        return []


def _save_posted(items: list[str]) -> None:
    POSTED_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
    POSTED_JSON_PATH.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")


def _deep_link(chapter_id: str, title_id: str = "") -> str:
    payload = f"{chapter_id}_{title_id}" if title_id else chapter_id
    return f"https://t.me/{BOT_USERNAME}?start=ch_{payload}"


def _title_link(title_id: str) -> str:
    return f"https://t.me/{BOT_USERNAME}?start=title_{title_id}"


def _display_title(item: dict) -> str:
    return item.get("display_title") or item.get("title") or "Novel"


def _post_key(item: dict) -> str:
    latest = item.get("latest_chapter") or {}
    chapter_id = str(latest.get("chapter_id") or item.get("chapter_id") or "").strip()
    if chapter_id:
        return chapter_id
    return str(item.get("title_id") or item.get("novel_id") or "").strip()


def _caption(item: dict) -> str:
    title = html.escape(_display_title(item))
    latest = item.get("latest_chapter") or {}
    chapter_number = html.escape(str(latest.get("chapter_number") or item.get("latest_chapter_number") or "?"))
    status = html.escape(str(item.get("status") or "Atualizado"))
    updated_at = html.escape(str(item.get("updated_at") or "agora ha pouco"))
    total_chapters = html.escape(str(item.get("total_chapters") or "?"))
    brand = html.escape(BOT_BRAND)

    lines = [
        "🆕 <b>{title}</b>",
        "",
        f"» <b>Capitulo:</b> <i>{chapter_number}</i>",
        f"» <b>Status:</b> <i>{status}</i>",
    ]
    if updated_at and updated_at != "agora ha pouco":
        lines.append(f"» <b>Atualizado:</b> <i>{updated_at}</i>")

    lines.extend(["", f"✨ <i>Abra no {brand} e continue a leitura.</i>"])
    return "\n".join(lines)


def _keyboard(item: dict) -> InlineKeyboardMarkup:
    latest = item.get("latest_chapter") or {}
    chapter_id = str(latest.get("chapter_id") or item.get("chapter_id") or "").strip()
    title_id = str(item.get("title_id") or item.get("novel_id") or "").strip()

    rows: list[list[InlineKeyboardButton]] = []
    if chapter_id:
        rows.append([InlineKeyboardButton("📖 Ler capitulo", url=_deep_link(chapter_id, title_id))])
    if title_id:
        rows.append([InlineKeyboardButton("📚 Abrir obra", url=_title_link(title_id))])
    return InlineKeyboardMarkup(rows)


async def _resolve_recent_item(item: dict) -> dict | None:
    title_id = str(item.get("title_id") or item.get("novel_id") or "").strip()
    if not title_id:
        return None

    bundle = get_cached_novel_bundle(title_id)
    if bundle is None:
        bundle = await asyncio.wait_for(get_novel_bundle(title_id), timeout=18.0)

    latest = bundle.get("latest_chapter") or {}
    if not latest.get("chapter_id"):
        return None

    merged = dict(item)
    merged.update(
        {
            "title_id": bundle.get("title_id") or title_id,
            "novel_id": bundle.get("title_id") or title_id,
            "title": bundle.get("title") or item.get("title") or "",
            "display_title": bundle.get("display_title") or bundle.get("title") or item.get("display_title") or "",
            "cover_url": bundle.get("cover_url") or item.get("cover_url") or "",
            "banner_url": bundle.get("banner_url") or bundle.get("cover_url") or item.get("banner_url") or "",
            "status": bundle.get("status") or item.get("status") or "",
            "updated_at": bundle.get("updated_at") or item.get("updated_at") or "",
            "total_chapters": bundle.get("total_chapters") or item.get("total_chapters") or "",
            "latest_chapter": latest,
            "latest_chapter_number": latest.get("chapter_number") or item.get("latest_chapter") or "",
        }
    )
    return merged


async def _send_recent_novel(bot, chat_id, item: dict) -> None:
    cover = item.get("banner_url") or item.get("cover_url") or None
    caption = _caption(item)
    keyboard = _keyboard(item)

    if cover:
        try:
            await bot.send_photo(
                chat_id=chat_id,
                photo=cover,
                caption=caption,
                parse_mode="HTML",
                reply_markup=keyboard,
            )
            return
        except Exception as error:
            print("ERRO POST NOVEL CAP FOTO:", repr(error))

    await bot.send_message(
        chat_id=chat_id,
        text=caption,
        parse_mode="HTML",
        reply_markup=keyboard,
        disable_web_page_preview=True,
    )


async def _post_recent_items(bot, destination, items: list[dict], posted: list[str]) -> tuple[int, int, list[str]]:
    posted_set = set(posted)
    sent = 0
    failed = 0

    for item in items:
        key = _post_key(item)
        if not key or key in posted_set:
            continue

        try:
            await _send_recent_novel(bot, destination, item)
        except Exception as error:
            failed += 1
            print("ERRO POST NOVEL CAP:", repr(error), item.get("title_id"), item.get("title"))
            continue

        posted.append(key)
        posted_set.add(key)
        sent += 1

    return sent, failed, posted


async def _collect_recent_items(limit: int) -> list[dict]:
    raw_items = await get_recent_updated_novels(limit=limit)
    if not raw_items:
        return []

    results: list[dict] = []
    seen_titles: set[str] = set()
    for raw_item in raw_items:
        title_id = str(raw_item.get("title_id") or raw_item.get("novel_id") or "").strip()
        if not title_id or title_id in seen_titles:
            continue
        seen_titles.add(title_id)
        try:
            resolved = await _resolve_recent_item(raw_item)
        except Exception as error:
            print("ERRO RESOLVE NOVEL CAP:", repr(error), title_id, raw_item.get("title"))
            continue
        if resolved:
            results.append(resolved)
    return results


async def postnovelcaps(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    user = update.effective_user

    if not message or not user or not _is_admin(user.id):
        if message:
            await message.reply_text("❌ <b>Voce nao tem permissao para usar esse comando.</b>", parse_mode="HTML")
        return

    status_message = await message.reply_text(
        "📤 <b>Verificando capitulos novos de novels...</b>",
        parse_mode="HTML",
    )

    try:
        items = await _collect_recent_items(AUTO_POST_LIMIT)
        if not items:
            await status_message.edit_text(
                "❌ <b>Nao encontrei atualizacoes de novels para postar agora.</b>",
                parse_mode="HTML",
            )
            return

        destination = await ensure_channel_target(
            context.bot,
            CANAL_POSTAGEM_NOVEL_CAPITULOS or message.chat_id,
        )
        posted = _load_posted()
        sent, failed, posted = await _post_recent_items(context.bot, destination, items, posted)
        _save_posted(posted[-500:])

        await status_message.edit_text(
            "✅ <b>Atualizacoes publicadas.</b>\n\n"
            f"<b>Capitulos enviados:</b> <code>{sent}</code>\n"
            f"<b>Falhas:</b> <code>{failed}</code>",
            parse_mode="HTML",
        )
    except Exception as error:
        print("ERRO POSTNOVELCAPS:", repr(error))
        await status_message.edit_text(
            f"❌ <b>Nao consegui concluir as atualizacoes agora.</b>\n\n"
            f"{html.escape(str(error) or 'Tente novamente em instantes.')}",
            parse_mode="HTML",
        )


async def auto_post_new_novel_caps_job(context: ContextTypes.DEFAULT_TYPE):
    if not CANAL_POSTAGEM_NOVEL_CAPITULOS:
        return

    try:
        destination = await ensure_channel_target(context.bot, CANAL_POSTAGEM_NOVEL_CAPITULOS)
        items = await _collect_recent_items(AUTO_POST_LIMIT)
        if not items:
            return

        posted = _load_posted()
        sent, failed, posted = await _post_recent_items(context.bot, destination, items, posted)
        if sent or failed:
            _save_posted(posted[-500:])
    except Exception as error:
        print("ERRO AUTO POST NOVEL CAP:", repr(error))
