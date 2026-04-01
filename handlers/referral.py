import html
from urllib.parse import quote

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from config import BOT_USERNAME
from services.referral_db import referral_ranking, referral_stats


def _display_name(row):
    if row["first_name"]:
        return row["first_name"]
    if row["username"]:
        return f"@{row['username']}"
    return str(row["user_id"])


async def _send_panel(message, user_id: int):
    stats = referral_stats(user_id)
    link = f"https://t.me/{BOT_USERNAME}?start=ref_{user_id}"

    telegram_share = (
        "https://t.me/share/url?"
        f"url={quote(link)}"
        "&text=" + quote("📖 Entra no bot de novels comigo:")
    )
    whatsapp_share = "https://wa.me/?text=" + quote(f"📖 Entra no bot de novels comigo:\n{link}")

    ranking = referral_ranking(3)
    medals = ["🥇", "🥈", "🥉"]
    ranking_text = ""

    if ranking:
        ranking_text += "\n🏆 <b>Ranking mensal</b>\n\n"
        for index, row in enumerate(ranking):
            ranking_text += f"{medals[index]} {html.escape(_display_name(row))} - <code>{row['total']}</code>\n"

    text = (
        "🎁 <b>Sistema de convites</b>\n\n"
        "Convide amigos para usar o bot e subir no ranking.\n\n"
        "📊 <b>Suas estatisticas</b>\n\n"
        f"👉 Cliques no seu link: <code>{stats['clicks']}</code>\n"
        f"📨 Registradas: <code>{stats['total']}</code>\n"
        f"⏳ Em analise: <code>{stats['pending']}</code>\n"
        f"✅ Aprovadas: <code>{stats['qualified']}</code>\n\n"
        "🔗 <b>Seu link</b>\n"
        f"<code>{link}</code>\n\n"
        "🛡 <b>Regras</b>\n"
        "• Autoindicacao nao conta\n"
        "• So a primeira indicacao e valida\n"
        "• O usuario precisa continuar no canal\n"
        "• Precisa usar o bot por alguns dias\n"
        f"{ranking_text}"
    )

    keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📨 Compartilhar no Telegram", url=telegram_share)],
            [InlineKeyboardButton("📦 Compartilhar no WhatsApp", url=whatsapp_share)],
        ]
    )

    await message.reply_text(
        text,
        parse_mode="HTML",
        reply_markup=keyboard,
        disable_web_page_preview=True,
    )


async def indicacoes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.effective_message
    if not user or not message:
        return
    await _send_panel(message, user.id)


async def referral_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = update.effective_user
    if not query or not user:
        return
    if query.data != "noop_indicar":
        return

    await query.answer()
    await _send_panel(query.message, user.id)
