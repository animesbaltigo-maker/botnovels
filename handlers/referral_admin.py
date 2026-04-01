import time

from telegram import Update
from telegram.ext import ContextTypes

from config import ADMIN_IDS, REQUIRED_CHANNEL
from services.referral_db import (
    MIN_INTERACTIONS_TO_QUALIFY,
    MIN_SECONDS_TO_QUALIFY,
    get_all_pending_referrals,
    referral_admin_overview,
    try_qualify_referral,
)


def _is_admin(user_id: int | None) -> bool:
    if user_id is None:
        return False
    return user_id in ADMIN_IDS


async def _is_user_in_required_channel(bot, user_id: int) -> bool:
    if not REQUIRED_CHANNEL:
        return True

    try:
        member = await bot.get_chat_member(REQUIRED_CHANNEL, user_id)
        return member.status not in ("left", "kicked")
    except Exception:
        return False


async def refstats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.effective_message

    if not user or not message or not _is_admin(user.id):
        return

    overview = referral_admin_overview()
    pending_rows = get_all_pending_referrals()

    now = int(time.time())

    pending_no_channel = 0
    pending_waiting_time = 0
    pending_waiting_interactions = 0
    pending_ready = 0
    blocked_users = 0

    for item in pending_rows:
        if int(item.get("is_blocked", 0) or 0) == 1:
            blocked_users += 1
            continue

        referred_user_id = int(item["referred_user_id"])
        interactions = int(item.get("interactions", 0) or 0)
        created_at = int(item["created_at"])
        age_seconds = now - created_at

        in_channel = await _is_user_in_required_channel(context.bot, referred_user_id)

        if not in_channel:
            pending_no_channel += 1
            continue

        if age_seconds < MIN_SECONDS_TO_QUALIFY:
            pending_waiting_time += 1
            continue

        if interactions < MIN_INTERACTIONS_TO_QUALIFY:
            pending_waiting_interactions += 1
            continue

        pending_ready += 1

    text = (
        "📊 <b>Análise global de indicações</b>\n\n"
        f"👆 <b>Cliques totais:</b> <code>{overview['clicks_total']}</code>\n"
        f"📨 <b>Registradas:</b> <code>{overview['registered_total']}</code>\n"
        f"⏳ <b>Pendentes:</b> <code>{overview['pending_total']}</code>\n"
        f"🚫 <b>Pendentes sem canal:</b> <code>{pending_no_channel}</code>\n"
        f"🕒 <b>Pendentes aguardando 7 dias:</b> <code>{pending_waiting_time}</code>\n"
        f"🎮 <b>Pendentes sem uso suficiente:</b> <code>{pending_waiting_interactions}</code>\n"
        f"✅ <b>Prontas para aprovar:</b> <code>{pending_ready}</code>\n"
        f"⛔ <b>Usuários bloqueados:</b> <code>{blocked_users}</code>\n"
        f"✔️ <b>Aprovadas:</b> <code>{overview['approved_total']}</code>\n"
        f"❌ <b>Rejeitadas:</b> <code>{overview['rejected_total']}</code>"
    )

    await message.reply_text(text, parse_mode="HTML")


async def auto_referral_check_job(context: ContextTypes.DEFAULT_TYPE):
    pending_rows = get_all_pending_referrals()

    for item in pending_rows:
        user_id = int(item["referred_user_id"])
        in_channel = await _is_user_in_required_channel(context.bot, user_id)

        try:
            try_qualify_referral(user_id, is_channel_member=in_channel)
        except Exception as e:
            print("ERRO AUTO REFERRAL CHECK:", repr(e))
