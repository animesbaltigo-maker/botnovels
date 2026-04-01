import html

from telegram import Update
from telegram.ext import ContextTypes

from config import ADMIN_IDS
from services.metrics import clear_all_metrics_data, get_metrics_report


def _is_admin(user_id: int | None) -> bool:
    return user_id is not None and user_id in ADMIN_IDS


def _fmt_rows(rows, empty_text="Nenhum dado ainda"):
    if not rows:
        return empty_text

    parts = []
    for index, row in enumerate(rows, start=1):
        label = html.escape(str(row["label"]))
        parts.append(f"{index}. <code>{label}</code> - <b>{row['total']}</b>")
    return "\n".join(parts)


def _normalize_period(args: list[str]) -> str:
    if not args:
        return "total"

    raw = (args[0] or "").strip().lower()
    aliases = {
        "hoje": "hoje",
        "today": "hoje",
        "7d": "7d",
        "7": "7d",
        "semana": "7d",
        "30d": "30d",
        "30": "30d",
        "mes": "30d",
        "mês": "30d",
        "total": "total",
    }
    return aliases.get(raw, "total")


def _period_label(period: str) -> str:
    labels = {
        "hoje": "Hoje",
        "7d": "Últimos 7 dias",
        "30d": "Últimos 30 dias",
        "total": "Total",
    }
    return labels.get(period, "Total")


async def metricas(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.effective_message

    if not user or not message or not _is_admin(user.id):
        if message:
            await message.reply_text("Você não tem permissão para usar esse comando.")
        return

    period = _normalize_period(context.args or [])
    data = get_metrics_report(limit=7, period=period)

    text = (
        f"📊 <b>Métricas do bot</b>\n"
        f"🗂 <b>Período:</b> {html.escape(_period_label(period))}\n\n"
        f"🔎 <b>Buscas mais feitas</b>\n{_fmt_rows(data['top_searches'])}\n\n"
        f"📚 <b>Obras mais abertas</b>\n{_fmt_rows(data['top_opened_titles'])}\n\n"
        f"📖 <b>Capítulos mais abertos</b>\n{_fmt_rows(data['top_opened_chapters'])}\n\n"
        f"🔥 <b>Leituras por obra</b>\n{_fmt_rows(data['top_read_titles'])}\n\n"
        f"📉 <b>Buscas sem resultado:</b> <b>{data['searches_without_result']}</b>\n"
        f"👤 <b>Novos usuários:</b> <b>{data['new_users']}</b>\n"
        f"🔁 <b>Usuários ativos:</b> <b>{data['active_users']}</b>\n"
        f"✅ <b>Leituras marcadas:</b> <b>{data['read_marks_total']}</b>"
    )

    await message.reply_text(text, parse_mode="HTML", disable_web_page_preview=True)


async def metricas_limpar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.effective_message

    if not user or not message or not _is_admin(user.id):
        if message:
            await message.reply_text("Você não tem permissão para usar esse comando.")
        return

    clear_all_metrics_data()
    await message.reply_text("🗑 <b>Métricas e histórico de leitura limpos.</b>", parse_mode="HTML")
