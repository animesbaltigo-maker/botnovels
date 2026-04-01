from config import BOT_BRAND
from utils.gatekeeper import ensure_channel_membership


async def ajuda(update, context):
    if not await ensure_channel_membership(update, context):
        return

    text = (
        f"📖 <b>Ajuda - {BOT_BRAND}</b>\n\n"
        "🔎 <b>Como buscar uma novel</b>\n"
        "Use <code>/novel nome da obra</code>\n\n"
        "📌 <b>Exemplos</b>\n"
        "• <code>/novel shadow slave</code>\n"
        "• <code>/novel supreme magus</code>\n"
        "• <code>/novel lord of mysteries</code>\n\n"
        "📚 <b>Fluxo de leitura</b>\n"
        "• Pesquise a obra\n"
        "• Abra os detalhes\n"
        "• Escolha um capitulo\n"
        "• Leia pelo Telegraph\n\n"
        "📢 <b>Comandos de admin</b>\n"
        "• <code>/postnovel nome da obra</code>\n"
        "• <code>/postnovelcaps</code> para os ultimos capitulos\n\n"
        "🎁 <b>Extras</b>\n"
        "• <code>/indicacoes</code> para seu link de convites\n"
        "• <code>/broadcast</code> para admins enviarem avisos\n"
        "• <code>/metricas</code> para admins acompanharem o uso"
    )

    await update.effective_message.reply_text(text, parse_mode="HTML")
