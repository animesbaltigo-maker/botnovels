import asyncio

from telegram import Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from config import BOT_TOKEN
from core.http_client import close_http_client
from core.pdf_queue import start_pdf_workers, stop_pdf_workers
from handlers.broadcast import (
    broadcast_callbacks,
    broadcast_command,
    broadcast_message_router,
)
from handlers.help import ajuda
from handlers.metricas import metricas, metricas_limpar
from handlers.novel import novel_command
from handlers.novel_callbacks import callbacks
from handlers.novel_updates import auto_post_new_novel_caps_job, postnovelcaps
from handlers.plan import plano
from handlers.postnovel import postnovel, posttodasnovels
from handlers.referral import indicacoes, referral_button
from handlers.start import start
from services.centralnovel_client import warm_catalog_cache
from services.metrics import init_metrics_db
from services.offline_access import init_offline_access_db
from services.referral_db import init_referral_db

init_metrics_db()
init_offline_access_db()
init_referral_db()

MAX_CONCURRENT_UPDATES = 96
BOT_API_CONNECTION_POOL = 48
BOT_API_POOL_TIMEOUT = 30.0
BOT_API_CONNECT_TIMEOUT = 10.0
BOT_API_READ_TIMEOUT = 25.0
BOT_API_WRITE_TIMEOUT = 25.0
INITIAL_WARMUP_DELAY = 10
AUTO_POST_FIRST_DELAY = 120
WARM_CACHE_FIRST_DELAY = 1800


async def _delayed_warm_catalog() -> None:
    await asyncio.sleep(INITIAL_WARMUP_DELAY)
    await warm_catalog_cache()


async def post_init(app: Application) -> None:
    await start_pdf_workers(app)
    app.create_task(_delayed_warm_catalog())


async def post_shutdown(app: Application) -> None:
    await stop_pdf_workers(app)
    await close_http_client()


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    print("ERRO:", repr(context.error))
    try:
        if isinstance(update, Update) and update.effective_message:
            await update.effective_message.reply_text(
                "Ocorreu um erro ao processar sua solicitacao.",
            )
    except Exception:
        pass


async def warm_catalog_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    await warm_catalog_cache()


def _register_jobs(app: Application) -> None:
    if not app.job_queue:
        return

    app.job_queue.run_repeating(
        auto_post_new_novel_caps_job,
        interval=600,
        first=AUTO_POST_FIRST_DELAY,
        name="auto_post_new_novel_chapters",
    )
    app.job_queue.run_repeating(
        warm_catalog_job,
        interval=3600,
        first=WARM_CACHE_FIRST_DELAY,
        name="warm_novel_catalog_cache",
    )


def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("Configure BOT_TOKEN nas variaveis de ambiente.")

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .concurrent_updates(MAX_CONCURRENT_UPDATES)
        .connection_pool_size(BOT_API_CONNECTION_POOL)
        .pool_timeout(BOT_API_POOL_TIMEOUT)
        .connect_timeout(BOT_API_CONNECT_TIMEOUT)
        .read_timeout(BOT_API_READ_TIMEOUT)
        .write_timeout(BOT_API_WRITE_TIMEOUT)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("novel", novel_command))
    app.add_handler(CommandHandler("buscar", novel_command))
    app.add_handler(CommandHandler("ajuda", ajuda))
    app.add_handler(CommandHandler("postnovel", postnovel))
    app.add_handler(CommandHandler("posttodasnovels", posttodasnovels))
    app.add_handler(CommandHandler("postallnovels", posttodasnovels))
    app.add_handler(CommandHandler("postnovelcaps", postnovelcaps))
    app.add_handler(CommandHandler("postnovelscaps", postnovelcaps))
    app.add_handler(CommandHandler("postnovosepsnovel", postnovelcaps))
    app.add_handler(CommandHandler("broadcast", broadcast_command))
    app.add_handler(CommandHandler("indicacoes", indicacoes))
    app.add_handler(CommandHandler("plano", plano))
    app.add_handler(CommandHandler("plan", plano))
    app.add_handler(CommandHandler("metricas", metricas))
    app.add_handler(CommandHandler("metricaslimpar", metricas_limpar))

    app.add_handler(CallbackQueryHandler(broadcast_callbacks, pattern=r"^bc\|"))
    app.add_handler(CallbackQueryHandler(referral_button, pattern=r"^noop_indicar$"))
    app.add_handler(CallbackQueryHandler(callbacks, pattern=r"^nv\|"))

    app.add_handler(
        MessageHandler(filters.ALL & ~filters.COMMAND, broadcast_message_router),
        group=99,
    )

    _register_jobs(app)
    app.add_error_handler(error_handler)

    print("Bot de novels rodando...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
