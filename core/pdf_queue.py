import asyncio
from dataclasses import dataclass

from telegram.error import TimedOut

from config import PDF_PROTECT_CONTENT, PDF_QUEUE_LIMIT, PDF_WORKERS_BULK, PDF_WORKERS_SINGLE
from services.epub_service import get_or_build_epub
from services.pdf_service import get_or_build_pdf


@dataclass
class PdfJob:
    chat_id: int
    chapter_id: str
    chapter_number: str
    title_name: str
    paragraphs: list[str]
    caption: str
    is_bulk: bool = False


@dataclass
class EpubJob:
    chat_id: int
    chapter_id: str
    chapter_number: str
    title_name: str
    paragraphs: list[str]
    caption: str
    is_bulk: bool = False


_single_workers = []
_bulk_workers = []
_active_jobs = {}


def _job_key(kind: str, chapter_id: str) -> str:
    return f"{kind}:{chapter_id}"


async def _safe_edit(message, text: str):
    try:
        await message.edit_text(text, parse_mode="HTML")
    except Exception:
        pass


async def _send_document_safe(bot, chat_id: int, file_path: str, file_name: str, caption: str):
    try:
        with open(file_path, "rb") as file:
            await bot.send_document(
                chat_id=chat_id,
                document=file,
                filename=file_name,
                caption=caption,
                parse_mode="HTML",
                protect_content=PDF_PROTECT_CONTENT,
            )
        return True
    except TimedOut:
        try:
            await bot.send_message(chat_id, "O envio demorou mais que o esperado. Confere se o arquivo ja chegou.")
        except Exception:
            pass
        return True


async def _progress(entry, title_name: str, chapter_number: str, done: int, total: int):
    pct = int((done / max(total, 1)) * 100)
    text = (
        f"{entry['icon']} <b>Gerando {entry['kind_label']}</b>\n\n"
        f"📚 <b>Obra:</b> {title_name}\n"
        f"📖 <b>Capitulo:</b> {chapter_number}\n"
        f"⏳ <b>Progresso:</b> {pct}%"
    )
    for message in list(entry["status_messages"]):
        await _safe_edit(message, text)


async def _process_pdf_job(app, job: PdfJob):
    entry = _active_jobs.get(_job_key("pdf", job.chapter_id))
    if not entry:
        return

    try:
        async def progress_cb(done, total):
            await _progress(entry, job.title_name, job.chapter_number, done, total)

        pdf_path, pdf_name = await get_or_build_pdf(
            chapter_id=job.chapter_id,
            chapter_number=job.chapter_number,
            title_name=job.title_name,
            paragraphs=job.paragraphs,
            progress_cb=progress_cb,
        )

        for waiter in entry["waiters"]:
            await _send_document_safe(app.bot, waiter["chat_id"], pdf_path, pdf_name, waiter["caption"])

        for message in list(entry["status_messages"]):
            await _safe_edit(
                message,
                (
                    "✅ <b>PDF pronto</b>\n\n"
                    f"📚 <b>Obra:</b> {job.title_name}\n"
                    f"📖 <b>Capitulo:</b> {job.chapter_number}"
                ),
            )
    except Exception as error:
        for message in list(entry["status_messages"]):
            await _safe_edit(message, f"❌ Falha ao gerar PDF:\n<code>{error}</code>")
    finally:
        _active_jobs.pop(_job_key("pdf", job.chapter_id), None)


async def _process_epub_job(app, job: EpubJob):
    entry = _active_jobs.get(_job_key("epub", job.chapter_id))
    if not entry:
        return

    try:
        async def progress_cb(done, total):
            await _progress(entry, job.title_name, job.chapter_number, done, total)

        epub_path, epub_name = await get_or_build_epub(
            chapter_id=job.chapter_id,
            chapter_number=job.chapter_number,
            title_name=job.title_name,
            paragraphs=job.paragraphs,
            progress_cb=progress_cb,
        )

        for waiter in entry["waiters"]:
            await _send_document_safe(app.bot, waiter["chat_id"], epub_path, epub_name, waiter["caption"])

        for message in list(entry["status_messages"]):
            await _safe_edit(
                message,
                (
                    "✅ <b>EPUB pronto</b>\n\n"
                    f"📚 <b>Obra:</b> {job.title_name}\n"
                    f"📖 <b>Capitulo:</b> {job.chapter_number}"
                ),
            )
    except Exception as error:
        for message in list(entry["status_messages"]):
            await _safe_edit(message, f"❌ Falha ao gerar EPUB:\n<code>{error}</code>")
    finally:
        _active_jobs.pop(_job_key("epub", job.chapter_id), None)


async def _worker(app, queue):
    while True:
        job = await queue.get()
        if job is None:
            queue.task_done()
            break
        if isinstance(job, EpubJob):
            await _process_epub_job(app, job)
        else:
            await _process_pdf_job(app, job)
        queue.task_done()


async def enqueue_pdf_job(app, job: PdfJob):
    single_queue = app.bot_data["single_pdf_queue"]
    bulk_queue = app.bot_data["bulk_pdf_queue"]
    key = _job_key("pdf", job.chapter_id)

    if key in _active_jobs:
        entry = _active_jobs[key]
        entry["waiters"].append({"chat_id": job.chat_id, "caption": job.caption})
        status = await app.bot.send_message(
            job.chat_id,
            (
                "⏳ <b>Pedido recebido</b>\n\n"
                f"📚 <b>Obra:</b> {job.title_name}\n"
                f"📖 <b>Capitulo:</b> {job.chapter_number}\n"
                "Status: <b>ja esta em processamento</b>"
            ),
            parse_mode="HTML",
        )
        entry["status_messages"].append(status)
        return single_queue.qsize() + bulk_queue.qsize()

    status = await app.bot.send_message(
        job.chat_id,
        (
            "⏳ <b>Pedido recebido</b>\n\n"
            f"📚 <b>Obra:</b> {job.title_name}\n"
            f"📖 <b>Capitulo:</b> {job.chapter_number}\n"
            "Status: <b>na fila</b>"
        ),
        parse_mode="HTML",
    )
    _active_jobs[key] = {
        "waiters": [{"chat_id": job.chat_id, "caption": job.caption}],
        "status_messages": [status],
        "kind_label": "PDF",
        "icon": "📥",
    }

    queue = bulk_queue if job.is_bulk else single_queue
    await queue.put(job)
    return single_queue.qsize() + bulk_queue.qsize()


async def enqueue_epub_job(app, job: EpubJob):
    single_queue = app.bot_data["single_pdf_queue"]
    bulk_queue = app.bot_data["bulk_pdf_queue"]
    key = _job_key("epub", job.chapter_id)

    if key in _active_jobs:
        entry = _active_jobs[key]
        entry["waiters"].append({"chat_id": job.chat_id, "caption": job.caption})
        status = await app.bot.send_message(
            job.chat_id,
            (
                "⏳ <b>Pedido recebido</b>\n\n"
                f"📚 <b>Obra:</b> {job.title_name}\n"
                f"📖 <b>Capitulo:</b> {job.chapter_number}\n"
                "Status: <b>ja esta em processamento</b>"
            ),
            parse_mode="HTML",
        )
        entry["status_messages"].append(status)
        return single_queue.qsize() + bulk_queue.qsize()

    status = await app.bot.send_message(
        job.chat_id,
        (
            "⏳ <b>Pedido recebido</b>\n\n"
            f"📚 <b>Obra:</b> {job.title_name}\n"
            f"📖 <b>Capitulo:</b> {job.chapter_number}\n"
            "Status: <b>na fila</b>"
        ),
        parse_mode="HTML",
    )
    _active_jobs[key] = {
        "waiters": [{"chat_id": job.chat_id, "caption": job.caption}],
        "status_messages": [status],
        "kind_label": "EPUB",
        "icon": "📦",
    }

    queue = bulk_queue if job.is_bulk else single_queue
    await queue.put(job)
    return single_queue.qsize() + bulk_queue.qsize()


async def start_pdf_workers(app):
    if app.bot_data.get("pdf_workers_started"):
        return

    app.bot_data["single_pdf_queue"] = asyncio.Queue(maxsize=PDF_QUEUE_LIMIT)
    app.bot_data["bulk_pdf_queue"] = asyncio.Queue(maxsize=PDF_QUEUE_LIMIT)

    for _ in range(PDF_WORKERS_SINGLE):
        _single_workers.append(asyncio.create_task(_worker(app, app.bot_data["single_pdf_queue"])))
    for _ in range(PDF_WORKERS_BULK):
        _bulk_workers.append(asyncio.create_task(_worker(app, app.bot_data["bulk_pdf_queue"])))

    app.bot_data["pdf_workers_started"] = True


async def stop_pdf_workers(app):
    for queue_name, workers in (
        ("single_pdf_queue", _single_workers),
        ("bulk_pdf_queue", _bulk_workers),
    ):
        queue = app.bot_data.get(queue_name)
        if queue is None:
            continue
        for _ in workers:
            await queue.put(None)
        await asyncio.gather(*workers, return_exceptions=True)
        workers.clear()
