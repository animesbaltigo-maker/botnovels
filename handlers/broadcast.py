import asyncio
import html
import time
from typing import Optional

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaPhoto,
    Message,
    Update,
)
from telegram.constants import ParseMode
from telegram.error import BadRequest, Forbidden, NetworkError, RetryAfter, TimedOut
from telegram.ext import ContextTypes

from config import ADMIN_IDS
from services.user_registry import get_all_users, get_total_users, remove_user


BROADCAST_STATE_KEY = "broadcast_state"
BROADCAST_DATA_KEY = "broadcast_data"
BROADCAST_LOCK_KEY = "broadcast_lock"
BROADCAST_LAST_KEY = "broadcast_last_action"

BROADCAST_COOLDOWN = 1.0

SEND_WORKERS = 4
PER_MESSAGE_DELAY = 0.05
STATUS_EVERY = 100
STATUS_MIN_INTERVAL = 3.0
PIN_MAX_USERS = 100

GLOBAL_BROADCAST_RUNNING_KEY = "broadcast_running"
GLOBAL_BROADCAST_TASK_KEY = "broadcast_task"


def _is_admin(user_id: int | None) -> bool:
    return user_id is not None and user_id in ADMIN_IDS


def _escape(text: str) -> str:
    return html.escape(text or "")


def _reset_broadcast(context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop(BROADCAST_STATE_KEY, None)
    context.user_data.pop(BROADCAST_DATA_KEY, None)
    context.user_data.pop(BROADCAST_LOCK_KEY, None)
    context.user_data.pop(BROADCAST_LAST_KEY, None)


def _set_state(context: ContextTypes.DEFAULT_TYPE, state: str):
    context.user_data[BROADCAST_STATE_KEY] = state


def _get_state(context: ContextTypes.DEFAULT_TYPE) -> str:
    return context.user_data.get(BROADCAST_STATE_KEY, "")


def _get_data(context: ContextTypes.DEFAULT_TYPE) -> dict:
    data = context.user_data.get(BROADCAST_DATA_KEY)
    if not isinstance(data, dict):
        data = {
            "mode": None,
            "target_user_id": None,
            "text": "",
            "photo": None,
            "button_text": "",
            "button_url": "",
            "pin": False,
        }
        context.user_data[BROADCAST_DATA_KEY] = data
    return data


async def _guard_action(context: ContextTypes.DEFAULT_TYPE):
    now = time.monotonic()

    if context.user_data.get(BROADCAST_LOCK_KEY):
        return "locked"

    last = context.user_data.get(BROADCAST_LAST_KEY, 0.0)
    if now - last < BROADCAST_COOLDOWN:
        return "cooldown"

    context.user_data[BROADCAST_LOCK_KEY] = True
    context.user_data[BROADCAST_LAST_KEY] = now
    return "ok"


def _release_guard(context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop(BROADCAST_LOCK_KEY, None)


def _broadcast_is_running(context: ContextTypes.DEFAULT_TYPE) -> bool:
    return bool(context.application.bot_data.get(GLOBAL_BROADCAST_RUNNING_KEY, False))


def _set_broadcast_running(context: ContextTypes.DEFAULT_TYPE, value: bool):
    context.application.bot_data[GLOBAL_BROADCAST_RUNNING_KEY] = value


def _set_broadcast_task(context: ContextTypes.DEFAULT_TYPE, task):
    context.application.bot_data[GLOBAL_BROADCAST_TASK_KEY] = task


def _main_menu_text(data: dict, running: bool = False) -> str:
    mode = data.get("mode")
    target_user_id = data.get("target_user_id")
    photo = data.get("photo")
    text = data.get("text") or ""
    button_text = data.get("button_text") or ""
    button_url = data.get("button_url") or ""
    pin = data.get("pin", False)

    if mode == "all":
        mode_label = "Todos os usuários"
    elif mode == "single":
        mode_label = "Usuário específico"
    else:
        mode_label = "Não definido"

    target_block = ""
    if mode == "single" and target_user_id:
        target_block = f"\n👤 <b>ID alvo:</b> <code>{target_user_id}</code>"

    status_block = (
        "🟢 <b>Status do sistema:</b> <code>Broadcast em andamento</code>\n\n"
        if running
        else "⚪️ <b>Status do sistema:</b> <code>Parado</code>\n\n"
    )

    return (
        f"📢 <b>Transmissão</b>\n\n"
        f"{status_block}"
        f"Configure e envie uma mensagem para o bot.\n\n"
        f"🎯 <b>Destino:</b> <code>{_escape(mode_label)}</code>"
        f"{target_block}\n"
        f"🖼 <b>Mídia:</b> <code>{'Sim' if photo else 'Não'}</code>\n"
        f"📝 <b>Texto:</b> <code>{'Sim' if text.strip() else 'Não'}</code>\n"
        f"🔘 <b>Botão:</b> <code>{'Sim' if button_text.strip() and button_url.strip() else 'Não'}</code>\n"
        f"📌 <b>Pin:</b> <code>{'Sim' if pin else 'Não'}</code>\n\n"
        f"👥 <b>Total salvo no bot:</b> <code>{get_total_users()}</code>\n\n"
        "Escolha uma opção abaixo."
    )


def _main_menu_keyboard(data: dict, running: bool = False):
    mode = data.get("mode")
    pin = data.get("pin", False)

    mode_label = "🌍 Todos" if mode == "all" else "👤 Usuário"
    pin_label = "📌 SIM" if pin else "❌ NÃO"
    send_label = "⏳ Rodando" if running else "🚀 Enviar"

    rows = [
        [
            InlineKeyboardButton(mode_label, callback_data="bc|set_mode"),
            InlineKeyboardButton("🖼 Mídia", callback_data="bc|set_media"),
        ],
        [
            InlineKeyboardButton("📝 Texto", callback_data="bc|set_text"),
            InlineKeyboardButton("🔘 Botão", callback_data="bc|set_button"),
        ],
        [
            InlineKeyboardButton(pin_label, callback_data="bc|toggle_pin"),
            InlineKeyboardButton("👀 Ver", callback_data="bc|preview"),
        ],
        [
            InlineKeyboardButton(send_label, callback_data="bc|send"),
            InlineKeyboardButton("🗑 Limpar", callback_data="bc|reset"),
        ],
        [
            InlineKeyboardButton("❌ Fechar", callback_data="bc|close"),
        ],
    ]

    return InlineKeyboardMarkup(rows)


def _build_message_keyboard(data: dict):
    button_text = (data.get("button_text") or "").strip()
    button_url = (data.get("button_url") or "").strip()

    if button_text and button_url:
        return InlineKeyboardMarkup(
            [[InlineKeyboardButton(button_text, url=button_url)]]
        )
    return None


def _preview_caption(data: dict) -> str:
    mode = data.get("mode")
    target_user_id = data.get("target_user_id")
    text = (data.get("text") or "").strip()
    pin = data.get("pin", False)

    if mode == "all":
        mode_label = "Todos os usuários"
    elif mode == "single":
        mode_label = "Usuário específico"
    else:
        mode_label = "Não definido"

    preview_header = (
        f"👀 <b>Pré-visualização da transmissão</b>\n\n"
        f"🎯 <b>Destino:</b> <code>{_escape(mode_label)}</code>\n"
    )

    if mode == "single" and target_user_id:
        preview_header += f"👤 <b>ID alvo:</b> <code>{target_user_id}</code>\n"

    preview_header += f"📌 <b>Pin:</b> <code>{'Sim' if pin else 'Não'}</code>\n\n"

    if text:
        return preview_header + text

    return preview_header + "<i>Sem texto.</i>"


async def _send_preview(query, context: ContextTypes.DEFAULT_TYPE):
    data = _get_data(context)

    caption = _preview_caption(data)
    keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🚀 Confirmar envio", callback_data="bc|send")],
            [InlineKeyboardButton("🔙 Voltar", callback_data="bc|menu")],
        ]
    )
    msg_keyboard = _build_message_keyboard(data)

    if msg_keyboard:
        keyboard.inline_keyboard.insert(0, msg_keyboard.inline_keyboard[0])

    photo = data.get("photo")
    if photo:
        try:
            await query.edit_message_media(
                media=InputMediaPhoto(
                    media=photo,
                    caption=caption,
                    parse_mode=ParseMode.HTML,
                ),
                reply_markup=keyboard,
            )
            return
        except Exception:
            try:
                await query.edit_message_caption(
                    caption=caption,
                    parse_mode=ParseMode.HTML,
                    reply_markup=keyboard,
                )
                return
            except Exception:
                pass

    try:
        await query.edit_message_text(
            text=caption,
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard,
            disable_web_page_preview=True,
        )
    except Exception:
        await query.message.reply_text(
            text=caption,
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard,
            disable_web_page_preview=True,
        )


async def _show_main_menu(query, context: ContextTypes.DEFAULT_TYPE):
    data = _get_data(context)
    text = _main_menu_text(data, running=_broadcast_is_running(context))
    keyboard = _main_menu_keyboard(data, running=_broadcast_is_running(context))

    try:
        await query.edit_message_text(
            text=text,
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard,
            disable_web_page_preview=True,
        )
    except Exception:
        try:
            await query.message.reply_text(
                text=text,
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard,
                disable_web_page_preview=True,
            )
        except Exception:
            pass


async def _send_broadcast_message(bot, chat_id: int, data: dict) -> Optional[Message]:
    text = (data.get("text") or "").strip()
    photo = data.get("photo")
    reply_markup = _build_message_keyboard(data)

    if photo:
        return await bot.send_photo(
            chat_id=chat_id,
            photo=photo,
            caption=text or None,
            parse_mode=ParseMode.HTML if text else None,
            reply_markup=reply_markup,
        )

    return await bot.send_message(
        chat_id=chat_id,
        text=text or "📢",
        parse_mode=ParseMode.HTML,
        reply_markup=reply_markup,
        disable_web_page_preview=True,
    )


async def _maybe_pin_message(bot, chat_id: int, message: Message | None, should_pin: bool):
    if not should_pin or not message:
        return

    try:
        await bot.pin_chat_message(
            chat_id=chat_id,
            message_id=message.message_id,
            disable_notification=True,
        )
    except Exception:
        pass


def _should_remove_user_on_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return (
        "blocked" in text
        or "chat not found" in text
        or "user is deactivated" in text
        or "bot was blocked by the user" in text
        or "forbidden" in text
    )


async def _safe_send_one(bot, user_id: int, data: dict, should_pin: bool) -> tuple[bool, bool]:
    try:
        msg = await _send_broadcast_message(bot, user_id, data)
        await _maybe_pin_message(bot, user_id, msg, should_pin)
        return True, False

    except RetryAfter as e:
        await asyncio.sleep(float(e.retry_after) + 1.0)
        try:
            msg = await _send_broadcast_message(bot, user_id, data)
            await _maybe_pin_message(bot, user_id, msg, should_pin)
            return True, False
        except Exception as exc2:
            return False, _should_remove_user_on_error(exc2)

    except (Forbidden, BadRequest) as exc:
        return False, _should_remove_user_on_error(exc)

    except (TimedOut, NetworkError):
        return False, False

    except Exception as exc:
        return False, _should_remove_user_on_error(exc)


async def _update_status_message(status_msg: Message, sent: int, failed: int, processed: int, total: int):
    try:
        await status_msg.edit_text(
            f"🚀 Transmissão em andamento...\n\n"
            f"✅ Enviadas: {sent}\n"
            f"❌ Falhas: {failed}\n"
            f"📦 Processadas: {processed}/{total}"
        )
    except Exception:
        pass


async def _broadcast_worker(
    queue: asyncio.Queue,
    bot,
    data: dict,
    should_pin: bool,
    counters: dict,
    status_msg: Message,
    total: int,
):
    while True:
        user_id = await queue.get()

        if user_id is None:
            queue.task_done()
            break

        ok, should_remove = await _safe_send_one(bot, int(user_id), data, should_pin)

        if ok:
            counters["sent"] += 1
        else:
            counters["failed"] += 1
            if should_remove:
                try:
                    remove_user(user_id)
                    counters["removed"] += 1
                except Exception:
                    pass

        counters["processed"] += 1

        now = time.monotonic()
        if (
            counters["processed"] % STATUS_EVERY == 0
            or now - counters["last_status_at"] >= STATUS_MIN_INTERVAL
        ):
            counters["last_status_at"] = now
            await _update_status_message(
                status_msg=status_msg,
                sent=counters["sent"],
                failed=counters["failed"],
                processed=counters["processed"],
                total=total,
            )

        await asyncio.sleep(PER_MESSAGE_DELAY)
        queue.task_done()


async def _execute_broadcast_background(
    context: ContextTypes.DEFAULT_TYPE,
    admin_chat_id: int,
    reply_to_message_id: int | None,
    data: dict,
):
    if _broadcast_is_running(context):
        await context.bot.send_message(
            chat_id=admin_chat_id,
            text="⚠️ Já existe uma transmissão em andamento.",
            reply_to_message_id=reply_to_message_id,
        )
        return

    _set_broadcast_running(context, True)

    try:
        mode = data.get("mode")
        text = (data.get("text") or "").strip()
        photo = data.get("photo")
        target_user_id = data.get("target_user_id")
        requested_pin = bool(data.get("pin"))

        if mode not in {"all", "single"}:
            await context.bot.send_message(
                chat_id=admin_chat_id,
                text="❌ Defina o destino primeiro.",
                reply_to_message_id=reply_to_message_id,
            )
            return

        if not text and not photo:
            await context.bot.send_message(
                chat_id=admin_chat_id,
                text="❌ Defina pelo menos um texto ou uma imagem.",
                reply_to_message_id=reply_to_message_id,
            )
            return

        if mode == "single":
            if not target_user_id:
                await context.bot.send_message(
                    chat_id=admin_chat_id,
                    text="❌ Informe o ID do usuário.",
                    reply_to_message_id=reply_to_message_id,
                )
                return

            ok, should_remove = await _safe_send_one(
                context.bot,
                int(target_user_id),
                data,
                requested_pin,
            )

            if should_remove:
                try:
                    remove_user(int(target_user_id))
                except Exception:
                    pass

            await context.bot.send_message(
                chat_id=admin_chat_id,
                text=(
                    f"✅ Envio finalizado.\n\n"
                    f"📤 Enviadas: {1 if ok else 0}\n"
                    f"❌ Falhas: {0 if ok else 1}"
                ),
                reply_to_message_id=reply_to_message_id,
            )
            return

        users = get_all_users()

        if not users:
            await context.bot.send_message(
                chat_id=admin_chat_id,
                text="❌ Não há usuários salvos ainda.",
                reply_to_message_id=reply_to_message_id,
            )
            return

        total = len(users)
        should_pin = requested_pin and total <= PIN_MAX_USERS

        status_text = f"🚀 Iniciando transmissão...\n\n👥 Total alvo: {total}"
        if requested_pin and not should_pin:
            status_text += f"\n📌 Pin desativado automaticamente para listas acima de {PIN_MAX_USERS} usuários."

        status_msg = await context.bot.send_message(
            chat_id=admin_chat_id,
            text=status_text,
            reply_to_message_id=reply_to_message_id,
        )

        queue: asyncio.Queue = asyncio.Queue()
        counters = {
            "sent": 0,
            "failed": 0,
            "processed": 0,
            "removed": 0,
            "last_status_at": time.monotonic(),
        }

        for user_id in users:
            await queue.put(user_id)

        workers = [
            asyncio.create_task(
                _broadcast_worker(
                    queue=queue,
                    bot=context.bot,
                    data=data,
                    should_pin=should_pin,
                    counters=counters,
                    status_msg=status_msg,
                    total=total,
                )
            )
            for _ in range(SEND_WORKERS)
        ]

        for _ in workers:
            await queue.put(None)

        await queue.join()
        await asyncio.gather(*workers, return_exceptions=True)

        final_text = (
            f"✅ Transmissão finalizada.\n\n"
            f"📤 Enviadas: {counters['sent']}\n"
            f"❌ Falhas: {counters['failed']}\n"
            f"🧹 Removidos: {counters['removed']}\n"
            f"👥 Total processado: {counters['processed']}"
        )

        try:
            await status_msg.edit_text(final_text)
        except Exception:
            await context.bot.send_message(
                chat_id=admin_chat_id,
                text=final_text,
                reply_to_message_id=reply_to_message_id,
            )

    finally:
        _set_broadcast_running(context, False)
        context.application.bot_data.pop(GLOBAL_BROADCAST_TASK_KEY, None)


async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not _is_admin(user.id if user else None):
        return

    _reset_broadcast(context)
    _get_data(context)

    text = _main_menu_text(_get_data(context), running=_broadcast_is_running(context))
    keyboard = _main_menu_keyboard(_get_data(context), running=_broadcast_is_running(context))

    await update.effective_message.reply_text(
        text=text,
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard,
        disable_web_page_preview=True,
    )


async def broadcast_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = update.effective_user

    if not query or not user or not _is_admin(user.id):
        return

    await query.answer()

    guard = await _guard_action(context)
    if guard == "locked":
        return
    if guard == "cooldown":
        return

    try:
        raw_data = query.data or ""
        print(f"[BROADCAST][CALLBACK] data={raw_data!r}")

        if not raw_data.startswith("bc|"):
            return

        action = raw_data.split("|", 1)[1]
        payload = _get_data(context)

        if action == "menu":
            await _show_main_menu(query, context)
            return

        if action == "close":
            _reset_broadcast(context)
            try:
                await query.edit_message_text("✅ Painel de transmissão fechado.")
            except Exception:
                pass
            return

        if action == "reset":
            _reset_broadcast(context)
            _get_data(context)
            await _show_main_menu(query, context)
            return

        if action == "set_mode":
            _set_state(context, "awaiting_mode")
            await query.edit_message_text(
                "🎯 <b>Escolha o destino</b>\n\n"
                "Envie:\n"
                "• <code>1</code> para todos os usuários\n"
                "• <code>2</code> para um usuário específico",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Voltar", callback_data="bc|menu")]]
                ),
            )
            return

        if action == "set_media":
            _set_state(context, "awaiting_media")
            await query.edit_message_text(
                "🖼 <b>Envie uma imagem</b>\n\n"
                "Ou envie <code>remover</code> para apagar a mídia atual.\n"
                "Ou <code>pular</code> para voltar.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Voltar", callback_data="bc|menu")]]
                ),
            )
            return

        if action == "set_text":
            _set_state(context, "awaiting_text")
            await query.edit_message_text(
                "📝 <b>Envie o texto da transmissão</b>\n\n"
                "Pode usar HTML simples do Telegram.\n"
                "Envie <code>remover</code> para apagar.\n"
                "Envie <code>pular</code> para voltar.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Voltar", callback_data="bc|menu")]]
                ),
                disable_web_page_preview=True,
            )
            return

        if action == "set_button":
            _set_state(context, "awaiting_button_text")
            await query.edit_message_text(
                "🔘 <b>Envie o texto do botão</b>\n\n"
                "Exemplo: <code>Assistir agora</code>\n\n"
                "Envie <code>remover</code> para apagar botão.\n"
                "Envie <code>pular</code> para voltar.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Voltar", callback_data="bc|menu")]]
                ),
            )
            return

        if action == "toggle_pin":
            payload["pin"] = not bool(payload.get("pin"))
            await _show_main_menu(query, context)
            return

        if action == "preview":
            await _send_preview(query, context)
            return

        if action == "send":
            if _broadcast_is_running(context):
                await query.answer("⚠️ Já existe uma transmissão em andamento.", show_alert=True)
                return

            mode = payload.get("mode")
            if mode not in {"all", "single"}:
                await query.answer("Defina o destino primeiro.", show_alert=True)
                return

            text = (payload.get("text") or "").strip()
            photo = payload.get("photo")
            if not text and not photo:
                await query.answer("Defina pelo menos texto ou imagem.", show_alert=True)
                return

            safe_payload = payload.copy()

            _reset_broadcast(context)

            task = context.application.create_task(
                _execute_broadcast_background(
                    context=context,
                    admin_chat_id=update.effective_chat.id,
                    reply_to_message_id=update.effective_message.message_id if update.effective_message else None,
                    data=safe_payload,
                )
            )
            _set_broadcast_task(context, task)

            try:
                await query.edit_message_text("🚀 Broadcast iniciado. Acompanhe pelo chat.")
            except Exception:
                pass
            return

    except Exception as e:
        print(f"[BROADCAST][ERRO] {repr(e)}")
        raise
    finally:
        _release_guard(context)


async def broadcast_message_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.effective_message

    if not user or not message or not _is_admin(user.id):
        return

    state = _get_state(context)
    if not state:
        return

    data = _get_data(context)

    if state == "awaiting_mode":
        text = (message.text or "").strip().lower()

        if text == "1":
            data["mode"] = "all"
            data["target_user_id"] = None
            _set_state(context, "")
            await message.reply_text("✅ Destino definido: todos os usuários.")
            return

        if text == "2":
            data["mode"] = "single"
            _set_state(context, "awaiting_target_user_id")
            await message.reply_text(
                "👤 Agora envie o <b>ID do usuário</b> que vai receber a transmissão.",
                parse_mode=ParseMode.HTML,
            )
            return

        await message.reply_text("⚠️ Envie apenas 1 ou 2.")
        return

    if state == "awaiting_target_user_id":
        raw = (message.text or "").strip()

        if not raw.isdigit():
            await message.reply_text("⚠️ Envie um ID numérico válido.")
            return

        data["target_user_id"] = int(raw)
        _set_state(context, "")
        await message.reply_text(
            f"✅ Usuário alvo definido: <code>{raw}</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    if state == "awaiting_media":
        raw = (message.text or "").strip().lower()

        if raw == "pular":
            _set_state(context, "")
            await message.reply_text("↩️ Voltei.")
            return

        if raw == "remover":
            data["photo"] = None
            _set_state(context, "")
            await message.reply_text("✅ Mídia removida.")
            return

        photo = message.photo[-1] if message.photo else None
        if not photo:
            await message.reply_text("⚠️ Envie uma imagem, ou use remover/pular.")
            return

        data["photo"] = photo.file_id
        _set_state(context, "")
        await message.reply_text("✅ Mídia salva.")
        return

    if state == "awaiting_text":
        raw = (message.text or "").strip()

        if raw.lower() == "pular":
            _set_state(context, "")
            await message.reply_text("↩️ Voltei.")
            return

        if raw.lower() == "remover":
            data["text"] = ""
            _set_state(context, "")
            await message.reply_text("✅ Texto removido.")
            return

        data["text"] = raw
        _set_state(context, "")
        await message.reply_text("✅ Texto salvo.")
        return

    if state == "awaiting_button_text":
        raw = (message.text or "").strip()

        if raw.lower() == "pular":
            _set_state(context, "")
            await message.reply_text("↩️ Voltei.")
            return

        if raw.lower() == "remover":
            data["button_text"] = ""
            data["button_url"] = ""
            _set_state(context, "")
            await message.reply_text("✅ Botão removido.")
            return

        data["button_text"] = raw
        _set_state(context, "awaiting_button_url")
        await message.reply_text(
            "🔗 Agora envie a URL do botão.\n\n"
            "Exemplo:\n"
            "<code>https://t.me/seucanal</code>",
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        return

    if state == "awaiting_button_url":
        raw = (message.text or "").strip()

        if raw.lower() == "pular":
            data["button_text"] = ""
            _set_state(context, "")
            await message.reply_text("↩️ Cancelado.")
            return

        if not (
            raw.startswith("http://")
            or raw.startswith("https://")
            or raw.startswith("tg://")
        ):
            await message.reply_text(
                "⚠️ URL inválida. Envie uma URL começando com http://, https:// ou tg://"
            )
            return

        data["button_url"] = raw
        _set_state(context, "")
        await message.reply_text(
            "✅ Botão salvo.",
            disable_web_page_preview=True,
        )
        return
