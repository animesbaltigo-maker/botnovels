from __future__ import annotations

from telegram.error import BadRequest, Forbidden


def normalize_channel_target(value):
    if value in (None, ""):
        return value

    if isinstance(value, int):
        return value

    text = str(value).strip()
    if not text:
        return text

    lowered = text.lower()
    for prefix in ("https://t.me/", "http://t.me/", "https://telegram.me/", "http://telegram.me/", "t.me/", "telegram.me/"):
        if lowered.startswith(prefix):
            text = text[len(prefix):].strip()
            break

    text = text.lstrip("@").strip().strip("/")
    if not text:
        return value

    if text.startswith("-100") and text[1:].isdigit():
        return int(text)
    if text.isdigit() or (text.startswith("-") and text[1:].isdigit()):
        return int(text)

    return f"@{text}"


async def ensure_channel_target(bot, value):
    target = normalize_channel_target(value)
    if target in (None, ""):
        return target

    try:
        await bot.get_chat(target)
        return target
    except BadRequest as error:
        message = str(error).lower()
        if "chat not found" in message:
            raise RuntimeError(
                f"Destino de canal invalido: {value!r}. Use @canal ou o ID numerico do canal."
            ) from error
        raise
    except Forbidden as error:
        raise RuntimeError(
            f"O bot nao tem acesso ao canal configurado em {value!r}. Adicione o bot como administrador."
        ) from error
