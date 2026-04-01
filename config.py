import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)


def _load_local_env() -> None:
    env_path = BASE_DIR / ".env"
    if not env_path.exists():
        return

    try:
        raw_lines = env_path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return

    for raw_line in raw_lines:
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key or key in os.environ:
            continue
        os.environ[key] = value.strip().strip('"').strip("'")


_load_local_env()


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name, "").strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "on", "sim", "s"}:
        return True
    if raw in {"0", "false", "no", "off", "nao", "nÃ£o", "n"}:
        return False
    return default


BOT_TOKEN = os.getenv("BOT_TOKEN", "8625342322:AAFekc5f1I0vp2MIxlSpKW1uZM56nUu5qok").strip()
BOT_USERNAME = os.getenv("BOT_USERNAME", "NovelsBrasil_Bot").strip().lstrip("@")
BOT_BRAND = os.getenv("BOT_BRAND", "Novels Baltigo").strip() or "Novels Baltigo"
CATALOG_SITE_BASE = (
    os.getenv("CATALOG_SITE_BASE", "").strip()
    or "https://centralnovel.com"
).rstrip("/")

REQUIRED_CHANNEL = os.getenv("REQUIRED_CHANNEL", "@NovelsBrasil").strip()
REQUIRED_CHANNEL_URL = os.getenv("REQUIRED_CHANNEL_URL", "https://t.me/NovelsBrasil").strip()

CANAL_POSTAGEM = os.getenv("CANAL_POSTAGEM", "@NovelsBrasil").strip()
CANAL_POSTAGEM_NOVELS = (
    os.getenv("CANAL_POSTAGEM_NOVELS", "@NovelsBrasil").strip()
    or os.getenv("CANAL_POSTAGEM", "@NovelsBrasil").strip()
)
CANAL_POSTAGEM_NOVEL_CAPITULOS = (
    os.getenv("CANAL_POSTAGEM_NOVEL_CAPITULOS", "@AtualizacoesOn").strip()
    or os.getenv("CANAL_POSTAGEM_CAPITULOS", "@AtualizacoesOn").strip()
)

ADMIN_IDS = [
    int(value.strip())
    for value in os.getenv("ADMIN_IDS", "1852596083").split(",")
    if value.strip().isdigit()
]

SEARCH_LIMIT = _env_int("SEARCH_LIMIT", 10)
CHAPTERS_PER_PAGE = _env_int("CHAPTERS_PER_PAGE", 15)
HOME_SECTION_LIMIT = _env_int("HOME_SECTION_LIMIT", 8)
API_CACHE_TTL_SECONDS = _env_int("API_CACHE_TTL_SECONDS", 1800)
HTTP_TIMEOUT = _env_int("HTTP_TIMEOUT", 35)
ANTI_FLOOD_SECONDS = _env_float("ANTI_FLOOD_SECONDS", 1.0)
AUTO_POST_LIMIT = _env_int("AUTO_POST_LIMIT", 8)

PROMO_BANNER_URL = os.getenv(
    "PROMO_BANNER_URL",
    "https://photo.chelpbot.me/AgACAgEAAxkBaVoTGGnNi8MFSYpTv6T5RQ1sZrFGXlCTAALEC2sbbQtxRpemdjcbCk3sAQADAgADeQADOgQ/photo.jpg",
).strip()
TELEGRAPH_AUTHOR = os.getenv("TELEGRAPH_AUTHOR", BOT_BRAND).strip() or BOT_BRAND
DISTRIBUTION_TAG = os.getenv("DISTRIBUTION_TAG", "@MangasBrasil").strip() or "@MangasBrasil"
PDF_CACHE_DIR = str(DATA_DIR / "pdf_cache")
PDF_NAME_PATTERN = os.getenv("PDF_NAME_PATTERN", "{title} - Capitulo {chapter}.pdf").strip() or "{title} - Capitulo {chapter}.pdf"
EPUB_CACHE_DIR = str(DATA_DIR / "epub_cache")
EPUB_NAME_PATTERN = os.getenv("EPUB_NAME_PATTERN", "{title} - Capitulo {chapter}.epub").strip() or "{title} - Capitulo {chapter}.epub"
PDF_QUEUE_LIMIT = _env_int("PDF_QUEUE_LIMIT", 30)
PDF_WORKERS_SINGLE = _env_int("PDF_WORKERS_SINGLE", 1)
PDF_WORKERS_BULK = _env_int("PDF_WORKERS_BULK", 1)
PDF_PROTECT_CONTENT = _env_bool("PDF_PROTECT_CONTENT", True)
STICKER_DIVISOR = os.getenv("STICKER_DIVISOR", "").strip()

