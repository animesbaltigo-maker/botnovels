from __future__ import annotations

import asyncio
import hashlib
import io
import re
import textwrap
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

from config import DISTRIBUTION_TAG, PDF_CACHE_DIR, PDF_NAME_PATTERN, PROMO_BANNER_URL
from core.http_client import get_http_client

PDF_CACHE_PATH = Path(PDF_CACHE_DIR)
PDF_CACHE_PATH.mkdir(parents=True, exist_ok=True)

PAGE_WIDTH = 1240
PAGE_HEIGHT = 1754
MARGIN_X = 96
MARGIN_Y = 96
CONTENT_WIDTH = PAGE_WIDTH - (MARGIN_X * 2)
CONTENT_HEIGHT = PAGE_HEIGHT - (MARGIN_Y * 2)

_BANNER_CACHE: Image.Image | None = None
_BANNER_LOCK = asyncio.Lock()


def _safe_filename(value: str) -> str:
    value = re.sub(r'[<>:"/\\|?*]+', "", str(value or "").strip())
    value = re.sub(r"\s+", " ", value).strip()
    return value or "Novel"


def _pdf_name(title_name: str, chapter_number: str) -> str:
    base_name = PDF_NAME_PATTERN.format(
        title=_safe_filename(title_name),
        chapter=_safe_filename(chapter_number),
    )
    if DISTRIBUTION_TAG.lower() not in base_name.lower():
        stem = base_name[:-4] if base_name.lower().endswith(".pdf") else base_name
        base_name = f"{stem} - {DISTRIBUTION_TAG}.pdf"
    return base_name


def _pdf_path(chapter_id: str) -> Path:
    safe = hashlib.sha1(chapter_id.encode("utf-8")).hexdigest()
    return PDF_CACHE_PATH / f"{safe}.pdf"


def _font_candidates() -> list[str]:
    return [
        "DejaVuSans.ttf",
        "arial.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf",
        "C:/Windows/Fonts/arial.ttf",
    ]


def _load_font(size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    for candidate in _font_candidates():
        try:
            return ImageFont.truetype(candidate, size=size)
        except Exception:
            continue
    return ImageFont.load_default()


TITLE_FONT = _load_font(50)
SUBTITLE_FONT = _load_font(34)
BODY_FONT = _load_font(32)
FOOTER_FONT = _load_font(24)


async def _download_banner_image() -> Image.Image:
    client = await get_http_client()
    response = await client.get(PROMO_BANNER_URL)
    response.raise_for_status()
    image = Image.open(io.BytesIO(response.content)).convert("RGB")
    return image


async def _get_banner_image() -> Image.Image:
    global _BANNER_CACHE
    if _BANNER_CACHE is not None:
        return _BANNER_CACHE.copy()

    async with _BANNER_LOCK:
        if _BANNER_CACHE is not None:
            return _BANNER_CACHE.copy()
        try:
            _BANNER_CACHE = await _download_banner_image()
        except Exception:
            _BANNER_CACHE = Image.new("RGB", (PAGE_WIDTH, PAGE_HEIGHT), "white")
            draw = ImageDraw.Draw(_BANNER_CACHE)
            draw.text((MARGIN_X, MARGIN_Y), DISTRIBUTION_TAG, fill="black", font=TITLE_FONT)
        return _BANNER_CACHE.copy()


def _measure_text(draw: ImageDraw.ImageDraw, text: str, font) -> int:
    left, top, right, bottom = draw.textbbox((0, 0), text, font=font)
    return max(0, right - left)


def _line_height(draw: ImageDraw.ImageDraw, font) -> int:
    left, top, right, bottom = draw.textbbox((0, 0), "Ag", font=font)
    return max(20, bottom - top + 10)


def _wrap_paragraph(draw: ImageDraw.ImageDraw, text: str, font, width: int) -> list[str]:
    text = " ".join(str(text or "").strip().split())
    if not text:
        return []

    words = text.split()
    lines: list[str] = []
    current = words[0]
    for word in words[1:]:
        candidate = f"{current} {word}"
        if _measure_text(draw, candidate, font) <= width:
            current = candidate
        else:
            lines.append(current)
            current = word
    if current:
        lines.append(current)
    return lines


def _new_page() -> tuple[Image.Image, ImageDraw.ImageDraw]:
    image = Image.new("RGB", (PAGE_WIDTH, PAGE_HEIGHT), "white")
    return image, ImageDraw.Draw(image)


def _render_banner_page_sync(title_name: str, chapter_number: str) -> Image.Image:
    base = Image.new("RGB", (PAGE_WIDTH, PAGE_HEIGHT), "white")
    banner = asyncio.run(_get_banner_image())  # not used directly; async wrapper handles cache
    banner = banner.copy()
    banner.thumbnail((PAGE_WIDTH, PAGE_HEIGHT - 260))
    paste_x = (PAGE_WIDTH - banner.width) // 2
    paste_y = 80
    base.paste(banner, (paste_x, paste_y))

    draw = ImageDraw.Draw(base)
    title = _safe_filename(title_name)
    subtitle = f"Capitulo {chapter_number} | {DISTRIBUTION_TAG}"
    draw.text((MARGIN_X, PAGE_HEIGHT - 220), title, fill="black", font=TITLE_FONT)
    draw.text((MARGIN_X, PAGE_HEIGHT - 150), subtitle, fill="#444444", font=SUBTITLE_FONT)
    return base


async def _render_banner_page(title_name: str, chapter_number: str) -> Image.Image:
    banner = await _get_banner_image()

    def _build() -> Image.Image:
        base = Image.new("RGB", (PAGE_WIDTH, PAGE_HEIGHT), "white")
        local_banner = banner.copy()
        local_banner.thumbnail((PAGE_WIDTH, PAGE_HEIGHT - 260))
        paste_x = (PAGE_WIDTH - local_banner.width) // 2
        paste_y = 80
        base.paste(local_banner, (paste_x, paste_y))

        draw = ImageDraw.Draw(base)
        title = _safe_filename(title_name)
        subtitle = f"Capitulo {chapter_number} | {DISTRIBUTION_TAG}"
        draw.text((MARGIN_X, PAGE_HEIGHT - 220), title, fill="black", font=TITLE_FONT)
        draw.text((MARGIN_X, PAGE_HEIGHT - 150), subtitle, fill="#444444", font=SUBTITLE_FONT)
        return base

    return await asyncio.to_thread(_build)


def _build_text_pages_sync(title_name: str, chapter_number: str, paragraphs: list[str]) -> list[Image.Image]:
    pages: list[Image.Image] = []
    page, draw = _new_page()
    y = MARGIN_Y
    body_line_height = _line_height(draw, BODY_FONT)
    section_gap = 18

    heading = _safe_filename(title_name)
    subheading = f"Capitulo {chapter_number}"
    draw.text((MARGIN_X, y), heading, fill="black", font=TITLE_FONT)
    y += _line_height(draw, TITLE_FONT) + 16
    draw.text((MARGIN_X, y), subheading, fill="#444444", font=SUBTITLE_FONT)
    y += _line_height(draw, SUBTITLE_FONT) + 28

    for paragraph in paragraphs:
        wrapped = _wrap_paragraph(draw, paragraph, BODY_FONT, CONTENT_WIDTH)
        if not wrapped:
            continue

        needed = (len(wrapped) * body_line_height) + section_gap
        if y + needed > PAGE_HEIGHT - MARGIN_Y - 80:
            draw.text((MARGIN_X, PAGE_HEIGHT - MARGIN_Y), DISTRIBUTION_TAG, fill="#666666", font=FOOTER_FONT)
            pages.append(page)
            page, draw = _new_page()
            y = MARGIN_Y

        for line in wrapped:
            draw.text((MARGIN_X, y), line, fill="black", font=BODY_FONT)
            y += body_line_height
        y += section_gap

    draw.text((MARGIN_X, PAGE_HEIGHT - MARGIN_Y), DISTRIBUTION_TAG, fill="#666666", font=FOOTER_FONT)
    pages.append(page)
    return pages


def _save_pdf(pdf_path: Path, images: list[Image.Image]) -> None:
    if not images:
        raise RuntimeError("Nenhuma pagina foi gerada para o PDF.")
    first = images[0]
    rest = images[1:]
    first.save(pdf_path, save_all=True, append_images=rest, resolution=150.0)


async def get_or_build_pdf(
    chapter_id: str,
    chapter_number: str,
    title_name: str,
    paragraphs: list[str],
    progress_cb=None,
) -> tuple[str, str]:
    pdf_path = _pdf_path(chapter_id)
    pdf_name = _pdf_name(title_name, chapter_number)

    if pdf_path.exists():
        return str(pdf_path), pdf_name

    normalized_paragraphs = [" ".join(str(item or "").strip().split()) for item in (paragraphs or []) if str(item or "").strip()]
    if not normalized_paragraphs:
        raise RuntimeError("Nenhum texto encontrado para gerar o PDF.")

    pages: list[Image.Image] = []

    if progress_cb:
        await progress_cb(1, 3)

    banner_page = await _render_banner_page(title_name, chapter_number)
    pages.append(banner_page)

    if progress_cb:
        await progress_cb(2, 3)

    text_pages = await asyncio.to_thread(
        _build_text_pages_sync,
        title_name,
        chapter_number,
        normalized_paragraphs,
    )
    pages.extend(text_pages)

    if progress_cb:
        await progress_cb(3, 3)

    await asyncio.to_thread(_save_pdf, pdf_path, pages)
    return str(pdf_path), pdf_name
