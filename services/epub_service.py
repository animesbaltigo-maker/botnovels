from __future__ import annotations

import asyncio
import hashlib
import html
import io
import re
import zipfile
from pathlib import Path

from config import DISTRIBUTION_TAG, EPUB_CACHE_DIR, EPUB_NAME_PATTERN, PROMO_BANNER_URL
from core.http_client import get_http_client

EPUB_CACHE_PATH = Path(EPUB_CACHE_DIR)
EPUB_CACHE_PATH.mkdir(parents=True, exist_ok=True)

_BANNER_BYTES: bytes | None = None
_BANNER_LOCK = asyncio.Lock()


def _safe_filename(value: str) -> str:
    value = re.sub(r'[<>:"/\\|?*]+', "", str(value or "").strip())
    value = re.sub(r"\s+", " ", value).strip()
    return value or "Novel"


def _book_name(title_name: str, chapter_number: str) -> str:
    base_name = EPUB_NAME_PATTERN.format(
        title=_safe_filename(title_name),
        chapter=_safe_filename(chapter_number),
    )
    if DISTRIBUTION_TAG.lower() not in base_name.lower():
        stem = base_name[:-5] if base_name.lower().endswith(".epub") else base_name
        base_name = f"{stem} - {DISTRIBUTION_TAG}.epub"
    return base_name


def _epub_path(chapter_id: str) -> Path:
    safe = hashlib.sha1(chapter_id.encode("utf-8")).hexdigest()
    return EPUB_CACHE_PATH / f"{safe}.epub"


async def _download_banner_bytes() -> bytes:
    client = await get_http_client()
    response = await client.get(PROMO_BANNER_URL)
    response.raise_for_status()
    return bytes(response.content)


async def _get_banner_bytes() -> bytes:
    global _BANNER_BYTES
    if _BANNER_BYTES is not None:
        return _BANNER_BYTES

    async with _BANNER_LOCK:
        if _BANNER_BYTES is not None:
            return _BANNER_BYTES
        try:
            _BANNER_BYTES = await _download_banner_bytes()
        except Exception:
            _BANNER_BYTES = b""
        return _BANNER_BYTES


def _normalize_paragraphs(paragraphs: list[str]) -> list[str]:
    return [" ".join(str(item or "").strip().split()) for item in (paragraphs or []) if str(item or "").strip()]


def _chapter_title(title_name: str, chapter_number: str) -> str:
    return f"{title_name} - Capitulo {chapter_number}"


def _xhtml_template(body: str, *, title: str) -> str:
    return (
        '<?xml version="1.0" encoding="utf-8"?>\n'
        '<!DOCTYPE html>\n'
        '<html xmlns="http://www.w3.org/1999/xhtml" lang="pt-BR">\n'
        "<head>\n"
        f"  <title>{html.escape(title)}</title>\n"
        '  <meta charset="utf-8"/>\n'
        '  <link rel="stylesheet" type="text/css" href="styles.css"/>\n'
        "</head>\n"
        f"<body>{body}</body>\n"
        "</html>\n"
    )


def _cover_body(title_name: str, chapter_number: str, has_banner: bool) -> str:
    parts = ['<section class="cover">']
    if has_banner:
        parts.append('<img class="banner" src="banner.jpg" alt="Banner"/>')
    parts.append(f"<h1>{html.escape(title_name)}</h1>")
    parts.append(f"<h2>Capitulo {html.escape(str(chapter_number))}</h2>")
    parts.append(f"<p class=\"tag\">{html.escape(DISTRIBUTION_TAG)}</p>")
    parts.append("</section>")
    return "".join(parts)


def _chapter_body(title_name: str, chapter_number: str, paragraphs: list[str]) -> str:
    parts = [
        '<section class="chapter">',
        f"<h1>{html.escape(_chapter_title(title_name, chapter_number))}</h1>",
    ]
    for paragraph in paragraphs:
        parts.append(f"<p>{html.escape(paragraph)}</p>")
    parts.append(f"<p class=\"tag\">{html.escape(DISTRIBUTION_TAG)}</p>")
    parts.append("</section>")
    return "".join(parts)


def _stylesheet() -> str:
    return """
body {
  font-family: Georgia, serif;
  line-height: 1.6;
  margin: 0;
  padding: 0;
  color: #111111;
}
.cover, .chapter {
  padding: 2rem 1.6rem;
}
.cover {
  text-align: center;
}
.banner {
  display: block;
  width: 100%;
  max-width: 860px;
  margin: 0 auto 1.5rem auto;
  border-radius: 0.6rem;
}
h1 {
  font-size: 1.7rem;
  margin: 0 0 0.8rem 0;
}
h2 {
  font-size: 1.15rem;
  margin: 0 0 1.2rem 0;
  color: #666666;
}
p {
  margin: 0 0 1rem 0;
  text-align: justify;
}
.tag {
  margin-top: 2rem;
  color: #666666;
  font-size: 0.95rem;
  text-align: center;
}
""".strip()


def _container_xml() -> str:
    return (
        '<?xml version="1.0"?>\n'
        '<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">\n'
        '  <rootfiles>\n'
        '    <rootfile full-path="OEBPS/content.opf" media-type="application/oebps-package+xml"/>\n'
        '  </rootfiles>\n'
        '</container>\n'
    )


def _content_opf(title_name: str, chapter_number: str, identifier: str, include_banner: bool) -> str:
    manifest_extra = (
        '    <item id="banner" href="banner.jpg" media-type="image/jpeg"/>\n'
        if include_banner
        else ""
    )
    return (
        '<?xml version="1.0" encoding="utf-8"?>\n'
        '<package xmlns="http://www.idpf.org/2007/opf" version="2.0" unique-identifier="bookid">\n'
        '  <metadata xmlns:dc="http://purl.org/dc/elements/1.1/">\n'
        f'    <dc:title>{html.escape(_chapter_title(title_name, chapter_number))}</dc:title>\n'
        f'    <dc:creator>{html.escape(DISTRIBUTION_TAG)}</dc:creator>\n'
        '    <dc:language>pt-BR</dc:language>\n'
        f'    <dc:identifier id="bookid">{html.escape(identifier)}</dc:identifier>\n'
        '  </metadata>\n'
        '  <manifest>\n'
        '    <item id="ncx" href="toc.ncx" media-type="application/x-dtbncx+xml"/>\n'
        '    <item id="cover" href="cover.xhtml" media-type="application/xhtml+xml"/>\n'
        '    <item id="chapter" href="chapter.xhtml" media-type="application/xhtml+xml"/>\n'
        '    <item id="styles" href="styles.css" media-type="text/css"/>\n'
        f"{manifest_extra}"
        '  </manifest>\n'
        '  <spine toc="ncx">\n'
        '    <itemref idref="cover"/>\n'
        '    <itemref idref="chapter"/>\n'
        '  </spine>\n'
        '</package>\n'
    )


def _toc_ncx(title_name: str, chapter_number: str, identifier: str) -> str:
    book_title = _chapter_title(title_name, chapter_number)
    return (
        '<?xml version="1.0" encoding="utf-8"?>\n'
        '<ncx xmlns="http://www.daisy.org/z3986/2005/ncx/" version="2005-1">\n'
        '  <head>\n'
        f'    <meta name="dtb:uid" content="{html.escape(identifier)}"/>\n'
        '  </head>\n'
        f'  <docTitle><text>{html.escape(book_title)}</text></docTitle>\n'
        '  <navMap>\n'
        '    <navPoint id="cover" playOrder="1">\n'
        '      <navLabel><text>Capa</text></navLabel>\n'
        '      <content src="cover.xhtml"/>\n'
        '    </navPoint>\n'
        '    <navPoint id="chapter" playOrder="2">\n'
        f'      <navLabel><text>{html.escape(book_title)}</text></navLabel>\n'
        '      <content src="chapter.xhtml"/>\n'
        '    </navPoint>\n'
        '  </navMap>\n'
        '</ncx>\n'
    )


def _build_epub_bytes(title_name: str, chapter_number: str, chapter_id: str, paragraphs: list[str], banner_bytes: bytes) -> bytes:
    identifier = hashlib.sha1(f"{chapter_id}:{chapter_number}".encode("utf-8")).hexdigest()
    include_banner = bool(banner_bytes)
    cover_xhtml = _xhtml_template(
        _cover_body(title_name, chapter_number, include_banner),
        title=f"{title_name} - Capa",
    )
    chapter_xhtml = _xhtml_template(
        _chapter_body(title_name, chapter_number, paragraphs),
        title=_chapter_title(title_name, chapter_number),
    )

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as epub:
        epub.writestr("mimetype", "application/epub+zip", compress_type=zipfile.ZIP_STORED)
        epub.writestr("META-INF/container.xml", _container_xml())
        epub.writestr("OEBPS/content.opf", _content_opf(title_name, chapter_number, identifier, include_banner))
        epub.writestr("OEBPS/toc.ncx", _toc_ncx(title_name, chapter_number, identifier))
        epub.writestr("OEBPS/styles.css", _stylesheet())
        epub.writestr("OEBPS/cover.xhtml", cover_xhtml)
        epub.writestr("OEBPS/chapter.xhtml", chapter_xhtml)
        if include_banner:
            epub.writestr("OEBPS/banner.jpg", banner_bytes)
    return buffer.getvalue()


async def get_or_build_epub(
    chapter_id: str,
    chapter_number: str,
    title_name: str,
    paragraphs: list[str],
    progress_cb=None,
) -> tuple[str, str]:
    epub_path = _epub_path(chapter_id)
    epub_name = _book_name(title_name, chapter_number)

    if epub_path.exists():
        return str(epub_path), epub_name

    normalized_paragraphs = _normalize_paragraphs(paragraphs)
    if not normalized_paragraphs:
        raise RuntimeError("Nenhum texto encontrado para gerar o EPUB.")

    if progress_cb:
        await progress_cb(1, 3)

    banner_bytes = await _get_banner_bytes()

    if progress_cb:
        await progress_cb(2, 3)

    epub_bytes = await asyncio.to_thread(
        _build_epub_bytes,
        title_name,
        chapter_number,
        chapter_id,
        normalized_paragraphs,
        banner_bytes,
    )

    if progress_cb:
        await progress_cb(3, 3)

    await asyncio.to_thread(epub_path.write_bytes, epub_bytes)
    return str(epub_path), epub_name
