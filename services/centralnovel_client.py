from __future__ import annotations

import asyncio
import hashlib
import os
import re
import time
import unicodedata
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus, urljoin, urlparse

import httpx
from bs4 import BeautifulSoup, Tag

if not os.getenv("PLAYWRIGHT_BROWSERS_PATH") and os.path.isdir("/root/.cache/ms-playwright"):
    os.environ["PLAYWRIGHT_BROWSERS_PATH"] = "/root/.cache/ms-playwright"
os.environ.setdefault("PLAYWRIGHT_SKIP_BROWSER_GC", "1")

try:
    from playwright.async_api import async_playwright
except Exception:  # pragma: no cover - optional dependency at runtime
    async_playwright = None

from config import (
    API_CACHE_TTL_SECONDS,
    CATALOG_SITE_BASE,
    HOME_SECTION_LIMIT,
    HTTP_TIMEOUT,
    SEARCH_LIMIT,
)
from core.http_client import get_http_client

BASE_URL = CATALOG_SITE_BASE.rstrip("/")
ARCHIVE_URL = f"{BASE_URL}/series/list-mode/"
RECENT_UPDATES_URL = f"{BASE_URL}/series/?status=&type=&order=update"
BLOG_URL = f"{BASE_URL}/blog/"
BROWSER_TIMEOUT_MS = 25000
HTTP_TIMEOUT_SECONDS = max(10, HTTP_TIMEOUT)
SEARCH_TTL = max(900, API_CACHE_TTL_SECONDS)
BUNDLE_TTL = max(1800, API_CACHE_TTL_SECONDS)
CHAPTER_TTL = max(1800, API_CACHE_TTL_SECONDS)
HOME_TTL = max(900, API_CACHE_TTL_SECONDS)
RECENT_TTL = max(600, API_CACHE_TTL_SECONDS)
SEARCH_CACHE_VERSION = "v1"

_CACHE: dict[str, dict[str, Any]] = {}
_INFLIGHT: dict[str, asyncio.Task] = {}
_SERIES_REF_INDEX: dict[str, str] = {}
_CHAPTER_REF_INDEX: dict[str, str] = {}
_CHAPTER_TO_SERIES: dict[str, str] = {}
_WARMUP_TASK: asyncio.Task | None = None
_BROWSER_FETCH_SEMAPHORE = asyncio.Semaphore(2)

_BROWSER_ROOT_CANDIDATES = [
    os.getenv("PLAYWRIGHT_BROWSERS_PATH", "").strip(),
    "/root/.cache/ms-playwright",
    "/app/.playwright",
    "/ms-playwright",
    str(Path.home() / ".cache" / "ms-playwright"),
]


def _clean(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _normalize_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", _clean(value).lower())
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    normalized = re.sub(r"[^a-z0-9\s-]", " ", normalized)
    return re.sub(r"\s+", " ", normalized).strip()


def _cache_get(key: str, ttl: int) -> Any | None:
    item = _CACHE.get(key)
    if not item:
        return None
    if time.time() - float(item["time"]) > ttl:
        _CACHE.pop(key, None)
        return None
    return item["data"]


def _cache_set(key: str, data: Any) -> Any:
    _CACHE[key] = {"time": time.time(), "data": data}
    return data


async def _dedup_fetch(key: str, ttl: int, coro_factory):
    cached = _cache_get(key, ttl)
    if cached is not None:
        return cached

    task = _INFLIGHT.get(key)
    if task:
        return await task

    async def _runner():
        return await coro_factory()

    task = asyncio.create_task(_runner())
    _INFLIGHT[key] = task
    try:
        data = await task
        return _cache_set(key, data)
    finally:
        _INFLIGHT.pop(key, None)


def _absolute_url(value: str) -> str:
    text = _clean(value)
    if not text:
        return ""
    return urljoin(f"{BASE_URL}/", text)


def _image_from_tag(img: Tag | None) -> str:
    if not img:
        return ""
    for attr in ("data-src", "data-lazy-src", "data-original", "src"):
        value = _clean(img.get(attr))
        if value:
            return _absolute_url(value)
    srcset = _clean(img.get("srcset") or img.get("data-srcset") or "")
    if srcset:
        first = srcset.split(",", 1)[0].strip().split(" ", 1)[0]
        return _absolute_url(first)
    return ""


def _key_from_value(value: str) -> str:
    return hashlib.sha1(_clean(value).encode("utf-8")).hexdigest()[:12]


def _series_key(url: str) -> str:
    slug = _series_slug(url)
    return slug or _key_from_value(_absolute_url(url))


def _chapter_key(url: str) -> str:
    slug = _chapter_slug(url)
    return slug or _key_from_value(_absolute_url(url))


def _remember_series(url: str) -> str:
    absolute = _absolute_url(url)
    key = _series_key(absolute)
    if absolute:
        _SERIES_REF_INDEX[key] = absolute
        legacy_key = _key_from_value(absolute)
        _SERIES_REF_INDEX[legacy_key] = absolute
    return key


def _remember_chapter(url: str, series_id: str = "") -> str:
    absolute = _absolute_url(url)
    key = _chapter_key(absolute)
    if absolute:
        _CHAPTER_REF_INDEX[key] = absolute
        legacy_key = _key_from_value(absolute)
        _CHAPTER_REF_INDEX[legacy_key] = absolute
    if series_id:
        _CHAPTER_TO_SERIES[key] = series_id
        _CHAPTER_TO_SERIES[_key_from_value(absolute)] = series_id
    return key


def _series_slug(series_url: str) -> str:
    path = urlparse(_absolute_url(series_url)).path
    match = re.search(r"/series/([^/?#]+)/?$", path, flags=re.IGNORECASE)
    return _clean(match.group(1)) if match else ""


def _series_slug_prefixes(series_url: str) -> list[str]:
    slug = _series_slug(series_url)
    if not slug:
        return []

    prefixes = [slug]
    base_slug = re.sub(r"-\d{6,10}$", "", slug)
    if base_slug and base_slug not in prefixes:
        prefixes.append(base_slug)
    return prefixes


def _chapter_slug(href: str) -> str:
    path = urlparse(_absolute_url(href)).path.strip("/")
    if not path:
        return ""
    return path.split("/", 1)[0]


def _chapter_belongs_to_series(href: str, series_url: str) -> bool:
    slug = _chapter_slug(href).lower()
    if not slug:
        return False
    prefixes = _series_slug_prefixes(series_url)
    if not prefixes:
        return True
    return any(slug.startswith(f"{prefix.lower()}-capitulo") for prefix in prefixes)


def _resolve_series_ref(series_ref: str) -> str:
    ref = _clean(series_ref)
    if not ref:
        return ""
    if ref in _SERIES_REF_INDEX:
        return _SERIES_REF_INDEX[ref]
    if ref.startswith(("http://", "https://")):
        return _absolute_url(ref)
    return _absolute_url(f"/series/{ref.strip('/')}/")


def _resolve_chapter_ref(chapter_ref: str) -> str:
    ref = _clean(chapter_ref)
    if not ref:
        return ""
    if ref in _CHAPTER_REF_INDEX:
        return _CHAPTER_REF_INDEX[ref]
    if ref.startswith(("http://", "https://")):
        return _absolute_url(ref)
    return _absolute_url(f"/{ref.strip('/')}/")


def _decimal_sort_value(value: Any) -> Decimal:
    text = _clean(value)
    if not text:
        return Decimal("0")
    try:
        return Decimal(text)
    except InvalidOperation:
        cleaned = re.sub(r"[^0-9.]", "", text)
        try:
            return Decimal(cleaned or "0")
        except InvalidOperation:
            return Decimal("0")


def _extract_meta_content(soup: BeautifulSoup, name: str) -> str:
    node = soup.find("meta", attrs={"property": name}) or soup.find("meta", attrs={"name": name})
    if not node:
        return ""
    return _clean(node.get("content"))


def _clean_title(raw: str) -> str:
    text = _clean(raw)
    text = re.sub(r"\s+\|\s+Central Novel$", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+[-|]\s+Cap[ií]tulo.*$", "", text, flags=re.IGNORECASE)
    return text.strip(" -|")


def _extract_lines(soup: BeautifulSoup) -> list[str]:
    return [_clean(line) for line in soup.get_text("\n").splitlines() if _clean(line)]


def _browser_root() -> str:
    for candidate in _BROWSER_ROOT_CANDIDATES:
        if candidate and candidate != "0" and Path(candidate).exists():
            return candidate
    return ""


def _resolve_playwright_executable() -> str:
    root = _browser_root()
    if not root:
        return ""

    patterns = [
        "chromium-*/chrome-linux/chrome",
        "chromium-*/chrome-win/chrome.exe",
        "chromium_headless_shell-*/chrome-linux/headless_shell",
        "chromium_headless_shell-*/chrome-win/headless_shell.exe",
    ]
    root_path = Path(root)
    for pattern in patterns:
        for match in sorted(root_path.glob(pattern), reverse=True):
            if match.is_file():
                return str(match)
    return ""


def _playwright_launch_kwargs() -> dict[str, Any]:
    executable_path = _resolve_playwright_executable()
    if executable_path:
        return {"headless": True, "executable_path": executable_path}
    return {"headless": True}


async def _fetch_html_via_playwright(url: str) -> str:
    if async_playwright is None:
        raise RuntimeError("Playwright nao esta instalado neste ambiente.")

    async with _BROWSER_FETCH_SEMAPHORE:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(**_playwright_launch_kwargs())
            page = await browser.new_page()
            await page.goto(url, wait_until="domcontentloaded", timeout=BROWSER_TIMEOUT_MS)
            await page.wait_for_timeout(500)
            html_text = await page.content()
            await browser.close()
    return html_text


async def _fetch_html(url: str) -> str:
    client = await get_http_client()
    try:
        response = await client.get(url, timeout=httpx.Timeout(float(HTTP_TIMEOUT_SECONDS)))
        response.raise_for_status()
        text = response.text
        if len(text) > 1000:
            return text
    except Exception:
        pass
    return await _fetch_html_via_playwright(url)


def _series_href_matches(href: str) -> bool:
    return bool(re.search(r"/series/[^/?#]+/?$", href, flags=re.IGNORECASE))


def _chapter_href_matches(href: str) -> bool:
    lowered = _clean(href).lower()
    if not lowered or "/series/" in lowered:
        return False
    return "capitulo" in lowered


def _clean_tag_text(text: str) -> str:
    value = _clean(text)
    blockers = {
        "series lists",
        "pesquisar",
        "modo imagem",
        "modo texto",
        "switch mode",
        "genero todos",
        "tipo todos",
        "status todos",
        "ordenar por todos",
    }
    if not value or _normalize_text(value) in blockers:
        return ""
    return value


def _find_container(anchor: Tag) -> Tag:
    for parent in [anchor, *anchor.parents]:
        if not isinstance(parent, Tag):
            continue
        if parent.name in {"article", "li"}:
            return parent
        if parent.name == "div":
            text = _clean(parent.get_text(" ", strip=True))
            series_links = [
                link
                for link in parent.find_all("a", href=True)
                if _series_href_matches(_absolute_url(link.get("href")))
            ]
            if len(text) > 20 and len(series_links) <= 3:
                return parent
    return anchor


def _parse_series_archive(html_text: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html_text, "html.parser")
    seen: set[str] = set()
    results: list[dict[str, Any]] = []

    for anchor in soup.find_all("a", href=True):
        href = _absolute_url(anchor.get("href"))
        if not _series_href_matches(href):
            continue
        if href.endswith("/series/") or href.endswith("/series/list-mode/"):
            continue
        if href in seen:
            continue

        container = _find_container(anchor)
        title = _clean_tag_text(anchor.get_text(" ", strip=True))
        if not title:
            title = _clean_tag_text(anchor.get("title") or "")
        if not title:
            image = anchor.find("img")
            if image:
                title = _clean_tag_text(image.get("alt") or image.get("title") or "")
        if not title:
            heading = container.find(["h1", "h2", "h3", "h4", "strong"])
            title = _clean_tag_text(heading.get_text(" ", strip=True) if heading else "")
        if not title or len(title) < 2:
            continue

        container_text = _clean(container.get_text("\n", strip=True))
        img = container.find("img") or anchor.find("img")
        if not img:
            for parent in container.parents:
                if not isinstance(parent, Tag):
                    continue
                img = parent.find("img")
                if img or parent.name in {"article", "li"}:
                    break
        cover_url = _image_from_tag(img)
        latest_match = re.search(
            r"(?:Vol\.\s*\d+\s*)?Cap\.\s*([0-9]+(?:\.[0-9]+)?)",
            container_text,
            flags=re.IGNORECASE,
        )
        latest_chapter = latest_match.group(1) if latest_match else ""

        status = ""
        lowered = _normalize_text(container_text)
        if any(token in lowered for token in ("completo", "completa", "finalizado", "fim")):
            status = "Completo"
        elif "hiato" in lowered:
            status = "Hiato"
        elif any(token in lowered for token in ("andamento", "em lancamento", "lancando")):
            status = "Em andamento"

        series_id = _remember_series(href)
        seen.add(href)
        results.append(
            {
                "novel_id": series_id,
                "title_id": series_id,
                "title": title,
                "display_title": title,
                "cover_url": cover_url,
                "banner_url": cover_url,
                "status": status,
                "latest_chapter": latest_chapter,
                "source_url": href,
            }
        )

    return results


def _parse_blog_posts(html_text: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html_text, "html.parser")
    seen: set[str] = set()
    results: list[dict[str, Any]] = []
    candidates = soup.select("article, .blogbox, .post, .hentry")

    if not candidates:
        candidates = [node for node in soup.find_all(["div", "li"]) if node.find("a", href=True)]

    for node in candidates:
        if not isinstance(node, Tag):
            continue
        link = (
            node.select_one(".entry-title a[href], h1 a[href], h2 a[href], h3 a[href]")
            or node.find("a", href=True)
        )
        if not link:
            continue
        href = _absolute_url(link.get("href"))
        path = urlparse(href).path.strip("/")
        if not href or href in seen or not path or path in {"blog", "blog/"}:
            continue
        if "/series/" in href or "capitulo" in path.lower():
            continue

        title = _clean_tag_text(link.get_text(" ", strip=True) or link.get("title") or "")
        if not title:
            img_for_title = link.find("img")
            title = _clean_tag_text(img_for_title.get("alt") or img_for_title.get("title") or "") if img_for_title else ""
        if not title or len(title) < 3:
            continue

        image_url = _image_from_tag(node.find("img"))
        excerpt_node = node.select_one(".entry-content p, .excerpt, .blog-excerpt, p")
        excerpt = _clean(excerpt_node.get_text(" ", strip=True) if excerpt_node else "")
        time_node = node.find("time")
        published_at = ""
        if time_node:
            published_at = _clean(time_node.get("datetime") or time_node.get_text(" ", strip=True))
        author_node = node.select_one(".author .fn, .byline .fn, .fn")
        author = _clean(author_node.get_text(" ", strip=True) if author_node else "")

        seen.add(href)
        results.append(
            {
                "title": title,
                "url": href,
                "image_url": image_url,
                "excerpt": excerpt,
                "published_at": published_at,
                "author": author,
            }
        )

    return results


def _merge_series_items(*groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for group in groups:
        for item in group:
            key = _clean(item.get("title_id") or item.get("novel_id") or item.get("source_url"))
            if not key:
                continue
            if key not in merged:
                merged[key] = dict(item)
                order.append(key)
                continue
            current = merged[key]
            for field in ("cover_url", "banner_url", "status", "latest_chapter", "source_url"):
                if item.get(field) and not current.get(field):
                    current[field] = item[field]
    return [merged[key] for key in order]


def _search_score(query: str, title: str) -> tuple[int, int]:
    normalized_query = _normalize_text(query)
    normalized_title = _normalize_text(title)
    if not normalized_query or not normalized_title:
        return (0, 0)
    if normalized_title == normalized_query:
        return (500, -len(normalized_title))
    if normalized_title.startswith(normalized_query):
        return (420, -len(normalized_title))
    if normalized_query in normalized_title:
        return (320, -len(normalized_title))

    query_words = set(normalized_query.split())
    title_words = set(normalized_title.split())
    overlap = len(query_words & title_words)
    return (120 + overlap * 15, -len(normalized_title))


def _extract_labeled_value(lines: list[str], labels: tuple[str, ...]) -> str:
    for index, line in enumerate(lines):
        normalized = _normalize_text(line)
        if not normalized:
            continue
        for label in labels:
            if not normalized.startswith(label):
                continue
            if ":" in line:
                value = _clean(line.split(":", 1)[1])
                if value:
                    return value
            if index + 1 < len(lines):
                next_line = _clean(lines[index + 1])
                next_normalized = _normalize_text(next_line)
                if next_line and next_normalized not in {
                    "status",
                    "tipo",
                    "autor",
                    "sinopse",
                    "generos",
                }:
                    return next_line
    return ""


def _extract_description(lines: list[str]) -> str:
    for index, line in enumerate(lines):
        if "sinopse" not in _normalize_text(line):
            continue

        collected: list[str] = []
        for current in lines[index + 1 :]:
            normalized = _normalize_text(current)
            if not normalized:
                continue
            if normalized in {
                "facebook",
                "twitter",
                "whatsapp",
                "pinterest",
                "telegram",
                "anterior",
                "indice",
                "proximo",
                "capitulos",
                "capítulos",
            }:
                break
            if any(
                normalized.startswith(prefix)
                for prefix in (
                    "status",
                    "tipo",
                    "autor",
                    "lancamento",
                    "atualizado em",
                    "postado em",
                    "generos",
                    "download pack",
                    "ultima leitura",
                    "última leitura",
                    "capitulos",
                    "capítulos",
                )
            ):
                break
            if "{{" in current or "}}" in current:
                break
            if "capitulo" in normalized and len(normalized.split()) <= 5:
                break
            collected.append(current)
            if len(" ".join(collected)) >= 850:
                break
        return " ".join(collected).strip()
    return ""


def _parse_metadata(lines: list[str]) -> dict[str, str]:
    return {
        "status": _extract_labeled_value(lines, ("status",)),
        "type": _extract_labeled_value(lines, ("tipo", "type")),
        "author": _extract_labeled_value(lines, ("autor",)),
        "launch_year": _extract_labeled_value(lines, ("lancamento",)),
        "updated_at": _extract_labeled_value(lines, ("atualizado em",)),
    }


def _extract_genres(soup: BeautifulSoup, lines: list[str]) -> list[str]:
    genres: list[str] = []
    seen: set[str] = set()

    for anchor in soup.find_all("a", href=True):
        href = _clean(anchor.get("href")).lower()
        if not any(token in href for token in ("/genre/", "/genero/", "genre=", "genero=")):
            continue
        name = _clean(anchor.get_text(" ", strip=True))
        if len(name) < 3:
            continue
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        genres.append(name)
        if len(genres) >= 8:
            return genres

    for index, line in enumerate(lines):
        if not _normalize_text(line).startswith("generos"):
            continue
        inline = _clean(line.split(":", 1)[1]) if ":" in line else ""
        candidates = [inline] if inline else lines[index + 1 : index + 4]
        for candidate in candidates:
            for part in re.split(r"[,/#|]", candidate):
                item = _clean(part)
                if len(item) < 3:
                    continue
                if _normalize_text(item) in {
                    "capitulos",
                    "status",
                    "tipo",
                    "autor",
                    "sinopse",
                }:
                    continue
                key = item.lower()
                if key in seen:
                    continue
                seen.add(key)
                genres.append(item)
                if len(genres) >= 8:
                    return genres
        break

    return genres[:8]


def _parse_chapter_anchors(soup: BeautifulSoup, series_id: str, series_url: str) -> list[dict[str, Any]]:
    seen: set[str] = set()
    strict_chapters: list[dict[str, Any]] = []
    loose_chapters: list[dict[str, Any]] = []

    for anchor in soup.find_all("a", href=True):
        href = _absolute_url(anchor.get("href"))
        if not _chapter_href_matches(href):
            continue
        if href in seen:
            continue

        text = _clean(anchor.get_text(" ", strip=True)) or _clean(anchor.get("title"))
        if not text:
            continue

        match = re.search(r"Cap[ií]tulo\s*([0-9]+(?:\.[0-9]+)?)", text, flags=re.IGNORECASE)
        if not match:
            match = re.search(r"capitulo-([0-9]+(?:-[0-9]+)?)", href, flags=re.IGNORECASE)
        chapter_number = _clean(match.group(1).replace("-", ".")) if match else ""

        chapter_id = _remember_chapter(href, series_id)
        item = {
            "chapter_id": chapter_id,
            "chapter_url": href,
            "chapter_number": chapter_number,
            "title": text,
        }
        loose_chapters.append(item)
        if _chapter_belongs_to_series(href, series_url):
            strict_chapters.append(item)
        seen.add(href)

    chapters = strict_chapters or loose_chapters
    chapters.sort(
        key=lambda item: (
            _decimal_sort_value(item.get("chapter_number")),
            item.get("chapter_url") or "",
        )
    )
    return chapters


def _parse_series_page(html_text: str, url: str) -> dict[str, Any]:
    soup = BeautifulSoup(html_text, "html.parser")
    lines = _extract_lines(soup)
    title = _clean_title(_extract_meta_content(soup, "og:title"))
    if not title:
        heading = soup.find(["h1", "h2"])
        title = _clean_title(heading.get_text(" ", strip=True) if heading else "")
    if not title:
        title = "Novel"

    cover_url = _absolute_url(_extract_meta_content(soup, "og:image"))
    if not cover_url:
        img = soup.find("img")
        if img:
            cover_url = _absolute_url(img.get("src") or img.get("data-src") or "")

    series_url = _absolute_url(url)
    series_id = _remember_series(series_url)
    metadata = _parse_metadata(lines)
    genres = _extract_genres(soup, lines)
    description = _extract_description(lines)
    chapters = _parse_chapter_anchors(soup, series_id, series_url)

    return {
        "novel_id": series_id,
        "title_id": series_id,
        "title": title,
        "display_title": title,
        "cover_url": cover_url,
        "banner_url": cover_url,
        "source_url": series_url,
        "status": metadata.get("status") or "",
        "type": metadata.get("type") or "",
        "author": metadata.get("author") or "",
        "launch_year": metadata.get("launch_year") or "",
        "updated_at": metadata.get("updated_at") or "",
        "description": description,
        "genres": genres,
        "chapters": chapters,
        "total_chapters": len(chapters),
        "latest_chapter": chapters[-1] if chapters else {},
        "first_chapter": chapters[0] if chapters else {},
    }


def _find_reader_root(soup: BeautifulSoup) -> Tag | None:
    best_node: Tag | None = None
    best_score = 0
    for node in soup.find_all(["article", "main", "div", "section"]):
        if not isinstance(node, Tag):
            continue
        paragraphs = node.find_all("p")
        if not paragraphs:
            continue
        text_length = sum(len(_clean(item.get_text(" ", strip=True))) for item in paragraphs)
        score = len(paragraphs) * 80 + text_length
        if score > best_score:
            best_score = score
            best_node = node
    return best_node


def _parse_chapter_page(html_text: str, url: str) -> dict[str, Any]:
    soup = BeautifulSoup(html_text, "html.parser")
    lines = _extract_lines(soup)
    title = _clean_title(_extract_meta_content(soup, "og:title"))
    if not title:
        heading = soup.find(["h1", "h2"])
        title = _clean(heading.get_text(" ", strip=True) if heading else "")

    chapter_id = _remember_chapter(url)
    reader_root = _find_reader_root(soup)
    paragraphs: list[str] = []

    if reader_root is not None:
        for paragraph in reader_root.find_all("p"):
            text = _clean(paragraph.get_text(" ", strip=True))
            if not text:
                continue
            normalized = _normalize_text(text)
            if normalized in {"anterior", "indice", "indice proximo", "proximo"}:
                continue
            if normalized.startswith("capitulo") and len(normalized.split()) <= 3:
                continue
            paragraphs.append(text)

    if not paragraphs:
        for line in lines:
            normalized = _normalize_text(line)
            if not normalized:
                continue
            if normalized in {"anterior", "indice", "proximo"}:
                continue
            if normalized.startswith("postado por ") or normalized.startswith("lancado em "):
                continue
            paragraphs.append(line)

    series_url = ""
    previous_url = ""
    next_url = ""

    for anchor in soup.find_all("a", href=True):
        text = _normalize_text(anchor.get_text(" ", strip=True))
        href = _absolute_url(anchor.get("href"))
        if text == "indice" and _series_href_matches(href):
            series_url = href
        elif text == "anterior" and _chapter_href_matches(href):
            previous_url = href
        elif text == "proximo" and _chapter_href_matches(href):
            next_url = href

    series_id = _remember_series(series_url) if series_url else _CHAPTER_TO_SERIES.get(chapter_id, "")
    if series_id:
        _CHAPTER_TO_SERIES[chapter_id] = series_id
    if previous_url and series_id:
        _remember_chapter(previous_url, series_id)
    if next_url and series_id:
        _remember_chapter(next_url, series_id)

    chapter_number = ""
    match = re.search(r"Cap[ií]tulo\s*([0-9]+(?:\.[0-9]+)?)", title, flags=re.IGNORECASE)
    if not match:
        match = re.search(r"capitulo-([0-9]+(?:-[0-9]+)?)", url, flags=re.IGNORECASE)
    if match:
        chapter_number = _clean(match.group(1).replace("-", "."))

    novel_title = title
    title_match = re.match(r"(.+?)\s+[-|]\s+Cap[ií]tulo", title, flags=re.IGNORECASE)
    if title_match:
        novel_title = _clean_title(title_match.group(1))

    return {
        "chapter_id": chapter_id,
        "chapter_url": _absolute_url(url),
        "chapter_title": title,
        "chapter_number": chapter_number,
        "title": novel_title,
        "title_id": series_id,
        "paragraphs": paragraphs,
        "previous_chapter": (
            {"chapter_id": _remember_chapter(previous_url, series_id), "chapter_url": previous_url}
            if previous_url
            else {}
        ),
        "next_chapter": (
            {"chapter_id": _remember_chapter(next_url, series_id), "chapter_url": next_url}
            if next_url
            else {}
        ),
        "series_url": series_url,
    }


async def get_series_catalog(limit: int = 0) -> list[dict[str, Any]]:
    cache_key = f"series-catalog:{max(0, int(limit or 0))}"

    async def _load():
        desired = max(120, int(limit or 240))
        page_count = min(6, max(2, (desired // 50) + 2))
        urls = [RECENT_UPDATES_URL]
        urls.extend(f"{BASE_URL}/series/page/{page}/?status=&type=&order=update" for page in range(2, page_count + 1))
        urls.append(ARCHIVE_URL)
        html_pages = await asyncio.gather(*(_fetch_html(url) for url in urls), return_exceptions=True)
        groups: list[list[dict[str, Any]]] = []
        for html_text in html_pages:
            if isinstance(html_text, Exception):
                continue
            groups.append(_parse_series_archive(html_text))
        return _merge_series_items(*groups)

    items = await _dedup_fetch(cache_key, HOME_TTL, _load)
    if limit:
        return list(items[: max(1, int(limit))])
    return list(items)


async def get_recent_updated_novels(limit: int = 0) -> list[dict[str, Any]]:
    cache_key = "series-recent-updates"

    async def _load():
        html_text = await _fetch_html(RECENT_UPDATES_URL)
        return _parse_series_archive(html_text)

    items = await _dedup_fetch(cache_key, RECENT_TTL, _load)
    if limit:
        return list(items[: max(1, int(limit))])
    return list(items)


async def get_blog_posts(limit: int = 0) -> list[dict[str, Any]]:
    cache_key = "blog-posts"

    async def _load():
        html_text = await _fetch_html(BLOG_URL)
        return _parse_blog_posts(html_text)

    items = await _dedup_fetch(cache_key, RECENT_TTL, _load)
    if limit:
        return list(items[: max(1, int(limit))])
    return list(items)


def get_cached_home_snapshot(limit: int = HOME_SECTION_LIMIT) -> dict[str, Any]:
    items = _cache_get("series-archive", HOME_TTL) or []
    return {"featured": list(items[: max(1, int(limit))])}


async def warm_catalog_cache() -> None:
    try:
        await get_series_catalog(limit=HOME_SECTION_LIMIT)
    except Exception:
        pass


def schedule_warm_catalog_cache() -> asyncio.Task | None:
    global _WARMUP_TASK
    if _WARMUP_TASK and not _WARMUP_TASK.done():
        return _WARMUP_TASK
    try:
        _WARMUP_TASK = asyncio.create_task(warm_catalog_cache())
    except RuntimeError:
        _WARMUP_TASK = None
    return _WARMUP_TASK


def get_cached_search_novels(query: str, limit: int = SEARCH_LIMIT) -> list[dict[str, Any]] | None:
    cache_key = f"novel-search:{SEARCH_CACHE_VERSION}:{_normalize_text(query)}:{int(limit)}"
    cached = _cache_get(cache_key, SEARCH_TTL)
    if cached is None:
        return None
    return list(cached)


def get_search_fallback_novels(query: str, limit: int = SEARCH_LIMIT) -> list[dict[str, Any]]:
    normalized_query = _normalize_text(query)
    if not normalized_query:
        return []

    catalog = _cache_get("series-archive", HOME_TTL) or []
    ranked = sorted(
        catalog,
        key=lambda item: _search_score(query, item.get("title") or item.get("display_title") or ""),
        reverse=True,
    )
    results = [item for item in ranked if _search_score(query, item.get("title") or "")[0] >= 120]
    return list(results[: max(1, int(limit))])


async def search_novels(query: str, limit: int = SEARCH_LIMIT) -> list[dict[str, Any]]:
    normalized_query = _normalize_text(query)
    cache_key = f"novel-search:{SEARCH_CACHE_VERSION}:{normalized_query}:{int(limit)}"

    async def _load():
        catalog = await get_series_catalog()
        ranked = sorted(
            catalog,
            key=lambda item: _search_score(query, item.get("title") or item.get("display_title") or ""),
            reverse=True,
        )
        results = [item for item in ranked if _search_score(query, item.get("title") or "")[0] >= 200]

        if len(results) < max(3, min(limit, 5)):
            search_url = f"{BASE_URL}/?s={quote_plus(query)}"
            try:
                html_text = await _fetch_html(search_url)
                merged = results + _parse_series_archive(html_text)
                deduped: list[dict[str, Any]] = []
                seen: set[str] = set()
                for item in sorted(
                    merged,
                    key=lambda raw: _search_score(query, raw.get("title") or raw.get("display_title") or ""),
                    reverse=True,
                ):
                    novel_id = item.get("novel_id") or ""
                    if not novel_id or novel_id in seen:
                        continue
                    if _search_score(query, item.get("title") or "")[0] < 150:
                        continue
                    seen.add(novel_id)
                    deduped.append(item)
                results = deduped
            except Exception:
                pass

        return results[: max(1, int(limit))]

    return await _dedup_fetch(cache_key, SEARCH_TTL, _load)


def get_cached_novel_bundle(novel_ref: str) -> dict[str, Any] | None:
    ref = _resolve_series_ref(novel_ref)
    if not ref:
        return None
    key = f"novel-bundle:{_series_key(ref)}"
    cached = _cache_get(key, BUNDLE_TTL)
    return dict(cached) if cached else None


async def get_novel_bundle(novel_ref: str) -> dict[str, Any]:
    ref = _resolve_series_ref(novel_ref)
    if not ref:
        raise RuntimeError("Nao consegui localizar a obra solicitada.")

    cache_key = f"novel-bundle:{_series_key(ref)}"

    async def _load():
        html_text = await _fetch_html(ref)
        return _parse_series_page(html_text, ref)

    return await _dedup_fetch(cache_key, BUNDLE_TTL, _load)


def get_cached_chapter_payload(chapter_ref: str) -> dict[str, Any] | None:
    ref = _resolve_chapter_ref(chapter_ref)
    if not ref:
        return None
    key = f"novel-chapter:{_chapter_key(ref)}"
    cached = _cache_get(key, CHAPTER_TTL)
    return dict(cached) if cached else None


async def get_chapter_payload(chapter_ref: str) -> dict[str, Any]:
    ref = _resolve_chapter_ref(chapter_ref)
    if not ref:
        raise RuntimeError("Nao consegui localizar esse capitulo.")

    cache_key = f"novel-chapter:{_chapter_key(ref)}"

    async def _load():
        html_text = await _fetch_html(ref)
        return _parse_chapter_page(html_text, ref)

    return await _dedup_fetch(cache_key, CHAPTER_TTL, _load)


def prefetch_novel_bundles(novel_refs: list[str], *, limit: int = 3) -> asyncio.Task | None:
    refs = [_clean(item) for item in novel_refs if _clean(item)]
    refs = refs[: max(0, limit)]
    if not refs:
        return None

    async def _runner():
        await asyncio.gather(*(get_novel_bundle(ref) for ref in refs), return_exceptions=True)

    try:
        return asyncio.create_task(_runner())
    except RuntimeError:
        return None


def prefetch_chapter_payloads(chapter_refs: list[str], *, limit: int = 3) -> asyncio.Task | None:
    refs = [_clean(item) for item in chapter_refs if _clean(item)]
    refs = refs[: max(0, limit)]
    if not refs:
        return None

    async def _runner():
        await asyncio.gather(*(get_chapter_payload(ref) for ref in refs), return_exceptions=True)

    try:
        return asyncio.create_task(_runner())
    except RuntimeError:
        return None
