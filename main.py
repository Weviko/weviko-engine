from __future__ import annotations

import asyncio
import contextlib
import hashlib
import io
import logging
import os
import re
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Callable
from urllib.parse import urljoin, urlparse, urlsplit, urlunsplit

import aiohttp
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from markdownify import markdownify as md
from pydantic import BaseModel, Field
from tenacity import retry, stop_after_attempt, wait_exponential

try:
    from langchain_google_genai import ChatGoogleGenerativeAI
except ImportError:
    ChatGoogleGenerativeAI = None

try:
    from supabase import create_client
except ImportError:
    create_client = None

if TYPE_CHECKING:
    from playwright.async_api import BrowserContext


load_dotenv()
logger = logging.getLogger(__name__)

DEFAULT_GEMINI_MODEL = "gemini-2.5-flash"
DEFAULT_GEMINI_FALLBACK_MODELS = (
    "gemini-2.5-flash",
    "gemini-2.5-flash-lite",
    "gemini-2.5-pro",
)
LEGACY_GEMINI_MODEL_ALIASES = {
    "gemini-1.5-pro": DEFAULT_GEMINI_MODEL,
    "gemini-1.5-pro-latest": DEFAULT_GEMINI_MODEL,
    "gemini-pro": DEFAULT_GEMINI_MODEL,
}


class FactData(BaseModel):
    part_number: str = Field(default="Unknown")
    oem_brand: str = Field(default="")
    document_type: str = Field(default="Unknown")
    title: str = Field(default="")
    summary: str = Field(default="")
    vehicle: dict[str, Any] = Field(default_factory=dict)
    compatibility: list[dict[str, Any]] = Field(default_factory=list)
    specifications: dict[str, Any] = Field(default_factory=dict)
    extracted_facts: dict[str, Any] = Field(default_factory=dict)
    cautions: list[str] = Field(default_factory=list)


@dataclass(slots=True)
class FetchResult:
    requested_url: str
    final_url: str
    status_code: int | None
    html: str
    route_status: str
    route_reason: str


@dataclass(slots=True)
class CrawlRunResult:
    start_url: str
    target_market: str
    queued_urls: list[str]
    rows: list[dict[str, Any]]
    route_status_counts: dict[str, int]
    log_lines: list[str]


class CallbackWriter(io.TextIOBase):
    def __init__(self, callback: Callable[[str], None] | None = None):
        self.callback = callback
        self.lines: list[str] = []
        self._buffer = ""

    def writable(self) -> bool:
        return True

    def write(self, text: str) -> int:
        self._buffer += text
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            if line:
                self.lines.append(line)
                if self.callback is not None:
                    self.callback(line)
        return len(text)

    def flush(self) -> None:
        if self._buffer:
            line = self._buffer
            self._buffer = ""
            self.lines.append(line)
            if self.callback is not None:
                self.callback(line)


def env_flag(name: str, default: bool) -> bool:
    raw_value = os.getenv(name, "").strip().lower()
    if not raw_value:
        return default
    return raw_value in {"1", "true", "yes", "on"}


def env_int(name: str, default: int) -> int:
    raw_value = os.getenv(name, "").strip()
    if not raw_value:
        return default
    try:
        return int(raw_value)
    except ValueError:
        return default


def env_csv_set(name: str, default: set[str]) -> set[str]:
    raw_value = os.getenv(name, "").strip()
    if not raw_value:
        return set(default)
    return {item.strip() for item in raw_value.split(",") if item.strip()}


def env_csv_list(name: str, default: list[str] | tuple[str, ...]) -> list[str]:
    raw_value = os.getenv(name, "").strip()
    source = raw_value.split(",") if raw_value else default
    values: list[str] = []
    for item in source:
        normalized = item.strip()
        if normalized and normalized not in values:
            values.append(normalized)
    return values


def build_proxy_config() -> dict[str, str] | None:
    server = os.getenv("PLAYWRIGHT_PROXY_SERVER", "").strip()
    if not server:
        return None

    proxy_config = {"server": server}
    username = os.getenv("PLAYWRIGHT_PROXY_USERNAME", "").strip()
    password = os.getenv("PLAYWRIGHT_PROXY_PASSWORD", "").strip()
    if username:
        proxy_config["username"] = username
    if password:
        proxy_config["password"] = password
    return proxy_config


def build_supabase_client():
    if create_client is None:
        print("[Setup] `supabase` is not installed. Cache and DB writes are disabled.")
        return None

    supabase_url = os.getenv("NEXT_PUBLIC_SUPABASE_URL", "").strip()
    supabase_key = (
        os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
        or os.getenv("SUPABASE_SECRET_KEY", "").strip()
        or os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY", "").strip()
    )

    if not supabase_url or not supabase_key:
        print("[Setup] Supabase env vars are missing. Cache and DB writes are disabled.")
        return None

    if not (
        os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
        or os.getenv("SUPABASE_SECRET_KEY", "").strip()
    ):
        print("[Setup] Using NEXT_PUBLIC_SUPABASE_ANON_KEY fallback. Write access depends on Supabase policies.")

    return create_client(supabase_url, supabase_key)


def normalize_gemini_model_name(model_name: str | None) -> str:
    raw_value = (model_name or "").strip()
    if raw_value.startswith("models/"):
        raw_value = raw_value.split("/", 1)[1].strip()
    legacy_target = LEGACY_GEMINI_MODEL_ALIASES.get(raw_value.lower())
    if legacy_target:
        return legacy_target
    return raw_value or DEFAULT_GEMINI_MODEL


def gemini_candidate_models(preferred_model: str | None = None) -> list[str]:
    candidates: list[str] = []
    primary_model = normalize_gemini_model_name(
        preferred_model or os.getenv("GEMINI_MODEL", DEFAULT_GEMINI_MODEL)
    )
    for raw_name in [primary_model, *env_csv_list("GEMINI_FALLBACK_MODELS", DEFAULT_GEMINI_FALLBACK_MODELS)]:
        normalized = normalize_gemini_model_name(raw_name)
        if normalized and normalized not in candidates:
            candidates.append(normalized)
    return candidates


def is_gemini_model_not_found_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return (
        ("not found" in text or "not_found" in text)
        and ("model" in text or "models/" in text)
    )


def build_llm(model_name: str | None = None):
    if ChatGoogleGenerativeAI is None:
        print("[Setup] `langchain-google-genai` is not installed. AI extraction is disabled.")
        return None

    google_api_key = (
        os.getenv("GOOGLE_API_KEY", "").strip()
        or os.getenv("GEMINI_API_KEY", "").strip()
    )
    if not google_api_key:
        print("[Setup] GOOGLE_API_KEY is missing. AI extraction is disabled.")
        return None

    return ChatGoogleGenerativeAI(
        model=normalize_gemini_model_name(model_name or os.getenv("GEMINI_MODEL", DEFAULT_GEMINI_MODEL)),
        temperature=0,
        google_api_key=google_api_key,
    )


def invoke_llm_with_fallback(
    payload: Any,
    *,
    structured_schema: type[BaseModel] | None = None,
):
    last_error: Exception | None = None
    for model_name in gemini_candidate_models():
        llm = build_llm(model_name)
        if llm is None:
            return None

        target = llm.with_structured_output(structured_schema) if structured_schema is not None else llm
        try:
            return target.invoke(payload)
        except Exception as exc:
            if is_gemini_model_not_found_error(exc):
                logger.warning("[Gemini] model `%s` is not available. Trying next fallback.", model_name)
                last_error = exc
                continue
            raise

    if last_error is not None:
        raise last_error
    return None


def build_factory_extraction_prompt(schema_key: str) -> str:
    base_prompt = (
        "You are extracting automotive service intelligence for a repair-shop search system. "
        "Return only structured data that can help technicians, parts advisors, and vehicle-registration staff. "
        "Do not invent values. If a value is missing, keep it empty or use `Unknown` for identifiers. "
        "Keep `summary` short and factual. Put only measurable or operational facts into `extracted_facts` and `specifications`."
    )

    schema_instructions = {
        "path_detail": (
            "Focus on part number, OEM brand, compatible vehicle models/years, engine or trim conditions, "
            "dimensions, torque, material, package contents, and interchange notes."
        ),
        "path_manual": (
            "Focus on service procedures, required tools, torque values, cautions, removal or installation order, "
            "and inspection checkpoints."
        ),
        "path_body_manual": (
            "Focus on body-panel procedure steps, attachment points, sealant or adhesive notes, fastening sequence, "
            "alignment notes, and cautions."
        ),
        "path_connector": (
            "Focus on connector name, location, pin count, pin map, wire colors, signal names, and mating target."
        ),
        "path_vehicle_id": (
            "This may be a VIN, paint code, engine code, or vehicle identification document. "
            "Do not force a part number. Prefer identification facts, label locations, and code examples."
        ),
        "path_wiring": (
            "Focus on wiring points, connectors, pin labels, measurable voltages or resistances, and diagnostic notes."
        ),
        "path_dtc": (
            "Focus on DTC code, symptoms, likely causes, inspection steps, and recommended repair actions."
        ),
        "path_community": (
            "Focus on repeatable field fixes, symptoms, verified compatibility clues, and practical workshop cautions."
        ),
    }
    return f"{base_prompt} {schema_instructions.get(schema_key, '')}".strip()


class WevikoSpider:
    ARTICLE_LINK_SELECTORS = (
        "article a[href]",
        "main a[href]",
        "[data-content] a[href]",
        ".content a[href]",
        ".entry-content a[href]",
        ".post-content a[href]",
        ".prose a[href]",
        ".markdown a[href]",
    )

    def __init__(
        self,
        product_path_hint: str = "/part/",
        discovery_max_pages: int = 6,
        discovery_max_matches: int = 12,
        discovery_max_depth: int = 2,
        extra_path_hints: tuple[str, ...] | list[str] | None = None,
        route_watch_hints: tuple[str, ...] | list[str] | None = None,
        max_queue_urls: int | None = None,
    ):
        self.url_queue: asyncio.Queue[str | None] = asyncio.Queue()
        self.visited_urls: set[str] = set()
        self.queued_urls: list[str] = []
        self.product_path_hint = product_path_hint.strip()
        self.discovery_max_pages = max(discovery_max_pages, 1)
        self.discovery_max_matches = max(discovery_max_matches, 1)
        self.discovery_max_depth = max(discovery_max_depth, 0)
        self.extra_path_hints = tuple(()) if extra_path_hints is None else tuple(extra_path_hints)
        self.route_watch_hints = (
            tuple(("/parts", "/dashboard"))
            if route_watch_hints is None
            else tuple(route_watch_hints)
        )
        self.max_queue_urls = max_queue_urls if max_queue_urls and max_queue_urls > 0 else None

    def normalize_url(self, url: str) -> str:
        parsed = urlsplit(url)
        path = parsed.path or "/"
        if path != "/":
            path = path.rstrip("/")
        return urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))

    def normalize_hint(self, hint: str) -> str:
        hint = hint.strip()
        if not hint:
            return ""
        if not hint.startswith("/"):
            hint = f"/{hint}"
        if hint != "/":
            hint = hint.rstrip("/")
        return hint

    def normalized_hint(self) -> str:
        return self.normalize_hint(self.product_path_hint)

    def all_hints(self) -> tuple[str, ...]:
        hints: list[str] = []
        for raw_hint in (self.product_path_hint, *self.extra_path_hints):
            hint = self.normalize_hint(raw_hint)
            if hint and hint not in hints:
                hints.append(hint)
        return tuple(hints)

    def all_route_watch_hints(self) -> tuple[str, ...]:
        hints: list[str] = []
        for raw_hint in self.route_watch_hints:
            hint = self.normalize_hint(raw_hint)
            if hint and hint not in hints:
                hints.append(hint)
        return tuple(hints)

    def path_matches_hint(self, path: str, hint: str | None = None) -> bool:
        hint = self.normalized_hint() if hint is None else self.normalize_hint(hint)
        if not hint:
            return False
        return path == hint or path.startswith(f"{hint}/")

    def path_matches_any_hint(self, path: str) -> bool:
        return any(self.path_matches_hint(path, hint) for hint in self.all_hints())

    def path_matches_route_watch_hint(self, path: str) -> bool:
        return any(self.path_matches_hint(path, hint) for hint in self.all_route_watch_hints())

    def is_detail_candidate(self, path: str, hint: str | None = None) -> bool:
        hint = self.normalized_hint() if hint is None else self.normalize_hint(hint)
        if not hint:
            return False
        return path.startswith(f"{hint}/")

    def extract_discovery_links(
        self,
        soup: BeautifulSoup,
        current_url: str,
        *,
        start_host: str,
        prefer_content_links: bool,
    ) -> list[tuple[str, str]]:
        selector_groups: list[tuple[str, ...]] = []
        if prefer_content_links:
            selector_groups.append(self.ARTICLE_LINK_SELECTORS)
        selector_groups.append(("a[href]",))

        discovered_links: list[tuple[str, str]] = []
        seen_candidates: set[str] = set()
        for selectors in selector_groups:
            for selector in selectors:
                for anchor in soup.select(selector):
                    href = anchor.get("href", "").strip()
                    if not href or href.startswith(("javascript:", "mailto:", "tel:", "#")):
                        continue

                    candidate = self.normalize_url(urljoin(current_url, href))
                    parsed = urlparse(candidate)
                    if parsed.scheme not in {"http", "https"}:
                        continue
                    if parsed.netloc != start_host:
                        continue

                    candidate_path = parsed.path or "/"
                    if not (
                        self.path_matches_any_hint(candidate_path)
                        or self.path_matches_route_watch_hint(candidate_path)
                    ):
                        continue
                    if candidate in seen_candidates:
                        continue

                    seen_candidates.add(candidate)
                    discovered_links.append((candidate, candidate_path))

        return discovered_links

    async def enqueue_url(self, url: str) -> bool:
        normalized_url = self.normalize_url(url.split("#", 1)[0])
        if normalized_url in self.visited_urls:
            return False
        if self.max_queue_urls is not None and len(self.queued_urls) >= self.max_queue_urls:
            return False

        await self.url_queue.put(normalized_url)
        self.visited_urls.add(normalized_url)
        self.queued_urls.append(normalized_url)
        print(f"   [Spider] queued: {normalized_url}")
        return True

    async def discover_urls(self, start_url: str) -> None:
        print(f"[Spider] discovering product URLs from: {start_url}")

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        }
        timeout = aiohttp.ClientTimeout(total=30)
        normalized_start_url = self.normalize_url(start_url)
        start_host = urlparse(normalized_start_url).netloc
        discovered_detail_urls = 0
        fallback_urls: list[str] = []
        discovery_queue = deque([(normalized_start_url, 0)])
        discovery_seen = {normalized_start_url}
        hint_paths = self.all_hints()

        async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
            scanned_pages = 0
            while discovery_queue:
                if scanned_pages >= self.discovery_max_pages:
                    break
                if discovered_detail_urls >= self.discovery_max_matches:
                    break

                current_url, depth = discovery_queue.popleft()
                scanned_pages += 1
                try:
                    async with session.get(current_url) as response:
                        response.raise_for_status()
                        html = await response.text()
                except Exception as exc:
                    print(f"[Spider] discovery fetch failed for {current_url}: {exc}")
                    continue

                current_path = urlparse(current_url).path or "/"
                current_is_detail = any(
                    self.is_detail_candidate(current_path, hint) for hint in hint_paths
                )
                if current_is_detail and current_url not in self.visited_urls:
                    if await self.enqueue_url(current_url):
                        discovered_detail_urls += 1

                soup = BeautifulSoup(html, "html.parser")
                if depth >= self.discovery_max_depth:
                    continue

                for candidate, candidate_path in self.extract_discovery_links(
                    soup,
                    current_url,
                    start_host=start_host,
                    prefer_content_links=current_is_detail,
                ):
                    candidate_matches_content_hint = self.path_matches_any_hint(candidate_path)
                    candidate_matches_route_watch = self.path_matches_route_watch_hint(candidate_path)

                    if candidate_matches_route_watch and candidate not in self.visited_urls:
                        await self.enqueue_url(candidate)

                    if not candidate_matches_content_hint:
                        continue

                    candidate_is_detail = any(
                        self.is_detail_candidate(candidate_path, hint) for hint in hint_paths
                    )

                    if candidate_is_detail:
                        if candidate not in self.visited_urls:
                            if await self.enqueue_url(candidate):
                                discovered_detail_urls += 1
                        if candidate not in discovery_seen:
                            discovery_seen.add(candidate)
                            discovery_queue.append((candidate, depth + 1))
                        if discovered_detail_urls >= self.discovery_max_matches:
                            break
                        continue

                    if candidate not in fallback_urls:
                        fallback_urls.append(candidate)
                    if candidate not in discovery_seen:
                        discovery_seen.add(candidate)
                        discovery_queue.append((candidate, depth + 1))

        if discovered_detail_urls == 0:
            if fallback_urls:
                print("[Spider] no detail pages found. Queueing matched category pages as fallback.")
                for candidate in fallback_urls[: self.discovery_max_matches]:
                    await self.enqueue_url(candidate)
            else:
                print("[Spider] no matching candidates found. Falling back to the start URL.")
                await self.enqueue_url(normalized_start_url)


class WevikoWorker:
    KEYWORD_PATTERN = re.compile(
        r"(torque|spec|weight|fit|compatible|part|oem|nm|lbs|kg|mm|"
        r"torque spec|part number|vehicle|application|"
        r"토크|규격|호환|품번|차종|부품|순정)",
        re.IGNORECASE,
    )

    def __init__(
        self,
        worker_id: int,
        *,
        blocked_resource_types: set[str],
        markdown_limit: int = 8000,
        fallback_chars: int = 1500,
        compressed_chars: int = 2000,
        context_lines: int = 1,
    ):
        self.worker_id = worker_id
        self.blocked_resource_types = blocked_resource_types
        self.markdown_limit = markdown_limit
        self.fallback_chars = fallback_chars
        self.compressed_chars = compressed_chars
        self.context_lines = max(context_lines, 0)

    def classify_route(
        self,
        requested_url: str,
        final_url: str,
        status_code: int | None,
        html: str,
    ) -> tuple[str, str]:
        requested_path = urlparse(requested_url).path or "/"
        final_path = urlparse(final_url).path or "/"
        html_lower = html.lower()
        text = BeautifulSoup(html, "html.parser").get_text(" ", strip=True).lower()
        text_preview = text[:2000]

        if status_code in {401, 403}:
            return "auth_required", f"http_status_{status_code}"

        if final_path == "/login" and requested_path != final_path:
            return "auth_required", "redirected_to_login"

        if requested_path.startswith("/dashboard"):
            if "로그인" in text_preview or "login" in text_preview:
                return "auth_required", "dashboard_login_gate"

        if status_code is not None and status_code >= 400:
            return "broken_public_route", f"http_status_{status_code}"

        if "__next_error__" in html_lower:
            return "broken_public_route", "next_error_boundary"

        return "content_page", "public_content"

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def fetch_page(self, context: "BrowserContext", url: str) -> FetchResult:
        blocked = ", ".join(sorted(self.blocked_resource_types)) or "none"
        print(f"[Worker-{self.worker_id}] fetching with blocked resources [{blocked}]: {url}")
        page = await context.new_page()

        try:
            response = await page.goto(url, timeout=30000, wait_until="domcontentloaded")
            await page.wait_for_timeout(2000)
            html = await page.content()
            final_url = page.url
            status_code = response.status if response is not None else None
            route_status, route_reason = self.classify_route(url, final_url, status_code, html)
            return FetchResult(
                requested_url=url,
                final_url=final_url,
                status_code=status_code,
                html=html,
                route_status=route_status,
                route_reason=route_reason,
            )
        finally:
            await page.close()

    def process_and_compress_html(self, html: str) -> str:
        soup = BeautifulSoup(html, "html.parser")
        for element in soup(["script", "style", "nav", "footer", "header"]):
            element.decompose()

        raw_markdown = md(str(soup), strip=["a", "img"])[: self.markdown_limit]
        clean_markdown = re.sub(r"\n\s*\n+", "\n", raw_markdown).strip()
        if not clean_markdown:
            return ""

        lines = clean_markdown.splitlines()
        kept_lines: set[int] = set()

        for index, line in enumerate(lines):
            if not self.KEYWORD_PATTERN.search(line):
                continue

            start = max(0, index - self.context_lines)
            end = min(len(lines) - 1, index + self.context_lines)
            kept_lines.update(range(start, end + 1))

        if not kept_lines:
            return clean_markdown[: self.fallback_chars]

        compressed_markdown = "\n".join(lines[index] for index in sorted(kept_lines)).strip()
        if not compressed_markdown:
            return clean_markdown[: self.fallback_chars]
        return compressed_markdown[: self.compressed_chars]


class WevikoCrawler:
    def __init__(
        self,
        proxy_url: str | None = None,
        *,
        user_agent: str | None = None,
        blocked_resource_types: set[str] | None = None,
        headless: bool | None = None,
    ):
        self.proxy_url = proxy_url.strip() if proxy_url else None
        self.user_agent = user_agent or (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
        self.blocked_resource_types = set(
            blocked_resource_types or {"image", "media", "font", "stylesheet"}
        )
        self.headless = env_flag("WEVIKO_HEADLESS", True) if headless is None else headless

    async def _intercept_route(self, route) -> None:
        if route.request.resource_type in self.blocked_resource_types:
            await route.abort()
            return
        await route.continue_()

    def _compress_html(self, raw_html: str) -> str:
        soup = BeautifulSoup(raw_html, "html.parser")
        for tag in soup(["script", "style", "nav", "footer", "header"]):
            tag.decompose()
        return soup.get_text(separator="\n", strip=True)

    async def scrape_target(self, url: str) -> str | None:
        try:
            from playwright.async_api import async_playwright
            from playwright_stealth import Stealth
        except ImportError as exc:
            logger.error("Playwright import failed: %s", exc)
            return None

        browser_kwargs: dict[str, Any] = {"headless": self.headless}
        if self.proxy_url:
            browser_kwargs["proxy"] = {"server": self.proxy_url}
        else:
            proxy_config = build_proxy_config()
            if proxy_config is not None:
                browser_kwargs["proxy"] = proxy_config

        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(**browser_kwargs)
            try:
                context = await browser.new_context(user_agent=self.user_agent, locale="en-US")
                page = await context.new_page()
                await Stealth().apply_stealth_async(page)
                await page.route("**/*", self._intercept_route)

                logger.info("접속 시도: %s", url)
                try:
                    response = await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    await page.wait_for_timeout(2000)
                    raw_html = await page.content()
                    clean_text = self._compress_html(raw_html)
                    status_code = response.status if response is not None else "unknown"
                    logger.info("수집 성공: 텍스트 길이 %s자 (status=%s)", len(clean_text), status_code)
                    return clean_text
                except Exception as exc:
                    logger.error("수집 실패 (%s): %s", url, exc)
                    return None
                finally:
                    await page.close()
            finally:
                await browser.close()


class WevikoBrain:
    def __init__(
        self,
        *,
        supabase_client: Any = None,
        llm: Any = None,
        cache_table: str = "crawling_logs",
        parts_table: str = "parts",
        write_destination: str = "parts",
        schema_key: str = "path_detail",
        source_type: str = "crawl_factory",
        target_market: str = "GLOBAL",
    ):
        self.supabase = supabase_client
        self.llm = llm
        self.cache_table = cache_table
        self.parts_table = parts_table
        self.write_destination = write_destination
        self.schema_key = schema_key
        self.source_type = source_type
        self.target_market = target_market
        self.route_status_counts: dict[str, int] = {}
        self.rows: list[dict[str, Any]] = []
        self.rows_by_url: dict[str, dict[str, Any]] = {}

    def generate_hash(self, content: str) -> str:
        return hashlib.md5(content.encode("utf-8")).hexdigest()

    def _is_cached(self, content_hash: str) -> bool:
        if self.supabase is None:
            return False

        try:
            response = (
                self.supabase.table(self.cache_table)
                .select("id")
                .eq("content_hash", content_hash)
                .limit(1)
                .execute()
            )
            return bool(getattr(response, "data", None))
        except Exception as exc:
            print(f"[Cache] lookup failed. Continuing without cache: {exc}")
            return False

    def _persist_result(self, url: str, content_hash: str, result: FactData | None) -> None:
        if self.supabase is None:
            return

        try:
            timestamp = datetime.now(timezone.utc).isoformat()
            (
                self.supabase.table(self.cache_table)
                .upsert(
                    {
                        "url": url,
                        "content_hash": content_hash,
                        "updated_at": timestamp,
                    }
                )
                .execute()
            )

            if result is not None and self.write_destination == "parts":
                (
                    self.supabase.table(self.parts_table)
                    .upsert(
                        {
                            "part_number": result.part_number,
                            "oem_brand": result.oem_brand,
                            "market": self.target_market,
                            "schema_key": self.schema_key,
                            "document_type": result.document_type,
                            "source_type": self.source_type,
                            "spec_data": result.model_dump(),
                            "updated_at": timestamp,
                        },
                        on_conflict="part_number",
                    )
                    .execute()
                )
        except Exception as exc:
            print(f"[DB] write failed. Continuing with the next URL: {exc}")

    def _ensure_row(self, fetch_result: FetchResult) -> dict[str, Any]:
        row = self.rows_by_url.get(fetch_result.requested_url)
        if row is None:
            row = {
                "url": fetch_result.requested_url,
                "final_url": fetch_result.final_url,
                "http_status": fetch_result.status_code,
                "route_status": fetch_result.route_status,
                "route_reason": fetch_result.route_reason,
                "status": "Queued",
                "target_market": self.target_market,
                "part_number": "",
                "oem_brand": "",
                "document_type": "",
                "title": "",
                "summary": "",
                "vehicle": {},
                "compatibility": [],
                "specifications": {},
                "extracted_facts_count": 0,
                "extracted_facts": {},
                "cautions": [],
                "content_hash": "",
                "compressed_chars": 0,
                "cache_hit": False,
                "skip_reason": "",
            }
            self.rows_by_url[fetch_result.requested_url] = row
            self.rows.append(row)

        row["final_url"] = fetch_result.final_url
        row["http_status"] = fetch_result.status_code
        row["route_status"] = fetch_result.route_status
        row["route_reason"] = fetch_result.route_reason
        row["target_market"] = self.target_market
        return row

    def record_route_status(self, fetch_result: FetchResult) -> None:
        route_status = fetch_result.route_status
        row = self._ensure_row(fetch_result)
        self.route_status_counts[route_status] = self.route_status_counts.get(route_status, 0) + 1

        if route_status == "content_page":
            row["status"] = "Fetched"
            row["skip_reason"] = ""
            return

        if route_status == "auth_required":
            row["status"] = "Auth Required"
        elif route_status == "broken_public_route":
            row["status"] = "Broken Public Route"
        else:
            row["status"] = route_status
        row["skip_reason"] = route_status

        extra = ""
        if fetch_result.final_url != fetch_result.requested_url:
            extra = f" -> {fetch_result.final_url}"
        status_fragment = (
            f", status={fetch_result.status_code}" if fetch_result.status_code is not None else ""
        )
        print(
            f"[Route] {route_status}: {fetch_result.requested_url}{extra} "
            f"({fetch_result.route_reason}{status_fragment})"
        )

    def record_empty_content(self, fetch_result: FetchResult) -> None:
        row = self._ensure_row(fetch_result)
        row["status"] = "No Useful Text"
        row["compressed_chars"] = 0
        row["skip_reason"] = "no_useful_text"

    def print_route_summary(self) -> None:
        if not self.route_status_counts:
            return

        summary = ", ".join(
            f"{route_status}={count}" for route_status, count in sorted(self.route_status_counts.items())
        )
        print(f"[Route Summary] {summary}")

    async def check_cache_and_extract(
        self,
        fetch_result: FetchResult,
        compressed_markdown: str,
    ) -> FactData | None:
        url = fetch_result.requested_url
        row = self._ensure_row(fetch_result)
        row["compressed_chars"] = len(compressed_markdown)
        content_hash = self.generate_hash(compressed_markdown)
        row["content_hash"] = content_hash
        if self._is_cached(content_hash):
            row["status"] = "Cache Hit"
            row["cache_hit"] = True
            row["skip_reason"] = "cache_hit"
            print(f"[Cache Hit] no content change. Skipping AI extraction: {url}")
            return None

        if self.llm is None:
            row["status"] = "Content Captured"
            row["skip_reason"] = "model_unavailable"
            print(f"[AI] model unavailable. Storing raw crawl metadata only: {url}")
            self._persist_result(url, content_hash, None)
            return None

        print(
            f"[AI] new content detected. Starting structured extraction "
            f"from {len(compressed_markdown)} characters..."
        )
        prompt = (
            f"{build_factory_extraction_prompt(self.schema_key)}\n"
            f"Active schema key: {self.schema_key}\n"
            "Return data that matches the provided structured schema."
        )
        result = await asyncio.to_thread(
            invoke_llm_with_fallback,
            f"{prompt}\n\n[Compressed Body]\n{compressed_markdown}",
            structured_schema=FactData,
        )
        if result is None:
            row["status"] = "Model Unavailable"
            row["skip_reason"] = "model_unavailable"
            print(f"[AI] no supported Gemini model is available right now: {url}")
            self._persist_result(url, content_hash, None)
            return None

        row["status"] = "AI Extracted"
        row["part_number"] = result.part_number
        row["oem_brand"] = result.oem_brand
        row["document_type"] = result.document_type
        row["title"] = result.title
        row["summary"] = result.summary
        row["vehicle"] = result.vehicle
        row["compatibility"] = result.compatibility
        row["specifications"] = result.specifications
        row["extracted_facts"] = result.extracted_facts
        row["extracted_facts_count"] = len(result.extracted_facts)
        row["cautions"] = result.cautions
        row["skip_reason"] = ""
        self._persist_result(url, content_hash, result)
        print(f"   -> processed successfully (hash: {content_hash[:8]}...)")
        return result


async def crawl_worker_task(
    worker_id: int,
    spider: WevikoSpider,
    context: "BrowserContext",
    brain: WevikoBrain,
    blocked_resource_types: set[str],
) -> None:
    worker = WevikoWorker(
        worker_id,
        blocked_resource_types=blocked_resource_types,
        markdown_limit=env_int("WEVIKO_MARKDOWN_LIMIT", 8000),
        fallback_chars=env_int("WEVIKO_FALLBACK_TEXT_CHARS", 1500),
        compressed_chars=env_int("WEVIKO_COMPRESSED_TEXT_CHARS", 2000),
        context_lines=env_int("WEVIKO_COMPRESSION_CONTEXT_LINES", 1),
    )

    while True:
        url = await spider.url_queue.get()
        try:
            if url is None:
                return

            fetch_result = await worker.fetch_page(context, url)
            brain.record_route_status(fetch_result)
            if fetch_result.route_status != "content_page":
                continue

            compressed_markdown = worker.process_and_compress_html(fetch_result.html)
            if not compressed_markdown:
                brain.record_empty_content(fetch_result)
                print(f"[Worker-{worker_id}] extracted no useful text. Skipping: {url}")
                continue
            await brain.check_cache_and_extract(fetch_result, compressed_markdown)
        except Exception as exc:
            print(f"[Worker-{worker_id}] failed after retries ({url}): {exc}")
        finally:
            spider.url_queue.task_done()


async def run_factory_async(
    *,
    start_url: str | None = None,
    num_workers: int | None = None,
    target_market: str = "GLOBAL",
    product_path_hint: str | None = None,
    discovery_extra_path_hints: list[str] | tuple[str, ...] | None = None,
    route_watch_hints: list[str] | tuple[str, ...] | None = None,
    discovery_max_pages: int | None = None,
    discovery_max_matches: int | None = None,
    discovery_max_depth: int | None = None,
    max_urls: int | None = None,
    blocked_resource_types: set[str] | None = None,
    user_agent: str | None = None,
    write_destination: str = "parts",
    schema_key: str = "path_detail",
    source_type: str = "crawl_factory",
) -> CrawlRunResult:
    print("Starting Weviko crawling pipeline...\n")

    resolved_start_url = start_url or os.getenv("WEVIKO_TARGET_URL", "https://www.weviko.com")
    resolved_num_workers = num_workers or env_int("WEVIKO_NUM_WORKERS", 3)
    resolved_discovery_max_matches = (
        max_urls if max_urls is not None else discovery_max_matches
    )
    spider = WevikoSpider(
        product_path_hint=(
            os.getenv("WEVIKO_PRODUCT_PATH_HINT", "/part/")
            if product_path_hint is None
            else product_path_hint
        ),
        discovery_max_pages=discovery_max_pages or env_int("WEVIKO_DISCOVERY_MAX_PAGES", 12),
        discovery_max_matches=(
            resolved_discovery_max_matches
            or env_int("WEVIKO_DISCOVERY_MAX_MATCHES", 20)
        ),
        discovery_max_depth=discovery_max_depth or env_int("WEVIKO_DISCOVERY_MAX_DEPTH", 2),
        extra_path_hints=(
            env_csv_list("WEVIKO_DISCOVERY_EXTRA_PATH_HINTS", ())
            if discovery_extra_path_hints is None
            else list(discovery_extra_path_hints)
        ),
        route_watch_hints=(
            env_csv_list("WEVIKO_ROUTE_WATCH_HINTS", ("/parts", "/dashboard"))
            if route_watch_hints is None
            else list(route_watch_hints)
        ),
        max_queue_urls=max_urls,
    )
    brain = WevikoBrain(
        supabase_client=build_supabase_client(),
        llm=build_llm(),
        cache_table=os.getenv("WEVIKO_CACHE_TABLE", "crawling_logs"),
        parts_table=os.getenv("WEVIKO_PARTS_TABLE", "parts"),
        write_destination=write_destination,
        schema_key=schema_key,
        source_type=source_type,
        target_market=target_market,
    )
    await spider.discover_urls(resolved_start_url)

    if spider.url_queue.empty():
        print("[Setup] no URLs were queued. Exiting.")
        return CrawlRunResult(
            start_url=resolved_start_url,
            target_market=target_market,
            queued_urls=[],
            rows=[],
            route_status_counts=dict(brain.route_status_counts),
            log_lines=[],
        )

    try:
        from playwright.async_api import async_playwright
        from playwright_stealth import Stealth
    except ImportError as exc:
        print("[Setup] Playwright failed to import in this environment.")
        print(f"   -> {exc}")
        print("   -> On this machine, the current Python 3.14 environment is failing in the Playwright/greenlet layer.")
        print("   -> Creating a fresh Python 3.12 or 3.13 virtual environment is the safest next step.")
        return CrawlRunResult(
            start_url=resolved_start_url,
            target_market=target_market,
            queued_urls=list(spider.queued_urls),
            rows=list(brain.rows),
            route_status_counts=dict(brain.route_status_counts),
            log_lines=[],
        )

    proxy_config = build_proxy_config()
    launch_kwargs: dict[str, Any] = {"headless": env_flag("WEVIKO_HEADLESS", True)}
    if proxy_config is not None:
        launch_kwargs["proxy"] = proxy_config

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(**launch_kwargs)
        try:
            context_kwargs: dict[str, Any] = {"locale": "en-US"}
            if user_agent:
                context_kwargs["user_agent"] = user_agent

            context = await browser.new_context(**context_kwargs)
            await Stealth().apply_stealth_async(context)

            resolved_blocked_resource_types = (
                env_csv_set(
                    "WEVIKO_BLOCKED_RESOURCE_TYPES",
                    {"image", "media", "font", "stylesheet"},
                )
                if blocked_resource_types is None
                else set(blocked_resource_types)
            )
            if resolved_blocked_resource_types:
                async def route_intercept(route):
                    if route.request.resource_type in resolved_blocked_resource_types:
                        await route.abort()
                        return
                    await route.continue_()

                await context.route("**/*", route_intercept)

            workers = [
                asyncio.create_task(
                    crawl_worker_task(
                        i + 1,
                        spider,
                        context,
                        brain,
                        resolved_blocked_resource_types,
                    )
                )
                for i in range(resolved_num_workers)
            ]

            await spider.url_queue.join()

            for _ in workers:
                await spider.url_queue.put(None)

            await spider.url_queue.join()
            await asyncio.gather(*workers, return_exceptions=True)
        finally:
            await browser.close()

    print("\nCrawling and extraction completed.")
    brain.print_route_summary()
    return CrawlRunResult(
        start_url=resolved_start_url,
        target_market=target_market,
        queued_urls=list(spider.queued_urls),
        rows=list(brain.rows),
        route_status_counts=dict(brain.route_status_counts),
        log_lines=[],
    )


def run_factory(
    *,
    start_url: str | None = None,
    num_workers: int | None = None,
    target_market: str = "GLOBAL",
    product_path_hint: str | None = None,
    discovery_extra_path_hints: list[str] | tuple[str, ...] | None = None,
    route_watch_hints: list[str] | tuple[str, ...] | None = None,
    discovery_max_pages: int | None = None,
    discovery_max_matches: int | None = None,
    discovery_max_depth: int | None = None,
    max_urls: int | None = None,
    blocked_resource_types: set[str] | None = None,
    user_agent: str | None = None,
    write_destination: str = "parts",
    schema_key: str = "path_detail",
    source_type: str = "crawl_factory",
    log_callback: Callable[[str], None] | None = None,
) -> CrawlRunResult:
    capture = CallbackWriter(log_callback)
    with contextlib.redirect_stdout(capture):
        result = asyncio.run(
            run_factory_async(
                start_url=start_url,
                num_workers=num_workers,
                target_market=target_market,
                product_path_hint=product_path_hint,
                discovery_extra_path_hints=discovery_extra_path_hints,
                route_watch_hints=route_watch_hints,
                discovery_max_pages=discovery_max_pages,
                discovery_max_matches=discovery_max_matches,
                discovery_max_depth=discovery_max_depth,
                max_urls=max_urls,
                blocked_resource_types=blocked_resource_types,
                user_agent=user_agent,
                write_destination=write_destination,
                schema_key=schema_key,
                source_type=source_type,
            )
        )
    capture.flush()
    result.log_lines = list(capture.lines)
    return result


def run_crawler_sync(
    url: str,
    proxy: str | None = None,
    *,
    user_agent: str | None = None,
    blocked_resource_types: set[str] | None = None,
    headless: bool | None = None,
) -> str | None:
    crawler = WevikoCrawler(
        proxy_url=proxy,
        user_agent=user_agent,
        blocked_resource_types=blocked_resource_types,
        headless=headless,
    )
    return asyncio.run(crawler.scrape_target(url))


async def main() -> None:
    await run_factory_async()


if __name__ == "__main__":
    asyncio.run(main())
