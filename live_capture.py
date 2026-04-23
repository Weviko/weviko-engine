from __future__ import annotations

import json
import os
import re
from typing import Any
from urllib.parse import urlparse

from bs4 import BeautifulSoup


DEFAULT_ALLOWED_CAPTURE_HOSTS = (
    "www.weviko.com",
    "weviko.com",
    "localhost",
    "127.0.0.1",
)

LIVE_CAPTURE_SCHEMA_GUESSES: tuple[tuple[str, str, str, str], ...] = (
    ("/item/detail/", "path_detail", "/item/detail/", "Parts detail live capture"),
    ("/shop/manual/", "path_manual", "/shop/manual/", "Service manual live capture"),
    ("/body/manual/", "path_body_manual", "/body/manual/", "Body manual live capture"),
    ("/contents/etc/", "path_wiring", "/contents/etc/", "Wiring live capture"),
    ("/wiring/connector/", "path_connector", "/wiring/connector/", "Connector live capture"),
    ("/dtc/", "path_dtc", "/dtc/", "DTC live capture"),
    ("/community/", "path_community", "/community/", "Community live capture"),
    ("/vehicle-id/", "path_vehicle_id", "/vehicle-id/", "Vehicle identification live capture"),
    ("/vin/", "path_vehicle_id", "/vehicle-id/", "Vehicle identification live capture"),
)

DEFAULT_LIVE_CAPTURE_SCHEMA = {
    "schema_key": "path_manual",
    "source_path_hint": "",
    "document_type": "Live browser capture",
}

PART_NUMBER_BLOCKLIST = {
    "HTTP",
    "HTTPS",
    "HTML",
    "WIDTH",
    "HEIGHT",
    "FALSE",
    "TRUE",
    "UNKNOWN",
    "GLOBAL",
    "LOGIN",
    "LOGOUT",
}


def _env_int(name: str, default: int) -> int:
    raw_value = str(os.getenv(name, "") or "").strip()
    if not raw_value:
        return default
    try:
        return int(raw_value)
    except ValueError:
        return default


def _env_flag(name: str, default: bool) -> bool:
    raw_value = str(os.getenv(name, "") or "").strip().lower()
    if not raw_value:
        return default
    return raw_value in {"1", "true", "yes", "on"}


def live_capture_host() -> str:
    return str(os.getenv("WEVIKO_LIVE_CAPTURE_HOST", "127.0.0.1") or "127.0.0.1").strip() or "127.0.0.1"


def live_capture_port() -> int:
    return _env_int("WEVIKO_LIVE_CAPTURE_PORT", 8765)


def live_capture_scheme() -> str:
    scheme = str(os.getenv("WEVIKO_LIVE_CAPTURE_SCHEME", "http") or "http").strip().lower()
    if scheme not in {"http", "https"}:
        return "http"
    return scheme


def live_capture_base_url() -> str:
    return f"{live_capture_scheme()}://{live_capture_host()}:{live_capture_port()}"


def live_capture_direct_enabled() -> bool:
    return _env_flag("WEVIKO_LIVE_CAPTURE_DIRECT_ENABLED", False)


def live_capture_allowed_hosts() -> list[str]:
    raw_hosts = str(os.getenv("WEVIKO_ALLOWED_CAPTURE_HOSTS", "") or "").strip()
    source = raw_hosts.split(",") if raw_hosts else DEFAULT_ALLOWED_CAPTURE_HOSTS
    normalized_hosts: list[str] = []
    for item in source:
        normalized = str(item or "").strip().lower()
        if normalized and normalized not in normalized_hosts:
            normalized_hosts.append(normalized)
    return normalized_hosts


def live_capture_limits() -> dict[str, int]:
    return {
        "max_html_chars": _env_int("WEVIKO_LIVE_CAPTURE_MAX_HTML_CHARS", 350000),
        "max_text_chars": _env_int("WEVIKO_LIVE_CAPTURE_MAX_TEXT_CHARS", 120000),
        "max_scraped_chars": _env_int("WEVIKO_LIVE_CAPTURE_MAX_SCRAPED_CHARS", 14000),
        "max_section_chars": _env_int("WEVIKO_LIVE_CAPTURE_MAX_SECTION_CHARS", 4000),
        "max_table_rows": _env_int("WEVIKO_LIVE_CAPTURE_MAX_TABLE_ROWS", 24),
    }


def _normalize_host(host: str | None) -> str:
    return str(host or "").strip().lower().rstrip(".")


def _host_matches_pattern(host: str, pattern: str) -> bool:
    normalized_host = _normalize_host(host)
    normalized_pattern = _normalize_host(pattern)
    if not normalized_host or not normalized_pattern:
        return False
    if normalized_pattern == "*":
        return True
    if normalized_pattern.startswith("*."):
        bare_pattern = normalized_pattern[2:]
        return normalized_host == bare_pattern or normalized_host.endswith(f".{bare_pattern}")
    return normalized_host == normalized_pattern


def is_capture_url_allowed(url: str, allowed_hosts: list[str] | None = None) -> tuple[bool, str]:
    try:
        parsed = urlparse(str(url or "").strip())
    except Exception:
        return False, "invalid_url"

    if parsed.scheme not in {"http", "https"}:
        return False, "only_http_https_urls_are_allowed"

    hostname = _normalize_host(parsed.hostname)
    if not hostname:
        return False, "missing_hostname"

    host_patterns = allowed_hosts if allowed_hosts is not None else live_capture_allowed_hosts()
    if not host_patterns:
        return False, "no_allowed_hosts_configured"

    if any(_host_matches_pattern(hostname, pattern) for pattern in host_patterns):
        return True, "allowed"
    return False, f"host_not_allowed:{hostname}"


def guess_live_capture_schema(url: str) -> dict[str, str]:
    path = urlparse(str(url or "").strip()).path.lower()
    for match_text, schema_key, source_path_hint, document_type in LIVE_CAPTURE_SCHEMA_GUESSES:
        if match_text in path:
            return {
                "schema_key": schema_key,
                "source_path_hint": source_path_hint,
                "document_type": document_type,
            }
    return dict(DEFAULT_LIVE_CAPTURE_SCHEMA)


def _normalize_text(value: Any) -> str:
    text = str(value or "")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    lines = [re.sub(r"\s+", " ", line).strip() for line in text.split("\n")]
    return "\n".join(line for line in lines if line)


def _truncate_text(value: str, max_chars: int) -> str:
    cleaned = _normalize_text(value)
    if max_chars <= 0:
        return cleaned
    if len(cleaned) <= max_chars:
        return cleaned
    return f"{cleaned[: max_chars - 3].rstrip()}..."


def _extract_heading_lines(soup: BeautifulSoup) -> str:
    headings: list[str] = []
    for tag in soup.find_all(["h1", "h2", "h3"], limit=12):
        text = _normalize_text(tag.get_text(" ", strip=True))
        if text and text not in headings:
            headings.append(text)
    return "\n".join(headings)


def _extract_breadcrumb_text(soup: BeautifulSoup) -> str:
    selectors = [
        "nav[aria-label*=breadcrumb]",
        "[class*=breadcrumb]",
        "ol.breadcrumb",
        "ul.breadcrumb",
    ]
    for selector in selectors:
        node = soup.select_one(selector)
        if node is None:
            continue
        text = _normalize_text(node.get_text(" > ", strip=True))
        if text:
            return text
    return ""


def _extract_table_lines(soup: BeautifulSoup, max_table_rows: int) -> str:
    rendered_rows: list[str] = []
    row_count = 0
    for table in soup.find_all("table", limit=6):
        for row in table.find_all("tr"):
            cells = [_normalize_text(cell.get_text(" ", strip=True)) for cell in row.find_all(["th", "td"])]
            cells = [cell for cell in cells if cell]
            if not cells:
                continue
            rendered_rows.append(" | ".join(cells))
            row_count += 1
            if row_count >= max_table_rows:
                return "\n".join(rendered_rows)
    return "\n".join(rendered_rows)


def _extract_meta_description(soup: BeautifulSoup) -> str:
    for selector in ('meta[name="description"]', 'meta[property="og:description"]'):
        node = soup.select_one(selector)
        if node is None:
            continue
        content = _normalize_text(node.get("content"))
        if content:
            return content
    return ""


def _pick_part_number_candidate(text: str) -> list[str]:
    candidates: list[tuple[int, str]] = []
    for token in re.findall(r"\b[A-Z0-9-]{5,20}\b", str(text or "").upper()):
        cleaned = token.strip("-")
        if cleaned in PART_NUMBER_BLOCKLIST:
            continue
        if not any(char.isalpha() for char in cleaned):
            continue
        if not any(char.isdigit() for char in cleaned):
            continue
        if cleaned.isdigit():
            continue
        digit_count = sum(char.isdigit() for char in cleaned)
        hyphen_bonus = 2 if "-" in cleaned else 0
        candidates.append((digit_count + len(cleaned) + hyphen_bonus, cleaned))
    candidates.sort(reverse=True)
    return [value for _, value in candidates[:5]]


def guess_part_number_hint(*texts: str) -> str:
    seen: list[str] = []
    for text in texts:
        for candidate in _pick_part_number_candidate(text):
            if candidate not in seen:
                seen.append(candidate)
    return seen[0] if seen else ""


def build_live_capture_scraped_text(capture_payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    limits = live_capture_limits()
    raw_url = str(capture_payload.get("url") or "").strip()
    raw_title = str(capture_payload.get("title") or "").strip()
    raw_html = str(capture_payload.get("html") or "")[: limits["max_html_chars"]]
    raw_text = str(capture_payload.get("text") or "")[: limits["max_text_chars"]]
    selection_text = str(capture_payload.get("selection_text") or "")
    operator_note = str(capture_payload.get("operator_note") or "").strip()
    operator_identifier = str(capture_payload.get("operator_identifier") or "").strip()
    capture_channel = str(capture_payload.get("capture_channel") or "bookmarklet").strip() or "bookmarklet"
    capture_client_version = str(
        capture_payload.get("capture_client_version")
        or capture_payload.get("bookmarklet_version")
        or "weviko-live-1"
    ).strip()

    guessed_schema = guess_live_capture_schema(raw_url)
    soup = BeautifulSoup(raw_html, "html.parser") if raw_html else None
    html_text = ""
    heading_lines = ""
    breadcrumb_text = ""
    table_lines = ""
    meta_description = ""

    if soup is not None:
        for tag in soup(["script", "style", "noscript", "template", "svg", "canvas"]):
            tag.decompose()
        heading_lines = _extract_heading_lines(soup)
        breadcrumb_text = _extract_breadcrumb_text(soup)
        table_lines = _extract_table_lines(soup, limits["max_table_rows"])
        meta_description = _extract_meta_description(soup)
        html_text = _normalize_text(soup.get_text("\n", strip=True))

    dominant_text = _normalize_text(raw_text) or html_text
    clean_selection = _truncate_text(selection_text, 1600)
    clean_meta_description = _truncate_text(meta_description, 1200)
    clean_headings = _truncate_text(heading_lines, 1200)
    clean_breadcrumbs = _truncate_text(breadcrumb_text, 600)
    clean_tables = _truncate_text(table_lines, limits["max_section_chars"])
    clean_visible_text = _truncate_text(dominant_text, limits["max_scraped_chars"])
    clean_title = _truncate_text(raw_title, 300)
    clean_operator_note = _truncate_text(operator_note, 300)

    guessed_part_number = guess_part_number_hint(clean_selection, clean_title, clean_tables, clean_visible_text)
    parsed_url = urlparse(raw_url)
    source_host = _normalize_host(parsed_url.hostname)

    sections: list[str] = []
    metadata_lines = [
        f"Source URL: {raw_url or 'Unknown'}",
        f"Source host: {source_host or 'Unknown'}",
        f"Page title: {clean_title or 'Unknown'}",
        f"Schema guess: {guessed_schema['schema_key']}",
        f"Source path hint: {guessed_schema['source_path_hint'] or parsed_url.path or '/'}",
    ]
    if clean_operator_note:
        metadata_lines.append(f"Operator note: {clean_operator_note}")
    if operator_identifier:
        metadata_lines.append(f"Operator identifier: {_truncate_text(operator_identifier, 240)}")
    if guessed_part_number:
        metadata_lines.append(f"Part number candidate: {guessed_part_number}")
    sections.append("[Live Capture Metadata]\n" + "\n".join(metadata_lines))

    if clean_selection:
        sections.append("[Operator Selection]\n" + clean_selection)
    if clean_breadcrumbs:
        sections.append("[Breadcrumb]\n" + clean_breadcrumbs)
    if clean_headings:
        sections.append("[Headings]\n" + clean_headings)
    if clean_meta_description:
        sections.append("[Meta Description]\n" + clean_meta_description)
    if clean_tables:
        sections.append("[Structured Table Extract]\n" + clean_tables)
    if clean_visible_text:
        sections.append("[Visible Page Text]\n" + clean_visible_text)

    combined = "\n\n".join(section for section in sections if section.strip())
    combined = _truncate_text(combined, limits["max_scraped_chars"])

    capture_meta = {
        "capture_type": "browser_live_capture",
        "capture_channel": capture_channel,
        "manual_capture": True,
        "source_host": source_host,
        "page_title": clean_title,
        "selection_preview": clean_selection,
        "operator_note": clean_operator_note,
        "capture_client_version": capture_client_version,
        "guessed_schema_key": guessed_schema["schema_key"],
        "guessed_path_hint": guessed_schema["source_path_hint"],
        "guessed_part_number": guessed_part_number,
        "html_chars": len(raw_html),
        "visible_text_chars": len(_normalize_text(raw_text)),
        "selection_chars": len(_normalize_text(selection_text)),
        "scraped_chars": len(combined),
    }
    return combined, capture_meta


def build_live_capture_bookmarklet(server_url: str | None = None) -> str:
    capture_endpoint = f"{(server_url or live_capture_base_url()).rstrip('/')}/capture"
    bookmarklet_js = f"""
javascript:(async()=>{{
const endpoint={json.dumps(capture_endpoint)};
const inferSchema=()=>{{
  const path=(location.pathname||'').toLowerCase();
  if(path.includes('/item/detail/')) return 'path_detail';
  if(path.includes('/shop/manual/')) return 'path_manual';
  if(path.includes('/body/manual/')) return 'path_body_manual';
  if(path.includes('/contents/etc/')) return 'path_wiring';
  if(path.includes('/wiring/connector/')) return 'path_connector';
  if(path.includes('/dtc/')) return 'path_dtc';
  if(path.includes('/community/')) return 'path_community';
  if(path.includes('/vehicle-id/')||path.includes('/vin/')) return 'path_vehicle_id';
  return 'path_manual';
}};
const clean=(value,maxChars)=>String(value||'').replace(/\\s+/g,' ').trim().slice(0,maxChars);
const schemaKey=(prompt('Schema key', inferSchema())||inferSchema()).trim()||inferSchema();
const destinationInput=(prompt('Destination: pending or parts', 'pending')||'pending').trim().toLowerCase();
const market=(prompt('Market', 'GLOBAL')||'GLOBAL').trim().toUpperCase()||'GLOBAL';
const operatorNote=(prompt('Operator note (optional)', '')||'').trim();
const selectionText=window.getSelection?String(window.getSelection().toString()||''):'';
const payload={{
  url: location.href,
  title: document.title||'',
  html: document.documentElement?document.documentElement.outerHTML.slice(0,350000):'',
  text: document.body?String(document.body.innerText||document.body.textContent||'').slice(0,120000):'',
  selection_text: selectionText.slice(0,12000),
  schema_key: schemaKey,
  destination: destinationInput==='parts'?'parts':'pending',
  market,
  operator_note: operatorNote.slice(0,500),
  document_type: '',
  source_path_hint: location.pathname||'/',
  bookmarklet_version: 'weviko-live-1',
  sent_at: new Date().toISOString()
}};
try {{
  const response=await fetch(endpoint,{{
    method:'POST',
    mode:'cors',
    headers:{{'Content-Type':'application/json'}},
    body:JSON.stringify(payload),
  }});
  const data=await response.json().catch(()=>({{}}));
  if(!response.ok){{
    throw new Error(data.message||('Capture failed ('+response.status+')'));
  }}
  alert(
    'Weviko capture complete\\n'
    +'Destination: '+(data.destination||'-')+'\\n'
    +'Score: '+(data.confidence_score??'-')+'\\n'
    +'Quality: '+(data.quality_status||'-')+'\\n'
    +(data.part_number?'Part: '+data.part_number+'\\n':'')
    +(data.message||'')
  );
}} catch (error) {{
  alert('Weviko capture failed\\n'+((error&&error.message)?error.message:String(error)));
}}
}})();
"""
    return re.sub(r"\s+", " ", bookmarklet_js).strip()
