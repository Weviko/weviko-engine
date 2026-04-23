from __future__ import annotations

import html
import json
import os
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from dotenv import load_dotenv

from live_capture import (
    build_live_capture_bookmarklet,
    build_live_capture_scraped_text,
    guess_live_capture_schema,
    is_capture_url_allowed,
    live_capture_allowed_hosts,
    live_capture_base_url,
    live_capture_direct_enabled,
    live_capture_host,
    live_capture_port,
)
from streamlit_services import log_dead_letter, process_scraped_text_and_save


load_dotenv()


KNOWN_SCHEMA_KEYS = {
    "path_manual",
    "path_detail",
    "path_body_manual",
    "path_wiring",
    "path_connector",
    "path_community",
    "path_dtc",
    "path_vehicle_id",
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _env_int(name: str, default: int) -> int:
    raw_value = str(os.getenv(name, "") or "").strip()
    if not raw_value:
        return default
    try:
        return int(raw_value)
    except ValueError:
        return default


def _server_message(message: str, *, extra: str = "") -> str:
    if extra:
        return f"{message} {extra}".strip()
    return message


def build_health_payload() -> dict[str, Any]:
    return {
        "ok": True,
        "server_url": live_capture_base_url(),
        "allowed_hosts": live_capture_allowed_hosts(),
        "direct_live_capture_enabled": live_capture_direct_enabled(),
        "timestamp": _utc_now(),
    }


def build_setup_html() -> str:
    bookmarklet = build_live_capture_bookmarklet(live_capture_base_url())
    health_url = f"{live_capture_base_url()}/health"
    bookmarklet_html = html.escape(bookmarklet)
    allowed_hosts = ", ".join(live_capture_allowed_hosts()) or "(none)"
    direct_mode = "enabled" if live_capture_direct_enabled() else "disabled"
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Weviko Live Capture</title>
  <style>
    body {{
      margin: 0;
      font-family: "Segoe UI", sans-serif;
      background: linear-gradient(180deg, #f4efe6 0%, #fbf6ed 100%);
      color: #1e261f;
    }}
    main {{
      max-width: 960px;
      margin: 0 auto;
      padding: 32px 20px 48px;
    }}
    .panel {{
      background: rgba(255,255,255,0.86);
      border: 1px solid #d7c9ad;
      border-radius: 18px;
      padding: 18px 20px;
      margin-bottom: 18px;
      box-shadow: 0 12px 28px rgba(58, 43, 16, 0.08);
    }}
    h1 {{
      margin-top: 0;
      font-size: 2rem;
    }}
    textarea {{
      width: 100%;
      min-height: 180px;
      font-family: Consolas, monospace;
      font-size: 12px;
      border-radius: 12px;
      border: 1px solid #d7c9ad;
      padding: 12px;
      box-sizing: border-box;
      background: #fffdfa;
    }}
    code {{
      background: rgba(36, 88, 70, 0.08);
      padding: 2px 6px;
      border-radius: 8px;
    }}
    ul {{
      margin-top: 8px;
      padding-left: 20px;
    }}
    .bookmarklet-link {{
      display: inline-block;
      margin-top: 10px;
      padding: 10px 14px;
      border-radius: 999px;
      background: #245846;
      color: white;
      text-decoration: none;
      font-weight: 700;
    }}
    .meta {{
      color: #5d685c;
      font-size: 0.96rem;
    }}
    button {{
      margin-top: 10px;
      border: 0;
      border-radius: 999px;
      padding: 10px 14px;
      background: #9c4f12;
      color: white;
      cursor: pointer;
      font-weight: 700;
    }}
  </style>
</head>
<body>
  <main>
    <div class="panel">
      <h1>Weviko Live Browser Capture</h1>
      <p class="meta">Server: <code>{html.escape(live_capture_base_url())}</code></p>
      <p class="meta">Allowed hosts: <code>{html.escape(allowed_hosts)}</code></p>
      <p class="meta">Direct live capture: <code>{direct_mode}</code></p>
      <p>Use this only on sites you own, administer, or are explicitly authorized to access. This flow is manual by design and stores structured facts instead of full page HTML.</p>
    </div>

    <div class="panel">
      <h2>Setup</h2>
      <ul>
        <li>Start the server with <code>python live_capture_server.py</code>.</li>
        <li>Open this page in your browser and drag the bookmarklet below to the bookmarks bar.</li>
        <li>Or load the unpacked Chrome extension from <code>chrome_extension/weviko-live-capture</code>.</li>
        <li>While viewing an allowed page, click the bookmarklet and confirm schema and destination.</li>
        <li>The capture result will be processed by the same Gemini and Supabase pipeline as the crawler.</li>
      </ul>
      <a class="bookmarklet-link" href="{bookmarklet_html}">Weviko Capture</a>
      <p class="meta">If drag-and-drop is awkward in your browser, copy the code below into a new bookmark URL.</p>
      <textarea id="bookmarklet-code" readonly>{bookmarklet_html}</textarea>
      <button type="button" onclick="navigator.clipboard.writeText(document.getElementById('bookmarklet-code').value)">Copy Bookmarklet</button>
    </div>

    <div class="panel">
      <h2>Health</h2>
      <p>Health endpoint: <a href="{html.escape(health_url)}">{html.escape(health_url)}</a></p>
      <p class="meta">Recommended rollout: keep destination on <code>pending</code> until the workshop validates the pages and host allowlist.</p>
    </div>
  </main>
</body>
</html>"""


class LiveCaptureHandler(BaseHTTPRequestHandler):
    server_version = "WevikoLiveCapture/1.0"

    def _send_json(self, status_code: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, status_code: int, payload: str) -> None:
        body = payload.encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/health":
            self._send_json(200, build_health_payload())
            return
        if self.path in {"/", "/setup"}:
            self._send_html(200, build_setup_html())
            return
        if self.path == "/bookmarklet":
            self._send_json(
                200,
                {
                    "ok": True,
                    "bookmarklet": build_live_capture_bookmarklet(live_capture_base_url()),
                    "server_url": live_capture_base_url(),
                },
            )
            return
        self._send_json(404, {"ok": False, "message": "Not found."})

    def do_POST(self) -> None:  # noqa: N802
        if self.path != "/capture":
            self._send_json(404, {"ok": False, "message": "Not found."})
            return
        self._handle_capture()

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return

    def _handle_capture(self) -> None:
        max_body_bytes = _env_int("WEVIKO_LIVE_CAPTURE_MAX_BODY_BYTES", 600000)
        content_length = _env_int("CONTENT_LENGTH", 0)
        if not content_length:
            try:
                content_length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                content_length = 0

        if content_length <= 0:
            self._send_json(400, {"ok": False, "message": "Capture payload is empty."})
            return
        if content_length > max_body_bytes:
            self._send_json(413, {"ok": False, "message": f"Capture payload exceeds {max_body_bytes} bytes."})
            return

        raw_body = self.rfile.read(content_length)
        try:
            capture_payload = json.loads(raw_body.decode("utf-8"))
        except Exception:
            self._send_json(400, {"ok": False, "message": "Capture payload must be valid JSON."})
            return

        source_url = str(capture_payload.get("url") or "").strip()
        if not source_url:
            self._send_json(400, {"ok": False, "message": "A source URL is required."})
            return

        is_allowed, allow_reason = is_capture_url_allowed(source_url)
        if not is_allowed:
            self._send_json(
                403,
                {
                    "ok": False,
                    "message": f"Capture blocked: {allow_reason}. Update WEVIKO_ALLOWED_CAPTURE_HOSTS if you are authorized to use this host.",
                    "allowed_hosts": live_capture_allowed_hosts(),
                },
            )
            return

        guessed_schema = guess_live_capture_schema(source_url)
        requested_schema_key = str(capture_payload.get("schema_key") or "").strip()
        schema_key = requested_schema_key if requested_schema_key in KNOWN_SCHEMA_KEYS else guessed_schema["schema_key"]
        raw_path_hint = str(capture_payload.get("source_path_hint") or "").strip()
        source_path_hint = guessed_schema["source_path_hint"] or raw_path_hint
        raw_document_type = str(capture_payload.get("document_type") or "").strip()
        raw_title = str(capture_payload.get("title") or "").strip()
        if not raw_document_type or raw_document_type == raw_title:
            document_type = guessed_schema["document_type"]
        else:
            document_type = raw_document_type
        market = str(capture_payload.get("market") or "GLOBAL").strip().upper() or "GLOBAL"

        requested_destination = str(capture_payload.get("destination") or "pending").strip().lower()
        destination = "parts" if requested_destination == "parts" else "pending"
        direct_requested = destination == "parts"
        if direct_requested and not live_capture_direct_enabled():
            destination = "pending"

        scraped_text, capture_meta = build_live_capture_scraped_text(capture_payload)
        guessed_part_number = str(capture_meta.get("guessed_part_number") or "").strip()
        part_number_hint = str(capture_payload.get("part_number_hint") or "").strip() or guessed_part_number

        if len(scraped_text.strip()) < 80:
            log_dead_letter(
                source_url,
                "live_capture_insufficient_text",
                source_type="browser_live_capture",
                schema_key=schema_key,
                source_path_hint=source_path_hint,
                payload={"capture_meta": capture_meta, "document_type": document_type},
            )
            self._send_json(
                422,
                {
                    "ok": False,
                    "message": "The current tab did not yield enough visible text to analyze.",
                    "schema_key": schema_key,
                },
            )
            return

        extra_metadata = {
            **capture_meta,
            "capture_requested_destination": requested_destination,
            "capture_effective_destination": destination,
            "capture_allowed_reason": allow_reason,
            "capture_document_type": document_type,
        }
        if direct_requested and destination != "parts":
            extra_metadata["direct_capture_downgraded"] = True

        payload, save_result = process_scraped_text_and_save(
            scraped_text=scraped_text,
            doc_type_key=schema_key,
            market=market,
            destination=destination,
            part_number_hint=part_number_hint,
            oem_brand=str(capture_payload.get("oem_brand") or "").strip(),
            source_path_hint=source_path_hint,
            document_type=document_type,
            source_url=source_url,
            vehicle_hint=str(capture_payload.get("vehicle_hint") or "").strip(),
            system_hint=str(capture_payload.get("system_hint") or "").strip(),
            operator_identifier=(
                str(capture_payload.get("operator_identifier") or "").strip()
                or str(capture_payload.get("operator_note") or "").strip()
            ),
            extra_metadata=extra_metadata,
            source_type_override="browser_live_capture",
        )

        if not save_result.get("saved"):
            log_dead_letter(
                source_url,
                f"live_capture_save_failed: {save_result.get('message', 'unknown_error')}",
                source_type="browser_live_capture",
                schema_key=schema_key,
                source_path_hint=source_path_hint,
                payload={"capture_meta": capture_meta, "payload": payload},
            )
            self._send_json(
                502,
                {
                    "ok": False,
                    "message": save_result.get("message", "Live capture save failed."),
                    "schema_key": schema_key,
                    "quality_status": payload.get("quality_status", "unknown"),
                },
            )
            return

        response_message = save_result.get("message", "Capture stored successfully.")
        if direct_requested and destination != "parts":
            response_message = _server_message(
                response_message,
                extra="Direct mode was downgraded to pending because WEVIKO_LIVE_CAPTURE_DIRECT_ENABLED is disabled.",
            )

        self._send_json(
            200,
            {
                "ok": True,
                "saved": True,
                "destination": save_result.get("destination", "Pending"),
                "confidence_score": save_result.get("confidence_score"),
                "confidence_threshold": save_result.get("confidence_threshold"),
                "quality_status": payload.get("quality_status"),
                "quality_reasons": payload.get("quality_reasons", []),
                "schema_key": payload.get("schema_key", schema_key),
                "document_type": payload.get("document_type", document_type),
                "part_number": payload.get("part_number", part_number_hint or "Unknown"),
                "page_title": payload.get("title") or capture_meta.get("page_title", ""),
                "message": response_message,
                "manual_capture": True,
                "direct_requested": direct_requested,
                "direct_applied": destination == "parts",
            },
        )


def run_server() -> None:
    host = live_capture_host()
    port = live_capture_port()
    server = ThreadingHTTPServer((host, port), LiveCaptureHandler)
    print(f"[Live Capture] listening on {live_capture_base_url()}")
    print(f"[Live Capture] allowed hosts: {', '.join(live_capture_allowed_hosts())}")
    print(f"[Live Capture] direct mode: {'enabled' if live_capture_direct_enabled() else 'disabled'}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[Live Capture] shutdown requested.")
    finally:
        server.server_close()


if __name__ == "__main__":
    run_server()
