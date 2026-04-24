from __future__ import annotations

import base64
import hashlib
import json
import os
import requests
import re
import traceback
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from langchain_core.messages import HumanMessage
from pydantic import BaseModel, Field

from weviko_engine import run_crawler_sync
from main import build_llm, build_supabase_client, invoke_llm_with_fallback

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
load_dotenv()


class VisionFactBundle(BaseModel):
    """Structured extraction result for uploaded automotive document images."""

    part_number: str = Field(
        default="Unknown", description="Detected automotive part number."
    )
    oem_brand: str = Field(
        default="", description="OEM brand associated with the part."
    )
    schema_key: str = Field(
        default="", description="Internal schema key selected by the operator."
    )
    source_path_hint: str = Field(
        default="", description="Source path hint selected by the operator."
    )
    document_type: str = Field(default="Unknown", description="Detected document type.")
    summary: str = Field(default="", description="Short summary of the image contents.")
    extracted_facts: dict[str, Any] = Field(
        default_factory=dict,
        description="Only measurable or operational automotive facts extracted from the image.",
    )
    cautions: list[str] = Field(
        default_factory=list,
        description="Short cautions or uncertainties found during extraction.",
    )


class CrawlFactBundle(BaseModel):
    """Structured extraction result for crawled automotive text pages."""

    part_number: str = Field(
        default="Unknown", description="Detected automotive part number."
    )
    oem_brand: str = Field(default="", description="OEM brand or manufacturer.")
    schema_key: str = Field(default="", description="Internal schema key.")
    source_path_hint: str = Field(default="", description="Selected source path hint.")
    document_type: str = Field(default="Unknown", description="Detected document type.")
    title: str = Field(default="", description="Short title for the crawled page.")
    summary: str = Field(default="", description="Short summary for technicians.")
    vehicle: dict[str, Any] = Field(
        default_factory=dict, description="Vehicle applicability metadata."
    )
    compatibility: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Vehicle compatibility entries when available.",
    )
    specifications: dict[str, Any] = Field(
        default_factory=dict,
        description="Structured numeric or categorical specifications.",
    )
    extracted_facts: dict[str, Any] = Field(
        default_factory=dict,
        description="Only measurable or operational facts extracted from the text.",
    )
    cautions: list[str] = Field(
        default_factory=list,
        description="Short cautions or ambiguities found during extraction.",
    )


class TranslationBundle(BaseModel):
    """Multilingual translation package for a structured automotive payload."""

    ko: dict[str, Any] = Field(default_factory=dict, description="Korean JSON payload.")
    en: dict[str, Any] = Field(
        default_factory=dict, description="English JSON payload."
    )
    vn: dict[str, Any] = Field(
        default_factory=dict, description="Vietnamese JSON payload."
    )
    notes: str = Field(default="", description="Short translation notes or warnings.")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _prompt_store_path() -> Path:
    raw_path = (
        os.getenv("WEVIKO_PROMPTS_FILE", "prompt_templates.json").strip()
        or "prompt_templates.json"
    )
    path = Path(raw_path)
    if not path.is_absolute():
        path = Path(__file__).resolve().parent / path
    return path


def _clean_json_value(value: Any) -> Any:
    return json.loads(json.dumps(value, ensure_ascii=False, default=str))


@lru_cache(maxsize=1)
def get_cached_llm():
    return build_llm()


@lru_cache(maxsize=1)
def get_cached_supabase_client():
    return build_supabase_client()


def llm_available() -> bool:
    return get_cached_llm() is not None


def supabase_available() -> bool:
    return get_cached_supabase_client() is not None


def prompt_tables_name() -> str:
    return (
        os.getenv("WEVIKO_PROMPTS_TABLE", "system_prompts").strip() or "system_prompts"
    )


def review_table_name() -> str:
    return (
        os.getenv("WEVIKO_REVIEW_TABLE", "review_decisions").strip()
        or "review_decisions"
    )


def translation_table_name() -> str:
    return (
        os.getenv("WEVIKO_TRANSLATIONS_TABLE", "part_translations").strip()
        or "part_translations"
    )


def vision_table_name() -> str:
    return (
        os.getenv("WEVIKO_VISION_TABLE", "vision_analysis").strip() or "vision_analysis"
    )


def parts_table_name() -> str:
    return os.getenv("WEVIKO_PARTS_TABLE", "parts").strip() or "parts"


def gsw_documents_table_name() -> str:
    return (
        os.getenv("WEVIKO_GSW_DOCUMENTS_TABLE", "gsw_documents").strip()
        or "gsw_documents"
    )


def pending_table_name() -> str:
    return os.getenv("WEVIKO_PENDING_TABLE", "pending_data").strip() or "pending_data"


def configs_table_name() -> str:
    return os.getenv("WEVIKO_CONFIGS_TABLE", "configs").strip() or "configs"


def dead_letters_table_name() -> str:
    return (
        os.getenv("WEVIKO_DEAD_LETTERS_TABLE", "dead_letters").strip() or "dead_letters"
    )


def scheduled_crawls_table_name() -> str:
    return (
        os.getenv("WEVIKO_SCHEDULED_CRAWLS_TABLE", "scheduled_crawls").strip()
        or "scheduled_crawls"
    )


def load_prompt_templates(defaults: dict[str, str]) -> tuple[dict[str, str], str]:
    prompts = dict(defaults)
    source = "defaults"
    prompt_file = _prompt_store_path()

    if prompt_file.exists():
        try:
            local_prompts = json.loads(prompt_file.read_text(encoding="utf-8"))
            if isinstance(local_prompts, dict):
                for name, text in local_prompts.items():
                    if isinstance(name, str) and isinstance(text, str) and text.strip():
                        prompts[name] = text
                source = "local_file"
        except Exception:
            pass

    client = get_cached_supabase_client()
    if client is not None:
        try:
            response = client.table(prompt_tables_name()).select("*").execute()
            for row in getattr(response, "data", []) or []:
                name = row.get("name") or row.get("prompt_name")
                prompt_text = (
                    row.get("prompt_text") or row.get("prompt") or row.get("content")
                )
                if name and prompt_text:
                    prompts[str(name)] = str(prompt_text)
                    source = "supabase"
        except Exception:
            pass

    return prompts, source


def load_config_prompts(defaults: dict[str, str]) -> tuple[dict[str, str], str]:
    prompts = dict(defaults)
    source = "defaults"
    prompt_file = _prompt_store_path()

    if prompt_file.exists():
        try:
            local_prompts = json.loads(prompt_file.read_text(encoding="utf-8"))
            if isinstance(local_prompts, dict):
                for name, text in local_prompts.items():
                    if isinstance(name, str) and isinstance(text, str) and text.strip():
                        prompts[name] = text
                source = "local_file"
        except Exception:
            pass

    client = get_cached_supabase_client()
    if client is None:
        return prompts, source

    try:
        response = (  # type: ignore
            client.table(configs_table_name())
            .select("prompt_key,prompt_value")
            .execute()
        )
        for row in getattr(response, "data", []) or []:
            prompt_key = row.get("prompt_key")
            prompt_value = row.get("prompt_value")
            if prompt_key and prompt_value:
                prompts[str(prompt_key)] = str(prompt_value)
                source = "supabase"
    except Exception as exc:
        logger.warning(
            f"[Configs] prompt load failed. Falling back to local/default prompts: {exc}"
        )
        pass

    return prompts, source


def save_prompt_template(name: str, prompt_text: str) -> dict[str, Any]:
    prompts, _ = load_prompt_templates({})
    prompts[name] = prompt_text
    prompt_file = _prompt_store_path()
    prompt_file.write_text(
        json.dumps(prompts, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    result = {
        "local_saved": True,
        "remote_saved": False,
        "source": "local_file",
        "message": "로컬 파일에 저장되었습니다.",
    }

    client = get_cached_supabase_client()
    if client is None:
        return result

    try:
        client.table(prompt_tables_name()).upsert(
            {
                "name": name,
                "prompt_text": prompt_text,
                "updated_at": _utc_now_iso(),
            },
            on_conflict="name",
        ).execute()
        result["remote_saved"] = True
        result["source"] = "supabase"
        result["message"] = "Supabase와 로컬 파일에 저장되었습니다."
    except Exception as exc:
        result["message"] = (
            f"로컬 파일 저장은 성공했고, Supabase 저장은 실패했습니다: {exc}"
        )

    return result


def get_config_prompt(prompt_key: str, fallback_text: str) -> tuple[str, str]:
    prompts, source = load_config_prompts({prompt_key: fallback_text})
    return prompts.get(prompt_key, fallback_text), source


def get_system_prompt(prompt_key: str, fallback_text: str = "") -> str:
    prompt_text, _ = get_config_prompt(prompt_key, fallback_text)
    return prompt_text


def get_config_int_value(config_key: str, default_value: int) -> int:
    raw_value = get_system_prompt(config_key, str(default_value)).strip()
    try:
        return int(raw_value)
    except (TypeError, ValueError):
        return default_value


def save_config_prompt(prompt_key: str, prompt_text: str) -> dict[str, Any]:
    prompts, _ = load_config_prompts({})
    prompts[prompt_key] = prompt_text
    prompt_file = _prompt_store_path()
    prompt_file.write_text(
        json.dumps(prompts, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    result = {
        "local_saved": True,
        "remote_saved": False,
        "source": "local_file",
        "message": "프롬프트를 로컬 파일에 저장했습니다.",
    }

    client = get_cached_supabase_client()
    if client is None:
        return result

    try:
        client.table(configs_table_name()).upsert(
            {
                "prompt_key": prompt_key,
                "prompt_value": prompt_text,
                "updated_at": _utc_now_iso(),
            },
            on_conflict="prompt_key",
        ).execute()
        result["remote_saved"] = True
        result["source"] = "supabase"
        result["message"] = "프롬프트를 Supabase와 로컬 파일에 저장했습니다."
    except Exception as exc:
        result["message"] = (
            f"로컬 파일 저장은 성공했고, Supabase 저장은 실패했습니다: {exc}"
        )

    return result


def reset_prompt_templates(defaults: dict[str, str]) -> dict[str, Any]:
    prompt_file = _prompt_store_path()
    prompt_file.write_text(
        json.dumps(defaults, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    result = {
        "local_saved": True,
        "remote_saved": False,
        "source": "local_file",
        "message": "기본 프롬프트를 로컬 파일에 복원했습니다.",
    }

    client = get_cached_supabase_client()
    if client is None:
        return result

    rows = [
        {
            "name": name,
            "prompt_text": text,
            "updated_at": _utc_now_iso(),
        }
        for name, text in defaults.items()
    ]
    try:
        client.table(prompt_tables_name()).upsert(rows, on_conflict="name").execute()
        result["remote_saved"] = True
        result["source"] = "supabase"
        result["message"] = "기본 프롬프트를 Supabase와 로컬 파일에 복원했습니다."
    except Exception as exc:
        result["message"] = (
            f"로컬 파일 복원은 성공했고, Supabase 반영은 실패했습니다: {exc}"
        )

    return result


def create_scheduled_crawl(
    start_url: str,
    schema_key: str,
    schedule_interval: str,
    num_workers: int | None = None,
    max_urls: int | None = None,
    discovery_max_pages: int | None = None,
    discovery_max_matches: int | None = None,
    discovery_max_depth: int | None = None,
    product_path_hint: str | None = None,
    discovery_extra_path_hints: list[str] | None = None,
    route_watch_hints: list[str] | None = None,
    blocked_resource_types: list[str] | None = None,
    user_agent: str | None = None,
    next_run_at: datetime | None = None,
) -> dict[str, Any]:
    client = get_cached_supabase_client()
    if client is None:
        return {
            "saved": False,
            "message": "Supabase가 설정되지 않아 스케줄을 생성할 수 없습니다.",
        }

    try:
        payload = {
            "start_url": start_url,
            "schema_key": schema_key,
            "schedule_interval": schedule_interval,
            "next_run_at": (next_run_at or datetime.now(timezone.utc)).isoformat(),
            "is_active": True,
            "progress_log": "",
            "current_progress": 0,
            "total_progress": 0,
            "num_workers": num_workers,
            "max_urls": max_urls,
            "discovery_max_pages": discovery_max_pages,
            "discovery_max_matches": discovery_max_matches,
            "discovery_max_depth": discovery_max_depth,
            "product_path_hint": product_path_hint,
            "discovery_extra_path_hints": discovery_extra_path_hints,
            "route_watch_hints": route_watch_hints,
            "blocked_resource_types": blocked_resource_types,
            "user_agent": user_agent,
            "created_at": _utc_now_iso(),
            "updated_at": _utc_now_iso(),
        }
        client.table(scheduled_crawls_table_name()).insert(payload).execute()
        return {"saved": True, "message": "크롤링 스케줄이 성공적으로 생성되었습니다."}
    except Exception as exc:
        logger.error(f"스케줄 생성 실패: {exc}", exc_info=True)
        return {"saved": False, "message": f"스케줄 생성 실패: {exc}"}


def fetch_scheduled_crawls() -> list[dict[str, Any]]:
    client = get_cached_supabase_client()
    if client is None:
        return []

    try:
        response = (
            client.table(scheduled_crawls_table_name())
            .select("*")
            .order("next_run_at", desc=False)
            .execute()
        )
        return list(getattr(response, "data", []) or [])
    except Exception as exc:
        logger.error(f"스케줄 조회 실패: {exc}", exc_info=True)
        return []


def delete_scheduled_crawl(schedule_id: Any) -> dict[str, Any]:
    client = get_cached_supabase_client()
    if client is None:
        return {
            "saved": False,
            "message": "Supabase가 설정되지 않아 스케줄을 삭제할 수 없습니다.",
        }

    try:
        client.table(scheduled_crawls_table_name()).delete().eq(
            "id", schedule_id
        ).execute()
        return {"saved": True, "message": "스케줄이 성공적으로 삭제되었습니다."}
    except Exception as exc:
        logger.error(f"스케줄 삭제 실패: {exc}", exc_info=True)
        return {"saved": False, "message": f"스케줄 삭제 실패: {exc}"}


def _calculate_next_run_time(schedule_interval: str) -> datetime:
    now = datetime.now(timezone.utc)
    if schedule_interval == "daily":
        return now + relativedelta(days=1)
    elif schedule_interval == "weekly":
        return now + relativedelta(weeks=1)
    elif schedule_interval == "monthly":
        return now + relativedelta(months=1)
    return now  # For 'once' or unknown, run immediately


def execute_scheduled_crawl_job(schedule_id: Any) -> dict[str, Any]:
    client = get_cached_supabase_client()
    if client is None:
        return {
            "success": False,
            "message": "Supabase가 설정되지 않아 스케줄을 실행할 수 없습니다.",
        }

    try:
        response = (
            client.table(scheduled_crawls_table_name())
            .select("*")
            .eq("id", schedule_id)
            .single()
            .execute()
        )
        schedule_config = response.data
        if not schedule_config:
            return {"success": False, "message": "스케줄을 찾을 수 없습니다."}

        # Update status to running
        client.table(scheduled_crawls_table_name()).update(  # type: ignore
            {"last_run_status": "running", "updated_at": _utc_now_iso()}
        ).eq("id", schedule_id).execute()

        # Call run_factory (this will block the Streamlit UI)
        from main import run_factory  # Import here to avoid circular dependency

        run_result = run_factory(
            start_url=schedule_config["start_url"],
            schema_key=schedule_config["schema_key"],
            source_type="scheduled_crawl",
            # Other parameters can be added to scheduled_crawls table if needed
        )

        # Update status after run
        is_active = schedule_config["is_active"]
        if schedule_config["schedule_interval"] == "once":
            next_run = datetime.max.replace(
                tzinfo=timezone.utc
            )  # Set to a very distant future
            is_active = False
        else:
            next_run = _calculate_next_run_time(schedule_config["schedule_interval"])
        final_status = (
            "success" if run_result.total_queued_for_processing > 0 else "failed"
        )
        final_log = f"Queued: {run_result.total_queued_for_processing}, Processed: {run_result.total_processed_by_ai}"

        client.table(scheduled_crawls_table_name()).update(  # type: ignore
            {
                "last_run_at": _utc_now_iso(),
                "last_run_status": final_status,
                "last_run_log": final_log,
                "next_run_at": next_run.isoformat(),
                "is_active": is_active,
                "updated_at": _utc_now_iso(),
            }
        ).eq("id", schedule_id).execute()

        return {"success": True, "message": f"스케줄 실행 완료: {final_log}"}
    except Exception as exc:
        logger.error(f"스케줄 즉시 실행 실패: {exc}", exc_info=True)
        client.table(scheduled_crawls_table_name()).update(  # type: ignore
            {
                "last_run_status": "failed",
                "last_run_log": str(exc),
                "updated_at": _utc_now_iso(),
            }
        ).eq("id", schedule_id).execute()
        return {"success": False, "message": f"스케줄 즉시 실행 실패: {exc}"}


def fetch_due_scheduled_crawls(limit: int = 5) -> list[dict[str, Any]]:
    client = get_cached_supabase_client()
    if client is None:
        return []

    try:
        response = (
            client.table(scheduled_crawls_table_name())
            .select("*")
            .eq("is_active", True)
            .lte("next_run_at", _utc_now_iso())
            .order("next_run_at", desc=False)
            .limit(limit)
            .execute()
        )
        return list(getattr(response, "data", []) or [])
    except Exception as exc:
        logger.error(f"기한이 된 스케줄 조회 실패: {exc}", exc_info=True)
        return []


def _insert_remote(table_name: str, payload: dict[str, Any]) -> tuple[bool, str]:
    client = get_cached_supabase_client()
    if client is None:
        return False, "Supabase가 설정되지 않아 원격 저장을 건너뜁니다."

    try:
        client.table(table_name).insert(_clean_json_value(payload)).execute()
        return True, "Supabase에 저장되었습니다."
    except Exception as exc:
        return False, str(exc)


def log_dead_letter(
    url: str,
    error_reason: str,
    *,
    final_url: str = "",
    source_type: str = "",
    schema_key: str = "",
    source_path_hint: str = "",
    # New parameter `payload` is already in the function signature, but adding it here for clarity.
    payload: dict[str, Any] | None = None,
    exception: Exception | None = None,  # New parameter
    extra_details: dict[str, Any] | None = None,  # New parameter
) -> dict[str, Any]:
    primary_url = str(url or final_url).strip()
    if not primary_url:
        return {  # type: ignore
            "saved": False,
            "message": "dead_letters entry skipped because URL is missing.",
        }

    metadata_parts: list[str] = []
    if final_url and final_url.strip() and final_url.strip() != primary_url:
        metadata_parts.append(f"final_url={final_url.strip()}")
    if source_type:
        metadata_parts.append(f"source_type={source_type}")
    if schema_key:
        metadata_parts.append(f"schema_key={schema_key}")
    if source_path_hint:
        metadata_parts.append(f"path_hint={source_path_hint}")
    if payload:
        # Store a sanitized/limited version of the payload to avoid excessive size
        payload_snapshot = {
            k: v
            for k, v in payload.items()
            if k not in ["raw_response", "scraped_text"]
        }
        # Limit size of payload_snapshot if it gets too big
        if len(json.dumps(payload_snapshot)) > 5000:
            payload_snapshot = {
                "_truncated": True,
                "part_number": payload.get("part_number"),
                "schema_key": payload.get("schema_key"),
            }

    full_error_details: dict[str, Any] = {}
    if extra_details:
        full_error_details.update(extra_details)
    if exception:
        full_error_details["traceback"] = traceback.format_exc() if exception else None
        full_error_details["exception_type"] = type(exception).__name__
        full_error_details["exception_message"] = str(exception)
    if payload_snapshot:  # Use the potentially truncated snapshot
        full_error_details["payload_snapshot"] = payload_snapshot

    reason = str(error_reason or "unknown_error").strip()
    if metadata_parts:
        reason = f"{reason} | {' | '.join(metadata_parts)}"

    saved, message = _insert_remote(
        dead_letters_table_name(),
        {
            "url": primary_url,
            "error_reason": reason[:3000],
            "resolved": False,
            "updated_at": _utc_now_iso(),
            "created_at": _utc_now_iso(),
            "schema_key": schema_key,
            "source_type": source_type,
            "error_details": full_error_details,  # New column
        },
    )
    return {"saved": saved, "message": message}


def _extract_response_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict):
                text = item.get("text") or item.get("content")
                if text:
                    parts.append(str(text))
            elif item is not None:
                parts.append(str(item))
        return "\n".join(parts).strip()
    return str(content).strip()


def _parse_json_text(raw_text: str) -> dict[str, Any]:
    cleaned_text = raw_text.replace("```json", "").replace("```", "").strip()
    if not cleaned_text:
        return {}
    try:
        loaded = json.loads(cleaned_text)
    except json.JSONDecodeError:
        return {"raw_response": cleaned_text}
    if isinstance(loaded, dict):
        return loaded
    return {"raw_response": loaded}


def _response_to_payload(response: Any) -> dict[str, Any]:
    if isinstance(response, BaseModel):
        return response.model_dump()
    return _parse_json_text(
        _extract_response_text(getattr(response, "content", response))
    )


def _guess_scraped_text_part_number(scraped_text: str) -> str:
    candidates: list[tuple[int, str]] = []
    for token in re.findall(r"\b[A-Z0-9-]{5,20}\b", str(scraped_text or "").upper()):
        cleaned = token.strip("-")
        if cleaned in {"HTTP", "HTTPS", "HTML", "GLOBAL", "UNKNOWN", "FALSE", "TRUE"}:
            continue
        if not any(char.isalpha() for char in cleaned):
            continue
        if not any(char.isdigit() for char in cleaned):
            continue
        candidates.append(
            (sum(char.isdigit() for char in cleaned) + len(cleaned), cleaned)
        )
    candidates.sort(reverse=True)
    return candidates[0][1] if candidates else ""


def _build_local_text_fallback_payload(
    scraped_text: str,
    *,
    doc_type_key: str,
    market: str,
    part_number_hint: str = "",
    oem_brand: str = "",
    source_path_hint: str = "",
    document_type: str = "",
    source_url: str = "",
    vehicle_hint: str = "",
    system_hint: str = "",
    operator_identifier: str = "",
    extra_metadata: dict[str, Any] | None = None,
    fallback_reason: str = "llm_unavailable",
) -> dict[str, Any]:
    lines = [
        line.strip() for line in str(scraped_text or "").splitlines() if line.strip()
    ]
    visible_lines = [
        line for line in lines if not line.startswith("[") and "]" not in line[:40]
    ]
    page_title = str((extra_metadata or {}).get("page_title") or "").strip()
    title = page_title or (
        visible_lines[0]
        if visible_lines
        else document_type or doc_type_key or "Captured page"
    )
    summary_source = (
        " ".join(visible_lines[:8]).strip() or str(scraped_text or "").strip()
    )
    summary = re.sub(r"\s+", " ", summary_source)[:320].strip()
    guessed_part_number = _guess_scraped_text_part_number(scraped_text)
    resolved_part_number = resolve_storage_part_number(
        doc_type_key,
        part_number_hint or guessed_part_number,
        fallback_part_number=part_number_hint or guessed_part_number or source_url,
    )

    payload: dict[str, Any] = {
        "part_number": resolved_part_number,
        "oem_brand": oem_brand,
        "schema_key": doc_type_key,
        "source_path_hint": source_path_hint,
        "document_type": document_type or doc_type_key,
        "market": market,
        "title": title[:180],
        "summary": summary
        or "Captured page stored without Gemini extraction. Manual review is required.",
        "vehicle": {},
        "compatibility": [],
        "specifications": {},
        "extracted_facts": {
            "manual_review_required": True,
            "fallback_reason": fallback_reason,
            "guessed_part_number": guessed_part_number,
            "compressed_chars": len(scraped_text or ""),
        },
        "cautions": [
            "Gemini structured extraction is unavailable, so this record was created in local fallback mode.",
        ],
        "analysis_mode": (
            "local_fallback_no_llm"
            if fallback_reason == "llm_unavailable"
            else "local_fallback_after_llm_error"
        ),
        "source_url": source_url,
        "compressed_chars": len(scraped_text or ""),
        "scraped_at": datetime.now().isoformat(timespec="seconds"),
    }
    apply_input_context_to_payload(
        payload,
        schema_key=doc_type_key,
        part_number_hint=part_number_hint,
        vehicle_hint=vehicle_hint,
        system_hint=system_hint,
        operator_identifier=operator_identifier,
    )
    if extra_metadata:
        capture_context = {
            key: value
            for key, value in extra_metadata.items()
            if value not in {"", None, [], {}}
        }
        if capture_context:
            payload["capture_context"] = capture_context
        capture_type = str(extra_metadata.get("capture_type") or "").strip()
        if capture_type:
            payload["capture_type"] = capture_type
    assess_analysis_quality(payload)
    return payload


PART_NUMBER_REQUIRED_SCHEMAS = {"path_detail"}


def schema_requires_part_number(schema_key: str | None) -> bool:
    return str(schema_key or "").strip() in PART_NUMBER_REQUIRED_SCHEMAS


def is_placeholder_identifier(value: str | None) -> bool:
    normalized = str(value or "").strip()
    return normalized in {"", "Unknown", "UNKNOWN", "UNKNOWN_PART", "AI_AUTO_DETECT"}


def _normalized_identity_text(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def looks_like_context_label(
    candidate: str | None,
    *,
    vehicle_hint: str = "",
    system_hint: str = "",
    operator_identifier: str = "",
) -> bool:
    normalized_candidate = _normalized_identity_text(candidate)
    if not normalized_candidate:
        return False

    vehicle_model_hint, vehicle_year_hint = _split_vehicle_hint(vehicle_hint)
    known_context_values = [
        vehicle_hint,
        vehicle_model_hint,
        vehicle_year_hint,
        system_hint,
        operator_identifier,
        f"[{vehicle_hint}] {system_hint}" if vehicle_hint and system_hint else "",
        f"{vehicle_hint} {system_hint}".strip(),
    ]
    normalized_contexts = {
        _normalized_identity_text(value)
        for value in known_context_values
        if _normalized_identity_text(value)
    }
    return normalized_candidate in normalized_contexts


def _split_vehicle_hint(vehicle_hint: str) -> tuple[str, str]:
    raw_value = str(vehicle_hint or "").strip()
    if not raw_value:
        return "", ""
    match = re.search(r"(19|20)\d{2}", raw_value)
    if not match:
        return raw_value, ""
    year_value = match.group(0)
    model_value = re.sub(r"(19|20)\d{2}", "", raw_value).strip(" -_/")
    return model_value, year_value


def apply_input_context_to_payload(
    payload: dict[str, Any],
    *,
    schema_key: str,
    part_number_hint: str = "",
    vehicle_hint: str = "",
    system_hint: str = "",
    operator_identifier: str = "",
) -> dict[str, Any]:
    cleaned_part_hint = str(part_number_hint or "").strip()
    cleaned_vehicle_hint = str(vehicle_hint or "").strip()
    cleaned_system_hint = str(system_hint or "").strip()
    cleaned_identifier = str(operator_identifier or "").strip()

    input_context = {
        "provided_part_number": cleaned_part_hint,
        "vehicle_hint": cleaned_vehicle_hint,
        "system_hint": cleaned_system_hint,
        "operator_identifier": cleaned_identifier,
    }
    payload["input_context"] = input_context

    vehicle_model_hint, vehicle_year_hint = _split_vehicle_hint(cleaned_vehicle_hint)
    if cleaned_vehicle_hint or cleaned_system_hint:
        vehicle_payload = payload.get("vehicle")
        if not isinstance(vehicle_payload, dict):
            vehicle_payload = {}
        if cleaned_vehicle_hint and not vehicle_payload.get("model"):
            vehicle_payload["model"] = vehicle_model_hint or cleaned_vehicle_hint
        if vehicle_year_hint and not vehicle_payload.get("year"):
            vehicle_payload["year"] = vehicle_year_hint
        if cleaned_system_hint and not vehicle_payload.get("system_hint"):
            vehicle_payload["system_hint"] = cleaned_system_hint
        if vehicle_payload:
            payload["vehicle"] = vehicle_payload

    current_part_number = str(payload.get("part_number", "") or "").strip()
    if cleaned_part_hint:
        if is_placeholder_identifier(current_part_number) or looks_like_context_label(
            current_part_number,
            vehicle_hint=cleaned_vehicle_hint,
            system_hint=cleaned_system_hint,
            operator_identifier=cleaned_identifier,
        ):
            payload["part_number"] = cleaned_part_hint
    else:
        if not schema_requires_part_number(schema_key):
            if is_placeholder_identifier(
                current_part_number
            ) or looks_like_context_label(
                current_part_number,
                vehicle_hint=cleaned_vehicle_hint,
                system_hint=cleaned_system_hint,
                operator_identifier=cleaned_identifier,
            ):
                payload["part_number"] = "UNKNOWN"
        elif looks_like_context_label(
            current_part_number,
            vehicle_hint=cleaned_vehicle_hint,
            system_hint=cleaned_system_hint,
            operator_identifier=cleaned_identifier,
        ):
            payload["part_number"] = "UNKNOWN"

    return payload


def _extract_vehicle_identifier_examples(raw_text: str) -> dict[str, list[str]]:
    text = raw_text or ""
    upper_text = text.upper()
    vin_examples = sorted(set(re.findall(r"\b[A-HJ-NPR-Z0-9]{17}\b", upper_text)))
    engine_code_examples = sorted(
        set(
            match
            for match in re.findall(r"\b[A-Z0-9-]{4,12}\b", upper_text)
            if any(char.isdigit() for char in match)
            and any(char.isalpha() for char in match)
        )
    )
    return {
        "vin_examples": vin_examples[:10],
        "engine_or_code_examples": engine_code_examples[:20],
    }


def _string_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        if ">" in value:
            return [segment.strip() for segment in value.split(">") if segment.strip()]
        if value.strip():
            return [value.strip()]
    return []


STRUCTURED_CONTENT_KEYS = {
    "extracted_facts",
    "vehicle_identifier_facts",
    "identification_points",
    "required_tools",
    "procedure_steps",
    "related_fasteners",
    "connector",
    "pin_map",
    "diagnostic_steps",
    "diagnostic_triggers",
    "diagnostic_results",
    "wiring_points",
    "compatibility",
    "specifications",
}


def _has_meaningful_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, list):
        return any(_has_meaningful_value(item) for item in value)
    if isinstance(value, dict):
        return any(_has_meaningful_value(item) for item in value.values())
    return True


def has_meaningful_structured_content(payload: dict[str, Any]) -> bool:
    for key in STRUCTURED_CONTENT_KEYS:
        if _has_meaningful_value(payload.get(key)):
            return True
    return False


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def assess_analysis_quality(payload: dict[str, Any]) -> dict[str, Any]:
    schema_key = str(payload.get("schema_key", "") or "")
    part_number = str(payload.get("part_number", "") or "").strip()
    raw_response = payload.get("raw_response")
    extracted_facts = payload.get("extracted_facts")
    summary = str(payload.get("summary", "") or "").strip()
    title = str(payload.get("title", "") or "").strip()
    route_status = str(payload.get("route_status", "") or "").strip()
    http_status = payload.get("http_status")
    compressed_chars = _safe_int(payload.get("compressed_chars"), 0)
    cautions = payload.get("cautions")
    caution_count = len(cautions) if isinstance(cautions, list) else 0

    reasons: list[str] = []
    score = 35

    if not route_status or route_status == "content_page":
        score += 8
    elif route_status == "auth_required":
        reasons.append("auth_required")
        score -= 25
    elif route_status == "broken_public_route":
        reasons.append("broken_public_route")
        score -= 30
    else:
        reasons.append(f"route_status_{route_status}")
        score -= 15

    if http_status in {None, 200}:
        score += 4
    elif isinstance(http_status, int) and 200 <= http_status < 400:
        score += 2
    elif isinstance(http_status, int):
        reasons.append(f"http_status_{http_status}")
        score -= 10

    if raw_response:
        reasons.append("raw_response_only")
        score -= 25
    if not isinstance(extracted_facts, dict) or not extracted_facts:
        reasons.append("empty_extracted_facts")
    else:
        score += 18
    if not has_meaningful_structured_content(payload):
        reasons.append("empty_structured_content")
    else:
        score += 20
    if not summary:
        reasons.append("missing_summary")
    else:
        score += 10
    if title:
        score += 4
    if schema_requires_part_number(schema_key) and is_placeholder_identifier(
        part_number
    ):
        reasons.append("placeholder_part_number")
        score -= 20
    elif schema_requires_part_number(schema_key):
        score += 12
    else:
        score += 4

    if compressed_chars >= 1200:
        score += 8
    elif compressed_chars >= 400:
        score += 5
    elif compressed_chars > 0:
        score += 2

    if caution_count >= 3:
        score -= 6
    elif caution_count == 1:
        score += 1

    score = max(0, min(100, score))

    if score >= 85:
        quality_status = "high"
    elif score >= 65:
        quality_status = "ok"
    else:
        quality_status = "low"

    payload["confidence_score"] = score
    payload["quality_status"] = quality_status
    payload["quality_reasons"] = reasons
    payload["needs_refinement"] = quality_status == "low"
    payload["auto_publish_ready"] = (
        quality_status != "low"
        and not raw_response
        and (
            not schema_requires_part_number(schema_key)
            or not is_placeholder_identifier(part_number)
        )
    )
    return payload


def resolve_storage_part_number(
    schema_key: str,
    raw_part_number: Any,
    *,
    fallback_part_number: str = "",
) -> str:
    cleaned_part_number = str(raw_part_number or "").strip()
    cleaned_fallback = str(fallback_part_number or "").strip()

    if cleaned_part_number and not is_placeholder_identifier(cleaned_part_number):
        return cleaned_part_number
    if schema_requires_part_number(schema_key):
        return cleaned_fallback or "UNKNOWN_PART"
    return "UNKNOWN"


def refine_vision_result_and_save(
    analysis_payload: dict[str, Any],
    *,
    schema_key: str,
    market: str,
    source_path_hint: str = "",
    document_type: str = "",
    part_number_hint: str = "",
    vehicle_hint: str = "",
    system_hint: str = "",
    operator_identifier: str = "",
    oem_brand: str = "",
) -> tuple[dict[str, Any], dict[str, Any]]:
    llm = get_cached_llm()
    if llm is None:
        return analysis_payload, {
            "saved": False,
            "message": "Gemini가 설정되지 않아 재구조화를 진행할 수 없습니다.",
        }

    raw_source = str(
        analysis_payload.get("raw_response")
        or analysis_payload.get("summary")
        or json.dumps(analysis_payload, ensure_ascii=False, indent=2)
    ).strip()
    if not raw_source:
        return analysis_payload, {
            "saved": False,
            "message": "재구조화할 원본 설명문이 없습니다.",
        }

    refinement_prompt = (
        f"{get_system_prompt(schema_key, '자동차 자료를 구조화 JSON으로 변환하세요.')}\n\n"
        "반드시 순수 JSON 객체만 반환하세요.\n"
        "설명문, 마크다운, 코드블록을 넣지 마세요.\n"
        "부품 문서가 아니면 `part_number`는 `UNKNOWN`으로 두고, 문서 성격을 `document_type`과 `summary`에 명확히 적으세요.\n"
        "설명 위주의 긴 문장을 그대로 넣지 말고, 핵심 사실만 필드로 정리하세요.\n"
        "가능하면 `summary`, `extracted_facts`, `cautions`를 포함하세요.\n"
    )

    if schema_key == "path_vehicle_id":
        refinement_prompt += (
            "이 자료는 차량 식별/VIN/페인트 코드/엔진 코드 해설 자료일 수 있습니다.\n"
            "`vehicle_identifier_facts` 아래에 `vin_examples`, `paint_code_examples`, `engine_or_code_examples`, "
            "`serial_or_label_examples` 같은 키를 사용해 정리하세요.\n"
            "정비 절차나 토크 문서로 오인하지 마세요.\n"
        )

    message = HumanMessage(
        content=[
            {"type": "text", "text": refinement_prompt},
            {"type": "text", "text": f"[원본 설명문]\n{raw_source}"},
        ]
    )

    try:
        response = invoke_llm_with_fallback([message])
        refined_payload = _parse_json_text(
            _extract_response_text(getattr(response, "content", response))
        )
    except Exception as exc:
        return analysis_payload, {
            "saved": False,
            "message": f"재구조화 중 Gemini 호출 실패: {exc}",
        }

    if schema_key == "path_vehicle_id":
        examples = _extract_vehicle_identifier_examples(raw_source)
        if examples["vin_examples"] or examples["engine_or_code_examples"]:
            vehicle_identifier_facts = refined_payload.setdefault(
                "vehicle_identifier_facts", {}
            )
            for key, values in examples.items():
                if values and key not in vehicle_identifier_facts:
                    vehicle_identifier_facts[key] = values

    refined_payload.setdefault(
        "part_number",
        (
            "UNKNOWN"
            if not schema_requires_part_number(schema_key)
            else (part_number_hint or "Unknown")
        ),
    )
    if not schema_requires_part_number(schema_key) and is_placeholder_identifier(
        refined_payload.get("part_number")
    ):
        refined_payload["part_number"] = "UNKNOWN"
    refined_payload.setdefault("oem_brand", oem_brand)
    refined_payload.setdefault("schema_key", schema_key)
    refined_payload.setdefault("source_path_hint", source_path_hint)
    refined_payload.setdefault("document_type", document_type or schema_key)
    refined_payload.setdefault("market", market)
    refined_payload["analysis_mode"] = "gemini_refined"
    refined_payload["captured_at"] = datetime.now().isoformat(timespec="seconds")
    apply_input_context_to_payload(
        refined_payload,
        schema_key=schema_key,
        part_number_hint=part_number_hint,
        vehicle_hint=vehicle_hint,
        system_hint=system_hint,
        operator_identifier=operator_identifier,
    )
    assess_analysis_quality(refined_payload)

    save_gsw_document(
        refined_payload,
        source_type="vision_refined",
        status="pending",
        fallback_schema_key=schema_key,
        fallback_document_type=document_type,
        fallback_source_path_hint=source_path_hint,
        fallback_oem_brand=oem_brand,
        fallback_market=market,
        fallback_part_number=part_number_hint,
    )

    _insert_remote(
        vision_table_name(),
        {
            "part_number": refined_payload.get(
                "part_number", part_number_hint or "Unknown"
            ),
            "oem_brand": refined_payload.get("oem_brand", oem_brand),
            "schema_key": refined_payload.get("schema_key", schema_key),
            "source_path_hint": refined_payload.get(
                "source_path_hint", source_path_hint
            ),
            "document_type": refined_payload.get(
                "document_type", document_type or "Unknown"
            ),
            "analysis": refined_payload,
            "created_at": _utc_now_iso(),
        },
    )

    pending_payload = {
        "part_number": refined_payload.get(
            "part_number", part_number_hint or "Unknown"
        ),
        "oem_brand": refined_payload.get("oem_brand", oem_brand),
        "schema_key": refined_payload.get("schema_key", schema_key),
        "source_path_hint": refined_payload.get("source_path_hint", source_path_hint),
        "market": market,
        "document_type": refined_payload.get("document_type", document_type),
        "source_type": "vision_refined",
        "raw_json": refined_payload,
        "status": "pending",
        "created_at": _utc_now_iso(),
    }
    saved, message = _insert_remote(pending_table_name(), pending_payload)
    return refined_payload, {
        "saved": saved,
        "message": message,
        "prompt_key": schema_key,
        "destination": "Pending",
    }


def build_gsw_document_record(
    payload: dict[str, Any],
    *,
    source_type: str,
    status: str,
    source_url: str = "",
    fallback_schema_key: str = "",
    fallback_document_type: str = "",
    fallback_source_path_hint: str = "",
    fallback_oem_brand: str = "",
    fallback_market: str = "GLOBAL",
    fallback_part_number: str = "",
) -> dict[str, Any]:
    schema_key = str(payload.get("schema_key") or fallback_schema_key or "").strip()
    document_type = str(
        payload.get("document_type") or fallback_document_type or schema_key or ""
    ).strip()
    source_path_hint = str(
        payload.get("source_path_hint") or fallback_source_path_hint or ""
    ).strip()
    oem_brand = str(payload.get("oem_brand") or fallback_oem_brand or "Hyundai").strip()
    market = str(payload.get("market") or fallback_market or "GLOBAL").strip()
    part_number = str(
        payload.get("part_number") or fallback_part_number or "UNKNOWN"
    ).strip()
    if not schema_requires_part_number(schema_key) and is_placeholder_identifier(
        part_number
    ):
        part_number = "UNKNOWN"

    vehicle = payload.get("vehicle") if isinstance(payload.get("vehicle"), dict) else {}
    input_context = (
        payload.get("input_context")
        if isinstance(payload.get("input_context"), dict)
        else {}
    )
    inferred_model, inferred_year = _split_vehicle_hint(
        str(input_context.get("vehicle_hint") or "")
    )
    breadcrumb_path = _string_list(
        payload.get("breadcrumbs") or payload.get("breadcrumb_path") or []
    )
    breadcrumb_text = " > ".join(breadcrumb_path)
    title = str(
        payload.get("title")
        or payload.get("page_title")
        or payload.get("section_title")
        or payload.get("summary")
        or document_type
        or schema_key
    ).strip()
    menu_family = str(
        payload.get("menu_family")
        or (breadcrumb_path[0] if breadcrumb_path else document_type)
    ).strip()
    summary = str(payload.get("summary") or "").strip()

    fingerprint_source = {
        "schema_key": schema_key,
        "document_type": document_type,
        "title": title,
        "breadcrumb_path": breadcrumb_path,
        "source_path_hint": source_path_hint,
        "market": market,
        "part_number": part_number,
        "source_url": source_url,
    }
    source_fingerprint = hashlib.sha256(
        json.dumps(fingerprint_source, ensure_ascii=False, sort_keys=True).encode(
            "utf-8"
        )
    ).hexdigest()

    return {
        "source_fingerprint": source_fingerprint,
        "source_system": "hyundai_gsw",
        "part_number": part_number,
        "oem_brand": oem_brand,
        "brand": str(vehicle.get("brand") or "Hyundai").strip(),
        "market": market,
        "vehicle_model": str(
            vehicle.get("model")
            or payload.get("vehicle_model")
            or payload.get("model")
            or inferred_model
            or input_context.get("vehicle_hint")
            or ""
        ).strip(),
        "vehicle_year": str(
            vehicle.get("year") or payload.get("year") or inferred_year or ""
        ).strip(),
        "vehicle_trim": str(
            vehicle.get("trim") or payload.get("vehicle_trim") or ""
        ).strip(),
        "engine_code": str(
            vehicle.get("engine") or payload.get("engine_code") or ""
        ).strip(),
        "transmission_code": str(
            vehicle.get("transmission") or payload.get("transmission_code") or ""
        ).strip(),
        "menu_family": menu_family,
        "schema_key": schema_key,
        "document_type": document_type,
        "title": title,
        "breadcrumb_text": breadcrumb_text,
        "breadcrumb_path": breadcrumb_path,
        "source_url": source_url,
        "source_path_hint": source_path_hint,
        "capture_type": str(payload.get("capture_type") or source_type).strip(),
        "source_type": source_type,
        "page_ref": str(
            payload.get("page_ref") or payload.get("page_number") or ""
        ).strip(),
        "summary": summary,
        "document_payload": payload,
        "status": status,
        "updated_at": _utc_now_iso(),
    }


def save_gsw_document(
    payload: dict[str, Any],
    *,
    source_type: str,
    status: str,
    source_url: str = "",
    fallback_schema_key: str = "",
    fallback_document_type: str = "",
    fallback_source_path_hint: str = "",
    fallback_oem_brand: str = "",
    fallback_market: str = "GLOBAL",
    fallback_part_number: str = "",
) -> dict[str, Any]:
    client = get_cached_supabase_client()
    if client is None:
        return {
            "saved": False,
            "message": "Supabase가 설정되지 않아 gsw_documents 저장을 건너뜁니다.",
        }

    record = build_gsw_document_record(
        payload,
        source_type=source_type,
        status=status,
        source_url=source_url,
        fallback_schema_key=fallback_schema_key,
        fallback_document_type=fallback_document_type,
        fallback_source_path_hint=fallback_source_path_hint,
        fallback_oem_brand=fallback_oem_brand,
        fallback_market=fallback_market,
        fallback_part_number=fallback_part_number,
    )

    # Check for semantic duplicates before upserting
    existing_id, existing_fingerprint = check_for_semantic_gsw_duplicate(payload)
    if existing_id and existing_fingerprint != record["source_fingerprint"]:
        # Semantic duplicate found with different content, log and skip
        log_dead_letter(
            source_url,
            f"semantic_duplicate_gsw: Existing ID {existing_id} with different content.",
            final_url=source_url,
            source_type=source_type,
            schema_key=record["schema_key"],
            source_path_hint=record["source_path_hint"],
            payload=payload,
            extra_details={
                "existing_gsw_id": existing_id,
                "existing_fingerprint": existing_fingerprint,
            },
        )
        return {
            "saved": False,
            "message": f"의미론적 중복으로 인해 gsw_documents 저장을 건너뛰었습니다 (기존 ID: {existing_id}).",
            "record": record,
        }

    try:
        client.table(gsw_documents_table_name()).upsert(
            _clean_json_value(record),
            on_conflict="source_fingerprint",
        ).execute()
        return {
            "saved": True,
            "message": "gsw_documents 마스터에 반영했습니다.",
            "record": record,
        }
    except Exception as exc:
        return {
            "saved": False,
            "message": f"gsw_documents 저장 실패: {exc}",
            "record": record,
        }


def check_for_semantic_gsw_duplicate(
    payload: dict[str, Any],
) -> tuple[str | None, str | None]:
    """
    Checks if a GSW document with the same part_number, oem_brand, and schema_key already exists.
    Returns (existing_id, existing_source_fingerprint) if found, otherwise (None, None).
    """
    client = get_cached_supabase_client()
    if client is None:
        return None, None

    part_number = str(payload.get("part_number", "")).strip()
    oem_brand = str(payload.get("oem_brand", "")).strip()
    schema_key = str(payload.get("schema_key", "")).strip()

    # Only perform semantic check if part_number is meaningful
    if is_placeholder_identifier(part_number) or not part_number:
        return None, None

    try:
        response = (
            client.table(gsw_documents_table_name())
            .select("id, source_fingerprint")
            .eq("part_number", part_number)
            .eq("oem_brand", oem_brand)
            .eq("schema_key", schema_key)
            .limit(1)
            .execute()
        )
        if response.data:
            return response.data[0]["id"], response.data[0]["source_fingerprint"]
    except Exception as exc:
        logger.error(f"Semantic GSW duplicate check failed: {exc}", exc_info=True)

    return None, None


def process_vision_and_save(
    file_bytes: bytes,
    file_type: str | None,
    part_num: str,
    doc_type_key: str,
    market: str,
    *,
    oem_brand: str = "",
    source_path_hint: str = "",
    document_type: str = "",
    prompt_override: str | None = None,
    vehicle_hint: str = "",
    system_hint: str = "",
    operator_identifier: str = "",
) -> tuple[dict[str, Any], dict[str, Any]]:
    llm = get_cached_llm()
    mime_type = file_type or "image/jpeg"
    system_prompt = prompt_override or get_system_prompt(
        doc_type_key,
        "자동차 정비 문서에서 측정 가능한 팩트와 핵심 구조화 정보만 JSON으로 추출하세요.",
    )

    if llm is None:
        payload = {
            "part_number": part_num or "Unknown",
            "oem_brand": oem_brand,
            "schema_key": doc_type_key,
            "source_path_hint": source_path_hint,
            "document_type": document_type,
            "market": market,
            "summary": "Gemini가 설정되지 않아 예시 기반 결과를 반환했습니다.",
            "extracted_facts": {"inspection_required": True},
            "cautions": ["GOOGLE_API_KEY 또는 GEMINI_API_KEY가 필요합니다."],
            "analysis_mode": "fallback",
            "captured_at": datetime.now().isoformat(timespec="seconds"),
        }
    else:
        file_b64 = base64.b64encode(file_bytes).decode("utf-8")
        msg = HumanMessage(
            content=[
                {"type": "text", "text": system_prompt},
                {
                    "type": "image_url",
                    "image_url": {"url": f"data:{mime_type};base64,{file_b64}"},
                },
            ]
        )
        try:
            response = invoke_llm_with_fallback(
                [msg], structured_schema=VisionFactBundle
            )
            payload = _response_to_payload(response)
            payload.setdefault("part_number", part_num or "Unknown")
            payload.setdefault("oem_brand", oem_brand)
            payload.setdefault("schema_key", doc_type_key)
            payload.setdefault("source_path_hint", source_path_hint)
            payload.setdefault("document_type", document_type)
            payload.setdefault("market", market)
            payload["analysis_mode"] = "gemini_structured"
            payload["captured_at"] = datetime.now().isoformat(timespec="seconds")
        except Exception as exc:
            payload = {
                "part_number": part_num or "Unknown",
                "oem_brand": oem_brand,
                "schema_key": doc_type_key,
                "source_path_hint": source_path_hint,
                "document_type": document_type,
                "market": market,
                "summary": "Gemini 호출 중 오류가 발생해 안전한 오류 응답으로 대체했습니다.",
                "extracted_facts": {"inspection_required": True},
                "cautions": [f"Gemini invocation failed: {exc}"],
                "analysis_mode": "error_fallback",
                "captured_at": datetime.now().isoformat(timespec="seconds"),
            }

    apply_input_context_to_payload(
        payload,
        schema_key=doc_type_key,
        part_number_hint=part_num,
        vehicle_hint=vehicle_hint,
        system_hint=system_hint,
        operator_identifier=operator_identifier,
    )
    assess_analysis_quality(payload)

    save_gsw_document(
        payload,
        source_type="vision_capture",
        status="pending",
        fallback_schema_key=doc_type_key,
        fallback_document_type=document_type,
        fallback_source_path_hint=source_path_hint,
        fallback_oem_brand=oem_brand,
        fallback_market=market,
        fallback_part_number=part_num,
    )

    _insert_remote(
        vision_table_name(),
        {
            "part_number": payload.get("part_number", part_num or "Unknown"),
            "oem_brand": payload.get("oem_brand", oem_brand),
            "schema_key": payload.get("schema_key", doc_type_key),
            "source_path_hint": payload.get("source_path_hint", source_path_hint),
            "document_type": payload.get("document_type", document_type or "Unknown"),
            "analysis": payload,
            "created_at": _utc_now_iso(),
        },
    )

    pending_payload = {
        "part_number": payload.get("part_number", part_num or "Unknown"),
        "oem_brand": payload.get("oem_brand", oem_brand),
        "schema_key": payload.get("schema_key", doc_type_key),
        "source_path_hint": payload.get("source_path_hint", source_path_hint),
        "market": market,
        "document_type": payload.get("document_type", document_type),
        "source_type": doc_type_key,
        "raw_json": payload,
        "status": "pending",
        "created_at": _utc_now_iso(),
    }
    saved, message = _insert_remote(pending_table_name(), pending_payload)
    return payload, {
        "saved": saved,
        "message": message,
        "mime_type": mime_type,
        "prompt_key": doc_type_key,
    }


def process_scraped_text_and_save(
    scraped_text: str,
    doc_type_key: str,
    market: str,
    destination: str,
    *,
    part_number_hint: str = "",
    oem_brand: str = "",
    source_path_hint: str = "",
    document_type: str = "",
    source_url: str = "",
    vehicle_hint: str = "",
    system_hint: str = "",
    operator_identifier: str = "",
    extra_metadata: dict[str, Any] | None = None,
    source_type_override: str = "",
) -> tuple[dict[str, Any], dict[str, Any]]:
    llm = get_cached_llm()
    client = get_cached_supabase_client()
    resolved_source_type = source_type_override or doc_type_key

    if client is None:
        return {}, {"saved": False, "message": "Supabase가 설정되지 않았습니다."}
    payload: dict[str, Any]

    system_prompt = get_system_prompt(
        doc_type_key,
        "자동차 부품/정비 텍스트에서 측정 가능한 팩트와 식별 정보만 JSON으로 추출하세요.",
    )
    msg = HumanMessage(
        content=[
            {"type": "text", "text": system_prompt},
            {"type": "text", "text": f"[수집된 원본 텍스트]\n{scraped_text}"},
        ]
    )

    if llm is None:
        payload = _build_local_text_fallback_payload(
            scraped_text,
            doc_type_key=doc_type_key,
            market=market,
            part_number_hint=part_number_hint,
            oem_brand=oem_brand,
            source_path_hint=source_path_hint,
            document_type=document_type,
            source_url=source_url,
            vehicle_hint=vehicle_hint,
            system_hint=system_hint,
            operator_identifier=operator_identifier,
            extra_metadata=extra_metadata,
            fallback_reason="llm_unavailable",
        )
    else:
        try:
            response = invoke_llm_with_fallback(
                [msg], structured_schema=CrawlFactBundle
            )
            payload = _response_to_payload(response)
            payload["part_number"] = resolve_storage_part_number(
                doc_type_key,
                payload.get("part_number"),
                fallback_part_number=part_number_hint,
            )
            payload.setdefault("oem_brand", "")
            payload.setdefault("source_path_hint", source_path_hint)
            payload.setdefault("schema_key", doc_type_key)
            payload.setdefault("document_type", document_type or doc_type_key)
            payload.setdefault("market", market)
            payload.setdefault("title", "")
            payload.setdefault("summary", "")
            payload.setdefault("vehicle", {})
            payload.setdefault("compatibility", [])
            payload.setdefault("specifications", {})
            payload.setdefault("extracted_facts", {})
            payload.setdefault("cautions", [])
            payload["analysis_mode"] = "gemini_structured"
            payload["source_url"] = source_url
            payload["compressed_chars"] = len(scraped_text)
            payload["scraped_at"] = datetime.now().isoformat(timespec="seconds")
            if oem_brand and not payload.get("oem_brand"):
                payload["oem_brand"] = oem_brand
            apply_input_context_to_payload(
                payload,
                schema_key=doc_type_key,
                part_number_hint=part_number_hint,
                vehicle_hint=vehicle_hint,
                system_hint=system_hint,
                operator_identifier=operator_identifier,
            )
            if extra_metadata:
                capture_context = payload.get("capture_context")
                if not isinstance(capture_context, dict):
                    capture_context = {}
                for key, value in extra_metadata.items():
                    if value is None:
                        continue
                    if isinstance(value, str) and not value.strip():
                        continue
                    if isinstance(value, (list, dict)) and not value:
                        continue
                    capture_context[key] = value
                if capture_context:
                    payload["capture_context"] = capture_context
                capture_type = str(extra_metadata.get("capture_type") or "").strip()
                if capture_type:
                    payload["capture_type"] = capture_type
                page_title = str(extra_metadata.get("page_title") or "").strip()
                if page_title and not payload.get("title"):
                    payload["title"] = page_title
            assess_analysis_quality(payload)
        except Exception as exc:
            payload = _build_local_text_fallback_payload(
                scraped_text,
                doc_type_key=doc_type_key,
                market=market,
                part_number_hint=part_number_hint,
                oem_brand=oem_brand,
                source_path_hint=source_path_hint,
                document_type=document_type,
                source_url=source_url,
                vehicle_hint=vehicle_hint,
                system_hint=system_hint,
                operator_identifier=operator_identifier,
                extra_metadata=extra_metadata,
                fallback_reason=f"llm_error:{exc}",
            )

    temp_part_number = resolve_storage_part_number(
        doc_type_key,
        payload.get("part_number"),
        fallback_part_number=part_number_hint,
    )
    payload["part_number"] = temp_part_number

    if destination == "parts":
        save_result = save_crawled_data(
            payload,
            temp_part_number,
            market=market,
            schema_key=doc_type_key,
            source_path_hint=source_path_hint,
            source_type=resolved_source_type,
            document_type=document_type or doc_type_key,
        )
        return payload, save_result

    gsw_result = save_gsw_document(
        payload,
        source_type=resolved_source_type,
        status="pending",
        source_url=source_url,
        fallback_schema_key=doc_type_key,
        fallback_document_type=document_type or doc_type_key,
        fallback_source_path_hint=source_path_hint,
        fallback_market=market,
        fallback_part_number=temp_part_number,
    )
    if not gsw_result["saved"]:
        return payload, {"saved": False, "message": gsw_result["message"]}

    try:
        client.table(pending_table_name()).insert(
            {
                "part_number": temp_part_number,
                "market": market,
                "schema_key": doc_type_key,
                "source_path_hint": source_path_hint,
                "document_type": document_type or doc_type_key,
                "source_type": resolved_source_type,
                "raw_json": payload,
                "status": "pending",
                "created_at": _utc_now_iso(),
            }
        ).execute()
        return payload, {
            "saved": True,
            "destination": "Pending",
            "message": "수집 결과를 gsw_documents와 검수 대기열에 저장했습니다.",
        }
    except Exception as exc:
        return payload, {
            "saved": False,
            "destination": "none",
            "message": f"저장 실패: {exc}",
        }


def analyze_uploaded_image(
    *,
    file_bytes: bytes,
    mime_type: str,
    part_number: str,
    oem_brand: str,
    schema_key: str,
    source_path_hint: str,
    document_type: str,
    prompt_text: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    llm = get_cached_llm()
    if llm is None:
        fallback = {
            "part_number": part_number or "Unknown",
            "oem_brand": oem_brand,
            "schema_key": schema_key,
            "source_path_hint": source_path_hint,
            "document_type": document_type,
            "summary": "Gemini가 설정되지 않아 예시 기반 Vision 결과를 반환했습니다.",
            "extracted_facts": {
                "inspection_required": True,
            },
            "cautions": ["GOOGLE_API_KEY 또는 GEMINI_API_KEY가 필요합니다."],
            "captured_at": datetime.now().isoformat(timespec="seconds"),
            "analysis_mode": "fallback",
        }
        return fallback, {"saved": False, "message": "Gemini 미설정 상태입니다."}

    image_data = base64.b64encode(file_bytes).decode("utf-8")
    instruction = (
        f"{prompt_text}\n\n"
        f"Requested document type: {document_type}\n"
        f"User supplied part number: {part_number or 'Unknown'}\n"
        f"User supplied OEM brand: {oem_brand or 'Unknown'}\n"
        f"Selected schema key: {schema_key or 'Unknown'}\n"
        f"Selected path hint: {source_path_hint or 'Unknown'}\n"
        "Return only structured automotive facts. Keep units and ranges exactly as shown. "
        "If the user supplied part number or OEM brand is valid and the image is ambiguous, prefer the supplied values."
    )
    message = HumanMessage(
        content=[
            {"type": "text", "text": instruction},
            {
                "type": "image_url",
                "image_url": {"url": f"data:{mime_type};base64,{image_data}"},
            },
        ]
    )
    try:
        result = invoke_llm_with_fallback([message], structured_schema=VisionFactBundle)
        payload = result.model_dump()
    except Exception as exc:
        fallback = {
            "part_number": part_number or "Unknown",
            "oem_brand": oem_brand,
            "schema_key": schema_key,
            "source_path_hint": source_path_hint,
            "document_type": document_type,
            "summary": "Gemini Vision 호출 중 오류가 발생해 안전한 오류 응답으로 대체했습니다.",
            "extracted_facts": {"inspection_required": True},
            "cautions": [f"Gemini invocation failed: {exc}"],
            "captured_at": datetime.now().isoformat(timespec="seconds"),
            "analysis_mode": "error_fallback",
        }
        return fallback, {"saved": False, "message": f"Gemini Vision 호출 실패: {exc}"}
    if part_number and payload.get("part_number", "Unknown") in {"", "Unknown"}:
        payload["part_number"] = part_number
    if oem_brand and not payload.get("oem_brand"):
        payload["oem_brand"] = oem_brand
    if schema_key and not payload.get("schema_key"):
        payload["schema_key"] = schema_key
    if source_path_hint and not payload.get("source_path_hint"):
        payload["source_path_hint"] = source_path_hint
    payload["document_type"] = document_type or payload.get("document_type", "Unknown")
    payload["captured_at"] = datetime.now().isoformat(timespec="seconds")
    payload["analysis_mode"] = "gemini"

    saved, message_text = _insert_remote(
        vision_table_name(),
        {
            "part_number": payload.get("part_number", "Unknown"),
            "oem_brand": payload.get("oem_brand", ""),
            "schema_key": payload.get("schema_key", ""),
            "source_path_hint": payload.get("source_path_hint", ""),
            "document_type": payload.get("document_type", "Unknown"),
            "analysis": payload,
            "created_at": _utc_now_iso(),
        },
    )
    return payload, {"saved": saved, "message": message_text}


def enqueue_pending_vision_result(
    *,
    part_number: str,
    oem_brand: str,
    schema_key: str,
    source_path_hint: str,
    market: str,
    document_type: str,
    analysis_payload: dict[str, Any],
    source_type: str = "vision_capture",
) -> dict[str, Any]:
    payload = {
        "part_number": part_number or analysis_payload.get("part_number", "Unknown"),
        "oem_brand": oem_brand or analysis_payload.get("oem_brand", ""),
        "schema_key": schema_key or analysis_payload.get("schema_key", ""),
        "source_path_hint": source_path_hint
        or analysis_payload.get("source_path_hint", ""),
        "market": market,
        "document_type": document_type,
        "source_type": source_type,
        "raw_json": analysis_payload,
        "status": "pending",
        "created_at": _utc_now_iso(),
    }
    saved, message = _insert_remote(pending_table_name(), payload)
    return {
        "saved": saved,
        "message": message,
        "payload": payload,
    }


def _build_translation_source(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "url": record.get("url", ""),
        "part_number": record.get("part_number", ""),
        "oem_brand": record.get("oem_brand", ""),
        "schema_key": record.get("schema_key", ""),
        "source_path_hint": record.get("source_path_hint", ""),
        "target_market": record.get("target_market", ""),
        "spec_data": record.get("spec_data") or record.get("extracted_facts", {}),
        "status": record.get("status", ""),
        "route_status": record.get("route_status", ""),
    }


def translate_record(
    *,
    record: dict[str, Any],
    prompt_text: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    llm = get_cached_llm()
    source_payload = _build_translation_source(record)

    if llm is None:
        fallback = {
            "ko": source_payload,
            "en": source_payload,
            "vn": source_payload,
            "notes": "Gemini가 설정되지 않아 원본 구조를 그대로 반환했습니다.",
            "translated_at": datetime.now().isoformat(timespec="seconds"),
            "translation_mode": "fallback",
        }
        return fallback, {"saved": False, "message": "Gemini 미설정 상태입니다."}

    instruction = (
        f"{prompt_text}\n\n"
        "Translate the structured automotive payload into Korean, English, and Vietnamese. "
        "Preserve part numbers, numbers, units, torque ranges, and codes exactly. "
        "Return compact JSON objects for each target language."
    )
    try:
        result = invoke_llm_with_fallback(
            f"{instruction}\n\n[Source JSON]\n{json.dumps(source_payload, ensure_ascii=False, indent=2)}",
            structured_schema=TranslationBundle,
        )
        payload = result.model_dump()
    except Exception as exc:
        fallback = {
            "ko": source_payload,
            "en": source_payload,
            "vn": source_payload,
            "notes": f"Gemini 번역 호출 실패로 원본 구조를 반환했습니다: {exc}",
            "translated_at": datetime.now().isoformat(timespec="seconds"),
            "translation_mode": "error_fallback",
        }
        return fallback, {"saved": False, "message": f"Gemini 번역 호출 실패: {exc}"}
    payload["translated_at"] = datetime.now().isoformat(timespec="seconds")
    payload["translation_mode"] = "gemini"

    saved, message_text = _insert_remote(
        translation_table_name(),
        {
            "source_url": record.get("url", ""),
            "part_number": record.get("part_number", ""),
            "oem_brand": record.get("oem_brand", ""),
            "schema_key": record.get("schema_key", ""),
            "source_path_hint": record.get("source_path_hint", ""),
            "translations": payload,
            "created_at": _utc_now_iso(),
        },
    )
    return payload, {"saved": saved, "message": message_text}


def fetch_pending_items(limit: int = 20) -> list[dict[str, Any]]:
    client = get_cached_supabase_client()
    if client is None:
        return []

    try:
        response = (
            client.table(pending_table_name())
            .select("*")
            .eq("status", "pending")
            .order("created_at", desc=False)
            .limit(limit)
            .execute()
        )
        return list(getattr(response, "data", []) or [])
    except Exception as exc:  # type: ignore
        logger.error(f"[DLQ] fetch failed: {exc}")
        return []


def fetch_rejected_items(limit: int = 50) -> list[dict[str, Any]]:
    client = get_cached_supabase_client()
    if client is None:
        return []

    try:
        response = (  # type: ignore
            client.table(pending_table_name())
            .select("*")
            .eq("status", "rejected")
            .order("rejected_at", desc=True)
            .limit(limit)
            .execute()
        )
        return list(getattr(response, "data", []) or [])
    except Exception as exc:  # type: ignore
        logger.error(f"[Rejected] fetch failed: {exc}")
        return []


def requeue_rejected_item(item_id: Any) -> dict[str, Any]:
    """Moves a rejected item back to the pending queue."""
    client = get_cached_supabase_client()
    if client is None:
        return {
            "saved": False,
            "message": "Supabase가 설정되지 않아 항목을 되돌릴 수 없습니다.",
        }

    try:
        client.table(pending_table_name()).update(
            {
                "status": "pending",
                "rejected_at": None,
                "rejection_reason": None,
            }
        ).eq("id", item_id).execute()
        return {
            "saved": True,
            "message": "반려된 항목을 검수 대기열로 되돌렸습니다.",
        }
    except Exception as exc:  # type: ignore
        return {
            "saved": False,
            "message": f"항목 되돌리기 실패: {exc}",
        }


def delete_pending_item(item_id: Any) -> dict[str, Any]:
    """Permanently deletes an item from the pending_data table."""
    client = get_cached_supabase_client()
    if client is None:
        return {
            "saved": False,
            "message": "Supabase가 설정되지 않아 항목을 삭제할 수 없습니다.",
        }

    try:
        client.table(pending_table_name()).delete().eq("id", item_id).execute()
        return {"saved": True, "message": "항목을 영구적으로 삭제했습니다."}
    except Exception as exc:  # type: ignore
        return {
            "saved": False,
            "message": f"항목 삭제 실패: {exc}",
        }


def get_live_capture_server_status() -> dict[str, Any] | None:
    """Fetches the status of the live capture server."""
    server_host = os.getenv("WEVIKO_LIVE_CAPTURE_HOST", "127.0.0.1")
    server_port = int(os.getenv("WEVIKO_LIVE_CAPTURE_PORT", "8765"))
    server_scheme = os.getenv("WEVIKO_LIVE_CAPTURE_SCHEME", "http")
    server_url = f"{server_scheme}://{server_host}:{server_port}/api/status"

    try:
        response = requests.get(server_url, timeout=5)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.ConnectionError:
        logger.warning(
            f"Live Capture Server is not running or unreachable at {server_url}"
        )
        return None
    except requests.exceptions.Timeout:
        logger.warning(f"Live Capture Server connection timed out at {server_url}")
        return None
    except Exception as exc:
        logger.error(
            f"Failed to fetch Live Capture Server status from {server_url}: {exc}"
        )
        return None


def fetch_recent_live_captures(limit: int = 10) -> list[dict[str, Any]]:
    """Fetches recent pending_data items from live capture."""
    client = get_cached_supabase_client()
    if client is None:
        return []

    try:
        response = (
            client.table(pending_table_name())
            .select("*")
            .eq("source_type", "browser_live_capture")
            .order("created_at", desc=True)
            .limit(limit)
            .execute()
        )
        return list(getattr(response, "data", []) or [])
    except Exception as exc:
        logger.error(f"[Live Capture] fetch recent captures failed: {exc}")
        return []


def bulk_requeue_rejected_items(item_ids: list[Any]) -> dict[str, Any]:
    """Moves multiple rejected items back to the pending queue."""
    client = get_cached_supabase_client()
    if client is None:
        return {
            "saved": False,
            "message": "Supabase가 설정되지 않아 항목을 되돌릴 수 없습니다.",
        }

    try:
        client.table(pending_table_name()).update(
            {
                "status": "pending",
                "rejected_at": None,
                "rejection_reason": None,
            }
        ).in_("id", item_ids).execute()
        return {
            "saved": True,
            "message": f"{len(item_ids)}개 반려 항목을 검수 대기열로 되돌렸습니다.",
        }
    except Exception as exc:  # type: ignore
        return {
            "saved": False,
            "message": f"항목 되돌리기 실패: {exc}",
        }


def bulk_delete_pending_items(item_ids: list[Any]) -> dict[str, Any]:
    """Permanently deletes multiple items from the pending_data table."""
    client = get_cached_supabase_client()
    if client is None:
        return {
            "saved": False,
            "message": "Supabase가 설정되지 않아 항목을 삭제할 수 없습니다.",
        }

    try:
        client.table(pending_table_name()).delete().in_("id", item_ids).execute()
        return {
            "saved": True,
            "message": f"{len(item_ids)}개 항목을 영구적으로 삭제했습니다.",
        }
    except Exception as exc:  # type: ignore
        return {
            "saved": False,
            "message": f"항목 삭제 실패: {exc}",
        }


def approve_pending_item(
    *,
    item_id: Any,
    item: dict[str, Any],
    edited_payload: dict[str, Any],
) -> dict[str, Any]:
    client = get_cached_supabase_client()
    if client is None:
        return {
            "saved": False,
            "message": "Supabase가 설정되지 않아 승인 이관을 진행할 수 없습니다.",
        }

    try:
        resolved_part_number = (
            edited_payload.get("part_number") or item.get("part_number") or "Unknown"
        )
        resolved_oem_brand = edited_payload.get("oem_brand") or item.get(
            "oem_brand", ""
        )
        resolved_schema_key = edited_payload.get("schema_key") or item.get(
            "schema_key", ""
        )
        resolved_source_path_hint = edited_payload.get("source_path_hint") or item.get(
            "source_path_hint", ""
        )
        resolved_market = edited_payload.get("market") or item.get("market", "GLOBAL")
        resolved_document_type = edited_payload.get("document_type") or item.get(
            "document_type", ""
        )
        resolved_source_type = edited_payload.get("source_type") or item.get(
            "source_type", ""
        )
        gsw_result = save_gsw_document(
            edited_payload,
            source_type=resolved_source_type or "review_approved",
            status="approved",
            fallback_schema_key=resolved_schema_key,
            fallback_document_type=resolved_document_type,
            fallback_source_path_hint=resolved_source_path_hint,
            fallback_oem_brand=resolved_oem_brand,
            fallback_market=resolved_market,
            fallback_part_number=resolved_part_number,
        )
        if not gsw_result["saved"]:
            raise RuntimeError(gsw_result["message"])

        part_saved = False
        if schema_requires_part_number(
            resolved_schema_key
        ) and not is_placeholder_identifier(resolved_part_number):
            client.table(parts_table_name()).upsert(
                {
                    "part_number": resolved_part_number,
                    "oem_brand": resolved_oem_brand,
                    "schema_key": resolved_schema_key,
                    "source_path_hint": resolved_source_path_hint,
                    "market": resolved_market,
                    "document_type": resolved_document_type,
                    "source_type": resolved_source_type,
                    "spec_data": edited_payload,
                    "updated_at": _utc_now_iso(),
                },
                on_conflict="part_number",
            ).execute()
            part_saved = True

        client.table(pending_table_name()).update(
            {
                "status": "approved",
                "approved_at": _utc_now_iso(),
                "raw_json": edited_payload,
                "part_number": resolved_part_number,
                "oem_brand": resolved_oem_brand,
                "schema_key": resolved_schema_key,
                "source_path_hint": resolved_source_path_hint,
                "market": resolved_market,
                "document_type": resolved_document_type,
                "source_type": resolved_source_type,
            }
        ).eq("id", item_id).execute()

        message_parts: list[str] = []
        if gsw_result["saved"]:
            message_parts.append("gsw_documents 마스터에 반영했습니다.")
        else:
            message_parts.append(gsw_result["message"])
        if part_saved:
            message_parts.append("parts 테이블에도 반영했습니다.")
        else:
            message_parts.append(
                "이번 문서는 GSW 문서형으로 분류되어 parts 업서는 건너뛰었습니다."
            )
        return {
            "saved": True,
            "message": " ".join(message_parts),
        }
    except Exception as exc:  # type: ignore
        return {
            "saved": False,
            "message": f"승인 이관 실패: {exc}",
        }


def reject_pending_item(item_id: Any, rejection_reason: str = "") -> dict[str, Any]:
    client = get_cached_supabase_client()
    if client is None:
        return {
            "saved": False,
            "message": "Supabase가 설정되지 않아 반려 처리를 진행할 수 없습니다.",
        }

    try:
        client.table(pending_table_name()).update(
            {
                "status": "rejected",
                "rejected_at": _utc_now_iso(),
                "rejection_reason": rejection_reason,
            }
        ).eq("id", item_id).execute()
        return {
            "saved": True,
            "message": "pending_data 상태를 rejected로 갱신했습니다.",
        }
    except Exception as exc:  # type: ignore
        return {
            "saved": False,
            "message": f"반려 처리 실패: {exc}",
        }


def fetch_untranslated_parts(limit: int = 5) -> list[dict[str, Any]]:
    client = get_cached_supabase_client()
    if client is None:
        return []

    try:
        response = (
            client.table(parts_table_name())
            .select("*")
            .is_("translations", "null")
            .limit(limit)
            .execute()
        )
        return list(getattr(response, "data", []) or [])
    except Exception as exc:  # type: ignore
        logger.error(f"[Parts] untranslated fetch failed: {exc}")
        return []


def save_part_translation(
    part_number: str, translations: dict[str, Any]
) -> dict[str, Any]:
    client = get_cached_supabase_client()
    if client is None:
        return {
            "saved": False,
            "message": "Supabase가 설정되지 않아 번역 결과를 저장할 수 없습니다.",
        }

    try:
        client.table(parts_table_name()).update(
            {
                "translations": translations,
                "updated_at": _utc_now_iso(),
            }
        ).eq("part_number", part_number).execute()
        return {
            "saved": True,
            "message": "parts 테이블에 translations를 저장했습니다.",
        }
    except Exception as exc:  # type: ignore
        return {
            "saved": False,
            "message": f"번역 저장 실패: {exc}",
        }


def save_crawled_data(
    raw_json: dict[str, Any],
    part_number: str,
    *,
    market: str = "GLOBAL",
    schema_key: str = "",
    source_path_hint: str = "",
    source_type: str = "crawl_factory",
    document_type: str = "크롤링 수집 결과",
) -> dict[str, Any]:
    client = get_cached_supabase_client()
    if client is None:
        return {
            "saved": False,
            "destination": "none",
            "message": "Supabase가 설정되지 않아 크롤링 결과를 저장할 수 없습니다.",
        }

    resolved_part_number = resolve_storage_part_number(
        schema_key,
        raw_json.get("part_number") or part_number,
        fallback_part_number=part_number,
    )
    raw_json["part_number"] = resolved_part_number
    raw_json.setdefault("schema_key", schema_key)
    raw_json.setdefault("source_path_hint", source_path_hint)
    raw_json.setdefault("document_type", document_type)
    raw_json.setdefault("market", market)
    assess_analysis_quality(raw_json)

    gsw_result = save_gsw_document(
        raw_json,
        source_type=source_type,
        status="crawled",
        source_url=str(
            raw_json.get("final_url")
            or raw_json.get("source_url")
            or raw_json.get("url")
            or ""
        ).strip(),
        fallback_schema_key=schema_key,
        fallback_document_type=document_type,
        fallback_source_path_hint=source_path_hint,
        fallback_market=market,
        fallback_part_number=resolved_part_number,
    )
    if not gsw_result["saved"]:
        log_dead_letter(
            str(raw_json.get("source_url") or raw_json.get("url") or ""),
            gsw_result["message"],
            final_url=str(raw_json.get("final_url") or ""),
            source_type=source_type,
            schema_key=schema_key,
            source_path_hint=source_path_hint,
            exception=None,
            payload=raw_json,
        )
        return {
            "saved": False,
            "destination": "none",
            "message": gsw_result["message"],
        }

    confidence_threshold = get_config_int_value("confidence_threshold", 90)
    score = raw_json.get("confidence_score", 0)
    auto_publish_ready = bool(raw_json.get("auto_publish_ready", False))
    try:
        score_value = int(score)
    except (TypeError, ValueError):
        score_value = 0

    timestamp = _utc_now_iso()

    try:
        if not schema_requires_part_number(schema_key):
            client.table(pending_table_name()).insert(
                {
                    "part_number": resolved_part_number,
                    "market": market,
                    "schema_key": schema_key,
                    "source_path_hint": source_path_hint,
                    "document_type": document_type,
                    "source_type": source_type,
                    "raw_json": raw_json,
                    "status": "pending",
                    "created_at": timestamp,
                }
            ).execute()
            return {
                "saved": True,
                "destination": "Pending",
                "confidence_score": score_value,
                "confidence_threshold": confidence_threshold,
                "message": "문서형 크롤링 결과라서 gsw_documents와 검수 대기열에 저장했습니다.",
            }

        if score_value >= confidence_threshold and auto_publish_ready:
            client.table(parts_table_name()).upsert(
                {
                    "part_number": resolved_part_number,
                    "market": market,
                    "schema_key": schema_key,
                    "source_path_hint": source_path_hint,
                    "document_type": document_type,
                    "source_type": source_type,
                    "status": "auto_verified",
                    "spec_data": raw_json,
                    "updated_at": timestamp,
                },
                on_conflict="part_number",
            ).execute()
            return {
                "saved": True,
                "destination": "Direct",
                "confidence_score": score_value,
                "confidence_threshold": confidence_threshold,
                "message": f"신뢰도 {score_value}점으로 정식 DB에 자동 등록했습니다.",
            }

        client.table(pending_table_name()).insert(
            {
                "part_number": resolved_part_number,
                "market": market,
                "schema_key": schema_key,
                "source_path_hint": source_path_hint,
                "document_type": document_type,
                "source_type": source_type,
                "raw_json": raw_json,
                "status": "pending",
                "created_at": timestamp,
            }
        ).execute()
        return {
            "saved": True,
            "destination": "Pending",
            "confidence_score": score_value,
            "confidence_threshold": confidence_threshold,
            "message": f"신뢰도 {score_value}점으로 검수 대기열에 저장했습니다.",
        }
    except Exception as exc:
        log_dead_letter(
            str(raw_json.get("source_url") or raw_json.get("url") or ""),
            f"crawl_save_failed: {exc}",
            final_url=str(raw_json.get("final_url") or ""),
            source_type=source_type,
            schema_key=schema_key,
            source_path_hint=source_path_hint,
            exception=exc,
            payload=raw_json,
        )
        return {
            "saved": False,
            "destination": "none",
            "confidence_score": score_value,
            "confidence_threshold": confidence_threshold,
            "message": f"크롤링 결과 저장 실패: {exc}",
        }


def persist_factory_rows(
    *,
    rows: list[dict[str, Any]],
    destination: str,
    market: str,
    schema_key: str,
    source_path_hint: str,
    source_type: str = "crawl_factory",
    document_type: str = "크롤링 수집 결과",
) -> dict[str, Any]:
    client = get_cached_supabase_client()
    if client is None:
        return {
            "saved": False,
            "saved_count": 0,
            "message": "Supabase가 설정되지 않아 팩토리 결과를 저장할 수 없습니다.",
        }

    saved_count = 0
    skipped_count = 0
    direct_count = 0
    pending_count = 0
    errors: list[str] = []

    for row in rows:
        route_status = row.get("route_status", "")
        extracted_facts = row.get("extracted_facts") or {}
        part_number = (row.get("part_number") or "").strip()
        identifier = resolve_storage_part_number(
            schema_key,
            part_number,
            fallback_part_number=row.get("final_url") or row.get("url") or "Unknown",
        )

        payload = {
            "part_number": identifier,
            "oem_brand": row.get("oem_brand", ""),
            "source_url": row.get("url", ""),
            "final_url": row.get("final_url", ""),
            "http_status": row.get("http_status"),
            "route_status": route_status,
            "route_reason": row.get("route_reason", ""),
            "document_type": row.get("document_type") or document_type,
            "title": row.get("title", ""),
            "summary": row.get("summary", ""),
            "vehicle": row.get("vehicle") or {},
            "compatibility": row.get("compatibility") or [],
            "specifications": row.get("specifications") or {},
            "content_hash": row.get("content_hash", ""),
            "compressed_chars": row.get("compressed_chars", 0),
            "extracted_facts": extracted_facts,
            "cautions": row.get("cautions") or [],
            "schema_key": schema_key,
            "source_path_hint": source_path_hint,
            "market": market,
            "status": row.get("status", ""),
        }
        assess_analysis_quality(payload)

        if row.get("cache_hit"):
            skipped_count += 1
            continue

        if route_status != "content_page":
            skipped_count += 1
            log_dead_letter(
                payload["source_url"],
                f"route_not_persisted: {route_status or 'unknown'} / {payload.get('route_reason', '')}",
                final_url=payload["final_url"],
                source_type=source_type,
                schema_key=schema_key,
                source_path_hint=source_path_hint,
                exception=None,
                payload=payload,
            )
            continue

        quality_status = payload.get("quality_status")
        if quality_status == "low":
            skipped_count += 1
            score = payload.get("confidence_score", 0)
            reasons = ",".join(payload.get("quality_reasons", []))
            log_dead_letter(
                payload["source_url"],
                f"low_quality_data: score={score}, reasons={reasons}",
                final_url=payload["final_url"],
                source_type=source_type,
                schema_key=schema_key,
                source_path_hint=source_path_hint,
                exception=None,
                payload=payload,
            )
            continue

        try:
            if destination == "parts":
                save_result = save_crawled_data(
                    payload,
                    identifier,
                    market=market,
                    schema_key=schema_key,
                    source_path_hint=source_path_hint,
                    source_type=source_type,
                    document_type=document_type,
                )
                if not save_result["saved"]:
                    raise RuntimeError(save_result["message"])
                if save_result["destination"] == "Direct":
                    direct_count += 1
                elif save_result["destination"] == "Pending":
                    pending_count += 1
            else:
                gsw_result = save_gsw_document(
                    payload,
                    source_type=source_type,
                    status="pending",
                    source_url=str(
                        row.get("final_url") or row.get("url") or ""
                    ).strip(),
                    fallback_schema_key=schema_key,
                    fallback_document_type=document_type,
                    fallback_source_path_hint=source_path_hint,
                    fallback_market=market,
                    fallback_part_number=identifier,
                )
                if not gsw_result["saved"]:
                    raise RuntimeError(gsw_result["message"])
                client.table(pending_table_name()).insert(
                    {
                        "part_number": identifier,
                        "market": market,
                        "schema_key": schema_key,
                        "source_path_hint": source_path_hint,
                        "document_type": document_type,
                        "source_type": source_type,
                        "raw_json": payload,
                        "status": "pending",
                        "created_at": _utc_now_iso(),
                    }
                ).execute()
                pending_count += 1
            saved_count += 1
        except Exception as exc:
            errors.append(str(exc))
            log_dead_letter(
                payload["source_url"],
                f"persist_factory_rows_failed: {exc}",
                final_url=payload["final_url"],
                source_type=source_type,
                schema_key=schema_key,
                source_path_hint=source_path_hint,
                exception=exc,
                payload=payload,
            )

    message = (
        f"{saved_count}건 저장 완료"
        f"{', ' + str(skipped_count) + '건 건너뜀' if skipped_count else ''}"
    )
    if direct_count or pending_count:
        message = f"{message} | Direct {direct_count}건 / Pending {pending_count}건"
    if errors:
        message = f"{message} | 오류 {len(errors)}건: {errors[0]}"

    return {
        "saved": saved_count > 0 and not errors,
        "saved_count": saved_count,
        "skipped_count": skipped_count,
        "direct_count": direct_count,
        "pending_count": pending_count,
        "errors": errors,
        "message": message,
    }


def fetch_dead_letters(
    limit: int = 200,
    error_reason_keyword: str | None = None,
    schema_key_filter: list[str] | None = None,
    source_type_filter: list[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    sort_by: str = "created_at",
    sort_order: str = "desc",
) -> list[dict[str, Any]]:
    client = get_cached_supabase_client()
    if client is None:
        return []

    try:
        query = (
            client.table(dead_letters_table_name()).select("*").eq("resolved", False)
        )

        if error_reason_keyword:
            query = query.ilike("error_reason", f"%{error_reason_keyword}%")

        if schema_key_filter:
            # Assume schema_key column exists. If not, this will raise an error.
            query = query.in_("schema_key", schema_key_filter)

        if source_type_filter:
            # Assume source_type column exists.
            query = query.in_("source_type", source_type_filter)

        if start_date:
            query = query.gte("created_at", start_date)

        if end_date:
            # Add one day to end_date to include the entire day
            end_datetime = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
            end_of_day = end_datetime.replace(
                hour=23, minute=59, second=59, microsecond=999999
            ).isoformat(timespec="microseconds")
            query = query.lte("created_at", end_of_day)

        # Validate sort_by column
        valid_sort_columns = [
            "created_at",
            "updated_at",
            "url",
            "error_reason",
            "schema_key",
            "source_type",
        ]
        if sort_by not in valid_sort_columns:
            sort_by = "created_at"  # Default to created_at if invalid

        is_desc = sort_order.lower() == "desc"
        query = query.order(sort_by, desc=is_desc)

        response = query.limit(limit).execute()
        return list(getattr(response, "data", []) or [])
    except Exception as exc:
        _safe_console_log(f"[DLQ] fetch failed: {exc}")
        return []


def resolve_dead_letter(item_id: Any) -> dict[str, Any]:
    """Marks a dead letter item as resolved."""
    client = get_cached_supabase_client()
    if client is None:
        return {
            "saved": False,
            "message": "Supabase가 설정되지 않아 dead letter를 처리할 수 없습니다.",
        }

    try:
        client.table(dead_letters_table_name()).update(
            {"resolved": True, "updated_at": _utc_now_iso()}
        ).eq("id", item_id).execute()
        return {
            "saved": True,
            "message": "dead_letters 항목을 resolved로 갱신했습니다.",
        }
    except Exception as exc:  # type: ignore
        return {
            "saved": False,
            "message": f"Dead letter 처리 실패: {exc}",
        }


def delete_dead_letter(item_id: Any) -> dict[str, Any]:
    """Permanently deletes a dead letter item."""
    client = get_cached_supabase_client()
    if client is None:
        return {
            "saved": False,
            "message": "Supabase가 설정되지 않아 dead letter를 삭제할 수 없습니다.",
        }

    try:
        client.table(dead_letters_table_name()).delete().eq("id", item_id).execute()
        return {
            "saved": True,
            "message": "dead_letters 항목을 영구적으로 삭제했습니다.",
        }
    except Exception as exc:  # type: ignore
        return {
            "saved": False,
            "message": f"Dead letter 삭제 실패: {exc}",
        }


def parse_dead_letter_metadata(error_reason: str) -> dict[str, str]:
    """Parses metadata from the pipe-separated error_reason string."""
    metadata = {}
    if " | " not in error_reason:
        return metadata

    parts = error_reason.split(" | ")
    for part in parts[1:]:  # Skip the main reason text
        if "=" in part:
            key, value = part.split("=", 1)
            metadata[key.strip()] = value.strip()
    return metadata


def retry_dead_letter_item(item: dict[str, Any]) -> dict[str, Any]:
    """Retries a single dead letter item manually."""
    item_id = item.get("id")
    url = item.get("url")
    error_reason = item.get("error_reason", "")
    if not item_id or not url:
        return {"success": False, "message": "Item ID 또는 URL이 없습니다."}

    proxy_url, _ = get_config_prompt("proxy_url", "")
    user_agent, _ = get_config_prompt("custom_user_agent", "")

    # Prioritize new columns if available, otherwise parse from error_reason
    schema_key = item.get("schema_key")
    source_type = item.get("source_type")
    source_path_hint = (
        ""  # source_path_hint is not a direct column, still in error_reason metadata
    )

    if not schema_key or not source_type:
        metadata = parse_dead_letter_metadata(error_reason)
        schema_key = schema_key or metadata.get("schema_key", "path_detail")
        source_type = source_type or metadata.get(
            "source_type", "dead_letter_manual_retry"
        )
        source_path_hint = metadata.get("path_hint", "")  # Only parse if needed

    try:
        scraped_text = run_crawler_sync(
            url,
            proxy=proxy_url.strip() or None,
            user_agent=user_agent.strip() or None,
        )

        if not scraped_text:
            return {
                "success": False,
                "message": f"재시도 실패: {url}에서 콘텐츠를 크롤링할 수 없습니다.",
            }

        _, save_result = process_scraped_text_and_save(
            scraped_text=scraped_text,
            doc_type_key=schema_key,
            market="GLOBAL",
            destination="pending",  # Manual retries should always go to pending
            source_path_hint=source_path_hint,
            document_type=f"Manual Retry of {schema_key}",
            source_url=url,
            source_type_override=source_type,
        )

        if save_result.get("saved"):
            resolve_dead_letter(item_id)  # Attempt to mark as resolved
            return {
                "success": True,
                "message": "성공적으로 재처리하여 검수 대기열로 보냈습니다.",
            }
        return {
            "success": False,
            "message": f"처리된 데이터 저장 실패: {save_result.get('message')}",
        }
    except Exception as exc:
        return {"success": False, "message": f"재처리 중 예기치 않은 오류 발생: {exc}"}


def fetch_parts_export() -> list[dict[str, Any]]:
    client = get_cached_supabase_client()
    if client is None:
        return []

    try:
        response = client.table(parts_table_name()).select("*").execute()
        return list(getattr(response, "data", []) or [])
    except Exception as exc:
        _safe_console_log(f"[Parts] export fetch failed: {exc}")
        return []


def fetch_parts_count() -> int:
    client = get_cached_supabase_client()
    if client is None:
        return 0

    try:
        response = (
            client.table(parts_table_name())
            .select("part_number", count="exact")
            .execute()
        )
        return int(getattr(response, "count", 0) or 0)
    except Exception as exc:  # type: ignore
        logger.error(f"[Parts] export fetch failed: {exc}")
        return 0


def persist_review_decision(
    *,
    original_record: dict[str, Any],
    reviewed_record: dict[str, Any],
    decision: str,
    notes: str,
) -> dict[str, Any]:
    review_payload = {
        "source_url": original_record.get("url", ""),
        "final_url": reviewed_record.get("final_url")
        or original_record.get("final_url", ""),
        "part_number": reviewed_record.get("part_number")
        or original_record.get("part_number")
        or "Unknown",
        "oem_brand": reviewed_record.get("oem_brand")
        or original_record.get("oem_brand", ""),
        "schema_key": reviewed_record.get("schema_key")
        or original_record.get("schema_key", ""),
        "source_path_hint": reviewed_record.get("source_path_hint")
        or original_record.get("source_path_hint", ""),
        "decision": decision,
        "notes": notes,
        "review_payload": reviewed_record,
        "reviewed_at": _utc_now_iso(),
    }

    saved_review, review_message = _insert_remote(review_table_name(), review_payload)
    upserted_parts = False
    parts_message = "parts 테이블 반영을 건너뜁니다."

    if decision in {"approved", "edited_approved"}:
        client = get_cached_supabase_client()
        if client is None:
            parts_message = "Supabase가 설정되지 않아 parts 테이블 반영을 건너뜁니다."
        else:
            try:
                resolved_part_number = (
                    reviewed_record.get("part_number")
                    or original_record.get("part_number")
                    or "Unknown"
                )
                resolved_oem_brand = reviewed_record.get(
                    "oem_brand"
                ) or original_record.get("oem_brand", "")
                resolved_schema_key = reviewed_record.get(
                    "schema_key"
                ) or original_record.get("schema_key", "")
                resolved_source_path_hint = reviewed_record.get(
                    "source_path_hint"
                ) or original_record.get("source_path_hint", "")
                resolved_market = reviewed_record.get("market") or original_record.get(
                    "market", "GLOBAL"
                )
                resolved_document_type = reviewed_record.get(
                    "document_type"
                ) or original_record.get("document_type", "")
                resolved_source_type = reviewed_record.get(
                    "source_type"
                ) or original_record.get("source_type", "")

                client.table(parts_table_name()).upsert(
                    {
                        "part_number": resolved_part_number,
                        "oem_brand": resolved_oem_brand,
                        "schema_key": resolved_schema_key,
                        "source_path_hint": resolved_source_path_hint,
                        "market": resolved_market,
                        "document_type": resolved_document_type,
                        "source_type": resolved_source_type,
                        "spec_data": reviewed_record or original_record,
                        "updated_at": _utc_now_iso(),
                    },
                    on_conflict="part_number",
                ).execute()
                upserted_parts = True
                parts_message = "parts 테이블에 승인 데이터를 반영했습니다."
            except Exception as exc:
                parts_message = f"parts 테이블 반영 실패: {exc}"

    return {
        "saved_review": saved_review,
        "review_message": review_message,
        "upserted_parts": upserted_parts,
        "parts_message": parts_message,
    }
