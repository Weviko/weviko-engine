from __future__ import annotations

import base64
import json
import os
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from langchain_core.messages import HumanMessage
from pydantic import BaseModel, Field

from main import build_llm, build_supabase_client


load_dotenv()


class VisionFactBundle(BaseModel):
    """Structured extraction result for uploaded automotive document images."""

    part_number: str = Field(default="Unknown", description="Detected automotive part number.")
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


class TranslationBundle(BaseModel):
    """Multilingual translation package for a structured automotive payload."""

    ko: dict[str, Any] = Field(default_factory=dict, description="Korean JSON payload.")
    en: dict[str, Any] = Field(default_factory=dict, description="English JSON payload.")
    vn: dict[str, Any] = Field(default_factory=dict, description="Vietnamese JSON payload.")
    notes: str = Field(default="", description="Short translation notes or warnings.")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _prompt_store_path() -> Path:
    raw_path = os.getenv("WEVIKO_PROMPTS_FILE", "prompt_templates.json").strip() or "prompt_templates.json"
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
    return os.getenv("WEVIKO_PROMPTS_TABLE", "system_prompts").strip() or "system_prompts"


def review_table_name() -> str:
    return os.getenv("WEVIKO_REVIEW_TABLE", "review_decisions").strip() or "review_decisions"


def translation_table_name() -> str:
    return os.getenv("WEVIKO_TRANSLATIONS_TABLE", "part_translations").strip() or "part_translations"


def vision_table_name() -> str:
    return os.getenv("WEVIKO_VISION_TABLE", "vision_analysis").strip() or "vision_analysis"


def parts_table_name() -> str:
    return os.getenv("WEVIKO_PARTS_TABLE", "parts").strip() or "parts"


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
                prompt_text = row.get("prompt_text") or row.get("prompt") or row.get("content")
                if name and prompt_text:
                    prompts[str(name)] = str(prompt_text)
                    source = "supabase"
        except Exception:
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
        result["message"] = f"로컬 파일 저장은 성공했고, Supabase 저장은 실패했습니다: {exc}"

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
        result["message"] = f"로컬 파일 복원은 성공했고, Supabase 반영은 실패했습니다: {exc}"

    return result


def _insert_remote(table_name: str, payload: dict[str, Any]) -> tuple[bool, str]:
    client = get_cached_supabase_client()
    if client is None:
        return False, "Supabase가 설정되지 않아 원격 저장을 건너뜁니다."

    try:
        client.table(table_name).insert(_clean_json_value(payload)).execute()
        return True, "Supabase에 저장되었습니다."
    except Exception as exc:
        return False, str(exc)


def analyze_uploaded_image(
    *,
    file_bytes: bytes,
    mime_type: str,
    part_number: str,
    document_type: str,
    prompt_text: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    llm = get_cached_llm()
    if llm is None:
        fallback = {
            "part_number": part_number or "Unknown",
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

    structured_llm = llm.with_structured_output(VisionFactBundle)
    image_data = base64.b64encode(file_bytes).decode("utf-8")
    instruction = (
        f"{prompt_text}\n\n"
        f"Requested document type: {document_type}\n"
        f"User supplied part number: {part_number or 'Unknown'}\n"
        "Return only structured automotive facts. Keep units and ranges exactly as shown. "
        "If the user supplied part number is valid and the image is ambiguous, prefer the supplied number."
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
    result = structured_llm.invoke([message])
    payload = result.model_dump()
    if part_number and payload.get("part_number", "Unknown") in {"", "Unknown"}:
        payload["part_number"] = part_number
    payload["document_type"] = document_type or payload.get("document_type", "Unknown")
    payload["captured_at"] = datetime.now().isoformat(timespec="seconds")
    payload["analysis_mode"] = "gemini"

    saved, message_text = _insert_remote(
        vision_table_name(),
        {
            "part_number": payload.get("part_number", "Unknown"),
            "document_type": payload.get("document_type", "Unknown"),
            "analysis": payload,
            "created_at": _utc_now_iso(),
        },
    )
    return payload, {"saved": saved, "message": message_text}


def _build_translation_source(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "url": record.get("url", ""),
        "part_number": record.get("part_number", ""),
        "target_market": record.get("target_market", ""),
        "extracted_facts": record.get("extracted_facts", {}),
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

    structured_llm = llm.with_structured_output(TranslationBundle)
    instruction = (
        f"{prompt_text}\n\n"
        "Translate the structured automotive payload into Korean, English, and Vietnamese. "
        "Preserve part numbers, numbers, units, torque ranges, and codes exactly. "
        "Return compact JSON objects for each target language."
    )
    result = structured_llm.invoke(
        f"{instruction}\n\n[Source JSON]\n{json.dumps(source_payload, ensure_ascii=False, indent=2)}"
    )
    payload = result.model_dump()
    payload["translated_at"] = datetime.now().isoformat(timespec="seconds")
    payload["translation_mode"] = "gemini"

    saved, message_text = _insert_remote(
        translation_table_name(),
        {
            "source_url": record.get("url", ""),
            "part_number": record.get("part_number", ""),
            "translations": payload,
            "created_at": _utc_now_iso(),
        },
    )
    return payload, {"saved": saved, "message": message_text}


def persist_review_decision(
    *,
    original_record: dict[str, Any],
    reviewed_record: dict[str, Any],
    decision: str,
    notes: str,
) -> dict[str, Any]:
    review_payload = {
        "source_url": original_record.get("url", ""),
        "final_url": reviewed_record.get("final_url") or original_record.get("final_url", ""),
        "part_number": reviewed_record.get("part_number") or original_record.get("part_number") or "Unknown",
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
                client.table(parts_table_name()).upsert(
                    {
                        "url": reviewed_record.get("url") or original_record.get("url", ""),
                        "part_number": reviewed_record.get("part_number")
                        or original_record.get("part_number")
                        or "Unknown",
                        "extracted_facts": reviewed_record.get("extracted_facts")
                        or original_record.get("extracted_facts")
                        or {},
                        "content_hash": reviewed_record.get("content_hash")
                        or original_record.get("content_hash", ""),
                        "updated_at": _utc_now_iso(),
                    },
                    on_conflict="url",
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
