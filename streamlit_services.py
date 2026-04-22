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
    oem_brand: str = Field(default="", description="OEM brand associated with the part.")
    schema_key: str = Field(default="", description="Internal schema key selected by the operator.")
    source_path_hint: str = Field(default="", description="Source path hint selected by the operator.")
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


def pending_table_name() -> str:
    return os.getenv("WEVIKO_PENDING_TABLE", "pending_data").strip() or "pending_data"


def configs_table_name() -> str:
    return os.getenv("WEVIKO_CONFIGS_TABLE", "configs").strip() or "configs"


def dead_letters_table_name() -> str:
    return os.getenv("WEVIKO_DEAD_LETTERS_TABLE", "dead_letters").strip() or "dead_letters"


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
        response = client.table(configs_table_name()).select("prompt_key,prompt_value").execute()
        for row in getattr(response, "data", []) or []:
            prompt_key = row.get("prompt_key")
            prompt_value = row.get("prompt_value")
            if prompt_key and prompt_value:
                prompts[str(prompt_key)] = str(prompt_value)
                source = "supabase"
    except Exception as exc:
        print(f"[Configs] prompt load failed. Falling back to local/default prompts: {exc}")
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


def get_config_prompt(prompt_key: str, fallback_text: str) -> tuple[str, str]:
    prompts, source = load_config_prompts({prompt_key: fallback_text})
    return prompts.get(prompt_key, fallback_text), source


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

    structured_llm = llm.with_structured_output(VisionFactBundle)
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
    result = structured_llm.invoke([message])
    payload = result.model_dump()
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
        "source_path_hint": source_path_hint or analysis_payload.get("source_path_hint", ""),
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
    except Exception as exc:
        print(f"[Pending] fetch failed: {exc}")
        return []


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
        resolved_part_number = edited_payload.get("part_number") or item.get("part_number") or "Unknown"
        resolved_oem_brand = edited_payload.get("oem_brand") or item.get("oem_brand", "")
        resolved_schema_key = edited_payload.get("schema_key") or item.get("schema_key", "")
        resolved_source_path_hint = (
            edited_payload.get("source_path_hint")
            or item.get("source_path_hint", "")
        )
        resolved_market = edited_payload.get("market") or item.get("market", "GLOBAL")
        resolved_document_type = (
            edited_payload.get("document_type")
            or item.get("document_type", "")
        )
        resolved_source_type = edited_payload.get("source_type") or item.get("source_type", "")

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
        return {
            "saved": True,
            "message": "정식 parts 테이블로 이관했고 pending_data 상태를 approved로 갱신했습니다.",
        }
    except Exception as exc:
        return {
            "saved": False,
            "message": f"승인 이관 실패: {exc}",
        }


def reject_pending_item(item_id: Any) -> dict[str, Any]:
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
            }
        ).eq("id", item_id).execute()
        return {
            "saved": True,
            "message": "pending_data 상태를 rejected로 갱신했습니다.",
        }
    except Exception as exc:
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
    except Exception as exc:
        print(f"[Parts] untranslated fetch failed: {exc}")
        return []


def save_part_translation(part_number: str, translations: dict[str, Any]) -> dict[str, Any]:
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
    except Exception as exc:
        return {
            "saved": False,
            "message": f"번역 저장 실패: {exc}",
        }


def fetch_dead_letters(limit: int = 200) -> list[dict[str, Any]]:
    client = get_cached_supabase_client()
    if client is None:
        return []

    try:
        response = (
            client.table(dead_letters_table_name())
            .select("*")
            .eq("resolved", False)
            .limit(limit)
            .execute()
        )
        return list(getattr(response, "data", []) or [])
    except Exception as exc:
        print(f"[DLQ] fetch failed: {exc}")
        return []


def fetch_parts_export() -> list[dict[str, Any]]:
    client = get_cached_supabase_client()
    if client is None:
        return []

    try:
        response = client.table(parts_table_name()).select("*").execute()
        return list(getattr(response, "data", []) or [])
    except Exception as exc:
        print(f"[Parts] export fetch failed: {exc}")
        return []


def fetch_parts_count() -> int:
    client = get_cached_supabase_client()
    if client is None:
        return 0

    try:
        response = client.table(parts_table_name()).select("part_number", count="exact").execute()
        return int(getattr(response, "count", 0) or 0)
    except Exception as exc:
        print(f"[Parts] count fetch failed: {exc}")
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
        "final_url": reviewed_record.get("final_url") or original_record.get("final_url", ""),
        "part_number": reviewed_record.get("part_number") or original_record.get("part_number") or "Unknown",
        "oem_brand": reviewed_record.get("oem_brand") or original_record.get("oem_brand", ""),
        "schema_key": reviewed_record.get("schema_key") or original_record.get("schema_key", ""),
        "source_path_hint": reviewed_record.get("source_path_hint") or original_record.get("source_path_hint", ""),
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
                resolved_oem_brand = reviewed_record.get("oem_brand") or original_record.get("oem_brand", "")
                resolved_schema_key = reviewed_record.get("schema_key") or original_record.get("schema_key", "")
                resolved_source_path_hint = (
                    reviewed_record.get("source_path_hint")
                    or original_record.get("source_path_hint", "")
                )
                resolved_market = reviewed_record.get("market") or original_record.get("market", "GLOBAL")
                resolved_document_type = (
                    reviewed_record.get("document_type")
                    or original_record.get("document_type", "")
                )
                resolved_source_type = (
                    reviewed_record.get("source_type")
                    or original_record.get("source_type", "")
                )

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
