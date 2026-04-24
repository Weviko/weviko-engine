from __future__ import annotations

import json
import time
import os
import difflib
from datetime import datetime

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from live_capture import (
    build_live_capture_bookmarklet,
    live_capture_allowed_hosts,
    live_capture_base_url,
    live_capture_direct_enabled,
)
from weviko_engine import run_crawler_sync
from weviko_factory import run_factory

from streamlit_services import (
    approve_pending_item,
    assess_analysis_quality,
    bulk_delete_dead_letters,
    bulk_retry_dead_letters,
    delete_dead_letter,
    create_scheduled_crawl,
    fetch_scheduled_crawls,
    delete_scheduled_crawl,
    get_live_capture_server_status,
    fetch_recent_live_captures,
    bulk_requeue_rejected_items,
    bulk_delete_pending_items,
    delete_pending_item,
    requeue_rejected_item,
    fetch_rejected_items,
    enqueue_pending_vision_result,
    fetch_dead_letters,
    fetch_gsw_documents_count,
    fetch_pending_data_count,
    fetch_dead_letters_count,
    run_scheduled_crawl_now,
    fetch_parts_count,
    fetch_parts_export,
    fetch_pending_items,
    fetch_untranslated_parts,
    get_config_prompt,
    llm_available,
    log_dead_letter,
    load_config_prompts,
    persist_factory_rows,
    process_scraped_text_and_save,
    process_vision_and_save,
    reject_pending_item,
    retry_dead_letter_item,
    refine_vision_result_and_save,
    save_config_prompt,
    save_part_translation,
    supabase_available,
    translate_record,
)


load_dotenv()

ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "").strip()
INSECURE_ADMIN_PASSWORDS = {"", "weviko1234!", "changeme", "admin"}

WEVIKO_PATH_MAP = {
    "🛠️ 정비 지침서 (/shop/manual/)": "path_manual",
    "⚙️ 부품 제원/호환성 (/item/detail/)": "path_detail",
    "🧱 차체매뉴얼 (/body/manual/)": "path_body_manual",
    "⚡ 회로도/배선도 (/contents/etc/)": "path_wiring",
    "🔌 와이어링 커넥터 (/wiring/connector/)": "path_connector",
    "🗣️ 포럼/실전 팁 (/community/)": "path_community",
    "⚠️ 고장 코드(DTC) (/dtc/)": "path_dtc",
}

VISION_DOC_TYPE_OPTIONS = {
    "정비 지침/토크": {"schema_key": "path_manual", "path_hint": "/shop/manual/"},
    "차체매뉴얼": {"schema_key": "path_body_manual", "path_hint": "/body/manual/"},
    "회로도/배선도": {"schema_key": "path_wiring", "path_hint": "/contents/etc/"},
    "와이어링 커넥터": {
        "schema_key": "path_connector",
        "path_hint": "/wiring/connector/",
    },
    "부품 제원/도해도": {"schema_key": "path_detail", "path_hint": "/item/detail/"},
    "차량 식별/VIN/페인트 코드": {
        "schema_key": "path_vehicle_id",
        "path_hint": "/vehicle-id/",
    },
    "고장 코드 DTC": {"schema_key": "path_dtc", "path_hint": "/dtc/"},
}

FACTORY_SCHEMA_OPTIONS = {
    "정비 지침서 (/shop/manual/)": {
        "schema_key": "path_manual",
        "path_hint": "/shop/manual/",
    },
    "부품 제원/호환성 (/item/detail/)": {
        "schema_key": "path_detail",
        "path_hint": "/item/detail/",
    },
    "차체매뉴얼 (/body/manual/)": {
        "schema_key": "path_body_manual",
        "path_hint": "/body/manual/",
    },
    "회로도/배선도 (/contents/etc/)": {
        "schema_key": "path_wiring",
        "path_hint": "/contents/etc/",
    },
    "와이어링 커넥터 (/wiring/connector/)": {
        "schema_key": "path_connector",
        "path_hint": "/wiring/connector/",
    },
    "고장 코드(DTC) (/dtc/)": {"schema_key": "path_dtc", "path_hint": "/dtc/"},
    "포럼/실전 팁 (/community/)": {
        "schema_key": "path_community",
        "path_hint": "/community/",
    },
}

DEFAULT_PROMPTS = {
    "crawling_ecommerce": (
        "자동차 부품 상세 페이지에서 부품번호, 차종, 연식, 호환 조건, 규격, 토크, "
        "치수, 중량을 추출해 구조화된 JSON으로 정리하세요."
    ),
    "vision_gsw": (
        "자동차 정비 매뉴얼, 도해도, 회로도, 토크 표 이미지입니다. "
        "저작권을 회피하고 원문을 길게 재현하지 말고, 수치화 가능한 팩트와 정비 핵심 정보만 JSON으로 추출하세요."
    ),
    "translation_vn": (
        "자동차 정비/부품 구조화 데이터를 영어(en)와 베트남어(vn)로 번역하세요. "
        "전문 정비 용어를 사용하고, 숫자, 단위, 부품번호는 원형을 유지하세요."
    ),
    "path_manual": (
        "정비 지침서 성격의 자료입니다. 작업 순서, 공구, 토크, 주의사항, 분해/조립 절차를 구조화하세요."
    ),
    "path_body_manual": (
        "차체매뉴얼 자료입니다. 패널명, 탈거/장착 순서, 체결부, 실러/접착, 조정 포인트, 주의사항을 구조화하세요."
    ),
    "path_detail": (
        "부품 제원/호환성 페이지입니다. 부품번호, 규격, OEM 정보, 적용 차종, 연식, 호환 조건을 우선 추출하세요."
    ),
    "path_connector": (
        "와이어링 커넥터 자료입니다. 커넥터명, 위치, 핀 수, 핀맵, 배선색, 신호명, 연결 대상만 구조화하세요."
    ),
    "path_vehicle_id": (
        "차량 식별/VIN/페인트 코드/엔진 코드 해설 자료입니다. "
        "부품번호를 억지로 만들지 말고, VIN 예시, 차대번호 규칙, 페인트 코드, 엔진/변속기 코드, "
        "시리얼/라벨 구조 같은 차량 식별 팩트만 JSON으로 정리하세요."
    ),
    "path_wiring": (
        "회로도/배선도 자료입니다. 커넥터, 핀, 회로명, 전압/저항 등 계측 가능한 사실만 구조화하세요."
    ),
    "path_community": (
        "포럼/실전 팁 자료입니다. 검증 가능한 정비 팁, 증상, 해결법, 반복되는 오류 패턴만 요약하세요."
    ),
    "path_dtc": (
        "고장 코드(DTC) 자료입니다. 코드, 증상, 원인, 점검 절차, 권장 조치를 구조화하세요."
    ),
    "proxy_url": "",
    "custom_user_agent": "",
    "confidence_threshold": "90",
}

GSW_JSON_TEMPLATES = {
    "path_vehicle_id": {
        "title": "식별번호 위치 안내",
        "document_type": "vehicle_identification_overview",
        "vehicle": {
            "brand": "Hyundai",
            "model": "EQ900",
            "year": 2019,
            "engine": "G 3.3 T-GDI",
        },
        "breadcrumbs": ["엔진", "G 3.3 T-GDI", "일반사항", "식별번호", "일반사항"],
        "summary": "차대번호, 엔진번호, 자동변속기번호 등 차량 식별 포인트 안내",
        "vehicle_identifier_facts": {
            "vin_examples": [],
            "paint_code_examples": [],
            "engine_or_code_examples": [],
            "serial_or_label_examples": [],
        },
        "identification_points": [
            {"item": "vin", "label": "차대번호", "location_description": ""},
            {"item": "engine_number", "label": "엔진번호", "location_description": ""},
            {
                "item": "transmission_number",
                "label": "자동변속기번호",
                "location_description": "",
            },
        ],
        "cautions": [],
    },
    "path_body_manual": {
        "title": "차체 작업 절차",
        "document_type": "body_manual_procedure",
        "vehicle": {"brand": "Hyundai", "model": "", "year": "", "body_type": ""},
        "breadcrumbs": [],
        "summary": "",
        "required_tools": [],
        "procedure_steps": [{"step": 1, "action": "", "note": "", "torque": ""}],
        "related_fasteners": [],
        "cautions": [],
    },
    "path_connector": {
        "title": "커넥터 배치 정보",
        "document_type": "wiring_connector_reference",
        "vehicle": {"brand": "Hyundai", "model": "", "year": ""},
        "breadcrumbs": [],
        "summary": "",
        "connector": {"name": "", "location": "", "pin_count": 0},
        "pin_map": [{"pin": "", "signal": "", "wire_color": "", "description": ""}],
        "cautions": [],
    },
}

PIPELINE_MODES = [
    "📷 수동 캡처(Vision)",
    "🛰️ 현재 탭 라이브 캡처",
    "🏭 대규모 양산 팩토리(URL)",
]

MANAGEMENT_MODES = [
    "🕵️ 데이터 검수소 (H-i-t-L)",
    "🌐 다국어 번역 엔진",
    "⚙️ 시스템 환경 설정",
    "🏥 실패 URL 병원",
    "📊 통합 현황 및 백업",
]


@st.cache_data
def convert_df(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8-sig")


def init_state() -> None:
    if "logged_in" not in st.session_state:
        st.session_state["logged_in"] = False
    if "active_section" not in st.session_state:
        st.session_state["active_section"] = "데이터 수집"
    if "active_pipeline_mode" not in st.session_state:
        st.session_state["active_pipeline_mode"] = PIPELINE_MODES[0]
    if "active_management_mode" not in st.session_state:
        st.session_state["active_management_mode"] = MANAGEMENT_MODES[0]
    if "prompt_values" not in st.session_state:
        prompt_values, source = load_config_prompts(DEFAULT_PROMPTS)
        st.session_state["prompt_values"] = prompt_values
        st.session_state["prompt_source"] = source
    if "prompt_source" not in st.session_state:
        st.session_state["prompt_source"] = "defaults"
    if "last_vision_result" not in st.session_state:
        st.session_state["last_vision_result"] = None
    if "last_vision_context" not in st.session_state:
        st.session_state["last_vision_context"] = None
    if "last_vision_queue_result" not in st.session_state:
        st.session_state["last_vision_queue_result"] = None
    if "last_translation_results" not in st.session_state:
        st.session_state["last_translation_results"] = {}
    if "last_factory_result" not in st.session_state:
        st.session_state["last_factory_result"] = None


def inject_styles() -> None:
    st.markdown(
        """
        <style>
        :root {
            --weviko-bg: #f4efe6;
            --weviko-surface: rgba(255, 251, 242, 0.92);
            --weviko-panel: rgba(255, 255, 255, 0.76);
            --weviko-line: #d7c9ad;
            --weviko-text: #1e261f;
            --weviko-muted: #5d685c;
            --weviko-accent: #9c4f12;
            --weviko-accent-2: #245846;
        }
        .stApp {
            background:
                radial-gradient(circle at 12% 16%, rgba(156, 79, 18, 0.10), transparent 26%),
                radial-gradient(circle at 88% 12%, rgba(36, 88, 70, 0.12), transparent 28%),
                linear-gradient(180deg, #fbf5eb 0%, var(--weviko-bg) 100%);
            color: var(--weviko-text);
        }
        .weviko-hero {
            background: linear-gradient(135deg, rgba(255, 250, 241, 0.98), rgba(244, 230, 204, 0.88));
            border: 1px solid var(--weviko-line);
            border-radius: 26px;
            padding: 1.4rem 1.5rem;
            box-shadow: 0 18px 45px rgba(83, 58, 19, 0.08);
            margin-bottom: 1rem;
        }
        .weviko-kicker {
            color: var(--weviko-accent-2);
            font-size: 0.80rem;
            letter-spacing: 0.16em;
            text-transform: uppercase;
            font-weight: 700;
        }
        .weviko-title {
            color: var(--weviko-text);
            font-size: 2rem;
            font-weight: 900;
            margin: 0.35rem 0 0.2rem;
        }
        .weviko-copy {
            color: var(--weviko-muted);
            margin: 0;
            line-height: 1.6;
        }
        .weviko-status {
            display: inline-block;
            padding: 0.35rem 0.7rem;
            border-radius: 999px;
            border: 1px solid var(--weviko-line);
            margin-right: 0.45rem;
            margin-bottom: 0.45rem;
            background: rgba(255, 255, 255, 0.74);
            color: var(--weviko-text);
            font-size: 0.92rem;
        }
        .weviko-card {
            background: var(--weviko-surface);
            border: 1px solid var(--weviko-line);
            border-radius: 20px;
            padding: 1rem 1.05rem;
            min-height: 118px;
            backdrop-filter: blur(5px);
        }
        .weviko-card-label {
            color: var(--weviko-muted);
            font-size: 0.78rem;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            font-weight: 800;
        }
        .weviko-card-value {
            color: var(--weviko-text);
            font-size: 1.8rem;
            font-weight: 900;
            margin-top: 0.35rem;
        }
        .weviko-card-copy {
            color: var(--weviko-muted);
            font-size: 0.92rem;
            margin-top: 0.2rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def ensure_login() -> None:
    ensure_login_secure()
    return

    if st.session_state["logged_in"]:
        return

    st.title("🔒 Weviko OS v5.0 Login")
    password = st.text_input("접근 암호", type="password")
    if st.button("시스템 가동", type="primary", use_container_width=True):
        if password == ADMIN_PASSWORD:
            st.session_state["logged_in"] = True
            st.rerun()
        st.error("접근 암호가 올바르지 않습니다.")
    st.stop()


def ensure_login_secure() -> None:
    if st.session_state["logged_in"]:
        return

    st.title("Weviko OS v5.0 Login")
    if ADMIN_PASSWORD in INSECURE_ADMIN_PASSWORDS:
        st.error("ADMIN_PASSWORD is required before this console can be used.")
        st.caption(
            "Set a strong ADMIN_PASSWORD in `.env` or your deployment secrets, then restart the app."
        )
        st.stop()

    password = st.text_input("Admin password", type="password")
    if st.button("Enter console", type="primary", use_container_width=True):
        if password == ADMIN_PASSWORD:
            st.session_state["logged_in"] = True
            st.rerun()
        st.error("The password is incorrect.")
    st.stop()


def render_header() -> None:
    st.markdown(
        """
        <div class="weviko-hero">
            <div class="weviko-kicker">Weviko OS v5.0</div>
            <div class="weviko-title">Master Command Center</div>
            <p class="weviko-copy">
                수동 캡처, URL 팩토리, 검수 대기열, 다국어 번역, 운영 설정, 백업까지
                하나의 운영 콘솔에서 이어서 관리합니다.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_status() -> None:
    prompt_source = st.session_state.get("prompt_source", "defaults")
    source_label = {
        "supabase": "프롬프트 소스: Supabase",
        "local_file": "프롬프트 소스: 로컬 파일",
        "defaults": "프롬프트 소스: 기본값",
    }.get(prompt_source, "프롬프트 소스: 기본값")
    st.markdown(
        f"""
        <div>
            <span class="weviko-status">🧠 {'Gemini 연결됨' if llm_available() else 'Gemini 미설정'}</span>
            <span class="weviko-status">🗄️ {'Supabase 연결됨' if supabase_available() else 'Supabase 미설정'}</span>
            <span class="weviko-status">💾 {source_label}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )


def refresh_prompts() -> None:
    prompt_values, source = load_config_prompts(DEFAULT_PROMPTS)
    st.session_state["prompt_values"] = prompt_values
    st.session_state["prompt_source"] = source


def prompt_value(prompt_key: str) -> str:
    prompts = st.session_state.get("prompt_values", {})
    return prompts.get(prompt_key, DEFAULT_PROMPTS[prompt_key])


def resolve_path_selection(selected_label: str, direct_path: str) -> tuple[str, str]:
    if selected_label == "직접 입력...":
        return "path_manual", direct_path.strip()
    raw_path = selected_label.split("(")[1].split(")")[0]
    return WEVIKO_PATH_MAP[selected_label], raw_path


def navigate_to_mode(section: str, mode: str) -> None:
    st.session_state["active_section"] = section
    if section == "데이터 수집":
        st.session_state["active_pipeline_mode"] = mode
    else:
        st.session_state["active_management_mode"] = mode
    st.rerun()


def render_sidebar() -> str:
    with st.sidebar:
        st.title("🌍 Weviko OS v5.0")
        st.caption("Global Automotive Data Pipeline")
        section_options = ["데이터 수집", "데이터 관리/제어"]
        current_section = st.session_state.get("active_section", section_options[0])
        if current_section not in section_options:
            current_section = section_options[0]
        section = st.radio(
            "작업 영역", section_options, index=section_options.index(current_section)
        )
        st.session_state["active_section"] = section
        st.divider()
        if section == "데이터 수집":
            current_mode = st.session_state.get(
                "active_pipeline_mode", PIPELINE_MODES[0]
            )
            if current_mode not in PIPELINE_MODES:
                current_mode = PIPELINE_MODES[0]
            mode = st.radio(
                "데이터 수집 파이프라인",
                PIPELINE_MODES,
                index=PIPELINE_MODES.index(current_mode),
            )
            st.session_state["active_pipeline_mode"] = mode
        else:
            current_mode = st.session_state.get(
                "active_management_mode", MANAGEMENT_MODES[0]
            )
            if current_mode not in MANAGEMENT_MODES:
                current_mode = MANAGEMENT_MODES[0]
            mode = st.radio(
                "데이터 관리 및 제어",
                MANAGEMENT_MODES,
                index=MANAGEMENT_MODES.index(current_mode),
            )
            st.session_state["active_management_mode"] = mode
        st.divider()
        if st.button("🔌 로그아웃", use_container_width=True):
            st.session_state["logged_in"] = False
            st.rerun()
    return mode


def render_vision_input_mode() -> None:
    st.title("📷 실전 수동 캡처 및 AI 분석")
    st.info(
        "💡 부품 번호를 몰라도 괜찮습니다. 아는 정보만 입력하면 AI가 문맥을 파악해 정리합니다."
    )

    st.subheader("📍 타겟 정보 (선택 입력)")

    col1, col2, col3 = st.columns(3)
    vehicle_text = col1.text_input("차종 및 연식", placeholder="예: 엑센트 2015")
    system_text = col2.text_input("시스템/부품명", placeholder="예: 앞 브레이크 패드")
    part_number = col3.text_input("부품 번호 (알면 입력)", placeholder="예: 58350H6A00")

    st.divider()

    col4, col5 = st.columns(2)
    selected_type = col4.selectbox("문서 종류", list(VISION_DOC_TYPE_OPTIONS.keys()))
    type_config = VISION_DOC_TYPE_OPTIONS[selected_type]
    schema_key = type_config["schema_key"]
    path_hint = type_config["path_hint"]
    market = col5.selectbox("타겟 시장", ["GLOBAL", "VN", "KR", "US"])

    st.caption(f"📍 현재 활성화된 AI 스키마: `{schema_key}` | 탐색 경로: `{path_hint}`")
    template_hint = GSW_JSON_TEMPLATES.get(schema_key)
    if template_hint:
        with st.expander("권장 JSON 템플릿", expanded=schema_key == "path_vehicle_id"):
            st.json(template_hint)

    uploaded_file = st.file_uploader(
        "문서/스크린샷 업로드 (최대 200MB)", type=["png", "jpg", "jpeg", "pdf"]
    )

    if uploaded_file is not None:
        if (uploaded_file.type or "").startswith("image/"):
            st.image(uploaded_file, caption="업로드 미리보기", use_container_width=True)
        else:
            st.caption(f"업로드 파일 타입: `{uploaded_file.type or 'unknown'}`")

    if uploaded_file and st.button(
        "🚀 캡처 데이터 AI 분석 가동", type="primary", use_container_width=True
    ):
        if not supabase_available() or not llm_available():
            st.error("환경 변수(Supabase/Google API)가 설정되지 않았습니다.")
            return

        vehicle_clean = vehicle_text.strip()
        system_clean = system_text.strip()
        part_clean = part_number.strip()

        if part_clean:
            operator_identifier = part_clean
        elif vehicle_clean and system_clean:
            operator_identifier = f"[{vehicle_clean}] {system_clean}"
        elif vehicle_clean or system_clean:
            operator_identifier = vehicle_clean or system_clean
        else:
            operator_identifier = "AI_AUTO_DETECT"

        context_lines = [
            f"Vehicle and model year hint: {vehicle_clean or 'Unknown'}",
            f"System or part hint: {system_clean or 'Unknown'}",
            f"Provided part number hint: {part_clean or 'Unknown'}",
            f"Operator identifier: {operator_identifier}",
            f"Selected schema key: {schema_key}",
            f"Selected path hint: {path_hint}",
            f"Selected market: {market}",
        ]
        template_block = ""
        if template_hint:
            template_block = "\n\n[권장 JSON 템플릿]\n" + json.dumps(
                template_hint, ensure_ascii=False, indent=2
            )

        with st.spinner(
            f"'{operator_identifier}' 문서를 해독 중입니다. 잠시만 기다려주세요..."
        ):
            analysis_result, queue_result = process_vision_and_save(
                file_bytes=uploaded_file.getvalue(),
                file_type=uploaded_file.type,
                part_num=part_clean,
                doc_type_key=schema_key,
                market=market,
                source_path_hint=path_hint,
                document_type=selected_type,
                prompt_override=f"{prompt_value(schema_key)}{template_block}\n\n"
                + "\n".join(context_lines),
                vehicle_hint=vehicle_clean,
                system_hint=system_clean,
                operator_identifier=operator_identifier,
            )

        analysis_result = assess_analysis_quality(analysis_result)
        st.session_state["last_vision_result"] = analysis_result
        st.session_state["last_vision_queue_result"] = queue_result
        st.session_state["last_vision_context"] = {
            "operator_identifier": operator_identifier,
            "part_number_hint": part_clean,
            "vehicle_hint": vehicle_clean,
            "system_hint": system_clean,
            "schema_key": schema_key,
            "path_hint": path_hint,
            "market": market,
            "document_type": selected_type,
        }

        analysis_mode = analysis_result.get("analysis_mode", "")
        quality_status = analysis_result.get("quality_status", "ok")
        if (
            analysis_mode
            in {"gemini", "gemini_structured", "gemini_unstructured_fallback"}
            and queue_result["saved"]
            and quality_status in {"ok", "high"}
        ):
            st.success("✅ 데이터 추출 성공! '검수 대기열'로 이동했습니다.")
        elif queue_result["saved"] and quality_status == "low":
            st.warning(
                "⚠️ 저장은 완료됐지만, 이번 결과는 구조화 정확도가 낮아서 재정리가 필요할 수 있습니다."
            )
        elif queue_result["saved"]:
            st.warning(
                "⚠️ 대기열 저장은 완료됐지만, Gemini 응답에 문제가 있어 오류 대체 JSON으로 저장됐습니다."
            )
        else:
            st.warning("분석은 완료됐지만 검수 대기열 저장은 되지 않았습니다.")

        st.caption(queue_result["message"])

        with st.expander("AI 추출 결과 확인 (JSON)", expanded=True):
            st.json(analysis_result)

    if st.session_state.get("last_vision_result") is not None:
        last_result = assess_analysis_quality(
            dict(st.session_state["last_vision_result"])
        )
        st.session_state["last_vision_result"] = last_result
        st.subheader("최근 Vision 분석 결과")
        st.json(last_result)

        if last_result.get("quality_status") == "low":
            reasons = ", ".join(last_result.get("quality_reasons", [])) or "low_quality"
            st.warning(
                "현재 결과는 구조화 정확도가 낮은 편입니다. " f"감지 사유: `{reasons}`"
            )

            raw_hint_text = str(last_result.get("raw_response", "")).lower()
            if any(
                keyword in raw_hint_text
                for keyword in ["vin", "차대번호", "paint code", "페인트 코드"]
            ):
                st.info(
                    "이 문서는 정비 지침서보다 `차량 식별/VIN/페인트 코드` 스키마에 더 가까워 보입니다. 해당 스키마로 다시 분석하면 품질이 좋아질 가능성이 큽니다."
                )

            context = st.session_state.get("last_vision_context") or {}
            if st.button(
                "🧪 원문 설명을 구조화 JSON으로 재정리", use_container_width=True
            ):
                with st.spinner(
                    "원문 설명을 구조화 JSON으로 다시 정리하고 있습니다..."
                ):
                    refined_result, refine_save_result = refine_vision_result_and_save(
                        last_result,
                        schema_key=context.get(
                            "schema_key", last_result.get("schema_key", "path_manual")
                        ),
                        market=context.get(
                            "market", last_result.get("market", "GLOBAL")
                        ),
                        source_path_hint=context.get(
                            "path_hint", last_result.get("source_path_hint", "")
                        ),
                        document_type=context.get(
                            "document_type", last_result.get("document_type", "")
                        ),
                        part_number_hint=context.get(
                            "part_number_hint", last_result.get("part_number", "")
                        ),
                        vehicle_hint=context.get("vehicle_hint", ""),
                        system_hint=context.get("system_hint", ""),
                        operator_identifier=context.get("operator_identifier", ""),
                        oem_brand=last_result.get("oem_brand", ""),
                    )
                st.session_state["last_vision_result"] = refined_result
                st.session_state["last_vision_queue_result"] = refine_save_result
                if refine_save_result.get("saved"):
                    st.success("✅ 재정리된 JSON을 새 검수 대기 항목으로 저장했습니다.")
                else:
                    st.warning(
                        refine_save_result.get(
                            "message", "재정리 결과 저장에 실패했습니다."
                        )
                    )
                st.rerun()

        queue_result = st.session_state.get("last_vision_queue_result") or {}
        action_col1, action_col2, action_col3 = st.columns(3)
        if queue_result.get("saved"):
            action_col1.success("검수 대기열 저장 완료")
        else:
            context = st.session_state.get("last_vision_context") or {}
            if action_col1.button(
                "📥 현재 JSON을 검수 대기열로 저장", use_container_width=True
            ):
                manual_save_result = enqueue_pending_vision_result(
                    part_number=context.get(
                        "part_number_hint", last_result.get("part_number", "")
                    ),
                    oem_brand=last_result.get("oem_brand", ""),
                    schema_key=context.get(
                        "schema_key", last_result.get("schema_key", "")
                    ),
                    source_path_hint=context.get(
                        "path_hint", last_result.get("source_path_hint", "")
                    ),
                    market=context.get("market", last_result.get("market", "GLOBAL")),
                    document_type=context.get(
                        "document_type", last_result.get("document_type", "")
                    ),
                    analysis_payload=last_result,
                    source_type="vision_manual_upload",
                )
                st.session_state["last_vision_queue_result"] = manual_save_result
                if manual_save_result.get("saved"):
                    st.success("✅ 현재 JSON을 검수 대기열로 저장했습니다.")
                else:
                    st.warning(
                        manual_save_result.get(
                            "message", "검수 대기열 저장에 실패했습니다."
                        )
                    )
                st.rerun()
        if action_col2.button("🕵️ 데이터 검수소로 이동", use_container_width=True):
            navigate_to_mode("데이터 관리/제어", "🕵️ 데이터 검수소 (H-i-t-L)")
        if action_col3.button("📊 통합 현황/백업 열기", use_container_width=True):
            navigate_to_mode("데이터 관리/제어", "📊 통합 현황 및 백업")


def render_live_capture_mode() -> None:
    st.title("🛰️ 현재 탭 라이브 캡처")
    st.markdown(
        "정비소에서 실제로 보고 있는 웹페이지를 작업자가 직접 보내면, "
        "현재의 Gemini 구조화와 Direct/Pending 저장 파이프라인으로 바로 이어집니다."
    )
    st.info(
        "수동 승인형 반자동 모드입니다. 허용된 호스트에서만 사용하고, 기본 운영은 `Pending` 중심으로 시작하는 것을 권장합니다."
    )

    st.subheader("Live Capture 서버 상태")
    server_status = get_live_capture_server_status()

    col_status1, col_status2, col_status3, col_status4 = st.columns(4)

    if server_status:
        col_status1.metric("서버 상태", "✅ 실행 중")
        col_status2.metric("업타임", server_status.get("uptime", "N/A"))
        col_status3.metric(
            "허용 호스트", f"{len(server_status.get('allowed_hosts', []))}개"
        )
        col_status4.metric(
            "Direct 모드",
            (
                "활성화"
                if server_status.get("direct_live_capture_enabled")
                else "비활성화"
            ),
        )
        st.caption(f"서버 시작 시간: {server_status.get('start_time', 'N/A')}")
    else:
        col_status1.metric("서버 상태", "❌ 오프라인")
        col_status2.metric("업타임", "N/A")
        col_status3.metric("허용 호스트", "N/A")
        col_status4.metric("Direct 모드", "N/A")
        st.error(
            "Live Capture 서버가 실행 중이 아니거나 접근할 수 없습니다. `python live_capture_server.py`를 실행해주세요."
        )

    if st.button("🔄 상태 새로고침", key="refresh_live_capture_status"):
        st.cache_data.clear()  # Clear cache for immediate refresh
        st.rerun()

    st.divider()

    st.subheader("최근 라이브 캡처 내역")
    if supabase_available():
        recent_captures = fetch_recent_live_captures(limit=10)
        if recent_captures:
            df_captures = pd.DataFrame(recent_captures)
            df_display = df_captures[
                [
                    "created_at",
                    "part_number",
                    "oem_brand",
                    "schema_key",
                    "document_type",
                    "status",
                ]
            ].copy()
            df_display.rename(
                columns={
                    "created_at": "캡처 시각",
                    "part_number": "부품 번호",
                    "oem_brand": "OEM 브랜드",
                    "schema_key": "스키마 키",
                    "document_type": "문서 종류",
                    "status": "상태",
                },
                inplace=True,
            )
            st.dataframe(df_display, use_container_width=True, hide_index=True)
        else:
            st.info("최근 라이브 캡처 내역이 없습니다.")
    else:
        st.warning("Supabase가 설정되지 않아 최근 캡처 내역을 불러올 수 없습니다.")

    st.divider()

    server_url = live_capture_base_url()
    bookmarklet_code = build_live_capture_bookmarklet(server_url)
    allowed_hosts = live_capture_allowed_hosts()

    st.subheader("Live Capture 설정 및 사용법")
    st.metric("Live Capture Server URL", server_url)
    st.metric(
        "Direct Live Capture",
        "Enabled" if live_capture_direct_enabled() else "Pending Only",
    )

    st.subheader("1. 서버 실행")
    st.code("python live_capture_server.py", language="bash")
    st.caption(
        f"실행 후 브라우저에서 `{server_url}` 를 열면 북마클릿 안내 페이지가 나타납니다."
    )

    st.subheader("2. 허용 호스트 확인")
    st.write(", ".join(allowed_hosts))
    st.caption(
        "`.env`의 `WEVIKO_ALLOWED_CAPTURE_HOSTS`에 명시한 도메인만 북마클릿 전송이 허용됩니다."
    )

    st.subheader("3. 북마클릿 만들기")
    st.caption(
        "브라우저 북마크를 새로 만들고, URL 칸에 아래 코드를 그대로 붙여 넣으세요."
    )
    st.code(bookmarklet_code, language="javascript")

    st.subheader("3A. Chrome 확장 사용")
    st.caption(
        "더 안정적인 현재 탭 수집이 필요하면 `chrome_extension/weviko-live-capture` 폴더를 Chrome의 `Load unpacked`로 로드하세요."
    )
    st.code("chrome_extension/weviko-live-capture", language="text")

    st.subheader("4. 현장 운영 순서")
    st.markdown(
        "\n".join(
            [
                "- 로그인이나 검색 필터를 이미 적용한 상태에서 대상 페이지를 엽니다.",
                "- 필요한 문장이나 부품번호를 마우스로 선택한 뒤 북마클릿을 누릅니다.",
                "- 스키마와 도착지(`pending` 권장)를 확인하고 전송합니다.",
                "- 결과는 기존 검수소, Parts, dead_letters에서 같은 방식으로 확인합니다.",
            ]
        )
    )

    st.subheader("안전 가이드")
    st.markdown(
        "\n".join(
            [
                "- 소유하거나 계약상 접근 권한이 있는 사이트에서만 사용하세요.",
                "- 우회 로그인, 차단 회피, 유료 콘텐츠 무단 복제 용도로 사용하지 마세요.",
                "- 고객 개인정보, 연락처, 차량번호, VIN 전체값은 필요한 경우에만 취급하고 최소한으로 저장하세요.",
                "- 초반에는 `WEVIKO_LIVE_CAPTURE_DIRECT_ENABLED=false` 상태로 검수형 운영을 권장합니다.",
            ]
        )
    )


def render_factory_crawl_tab() -> None:
    st.title("🏭 대규모 크롤링 양산 팩토리")
    st.markdown(
        "특정 카테고리 URL을 입력하면, 하위 페이지를 찾아 병렬 수집한 뒤 원하는 목적지로 보냅니다."
    )

    crawl_source_option = st.radio(
        "크롤링 소스 선택",
        ["시작 URL에서 탐색", "URL 목록 파일 업로드"],
        horizontal=True,
        key="crawl_source_option",
    )

    start_url = ""
    uploaded_urls: list[str] = []
    if crawl_source_option == "시작 URL에서 탐색":
        start_url = st.text_input(
            "시작 카테고리 URL",
            placeholder="예: https://www.weviko.com/community/parts-info",
            key="factory_start_url",
        )
    else:  # "URL 목록 파일 업로드"
        uploaded_file = st.file_uploader(
            "크롤링할 URL 목록 파일 (.txt)", type=["txt"], key="url_list_uploader"
        )
        if uploaded_file is not None:
            string_data = uploaded_file.getvalue().decode("utf-8")
            uploaded_urls = [
                line.strip() for line in string_data.splitlines() if line.strip()
            ]
            st.info(f"업로드된 URL: {len(uploaded_urls)}개")
            if len(uploaded_urls) > 50:
                st.warning(
                    "업로드된 URL이 많습니다. 너무 많은 URL은 시스템 부하를 증가시킬 수 있습니다."
                )
            with st.expander("업로드된 URL 미리보기"):
                st.code(
                    "\n".join(uploaded_urls[:20])
                    + ("\n..." if len(uploaded_urls) > 20 else "")
                )
        else:
            st.warning("URL 목록 파일을 업로드해주세요.")

    execution_mode = st.radio(
        "실행 모드",
        ["단일 타겟 엔진", "하위 URL 병렬 팩토리"],
        horizontal=True,
    )

    col1, col2 = st.columns(2)
    selected_schema_label = col1.selectbox(
        "수집 타겟 및 스키마", list(FACTORY_SCHEMA_OPTIONS.keys())
    )
    worker_count = col2.slider(
        "동시 워커(Worker) 수",
        1,
        10,
        3,
        help="숫자가 높을수록 빠르지만 차단 위험이 커집니다.",
        disabled=(
            crawl_source_option == "URL 목록 파일 업로드"
            and execution_mode == "단일 타겟 엔진"
        ),
    )

    col3, col4 = st.columns(2)
    max_urls = col3.number_input(
        "최대 탐색 URL 수",
        min_value=10,
        max_value=5000,
        value=100,
        step=10,
        disabled=(crawl_source_option == "URL 목록 파일 업로드"),
    )
    max_depth = col4.slider(
        "탐색 깊이", 1, 5, 2, disabled=(crawl_source_option == "URL 목록 파일 업로드")
    )

    destination_label = st.radio(
        "데이터 도착지 설정",
        [
            "1️⃣ 안전 모드: 수집 후 '검수 대기열(Pending)'로 보내기",
            "2️⃣ 직행 모드: 수집 즉시 '정식 DB(Parts)'에 적재하기",
        ],
    )

    schema_config = FACTORY_SCHEMA_OPTIONS[selected_schema_label]
    schema_key = schema_config["schema_key"]
    path_hint = schema_config["path_hint"]

    proxy_value, _ = get_config_prompt("proxy_url", "")
    user_agent_value, _ = get_config_prompt("custom_user_agent", "")

    if st.button("🔥 팩토리 풀가동 시작", type="primary", use_container_width=True):
        if crawl_source_option == "시작 URL에서 탐색" and not start_url.strip():
            st.error("시작 URL을 입력해주세요.")
            return
        if crawl_source_option == "URL 목록 파일 업로드" and not uploaded_urls:
            st.error("URL 목록 파일을 업로드해주세요.")
            return

        destination = "pending" if destination_label.startswith("1") else "parts"

        if execution_mode == "단일 타겟 엔진":
            target_url = (
                start_url.strip()
                if crawl_source_option == "시작 URL에서 탐색"
                else (uploaded_urls[0] if uploaded_urls else "")
            )
            if not target_url or not llm_available() or not supabase_available():
                st.error("환경 변수(Supabase/Google API)가 설정되지 않았습니다.")
                return

            with st.spinner(f"'{start_url.strip()}' 타겟 수집 및 AI 정제 중..."):
                scraped_text = run_crawler_sync(
                    start_url.strip(),
                    proxy=proxy_value.strip() or None,
                    user_agent=user_agent_value.strip() or None,
                )
            if not scraped_text:
                log_dead_letter(
                    start_url.strip(),
                    "single_target_crawl_failed",
                    source_type="single_target_engine",
                    schema_key=schema_key,
                    source_path_hint=path_hint,
                )

            if not scraped_text:
                st.error(
                    "❌ 크롤링 실패 (방화벽 차단 또는 타임아웃). 실패 URL 병원을 확인하세요."
                )
                return

            payload, save_result = process_scraped_text_and_save(
                scraped_text=scraped_text,
                doc_type_key=schema_key,
                market="GLOBAL",
                destination=destination,
                source_path_hint=path_hint,
                document_type=selected_schema_label,
                source_url=start_url.strip(),
            )

            st.session_state["last_factory_result"] = {
                "mode": "single_target",
                "payload": payload,
                "persist": save_result,
                "scraped_chars": len(scraped_text),
                "start_url": start_url.strip(),
            }

            if save_result.get("saved"):
                actual_destination = save_result.get("destination", "Pending")
                destination_name = {
                    "Direct": "정식 DB(Parts)",
                    "Pending": "검수 대기열(Pending)",
                }.get(actual_destination, actual_destination)
                st.success(f"✅ 수집 성공! [{destination_name}]로 반영되었습니다.")
            else:
                st.error(save_result.get("message", "크롤링 결과 저장에 실패했습니다."))

            st.caption(
                f"수집 텍스트 길이: {len(scraped_text):,}자 | "
                f"스키마: `{schema_key}` | 경로 힌트: `{path_hint}`"
            )
            if save_result.get("confidence_score") is not None:
                st.caption(
                    f"Confidence: {save_result.get('confidence_score', 0)} / "
                    f"Threshold {save_result.get('confidence_threshold', '-')}"
                    f" | Quality: `{payload.get('quality_status', 'unknown')}`"
                )
            st.caption(save_result.get("message", ""))
            st.json(payload)
            action_col1, action_col2 = st.columns(2)
            if action_col1.button(
                "🕵️ 검수 대기열/데이터 검수소로 이동", use_container_width=True
            ):
                navigate_to_mode("데이터 관리/제어", "🕵️ 데이터 검수소 (H-i-t-L)")
            if action_col2.button("📊 통합 현황/백업 열기", use_container_width=True):
                navigate_to_mode("데이터 관리/제어", "📊 통합 현황 및 백업")
            return

        progress_box = st.empty()
        log_box = st.empty()
        logs: list[str] = []

        # Initialize progress bar and metrics
        progress_bar = st.progress(0, text="크롤링 시작 준비 중...")
        status_metrics = st.columns(4)

        def handle_log(line: str) -> None:
            logs.append(line)
            log_box.code("\n".join(logs[-30:]), language="bash")

        def handle_numerical_progress(current: int, total: int) -> None:
            if total > 0:
                percentage = int((current / total) * 100)
                progress_bar.progress(percentage, text=f"처리 중: {current}/{total} URL")
            else:
                progress_bar.progress(0, text="처리 중: 0/0 URL")
            # Update the main status container label with current progress
            status_container.update(label=f"크롤링 작업 진행 중... ({current}/{total})", state="running", expanded=True)


        def handle_progress_text(line: str) -> None:
            progress_bar.progress(0, text=line) # Update text, keep value at 0 until completion

        previous_proxy = os.getenv("PLAYWRIGHT_PROXY_SERVER")
        destination = "pending" if destination_label.startswith("1") else "parts"

        if proxy_value.strip():
            os.environ["PLAYWRIGHT_PROXY_SERVER"] = proxy_value.strip()

        # Use st.status for real-time progress bar updates
        with st.status("크롤링 작업 시작 중...", expanded=True) as status_container:
            progress_bar = status_container.progress(0, text="초기화 중...")
            log_box = st.empty()  # Keep log_box for detailed logs
            logs: list[str] = []

            def handle_log(line: str) -> None:
                logs.append(line)
                log_box.code("\n".join(logs[-30:]), language="bash")
                status_container.update(label="크롤링 작업 진행 중...", state="running", expanded=True) # Update status label

            def handle_progress_text(line: str) -> None:
                # This will update the text of the main status container
                status_container.update(label=f"크롤링 작업 진행 중... {line}", state="running", expanded=True)

            def handle_numerical_progress(current: int, total: int) -> None:
                if total > 0:
                    percentage = int((current / total) * 100)
                    progress_bar.progress(percentage, text=f"처리 중: {current}/{total} URL")
                else:
                    progress_bar.progress(0, text="처리 중: 0/0 URL")
                status_container.update(label=f"크롤링 작업 진행 중... ({current}/{total})", state="running", expanded=True)

            try:
                run_result = run_factory(
                    start_url=(
                        start_url.strip()
                        if crawl_source_option == "시작 URL에서 탐색"
                        else None
                    ),
                    initial_urls=(
                        uploaded_urls
                        if crawl_source_option == "URL 목록 파일 업로드"
                        else None
                    ),
                    num_workers=worker_count,
                    target_market="GLOBAL",
                    product_path_hint=path_hint,
                    discovery_extra_path_hints=[path_hint],
                    max_urls=(
                        int(max_urls)
                        if crawl_source_option == "시작 URL에서 탐색"
                        else len(uploaded_urls)
                    ),
                    discovery_max_depth=(
                        max_depth if crawl_source_option == "시작 URL에서 탐색" else 0
                    ),
                    user_agent=user_agent_value.strip() or None,
                    write_destination="none",
                    schema_key=schema_key,
                    source_type="crawl_factory",
                    log_callback=handle_log,
                    progress_text_callback=handle_progress_text,
                    progress_update_callback=handle_numerical_progress,  # Pass the new numerical callback
                )

                # Update progress bar and metrics after run_factory completes
                total_queued = run_result.total_queued_for_processing
                total_processed = run_result.total_processed_by_ai

                progress_bar.progress(100, text="크롤링 및 AI 정제 완료!")
                status_container.update(label="크롤링 및 AI 정제 완료!", state="complete", expanded=False) # Final status update

                status_metrics = st.columns(4)  # Re-declare columns here to place metrics below status
                status_metrics[0].metric("총 큐에 추가된 URL", f"{total_queued:,}개")
                status_metrics[1].metric("AI 처리된 URL", f"{total_processed:,}개")
                # Saved, Skipped, Failed will come from persist_result

        finally:
            if previous_proxy is None:
                os.environ.pop("PLAYWRIGHT_PROXY_SERVER", None)
            else:
                os.environ["PLAYWRIGHT_PROXY_SERVER"] = previous_proxy

            # Update status container for persistence phase
            status_container.update(label="수집 결과를 목적지에 맞게 정리 중...", state="running", expanded=True)

        persist_result = persist_factory_rows(
            rows=run_result.rows,
            destination=destination,
            market=run_result.target_market,
            schema_key=schema_key,
            source_path_hint=path_hint,
            source_type="crawl_factory",
            document_type=selected_schema_label,
        )

        st.session_state["last_factory_result"] = {
            "result": run_result,
            "persist": persist_result,
            "destination": destination,
        }

        destination_name = (
            "검수 대기열(Pending)" if destination == "pending" else "정식 DB(Parts)"
        )

        # Update metrics with persist_result
        status_metrics[2].metric("DB 저장 성공", f"{persist_result['saved_count']:,}건")
        status_metrics[3].metric(
            "DB 저장 건너뜀/실패",
            f"{persist_result['skipped_count'] + len(persist_result['errors']):,}건",
        )

        st.success(
            f"✅ 총 {run_result.total_queued_for_processing:,}개 URL 탐색 완료. "
            f"결과 {persist_result['saved_count']:,}건을 {destination_name}에 반영했습니다."
        )
        st.caption(persist_result["message"])
        if destination == "parts":
            st.info(
                f"Direct {persist_result['direct_count']:,} items | "
                f"Pending {persist_result['pending_count']:,} items | "
                f"Skipped {persist_result['skipped_count']:,} items"
            )
        if run_result.route_status_counts:
            status_df = pd.DataFrame(
                [
                    {"route_status": key, "count": value}
                    for key, value in sorted(run_result.route_status_counts.items())
                ]
            )
            st.dataframe(status_df, use_container_width=True, hide_index=True)

        if run_result.rows:
            st.dataframe(
                pd.DataFrame(run_result.rows), use_container_width=True, hide_index=True
            )
        action_col1, action_col2 = st.columns(2)
        if action_col1.button("🕵️ 데이터 검수소로 이동", use_container_width=True):
            navigate_to_mode("데이터 관리/제어", "🕵️ 데이터 검수소 (H-i-t-L)")
        if action_col2.button("📊 통합 현황/백업 열기", use_container_width=True):
            navigate_to_mode("데이터 관리/제어", "📊 통합 현황 및 백업")
        return


def render_scheduler_tab() -> None:
    st.title("⏰ 크롤링 스케줄러")
    st.markdown("정기적으로 실행할 크롤링 작업을 스케줄링하고 관리합니다.")

    if not supabase_available():
        st.error("Supabase 환경 변수가 설정되지 않았습니다.")
        return

    st.subheader("새 스케줄 추가")
    with st.form("new_schedule_form"):
        schedule_start_url = st.text_input(
            "시작 URL", placeholder="예: https://www.weviko.com/community/parts-info"
        )
        schema_key_options = list(FACTORY_SCHEMA_OPTIONS.keys())
        schedule_schema_label = st.selectbox(
            "AI 스키마", schema_key_options, key="schedule_schema_key_select"
        )
        schedule_interval = st.selectbox(
            "반복 주기", ["once", "daily", "weekly", "monthly"]
        )

        submitted = st.form_submit_button("➕ 스케줄 추가", type="primary")
        if submitted:
            if not schedule_start_url.strip():
                st.error("시작 URL을 입력해주세요.")
            else:
                schema_config = FACTORY_SCHEMA_OPTIONS[schedule_schema_label]
                result = create_scheduled_crawl(
                    start_url=schedule_start_url.strip(),
                    schema_key=schema_config["schema_key"],
                    schedule_interval=schedule_interval,
                )
                if result["saved"]:
                    st.success(result["message"])
                    st.rerun()
                else:
                    st.error(result["message"])

    st.subheader("크롤링 상세 설정 (선택 사항)")
    with st.expander("상세 설정 펼치기", expanded=False):
        col_s1, col_s2 = st.columns(2)
        schedule_num_workers = col_s1.number_input(
            "동시 워커(Worker) 수",
            1,
            10,
            3,
            help="숫자가 높을수록 빠르지만 차단 위험이 커집니다.",
            key="schedule_num_workers",
        )
        schedule_max_urls = col_s2.number_input(
            "최대 탐색 URL 수",
            10,
            5000,
            100,
            step=10,
            help="탐색 모드에서만 적용됩니다.",
            key="schedule_max_urls",
        )

        col_s3, col_s4, col_s5 = st.columns(3)
        schedule_discovery_max_pages = col_s3.number_input(
            "탐색 최대 페이지 수",
            1,
            50,
            12,
            help="탐색 모드에서 스파이더가 방문할 최대 페이지 수",
            key="schedule_discovery_max_pages",
        )
        schedule_discovery_max_matches = col_s4.number_input(
            "탐색 최대 매치 수",
            1,
            100,
            20,
            help="탐색 모드에서 스파이더가 큐에 추가할 최대 URL 수",
            key="schedule_discovery_max_matches",
        )
        schedule_discovery_max_depth = col_s5.number_input(
            "탐색 깊이",
            0,
            5,
            2,
            help="탐색 모드에서 스파이더가 링크를 따라갈 최대 깊이",
            key="schedule_discovery_max_depth",
        )

        schedule_product_path_hint = st.text_input(
            "제품 경로 힌트 (예: /part/)",
            value="/part/",
            help="제품 상세 페이지를 식별하는 URL 경로 힌트",
            key="schedule_product_path_hint",
        )
        schedule_discovery_extra_path_hints = st.text_input(
            "추가 탐색 경로 힌트 (콤마로 구분)",
            value="",
            help="제품 상세 페이지 외에 탐색할 추가 경로 힌트 (예: /category/,/search/)",
            key="schedule_discovery_extra_path_hints",
        )
        schedule_route_watch_hints = st.text_input(
            "라우트 감시 힌트 (콤마로 구분)",
            value="/parts,/dashboard",
            help="크롤러가 방문해야 하는 특정 라우트 힌트 (예: /login,/cart)",
            key="schedule_route_watch_hints",
        )
        schedule_blocked_resource_types = st.text_input(
            "차단할 리소스 타입 (콤마로 구분)",
            value="image,media,font,stylesheet",
            help="크롤링 시 로드하지 않을 리소스 타입 (예: image,media,font,stylesheet)",
            key="schedule_blocked_resource_types",
        )
        schedule_user_agent = st.text_area(
            "Custom User-Agent",
            value="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            height=70,
            help="크롤링 시 사용할 사용자 에이전트 문자열",
            key="schedule_user_agent",
        )

        submitted = st.form_submit_button("➕ 스케줄 추가", type="primary")
        if submitted:
            if not schedule_start_url.strip():
                st.error("시작 URL을 입력해주세요.")
            else:
                schema_config = FACTORY_SCHEMA_OPTIONS[schedule_schema_label]
                result = create_scheduled_crawl(
                    start_url=schedule_start_url.strip(),
                    schema_key=schema_config["schema_key"],
                    schedule_interval=schedule_interval,
                    num_workers=schedule_num_workers,
                    max_urls=schedule_max_urls,
                    discovery_max_pages=schedule_discovery_max_pages,
                    discovery_max_matches=schedule_discovery_max_matches,
                    discovery_max_depth=schedule_discovery_max_depth,
                    product_path_hint=schedule_product_path_hint.strip() or None,
                    discovery_extra_path_hints=[h.strip() for h in schedule_discovery_extra_path_hints.split(',') if h.strip()] if schedule_discovery_extra_path_hints.strip() else [],
                    route_watch_hints=[h.strip() for h in schedule_route_watch_hints.split(',') if h.strip()] if schedule_route_watch_hints.strip() else [],
                    blocked_resource_types=[t.strip() for t in schedule_blocked_resource_types.split(',') if t.strip()] if schedule_blocked_resource_types.strip() else [],
                    user_agent=schedule_user_agent.strip() or None,
                )
                if result["saved"]:
                    st.success(result["message"])
                    st.rerun()
                else:
                    st.error(result["message"])

    st.subheader("현재 스케줄 목록")
    schedules = fetch_scheduled_crawls()
    if not schedules:
        st.info("현재 스케줄링된 크롤링 작업이 없습니다.")
        return

    # Add a refresh mechanism for near real-time updates
    refresh_interval = st.slider("자동 새로고침 간격 (초)", 0, 60, 5, help="0으로 설정하면 자동 새로고침 안함")
    if refresh_interval > 0:
        st.rerun() # Trigger rerun to update UI
        time.sleep(refresh_interval)

    df_schedules = pd.DataFrame(schedules)
    df_display = df_schedules[
        [
            "id",
            "start_url",
            "schema_key",
            "schedule_interval",
            "next_run_at",
            "last_run_at",
            "last_run_status",
            "is_active",
            "progress_log",
            "current_progress",
            "total_progress",
            "num_workers",
            "max_urls",
            "discovery_max_pages",
            "discovery_max_matches",
            "discovery_max_depth",
            "product_path_hint",
            "discovery_extra_path_hints",
            "route_watch_hints",
            "blocked_resource_types",
            "user_agent",
        ]
    ].copy()
    df_display.rename(
        columns={
            "id": "ID",
            "start_url": "시작 URL",
            "schema_key": "스키마 키",
            "schedule_interval": "반복 주기",
            "next_run_at": "다음 실행 시각",
            "last_run_at": "마지막 실행 시각",
            "last_run_status": "마지막 실행 상태",
            "is_active": "활성화",
            "progress_log": "진행 로그",
            "current_progress": "현재 진행",
            "total_progress": "총 진행",
            "num_workers": "워커 수",
            "max_urls": "최대 URL",
            "discovery_max_pages": "탐색 페이지",
            "discovery_max_matches": "탐색 매치",
            "discovery_max_depth": "탐색 깊이",
            "product_path_hint": "제품 경로 힌트",
            "discovery_extra_path_hints": "추가 탐색 힌트",
            "route_watch_hints": "라우트 감시 힌트",
            "blocked_resource_types": "차단 리소스",
            "user_agent": "User-Agent",
        },
        inplace=True,
    )

    for index, row in df_display.iterrows():
        st.markdown(f"#### {index + 1}. {row['시작 URL']}")
        st.markdown(f"**스키마:** `{row['스키마 키']}` | **반복 주기:** `{row['반복 주기']}` | **활성화:** `{row['활성화']}`")
        st.markdown(f"**다음 실행:** `{row['다음 실행 시각']}` | **마지막 실행:** `{row['마지막 실행 시각'] or 'N/A'}` ({row['마지막 실행 상태'] or 'N/A'})")

        # Display progress bar for active jobs
        if row['last_run_status'] == 'running' and row['total_progress'] > 0:
            progress_percentage = int((row['current_progress'] / row['total_progress']) * 100)
            st.progress(progress_percentage, text=row['진행 로그'])
        elif row['last_run_status'] == 'running':
            st.info(f"진행 중: {row['진행 로그']}")

        with st.expander("상세 설정 및 로그 보기"):
            st.write(f"**워커 수:** `{row['워커 수'] or '기본값'}`")
            st.write(f"**최대 URL:** `{row['최대 URL'] or '기본값'}`")
            st.write(f"**탐색 페이지:** `{row['탐색 페이지'] or '기본값'}`")
            st.write(f"**탐색 매치:** `{row['탐색 매치'] or '기본값'}`")
            st.write(f"**탐색 깊이:** `{row['탐색 깊이'] or '기본값'}`")
            st.write(f"**제품 경로 힌트:** `{row['제품 경로 힌트'] or '기본값'}`")
            st.write(f"**추가 탐색 힌트:** `{', '.join(json.loads(row['추가 탐색 힌트']) if isinstance(row['추가 탐색 힌트'], str) else row['추가 탐색 힌트']) or '없음'}`")
            st.write(f"**라우트 감시 힌트:** `{', '.join(json.loads(row['라우트 감시 힌트']) if isinstance(row['라우트 감시 힌트'], str) else row['라우트 감시 힌트']) or '없음'}`")
            st.write(f"**차단 리소스:** `{', '.join(json.loads(row['차단 리소스']) if isinstance(row['차단 리소스'], str) else row['차단 리소스']) or '없음'}`")
            st.write(f"**User-Agent:** `{row['User-Agent'] or '기본값'}`")
            if row['last_run_log']:
                st.subheader("마지막 실행 전체 로그")
                st.code(row['last_run_log'], language="bash")

        col_actions = st.columns(2)
        if col_actions[0].button("즉시 실행", key=f"run_now_{row['ID']}", use_container_width=True):
            with st.spinner("스케줄을 즉시 실행합니다... (Streamlit UI가 블로킹됩니다)"):
                result = run_scheduled_crawl_now(row["ID"])
                if result["success"]:
                    st.success(result["message"])
                else:
                    st.error(result["message"])
                st.rerun()
        if col_actions[1].button("삭제", key=f"delete_{row['ID']}", use_container_width=True):
            result = delete_scheduled_crawl(row["ID"])
            if result["saved"]:
                st.warning(result["message"])
            else:
                st.error(result["message"])
            st.rerun()
        st.divider()


def render_factory_mode() -> None:
    st.title("🏭 대규모 크롤링 양산 팩토리")
    tab1, tab2 = st.tabs(["수동 크롤링 실행", "크롤링 스케줄러"])

    with tab1:
        render_factory_crawl_tab()
    with tab2:
        render_scheduler_tab()


def render_review_mode() -> None:
    st.title("🕵️ 데이터 검수소 (H-i-t-L)")
    tab1, tab2 = st.tabs(["검수 대기열", "반려된 항목"])

    with tab1:
        render_pending_review_tab()

    with tab2:
        render_rejected_items_tab()


def render_translation_mode() -> None:
    st.title("🌐 원클릭 글로벌 다국어 번역")
    st.write(
        "정식 DB(`parts`)에서 `translations`가 비어 있는 항목을 찾아 영어와 베트남어로 변환합니다."
    )

    if not supabase_available():
        st.error("Supabase 환경 변수가 설정되지 않았습니다.")
        return

    prompt_text = st.text_area(
        "번역 프롬프트",
        value=prompt_value("translation_vn"),
        height=120,
    )
    batch_size = st.slider("한 번에 처리할 건수", 1, 20, 5)

    if st.button("🚀 미번역 데이터 일괄 번역 가동", type="primary"):
        parts_rows = fetch_untranslated_parts(limit=batch_size)
        if not parts_rows:
            st.info("모든 데이터가 번역되어 있거나 대상이 없습니다.")
            return

        progress = st.progress(0, text="번역 준비 중...")
        for index, row in enumerate(parts_rows, start=1):
            part_number = row.get("part_number", "Unknown")
            oem_brand = row.get("oem_brand", "") or "-"
            st.write(f"번역 중: {part_number} ({oem_brand})")
            translation_payload, translation_status = translate_record(
                record=row,
                prompt_text=prompt_text,
            )
            save_status = save_part_translation(part_number, translation_payload)
            st.session_state["last_translation_results"][
                part_number
            ] = translation_payload
            progress.progress(
                int(index / len(parts_rows) * 100),
                text=f"번역 중... ({index}/{len(parts_rows)})",
            )
            st.caption(translation_status["message"])
            st.caption(save_status["message"])
        progress.progress(100, text="번역 완료")
        st.success("번역 완료!")

    if st.session_state["last_translation_results"]:
        preview_key = st.selectbox(
            "최근 번역 결과 미리보기",
            list(st.session_state["last_translation_results"].keys()),
        )
        st.json(st.session_state["last_translation_results"][preview_key])


def render_settings_mode() -> None:
    st.title("⚙️ 프롬프트 및 우회 설정")
    tab1, tab2, tab3 = st.tabs(
        ["🧠 AI 시스템 프롬프트", "🛡️ 봇 우회 (Proxy) 설정", "📊 Supabase 테이블 현황"]
    )

    with tab1:
        prompt_key = st.selectbox(
            "관리할 프롬프트",
            [
                "path_manual",
                "path_body_manual",
                "path_detail",
                "path_connector",
                "path_vehicle_id",
                "path_wiring",
                "path_dtc",
                "path_community",
                "translation_vn",
                "crawling_ecommerce",
            ],
        )
        current_value, source = get_config_prompt(
            prompt_key, DEFAULT_PROMPTS[prompt_key]
        )
        new_value = st.text_area("프롬프트 내용", value=current_value, height=220)
        st.caption(f"현재 로드 소스: {source}")

        if st.button("💾 프롬프트 저장", type="primary"):
            result = save_config_prompt(prompt_key, new_value)
            refresh_prompts()
            if result["remote_saved"] or result["local_saved"]:
                st.success(result["message"])
            else:
                st.error(result["message"])

    with tab2:
        st.markdown("대규모 양산 시 타겟 사이트 방화벽 우회를 위한 설정입니다.")
        proxy_value, _ = get_config_prompt("proxy_url", "")
        user_agent_value, _ = get_config_prompt("custom_user_agent", "")
        confidence_threshold_value, _ = get_config_prompt("confidence_threshold", "90")

        proxy_url = st.text_input(
            "Proxy URL (형식: http://user:pass@ip:port)",
            value=proxy_value,
        )
        user_agent = st.text_area(
            "Custom User-Agent",
            value=user_agent_value
            or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            height=100,
        )
        confidence_threshold = st.number_input(
            "자동 직행 신뢰도 기준점",
            min_value=0,
            max_value=100,
            value=int(confidence_threshold_value or "90"),
            step=1,
            help="이 점수 이상이면 검수 없이 정식 DB로 자동 등록합니다.",
        )

        if st.button("🛡️ 보안 설정 저장", type="primary"):
            proxy_result = save_config_prompt("proxy_url", proxy_url)
            ua_result = save_config_prompt("custom_user_agent", user_agent)
            threshold_result = save_config_prompt(
                "confidence_threshold", str(confidence_threshold)
            )
            if (
                (proxy_result["remote_saved"] or proxy_result["local_saved"])
                and (ua_result["remote_saved"] or ua_result["local_saved"])
                and (
                    threshold_result["remote_saved"] or threshold_result["local_saved"]
                )
            ):
                st.success("프록시, User-Agent, 신뢰도 기준점이 저장되었습니다.")
            else:
                st.error("일부 설정 저장에 실패했습니다.")


def render_dead_letter_mode() -> None:
    st.title("🏥 실패 URL 병원")
    if not supabase_available():
        st.error("Supabase 환경 변수가 설정되지 않았습니다.")
        return

    st.subheader("필터 및 정렬 옵션")
    with st.expander("옵션 설정", expanded=False):
        col_filter1, col_filter2 = st.columns(2)
        error_reason_keyword = col_filter1.text_input(
            "실패 사유 키워드 검색", key="dlq_error_keyword"
        )

        # Dynamically get schema_key and source_type options from FACTORY_SCHEMA_OPTIONS
        schema_key_options = sorted(
            list(
                set(config["schema_key"] for config in FACTORY_SCHEMA_OPTIONS.values())
            )
        )
        selected_schema_keys = col_filter2.multiselect(
            "스키마 키 필터", options=schema_key_options, key="dlq_schema_filter"
        )

        col_filter3, col_filter4 = st.columns(2)
        # Common source types, can be expanded dynamically if needed
        source_type_options = sorted(
            [
                "crawl_factory",
                "dead_letter_retry",
                "dead_letter_manual_retry",
                "low_quality_data",
                "single_target_crawl_failed",
                "browser_live_capture",
                "crawl_save_failed",
                "persist_factory_rows_failed",
                "vision_capture",
                "vision_manual_upload",
                "gemini_refined",
            ]
        )
        selected_source_types = col_filter3.multiselect(
            "소스 타입 필터", options=source_type_options, key="dlq_source_type_filter"
        )

        col_date1, col_date2 = st.columns(2)
        start_date_val = col_date1.date_input(
            "시작 날짜", value=None, key="dlq_start_date"
        )
        end_date_val = col_date2.date_input("종료 날짜", value=None, key="dlq_end_date")

        col_sort1, col_sort2 = st.columns(2)
        sort_options = {
            "생성 시각": "created_at",
            "최근 변경 시각": "updated_at",
            "URL": "url",
            "실패 사유": "error_reason",
            "스키마 키": "schema_key",
            "소스 타입": "source_type",
        }
        sort_by_label = col_sort1.selectbox(
            "정렬 기준", list(sort_options.keys()), index=0, key="dlq_sort_by"
        )
        sort_order = col_sort2.radio(
            "정렬 순서",
            ["내림차순", "오름차순"],
            horizontal=True,
            index=0,
            key="dlq_sort_order",
        )

    rows = fetch_dead_letters(
        limit=500,  # Fetch more to allow in-memory filtering if needed, though now Supabase handles it
        error_reason_keyword=error_reason_keyword,
        schema_key_filter=selected_schema_keys if selected_schema_keys else None,
        source_type_filter=selected_source_types if selected_source_types else None,
        start_date=start_date_val.isoformat() if start_date_val else None,
        end_date=end_date_val.isoformat() if end_date_val else None,
        sort_by=sort_options[sort_by_label],
        sort_order="desc" if sort_order == "내림차순" else "asc",
    )
    if not rows:
        st.success("🎉 처리할 실패 URL이 없습니다.")
        return

    st.write(f"총 {len(rows)}개의 미해결 URL이 있습니다.")

    df = pd.DataFrame(rows)
    df_display = df[
        [
            "id",
            "url",
            "error_reason",
            "schema_key",
            "source_type",
            "error_details",  # New column
            "created_at",
            "updated_at",
        ]
    ].copy()
    df_display.rename(
        columns={
            "id": "ID",
            "url": "URL",
            "error_reason": "실패 사유",
            "schema_key": "스키마 키",
            "source_type": "소스 타입",
            "error_details": "상세 오류",  # New column
            "created_at": "생성 시각",
            "updated_at": "최근 변경 시각",
        },
        inplace=True,
    )

    st.subheader("미해결 실패 URL 목록")
    st.data_editor(
        df_display,
        key="dead_letters_editor",
        column_config={
            "ID": st.column_config.TextColumn(disabled=True),
            "URL": st.column_config.LinkColumn(disabled=True),
            "실패 사유": st.column_config.TextColumn(disabled=True),
            "스키마 키": st.column_config.TextColumn(disabled=True),
            "소스 타입": st.column_config.TextColumn(disabled=True),
            "상세 오류": st.column_config.JsonColumn(disabled=True),  # New column
            "생성 시각": st.column_config.DatetimeColumn(disabled=True),
            "최근 변경 시각": st.column_config.DatetimeColumn(disabled=True),
        },
        hide_index=True,
        use_container_width=True,
        num_rows="dynamic",
    )

    selected_indices = st.session_state.dead_letters_editor.get("selection", [])
    selected_item_ids = [df.loc[idx, "id"] for idx in selected_indices]

    st.write(f"선택된 항목: {len(selected_item_ids)}개")

    col_bulk_retry, col_bulk_delete = st.columns(2)

    if col_bulk_retry.button(
        "🔄 선택 항목 일괄 재처리",
        use_container_width=True,
        disabled=not selected_item_ids,
    ):
        if selected_item_ids:
            with st.spinner(f"{len(selected_item_ids)}개 항목을 재처리 중..."):
                bulk_result = bulk_retry_dead_letters(selected_item_ids)
                if bulk_result["success_count"] > 0:
                    st.success(
                        f"✅ {bulk_result['success_count']}개 항목 재처리 성공. {bulk_result['fail_count']}개 항목 실패."
                    )
                if bulk_result["fail_count"] > 0:
                    st.error(f"❌ {bulk_result['fail_count']}개 항목 재처리 실패.")
                st.rerun()
        else:
            st.warning("재처리할 항목을 선택해주세요.")

    if col_bulk_delete.button(
        "🗑️ 선택 항목 일괄 영구 삭제",
        use_container_width=True,
        disabled=not selected_item_ids,
    ):
        if selected_item_ids:
            if st.popover("정말 삭제하시겠습니까?"):
                if st.button(
                    "예, 영구 삭제합니다.", type="primary", key="confirm_bulk_delete"
                ):
                    with st.spinner(
                        f"{len(selected_item_ids)}개 항목을 영구 삭제 중..."
                    ):
                        bulk_result = bulk_delete_dead_letters(selected_item_ids)
                        if bulk_result["success_count"] > 0:
                            st.success(
                                f"✅ {bulk_result['success_count']}개 항목 영구 삭제 성공."
                            )
                        if bulk_result["fail_count"] > 0:
                            st.error(
                                f"❌ {bulk_result['fail_count']}개 항목 영구 삭제 실패."
                            )
                        st.rerun()
        else:
            st.warning("삭제할 항목을 선택해주세요.")


def render_export_mode() -> None:
    st.title("📊 통합 현황 및 백업")
    if not supabase_available():
        st.error("Supabase 환경 변수가 설정되지 않았습니다.")
        return

    col1, col2, col3 = st.columns(3)
    col1.metric("현재 상용 DB 적재량", f"{fetch_parts_count():,} 개")
    col2.metric(
        "최근 Vision 분석",
        "1건" if st.session_state.get("last_vision_result") else "0건",
    )
    col3.metric(
        "최근 번역 결과",
        f"{len(st.session_state.get('last_translation_results', {})):,} 건",
    )

    st.divider()
    st.write("전체 데이터를 엑셀(CSV) 형식으로 백업합니다.")

    if st.button("데이터 불러오기 준비"):
        rows = fetch_parts_export()
        if not rows:
            st.info("내보낼 데이터가 없습니다.")
            return

        df = pd.DataFrame(rows)
        csv_data = convert_df(df)
        st.download_button(
            label="📥 CSV 다운로드 실행",
            data=csv_data,
            file_name=f"weviko_db_export_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
            type="primary",
        )
        st.dataframe(df, use_container_width=True, hide_index=True)


def main() -> None:
    st.set_page_config(page_title="Weviko Master OS", page_icon="🌍", layout="wide")
    init_state()
    inject_styles()
    ensure_login_secure()

    mode = render_sidebar()
    render_header()
    render_status()

    if mode == "📷 수동 캡처(Vision)":
        render_vision_input_mode()
    elif mode == "🛰️ 현재 탭 라이브 캡처":
        render_live_capture_mode()
    elif mode == "🏭 대규모 양산 팩토리(URL)":
        render_factory_mode()
    elif mode == "🕵️ 데이터 검수소 (H-i-t-L)":
        render_review_mode()
    elif mode == "🌐 다국어 번역 엔진":
        render_translation_mode()
    elif mode == "⚙️ 시스템 환경 설정":
        render_settings_mode()
    elif mode == "🏥 실패 URL 병원":
        render_dead_letter_mode()
    else:
        render_export_mode()


if __name__ == "__main__":
    main()
    if st.session_state.get("last_factory_result"):
        st.divider()
        st.subheader("🏭 최근 팩토리 가동 결과")
        last_run = st.session_state["last_factory_result"]

        if last_run.get("mode") == "single_target":
            st.write(f"**모드:** 단일 타겟 엔진")
            st.write(f"**URL:** `{last_run.get('start_url')}`")
            persist_result = last_run.get("persist", {})
            if persist_result.get("saved"):
                st.success(f"✅ 저장 성공: {persist_result.get('message')}")
            else:
                st.error(f"❌ 저장 실패: {persist_result.get('message')}")
            with st.expander("AI 추출 결과 보기"):
                st.json(last_run.get("payload", {}))
        elif last_run.get("result"):  # Parallel factory mode
            run_result = last_run.get("result")
            persist_result = last_run.get("persist", {})
            destination = last_run.get("destination", "pending")
            destination_name = (
                "검수 대기열(Pending)" if destination == "pending" else "정식 DB(Parts)"
            )

            st.write(f"**모드:** 하위 URL 병렬 팩토리")

            col_last_run_metrics = st.columns(4)
            col_last_run_metrics[0].metric(
                "총 큐에 추가된 URL", f"{run_result.total_queued_for_processing:,}개"
            )
            col_last_run_metrics[1].metric(
                "AI 처리된 URL", f"{run_result.total_processed_by_ai:,}개"
            )
            col_last_run_metrics[2].metric(
                "DB 저장 성공", f"{persist_result.get('saved_count', 0):,}건"
            )
            col_last_run_metrics[3].metric(
                "DB 저장 건너뜀/실패",
                f"{persist_result.get('skipped_count', 0) + len(persist_result.get('errors', [])):,}건",
            )

            st.success(
                f"✅ 총 {run_result.total_queued_for_processing:,}개 URL 탐색 완료. "
                f"결과 {persist_result.get('saved_count', 0):,}건을 {destination_name}에 반영했습니다."
            )
            st.caption(persist_result.get("message", ""))
            if destination == "parts":
                st.info(
                    f"Direct {persist_result.get('direct_count', 0):,} items | "
                    f"Pending {persist_result.get('pending_count', 0):,} items | "
                    f"Skipped {persist_result.get('skipped_count', 0):,} items"
                )

            if run_result.route_status_counts:
                with st.expander("라우트 상태별 카운트", expanded=True): # Make it expanded by default
                    status_df = pd.DataFrame(
                        [
                            {"route_status": key, "count": value}
                            for key, value in sorted(
                                run_result.route_status_counts.items()
                            )
                        ]
                    )
                    st.dataframe(status_df, use_container_width=True, hide_index=True)

            if run_result.rows:
                with st.expander("수집된 데이터 상세 보기"):
                    st.dataframe(
                        pd.DataFrame(run_result.rows),
                        use_container_width=True,
                        hide_index=True,
                    )

        action_col1, action_col2 = st.columns(2)
        if action_col1.button(
            "🕵️ 데이터 검수소로 이동", use_container_width=True, key="factory_review_nav"
        ):
            navigate_to_mode("데이터 관리/제어", "🕵️ 데이터 검수소 (H-i-t-L)")
        if action_col2.button(
            "📊 통합 현황/백업 열기", use_container_width=True, key="factory_export_nav"
        ):
            navigate_to_mode("데이터 관리/제어", "📊 통합 현황 및 백업")


def render_review_mode() -> None:
    st.title("🕵️ 데이터 검수소 (H-i-t-L)")
    st.markdown(
        "수동 캡처나 자동 파이프라인에서 수집한 `pending_data` 대기열을 승인하거나 반려합니다."
    )

    if not supabase_available():
        st.error("Supabase 환경 변수가 설정되지 않았습니다.")
        return

    pending_items = fetch_pending_items(limit=50)
    if not pending_items:
        st.info("현재 검수 대기 중인 데이터가 없습니다.")
        return

    item_lookup = {
        (
            f"{item.get('part_number', 'Unknown')} | "
            f"{item.get('oem_brand', '-') or '-'} | "
            f"{item.get('market', 'GLOBAL')} | "
            f"{item.get('source_type', 'unknown')} | "
            f"{item.get('id', '-')}"
        ): item
        for item in pending_items
    }
    selected_label = st.selectbox("검수 대상", list(item_lookup.keys()))
    if not selected_label:
        return
    item = item_lookup[selected_label]

    raw_json = item.get("raw_json", {})
    display_title = (
        raw_json.get("title")
        or raw_json.get("summary")
        or item.get("part_number", "Unknown")
    )
    st.write(f"### 문서/식별자: `{display_title}`")
    st.caption(
        f"수집 소스: {item.get('source_type', 'unknown')} | "
        f"시장: {item.get('market', 'GLOBAL')} | "
        f"수집 시각: {item.get('created_at')}"
    )

    st.subheader("📝 핵심 정보 수정")

    col1, col2 = st.columns(2)
    part_number = col1.text_input(
        "부품 번호 (Part Number)", value=raw_json.get("part_number", "")
    )
    oem_brand = col2.text_input("OEM 브랜드", value=raw_json.get("oem_brand", ""))

    col3, col4 = st.columns(2)
    schema_key_options = list(FACTORY_SCHEMA_OPTIONS.keys())
    current_schema_key = raw_json.get("schema_key", "")
    current_schema_label = next(
        (
            label
            for label, config in FACTORY_SCHEMA_OPTIONS.items()
            if config["schema_key"] == current_schema_key
        ),
        None,
    )
    try:
        schema_key_index = (
            schema_key_options.index(current_schema_label)
            if current_schema_label in schema_key_options
            else 0
        )
    except ValueError:
        schema_key_index = 0

    selected_schema_label = col3.selectbox(
        "AI 스키마", schema_key_options, index=schema_key_index
    )
    schema_config = FACTORY_SCHEMA_OPTIONS[selected_schema_label]
    schema_key = schema_config["schema_key"]
    source_path_hint = schema_config["path_hint"]

    document_type = col4.text_input(
        "문서 종류 (Document Type)", value=raw_json.get("document_type", "")
    )

    title = st.text_input("문서 제목 (Title)", value=raw_json.get("title", ""))
    summary = st.text_area(
        "요약 (Summary)", value=raw_json.get("summary", ""), height=100
    )

    st.subheader("🔬 상세 구조화 데이터 (JSON)")

    def _render_json_editor(label: str, data: Any, height: int) -> str:
        return st.text_area(
            label,
            value=json.dumps(data, indent=2, ensure_ascii=False),
            height=height,
            label_visibility="collapsed",
        )

    with st.expander("차량 정보 (Vehicle)"):
        vehicle_json = _render_json_editor(
            "vehicle_json", raw_json.get("vehicle", {}), 150
        )

    with st.expander("호환성 정보 (Compatibility)"):
        compatibility_json = _render_json_editor(
            "compatibility_json", raw_json.get("compatibility", []), 200
        )

    with st.expander("기술 제원 (Specifications)"):
        specifications_json = _render_json_editor(
            "specifications_json", raw_json.get("specifications", {}), 200
        )

    with st.expander("추가 추출 팩트 (Extracted Facts)"):
        extracted_facts_json = _render_json_editor(
            "extracted_facts_json", raw_json.get("extracted_facts", {}), 200
        )

    with st.expander("주의사항 (Cautions)"):
        cautions_json = _render_json_editor(
            "cautions_json", raw_json.get("cautions", []), 100
        )

    # Display original JSON for reference
    with st.expander("전체 원본 JSON 보기 (참고용)"):
        st.json(raw_json)

    # --- Diff generation ---
    payload_for_diff = dict(raw_json)

    def silent_json_load(json_string: str, default_value: Any) -> Any:
        try:
            return json.loads(json_string)
        except json.JSONDecodeError:
            return default_value

    payload_for_diff["part_number"] = part_number
    payload_for_diff["oem_brand"] = oem_brand
    payload_for_diff["schema_key"] = schema_key
    payload_for_diff["source_path_hint"] = source_path_hint
    payload_for_diff["document_type"] = document_type
    payload_for_diff["title"] = title
    payload_for_diff["summary"] = summary
    payload_for_diff["vehicle"] = silent_json_load(
        vehicle_json, raw_json.get("vehicle", {})
    )
    payload_for_diff["compatibility"] = silent_json_load(
        compatibility_json, raw_json.get("compatibility", [])
    )
    payload_for_diff["specifications"] = silent_json_load(
        specifications_json, raw_json.get("specifications", {})
    )
    payload_for_diff["extracted_facts"] = silent_json_load(
        extracted_facts_json, raw_json.get("extracted_facts", {})
    )
    payload_for_diff["cautions"] = silent_json_load(
        cautions_json, raw_json.get("cautions", [])
    )

    original_str = json.dumps(raw_json, indent=2, sort_keys=True, ensure_ascii=False)
    edited_str = json.dumps(
        payload_for_diff, indent=2, sort_keys=True, ensure_ascii=False
    )

    if original_str != edited_str:
        st.subheader("🔍 변경 사항 미리보기")
        with st.expander("자세히 보기 (Diff)", expanded=True):
            diff_result = difflib.unified_diff(
                original_str.splitlines(keepends=True),
                edited_str.splitlines(keepends=True),
                fromfile="원본 (Original)",
                tofile="수정본 (Edited)",
            )
            st.code("".join(diff_result), language="diff")
    else:
        st.info("ℹ️ 현재 수정된 내용이 없습니다.")

    col_approve, col_reject = st.columns(2)
    if col_approve.button(
        "✅ 승인 및 정식 문서 DB 반영", type="primary", use_container_width=True
    ):
        edited_payload = dict(raw_json)

        def safe_json_load(json_string: str, default_value: Any) -> Any:
            try:
                return json.loads(json_string)
            except json.JSONDecodeError:
                st.warning(
                    f"JSON 파싱 오류가 있어 해당 필드는 원본 값을 유지합니다: {json_string[:100]}..."
                )
                return default_value

        try:
            edited_payload["part_number"] = part_number
            edited_payload["oem_brand"] = oem_brand
            edited_payload["schema_key"] = schema_key
            edited_payload["source_path_hint"] = source_path_hint
            edited_payload["document_type"] = document_type
            edited_payload["title"] = title
            edited_payload["summary"] = summary
            edited_payload["vehicle"] = safe_json_load(
                vehicle_json, raw_json.get("vehicle", {})
            )
            edited_payload["compatibility"] = safe_json_load(
                compatibility_json, raw_json.get("compatibility", [])
            )
            edited_payload["specifications"] = safe_json_load(
                specifications_json, raw_json.get("specifications", {})
            )
            edited_payload["extracted_facts"] = safe_json_load(
                extracted_facts_json, raw_json.get("extracted_facts", {})
            )
            edited_payload["cautions"] = safe_json_load(
                cautions_json, raw_json.get("cautions", [])
            )
        except Exception as exc:
            st.error(f"수정된 데이터 취합 중 오류 발생: {exc}")
            return

        result = approve_pending_item(
            item_id=item.get("id"),
            item=item,
            edited_payload=edited_payload,
        )
        if result["saved"]:
            st.success(result["message"])
            st.rerun()
        else:
            st.error(result["message"])

    with col_reject:
        with st.popover("🗑️ 반려 (삭제)", use_container_width=True):
            st.write("반려 사유를 입력해주세요.")
            rejection_reason = st.text_area(
                "사유", key="rejection_reason_input", label_visibility="collapsed"
            )
            col_confirm, col_cancel = st.columns(2)
            if col_confirm.button(
                "확인 (반려)", type="secondary", use_container_width=True
            ):
                if not rejection_reason.strip():
                    st.error("반려 사유를 입력해주세요.")
                else:
                    result = reject_pending_item(item.get("id"), rejection_reason)
                    if result["saved"]:
                        st.warning("반려 처리되었습니다.")
                        st.rerun()
                    else:
                        st.error(result["message"])
            if col_cancel.button("취소", use_container_width=True):
                # Just close the popover, no action needed
                st.info("반려 취소되었습니다.")
                st.rerun()


def render_translation_mode() -> None:
    st.title("🌐 원클릭 글로벌 다국어 번역")
    st.write(
        "정식 DB(`parts`)에서 `translations`가 비어 있는 항목을 찾아 영어와 베트남어로 변환합니다."
    )

    if not supabase_available():
        st.error("Supabase 환경 변수가 설정되지 않았습니다.")
        return

    prompt_text = st.text_area(
        "번역 프롬프트",
        value=prompt_value("translation_vn"),
        height=120,
    )
    batch_size = st.slider("한 번에 처리할 건수", 1, 20, 5)

    if st.button("🚀 미번역 데이터 일괄 번역 가동", type="primary"):
        parts_rows = fetch_untranslated_parts(limit=batch_size)
        if not parts_rows:
            st.info("모든 데이터가 번역되어 있거나 대상이 없습니다.")
            return

        progress = st.progress(0, text="번역 준비 중...")
        for index, row in enumerate(parts_rows, start=1):
            part_number = row.get("part_number", "Unknown")
            oem_brand = row.get("oem_brand", "") or "-"
            st.write(f"번역 중: {part_number} ({oem_brand})")
            translation_payload, translation_status = translate_record(
                record=row,
                prompt_text=prompt_text,
            )
            save_status = save_part_translation(part_number, translation_payload)
            st.session_state["last_translation_results"][
                part_number
            ] = translation_payload
            progress.progress(
                int(index / len(parts_rows) * 100),
                text=f"번역 중... ({index}/{len(parts_rows)})",
            )
            st.caption(translation_status["message"])
            st.caption(save_status["message"])
        progress.progress(100, text="번역 완료")
        st.success("번역 완료!")

    if st.session_state["last_translation_results"]:
        preview_key = st.selectbox(
            "최근 번역 결과 미리보기",
            list(st.session_state["last_translation_results"].keys()),
        )
        st.json(st.session_state["last_translation_results"][preview_key])


def render_settings_mode() -> None:
    st.title("⚙️ 프롬프트 및 우회 설정")
    tab1, tab2, tab3 = st.tabs(  # Removed tab3 from here, it's added below
        ["🧠 AI 시스템 프롬프트", "🛡️ 봇 우회 (Proxy) 설정", "📊 Supabase 테이블 현황"]
    )

    with tab1:
        prompt_key = st.selectbox(
            "관리할 프롬프트",
            [
                "path_manual",
                "path_body_manual",
                "path_detail",
                "path_connector",
                "path_vehicle_id",
                "path_wiring",
                "path_dtc",
                "path_community",
                "translation_vn",
                "crawling_ecommerce",
            ],
        )
        current_value, source = get_config_prompt(
            prompt_key, DEFAULT_PROMPTS[prompt_key]
        )
        new_value = st.text_area("프롬프트 내용", value=current_value, height=220)
        st.caption(f"현재 로드 소스: {source}")

        if st.button("💾 프롬프트 저장", type="primary"):
            result = save_config_prompt(prompt_key, new_value)
            refresh_prompts()
            if result["remote_saved"] or result["local_saved"]:
                st.success(result["message"])
            else:
                st.error(result["message"])

    with tab2:
        st.markdown("대규모 양산 시 타겟 사이트 방화벽 우회를 위한 설정입니다.")
        proxy_value, _ = get_config_prompt("proxy_url", "")
        user_agent_value, _ = get_config_prompt("custom_user_agent", "")
        confidence_threshold_value, _ = get_config_prompt("confidence_threshold", "90")

        proxy_url = st.text_input(
            "Proxy URL (형식: http://user:pass@ip:port)",
            value=proxy_value,
        )
        user_agent = st.text_area(
            "Custom User-Agent",
            value=user_agent_value
            or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            height=100,
        )
        confidence_threshold = st.number_input(
            "자동 직행 신뢰도 기준점",
            min_value=0,
            max_value=100,
            value=int(confidence_threshold_value or "90"),
            step=1,
            help="이 점수 이상이면 검수 없이 정식 DB로 자동 등록합니다.",
        )

        if st.button("🛡️ 보안 설정 저장", type="primary"):
            proxy_result = save_config_prompt("proxy_url", proxy_url)
            ua_result = save_config_prompt("custom_user_agent", user_agent)
            threshold_result = save_config_prompt(
                "confidence_threshold", str(confidence_threshold)
            )
            if (
                (proxy_result["remote_saved"] or proxy_result["local_saved"])
                and (ua_result["remote_saved"] or ua_result["local_saved"])
                and (
                    threshold_result["remote_saved"] or threshold_result["local_saved"]
                )
            ):
                st.success("프록시, User-Agent, 신뢰도 기준점이 저장되었습니다.")
            else:
                st.error("일부 설정 저장에 실패했습니다.")


def render_dead_letter_mode() -> None:
    st.title("🏥 실패 URL 병원")
    if not supabase_available():
        st.error("Supabase 환경 변수가 설정되지 않았습니다.")
        return

    st.subheader("필터 및 정렬 옵션")
    with st.expander("옵션 설정", expanded=False):
        col_filter1, col_filter2 = st.columns(2)
        error_reason_keyword = col_filter1.text_input(
            "실패 사유 키워드 검색", key="dlq_error_keyword"
        )

        # Dynamically get schema_key and source_type options from FACTORY_SCHEMA_OPTIONS
        schema_key_options = sorted(
            list(
                set(config["schema_key"] for config in FACTORY_SCHEMA_OPTIONS.values())
            )
        )
        selected_schema_keys = col_filter2.multiselect(
            "스키마 키 필터", options=schema_key_options, key="dlq_schema_filter"
        )

        col_filter3, col_filter4 = st.columns(2)
        # Common source types, can be expanded dynamically if needed
        source_type_options = sorted(
            [
                "crawl_factory",
                "dead_letter_retry",
                "dead_letter_manual_retry",
                "low_quality_data",
                "single_target_crawl_failed",
                "browser_live_capture",
                "crawl_save_failed",
                "persist_factory_rows_failed",
                "vision_capture",
                "vision_manual_upload",
                "gemini_refined",
            ]
        )
        selected_source_types = col_filter3.multiselect(
            "소스 타입 필터", options=source_type_options, key="dlq_source_type_filter"
        )

        col_date1, col_date2 = st.columns(2)
        start_date_val = col_date1.date_input(
            "시작 날짜", value=None, key="dlq_start_date"
        )
        end_date_val = col_date2.date_input("종료 날짜", value=None, key="dlq_end_date")

        col_sort1, col_sort2 = st.columns(2)
        sort_options = {
            "생성 시각": "created_at",
            "최근 변경 시각": "updated_at",
            "URL": "url",
            "실패 사유": "error_reason",
            "스키마 키": "schema_key",
            "소스 타입": "source_type",
        }
        sort_by_label = col_sort1.selectbox(
            "정렬 기준", list(sort_options.keys()), index=0, key="dlq_sort_by"
        )
        sort_order = col_sort2.radio(
            "정렬 순서",
            ["내림차순", "오름차순"],
            horizontal=True,
            index=0,
            key="dlq_sort_order",
        )

    rows = fetch_dead_letters(
        limit=500,  # Fetch more to allow in-memory filtering if needed, though now Supabase handles it
        error_reason_keyword=error_reason_keyword,
        schema_key_filter=selected_schema_keys if selected_schema_keys else None,
        source_type_filter=selected_source_types if selected_source_types else None,
        start_date=start_date_val.isoformat() if start_date_val else None,
        end_date=end_date_val.isoformat() if end_date_val else None,
        sort_by=sort_options[sort_by_label],
        sort_order="desc" if sort_order == "내림차순" else "asc",
    )
    if not rows:
        st.success("🎉 처리할 실패 URL이 없습니다.")
        return

    st.write(f"총 {len(rows)}개의 미해결 URL이 있습니다.")

    df = pd.DataFrame(rows)
    df_display = df[
        [
            "id",
            "url",
            "error_reason",
            "schema_key",
            "source_type",
            "error_details",  # New column
            "created_at",
            "updated_at",
        ]
    ].copy()
    df_display.rename(
        columns={
            "id": "ID",
            "url": "URL",
            "error_reason": "실패 사유",
            "schema_key": "스키마 키",
            "source_type": "소스 타입",
            "error_details": "상세 오류",  # New column
            "created_at": "생성 시각",
            "updated_at": "최근 변경 시각",
        },
        inplace=True,
    )

    st.subheader("미해결 실패 URL 목록")
    st.data_editor(
        df_display,
        key="dead_letters_editor",
        column_config={
            "ID": st.column_config.TextColumn(disabled=True),
            "URL": st.column_config.LinkColumn(disabled=True),
            "실패 사유": st.column_config.TextColumn(disabled=True),
            "스키마 키": st.column_config.TextColumn(disabled=True),
            "소스 타입": st.column_config.TextColumn(disabled=True),
            "상세 오류": st.column_config.JsonColumn(disabled=True),  # New column
            "생성 시각": st.column_config.DatetimeColumn(disabled=True),
            "최근 변경 시각": st.column_config.DatetimeColumn(disabled=True),
        },
        hide_index=True,
        use_container_width=True,
        num_rows="dynamic",
    )

    selected_indices = st.session_state.dead_letters_editor.get("selection", [])
    selected_item_ids = [df.loc[idx, "id"] for idx in selected_indices]

    st.write(f"선택된 항목: {len(selected_item_ids)}개")

    col_bulk_retry, col_bulk_delete = st.columns(2)

    if col_bulk_retry.button(
        "🔄 선택 항목 일괄 재처리",
        use_container_width=True,
        disabled=not selected_item_ids,
    ):
        if selected_item_ids:
            with st.spinner(f"{len(selected_item_ids)}개 항목을 재처리 중..."):
                bulk_result = bulk_retry_dead_letters(selected_item_ids)
                if bulk_result["success_count"] > 0:
                    st.success(
                        f"✅ {bulk_result['success_count']}개 항목 재처리 성공. {bulk_result['fail_count']}개 항목 실패."
                    )
                if bulk_result["fail_count"] > 0:
                    st.error(f"❌ {bulk_result['fail_count']}개 항목 재처리 실패.")
                st.rerun()
        else:
            st.warning("재처리할 항목을 선택해주세요.")

    if col_bulk_delete.button(
        "🗑️ 선택 항목 일괄 영구 삭제",
        use_container_width=True,
        disabled=not selected_item_ids,
    ):
        if selected_item_ids:
            if st.popover("정말 삭제하시겠습니까?"):
                if st.button(
                    "예, 영구 삭제합니다.", type="primary", key="confirm_bulk_delete"
                ):
                    with st.spinner(
                        f"{len(selected_item_ids)}개 항목을 영구 삭제 중..."
                    ):
                        bulk_result = bulk_delete_dead_letters(selected_item_ids)
                        if bulk_result["success_count"] > 0:
                            st.success(
                                f"✅ {bulk_result['success_count']}개 항목 영구 삭제 성공."
                            )
                        if bulk_result["fail_count"] > 0:
                            st.error(
                                f"❌ {bulk_result['fail_count']}개 항목 영구 삭제 실패."
                            )
                        st.rerun()
        else:
            st.warning("삭제할 항목을 선택해주세요.")


def render_export_mode() -> None:
    st.title("📊 통합 현황 및 백업")
    if not supabase_available():
        st.error("Supabase 환경 변수가 설정되지 않았습니다.")
        return

    col1, col2, col3 = st.columns(3)
    col1.metric("현재 상용 DB 적재량", f"{fetch_parts_count():,} 개")
    col2.metric(
        "최근 Vision 분석",
        "1건" if st.session_state.get("last_vision_result") else "0건",
    )
    col3.metric(
        "최근 번역 결과",
        f"{len(st.session_state.get('last_translation_results', {})):,} 건",
    )

    st.divider()
    st.write("전체 데이터를 엑셀(CSV) 형식으로 백업합니다.")

    if st.button("데이터 불러오기 준비"):
        rows = fetch_parts_export()
        if not rows:
            st.info("내보낼 데이터가 없습니다.")
            return

        df = pd.DataFrame(rows)
        csv_data = convert_df(df)
        st.download_button(
            label="📥 CSV 다운로드 실행",
            data=csv_data,
            file_name=f"weviko_db_export_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
            type="primary",
        )
        st.dataframe(df, use_container_width=True, hide_index=True)


def main() -> None:
    st.set_page_config(page_title="Weviko Master OS", page_icon="🌍", layout="wide")
    init_state()
    inject_styles()
    ensure_login_secure()

    mode = render_sidebar()
    render_header()
    render_status()

    if mode == "📷 수동 캡처(Vision)":
        render_vision_input_mode()
    elif mode == "🛰️ 현재 탭 라이브 캡처":
        render_live_capture_mode()
    elif mode == "🏭 대규모 양산 팩토리(URL)":
        render_factory_mode()
    elif mode == "🕵️ 데이터 검수소 (H-i-t-L)":
        render_review_mode()
    elif mode == "🌐 다국어 번역 엔진":
        render_translation_mode()
    elif mode == "⚙️ 시스템 환경 설정":
        render_settings_mode()
    elif mode == "🏥 실패 URL 병원":
        render_dead_letter_mode()
    else:
        render_export_mode()


if __name__ == "__main__":
    main()
