from __future__ import annotations

import json
import os
from datetime import datetime

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from weviko_engine import run_crawler_sync
from weviko_factory import run_factory

from streamlit_services import (
    approve_pending_item,
    assess_analysis_quality,
    fetch_dead_letters,
    fetch_parts_count,
    fetch_parts_export,
    fetch_pending_items,
    fetch_untranslated_parts,
    get_config_prompt,
    llm_available,
    load_config_prompts,
    persist_factory_rows,
    process_scraped_text_and_save,
    process_vision_and_save,
    reject_pending_item,
    refine_vision_result_and_save,
    save_config_prompt,
    save_part_translation,
    supabase_available,
    translate_record,
)


load_dotenv()

ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "weviko1234!")

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
    "와이어링 커넥터": {"schema_key": "path_connector", "path_hint": "/wiring/connector/"},
    "부품 제원/도해도": {"schema_key": "path_detail", "path_hint": "/item/detail/"},
    "차량 식별/VIN/페인트 코드": {"schema_key": "path_vehicle_id", "path_hint": "/vehicle-id/"},
    "고장 코드 DTC": {"schema_key": "path_dtc", "path_hint": "/dtc/"},
}

FACTORY_SCHEMA_OPTIONS = {
    "정비 지침서 (/shop/manual/)": {"schema_key": "path_manual", "path_hint": "/shop/manual/"},
    "부품 제원/호환성 (/item/detail/)": {"schema_key": "path_detail", "path_hint": "/item/detail/"},
    "차체매뉴얼 (/body/manual/)": {"schema_key": "path_body_manual", "path_hint": "/body/manual/"},
    "와이어링 커넥터 (/wiring/connector/)": {"schema_key": "path_connector", "path_hint": "/wiring/connector/"},
    "포럼/실전 팁 (/community/)": {"schema_key": "path_community", "path_hint": "/community/"},
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
        "vehicle": {"brand": "Hyundai", "model": "EQ900", "year": 2019, "engine": "G 3.3 T-GDI"},
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
            {"item": "transmission_number", "label": "자동변속기번호", "location_description": ""},
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
        "procedure_steps": [
            {"step": 1, "action": "", "note": "", "torque": ""}
        ],
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
        "pin_map": [
            {"pin": "", "signal": "", "wire_color": "", "description": ""}
        ],
        "cautions": [],
    },
}

PIPELINE_MODES = [
    "📷 수동 캡처(Vision)",
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


def render_sidebar() -> str:
    with st.sidebar:
        st.title("🌍 Weviko OS v5.0")
        st.caption("Global Automotive Data Pipeline")
        section = st.radio("작업 영역", ["데이터 수집", "데이터 관리/제어"])
        st.session_state["active_section"] = section
        st.divider()
        if section == "데이터 수집":
            mode = st.radio("데이터 수집 파이프라인", PIPELINE_MODES)
        else:
            mode = st.radio("데이터 관리 및 제어", MANAGEMENT_MODES)
        st.divider()
        if st.button("🔌 로그아웃", use_container_width=True):
            st.session_state["logged_in"] = False
            st.rerun()
    return mode


def render_vision_input_mode() -> None:
    st.title("📷 실전 수동 캡처 및 AI 분석")
    st.info("💡 부품 번호를 몰라도 괜찮습니다. 아는 정보만 입력하면 AI가 문맥을 파악해 정리합니다.")

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

    uploaded_file = st.file_uploader("문서/스크린샷 업로드 (최대 200MB)", type=["png", "jpg", "jpeg", "pdf"])

    if uploaded_file is not None:
        if (uploaded_file.type or "").startswith("image/"):
            st.image(uploaded_file, caption="업로드 미리보기", use_container_width=True)
        else:
            st.caption(f"업로드 파일 타입: `{uploaded_file.type or 'unknown'}`")

    if uploaded_file and st.button("🚀 캡처 데이터 AI 분석 가동", type="primary", use_container_width=True):
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
            template_block = (
                "\n\n[권장 JSON 템플릿]\n"
                + json.dumps(template_hint, ensure_ascii=False, indent=2)
            )

        with st.spinner(f"'{operator_identifier}' 문서를 해독 중입니다. 잠시만 기다려주세요..."):
            analysis_result, queue_result = process_vision_and_save(
                file_bytes=uploaded_file.getvalue(),
                file_type=uploaded_file.type,
                part_num=part_clean,
                doc_type_key=schema_key,
                market=market,
                source_path_hint=path_hint,
                document_type=selected_type,
                prompt_override=f"{prompt_value(schema_key)}{template_block}\n\n" + "\n".join(context_lines),
                vehicle_hint=vehicle_clean,
                system_hint=system_clean,
                operator_identifier=operator_identifier,
            )

        analysis_result = assess_analysis_quality(analysis_result)
        st.session_state["last_vision_result"] = analysis_result
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
        if analysis_mode == "gemini" and queue_result["saved"] and quality_status == "ok":
            st.success("✅ 데이터 추출 성공! '검수 대기열'로 이동했습니다.")
        elif queue_result["saved"] and quality_status == "low":
            st.warning("⚠️ 저장은 완료됐지만, 이번 결과는 구조화 정확도가 낮아서 재정리가 필요할 수 있습니다.")
        elif queue_result["saved"]:
            st.warning("⚠️ 대기열 저장은 완료됐지만, Gemini 응답에 문제가 있어 오류 대체 JSON으로 저장됐습니다.")
        else:
            st.warning("분석은 완료됐지만 검수 대기열 저장은 되지 않았습니다.")

        st.caption(queue_result["message"])

        with st.expander("AI 추출 결과 확인 (JSON)", expanded=True):
            st.json(analysis_result)

    if st.session_state.get("last_vision_result") is not None:
        last_result = assess_analysis_quality(dict(st.session_state["last_vision_result"]))
        st.session_state["last_vision_result"] = last_result
        st.subheader("최근 Vision 분석 결과")
        st.json(last_result)

        if last_result.get("quality_status") == "low":
            reasons = ", ".join(last_result.get("quality_reasons", [])) or "low_quality"
            st.warning(
                "현재 결과는 구조화 정확도가 낮은 편입니다. "
                f"감지 사유: `{reasons}`"
            )

            raw_hint_text = str(last_result.get("raw_response", "")).lower()
            if any(keyword in raw_hint_text for keyword in ["vin", "차대번호", "paint code", "페인트 코드"]):
                st.info("이 문서는 정비 지침서보다 `차량 식별/VIN/페인트 코드` 스키마에 더 가까워 보입니다. 해당 스키마로 다시 분석하면 품질이 좋아질 가능성이 큽니다.")

            context = st.session_state.get("last_vision_context") or {}
            if st.button("🧪 원문 설명을 구조화 JSON으로 재정리", use_container_width=True):
                with st.spinner("원문 설명을 구조화 JSON으로 다시 정리하고 있습니다..."):
                    refined_result, refine_save_result = refine_vision_result_and_save(
                        last_result,
                        schema_key=context.get("schema_key", last_result.get("schema_key", "path_manual")),
                        market=context.get("market", last_result.get("market", "GLOBAL")),
                        source_path_hint=context.get("path_hint", last_result.get("source_path_hint", "")),
                        document_type=context.get("document_type", last_result.get("document_type", "")),
                        part_number_hint=context.get("part_number_hint", last_result.get("part_number", "")),
                        vehicle_hint=context.get("vehicle_hint", ""),
                        system_hint=context.get("system_hint", ""),
                        operator_identifier=context.get("operator_identifier", ""),
                        oem_brand=last_result.get("oem_brand", ""),
                    )
                st.session_state["last_vision_result"] = refined_result
                if refine_save_result.get("saved"):
                    st.success("✅ 재정리된 JSON을 새 검수 대기 항목으로 저장했습니다.")
                else:
                    st.warning(refine_save_result.get("message", "재정리 결과 저장에 실패했습니다."))
                st.rerun()


def render_factory_mode() -> None:
    st.title("🏭 대규모 크롤링 양산 팩토리")
    st.markdown("특정 카테고리 URL을 입력하면, 하위 페이지를 찾아 병렬 수집한 뒤 원하는 목적지로 보냅니다.")

    start_url = st.text_input(
        "시작 카테고리 URL",
        placeholder="예: https://www.weviko.com/community/parts-info",
    )

    execution_mode = st.radio(
        "실행 모드",
        [
            "단일 타겟 엔진",
            "하위 URL 병렬 팩토리",
        ],
        horizontal=True,
    )

    col1, col2 = st.columns(2)
    selected_schema_label = col1.selectbox("수집 타겟 및 스키마", list(FACTORY_SCHEMA_OPTIONS.keys()))
    worker_count = col2.slider(
        "동시 워커(Worker) 수",
        1,
        10,
        3,
        help="숫자가 높을수록 빠르지만 차단 위험이 커집니다.",
    )

    col3, col4 = st.columns(2)
    max_urls = col3.number_input("최대 탐색 URL 수", min_value=10, max_value=5000, value=100, step=10)
    max_depth = col4.slider("탐색 깊이", 1, 5, 2)

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
        if not start_url.strip():
            st.error("시작 URL을 입력해주세요.")
            return

        destination = "pending" if destination_label.startswith("1") else "parts"

        if execution_mode == "단일 타겟 엔진":
            if not llm_available() or not supabase_available():
                st.error("환경 변수(Supabase/Google API)가 설정되지 않았습니다.")
                return

            with st.spinner(f"'{start_url.strip()}' 타겟 수집 및 AI 정제 중..."):
                scraped_text = run_crawler_sync(
                    start_url.strip(),
                    proxy=proxy_value.strip() or None,
                    user_agent=user_agent_value.strip() or None,
                )

            if not scraped_text:
                st.error("❌ 크롤링 실패 (방화벽 차단 또는 타임아웃). 실패 URL 병원을 확인하세요.")
                return

            payload, save_result = process_scraped_text_and_save(
                scraped_text=scraped_text,
                doc_type_key=schema_key,
                market="GLOBAL",
                destination=destination,
                source_path_hint=path_hint,
                document_type=selected_schema_label,
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
            st.caption(save_result.get("message", ""))
            st.json(payload)
            return

        progress_box = st.empty()
        log_box = st.empty()
        logs: list[str] = []

        def handle_log(line: str) -> None:
            logs.append(line)
            log_box.code("\n".join(logs[-30:]), language="bash")

        previous_proxy = os.getenv("PLAYWRIGHT_PROXY_SERVER")
        destination = "pending" if destination_label.startswith("1") else "parts"

        if proxy_value.strip():
            os.environ["PLAYWRIGHT_PROXY_SERVER"] = proxy_value.strip()

        try:
            progress_box.info("스파이더가 하위 URL을 수집하고 있습니다...")
            with st.spinner("스파이더가 URL을 수집하고 AI 정제를 진행 중입니다..."):
                run_result = run_factory(
                    start_url=start_url.strip(),
                    num_workers=worker_count,
                    target_market="GLOBAL",
                    product_path_hint=path_hint,
                    discovery_extra_path_hints=[path_hint],
                    max_urls=int(max_urls),
                    discovery_max_depth=max_depth,
                    user_agent=user_agent_value.strip() or None,
                    write_destination="none",
                    schema_key=schema_key,
                    source_type="crawl_factory",
                    log_callback=handle_log,
                )
        finally:
            if previous_proxy is None:
                os.environ.pop("PLAYWRIGHT_PROXY_SERVER", None)
            else:
                os.environ["PLAYWRIGHT_PROXY_SERVER"] = previous_proxy

        progress_box.info("수집 결과를 목적지에 맞게 정리하고 있습니다...")
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
        }

        progress_box.empty()
        destination_name = "검수 대기열(Pending)" if destination == "pending" else "정식 DB(Parts)"
        st.success(
            f"✅ 총 {len(run_result.queued_urls):,}개의 URL 탐색 완료. "
            f"결과 {persist_result['saved_count']:,}건을 {destination_name}에 반영했습니다."
        )
        st.caption(persist_result["message"])

        if run_result.route_status_counts:
            status_df = pd.DataFrame(
                [
                    {"route_status": key, "count": value}
                    for key, value in sorted(run_result.route_status_counts.items())
                ]
            )
            st.dataframe(status_df, use_container_width=True, hide_index=True)

        if run_result.rows:
            st.dataframe(pd.DataFrame(run_result.rows), use_container_width=True, hide_index=True)


def render_review_mode() -> None:
    st.title("🕵️ 데이터 검수소 (H-i-t-L)")
    st.markdown("수동 캡처나 자동 파이프라인에서 수집한 `pending_data` 대기열을 승인하거나 반려합니다.")

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
    item = item_lookup[selected_label]

    raw_json = item.get("raw_json", {})
    display_title = raw_json.get("title") or raw_json.get("summary") or item.get("part_number", "Unknown")
    st.write(f"### 문서/식별자: `{display_title}`")
    st.caption(
        f"OEM 브랜드: {item.get('oem_brand', '-') or '-'} | "
        f"시장: {item.get('market', 'GLOBAL')} | "
        f"문서 종류: {item.get('document_type', '-')}"
    )
    edited_json = st.text_area(
        "AI 추출 원본 (수정 가능)",
        value=json.dumps(raw_json, indent=2, ensure_ascii=False),
        height=320,
    )

    col1, col2 = st.columns(2)
    if col1.button("✅ 승인 및 정식 문서 DB 반영", type="primary", use_container_width=True):
        try:
            edited_payload = json.loads(edited_json)
            if not isinstance(edited_payload, dict):
                raise ValueError("JSON 객체 형태여야 합니다.")
        except Exception as exc:
            st.error(f"JSON 파싱 실패: {exc}")
            return

        result = approve_pending_item(
            item_id=item.get("id"),
            item=item,
            edited_payload=edited_payload,
        )
        if result["saved"]:
            st.success(result["message"])
            st.rerun()
        st.error(result["message"])

    if col2.button("🗑️ 반려 (삭제)", use_container_width=True):
        result = reject_pending_item(item.get("id"))
        if result["saved"]:
            st.warning("반려 처리되었습니다.")
            st.rerun()
        st.error(result["message"])


def render_translation_mode() -> None:
    st.title("🌐 원클릭 글로벌 다국어 번역")
    st.write("정식 DB(`parts`)에서 `translations`가 비어 있는 항목을 찾아 영어와 베트남어로 변환합니다.")

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
            st.session_state["last_translation_results"][part_number] = translation_payload
            progress.progress(int(index / len(parts_rows) * 100), text=f"번역 중... ({index}/{len(parts_rows)})")
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
    tab1, tab2 = st.tabs(["🧠 AI 시스템 프롬프트", "🛡️ 봇 우회 (Proxy) 설정"])

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
        current_value, source = get_config_prompt(prompt_key, DEFAULT_PROMPTS[prompt_key])
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
            value=user_agent_value or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
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
            threshold_result = save_config_prompt("confidence_threshold", str(confidence_threshold))
            if (
                (proxy_result["remote_saved"] or proxy_result["local_saved"])
                and (ua_result["remote_saved"] or ua_result["local_saved"])
                and (threshold_result["remote_saved"] or threshold_result["local_saved"])
            ):
                st.success("프록시, User-Agent, 신뢰도 기준점이 저장되었습니다.")
            else:
                st.error("일부 설정 저장에 실패했습니다.")


def render_dead_letter_mode() -> None:
    st.title("🏥 실패 URL 병원")
    if not supabase_available():
        st.error("Supabase 환경 변수가 설정되지 않았습니다.")
        return

    rows = fetch_dead_letters(limit=200)
    df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["url", "error_reason", "created_at"])
    st.dataframe(df, use_container_width=True, hide_index=True)
    if st.button("🔄 미해결 URL 전체 재수집 큐에 넣기"):
        st.info("재수집 워커는 아직 연결되지 않았습니다. 다음 단계에서 백그라운드 큐와 붙일 수 있습니다.")


def render_export_mode() -> None:
    st.title("📊 통합 현황 및 백업")
    if not supabase_available():
        st.error("Supabase 환경 변수가 설정되지 않았습니다.")
        return

    col1, col2, col3 = st.columns(3)
    col1.metric("현재 상용 DB 적재량", f"{fetch_parts_count():,} 개")
    col2.metric("최근 Vision 분석", "1건" if st.session_state.get("last_vision_result") else "0건")
    col3.metric("최근 번역 결과", f"{len(st.session_state.get('last_translation_results', {})):,} 건")

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
    ensure_login()

    mode = render_sidebar()
    render_header()
    render_status()

    if mode == "📷 수동 캡처(Vision)":
        render_vision_input_mode()
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
