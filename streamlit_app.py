from __future__ import annotations

import json
import os
from datetime import datetime

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from streamlit_services import (
    analyze_uploaded_image,
    approve_pending_item,
    enqueue_pending_vision_result,
    fetch_dead_letters,
    fetch_parts_count,
    fetch_parts_export,
    fetch_pending_items,
    fetch_untranslated_parts,
    get_config_prompt,
    llm_available,
    load_config_prompts,
    reject_pending_item,
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
    "⚡ 회로도/배선도 (/contents/etc/)": "path_wiring",
    "🗣️ 포럼/실전 팁 (/community/)": "path_community",
    "⚠️ 고장 코드(DTC) (/dtc/)": "path_dtc",
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
    "path_detail": (
        "부품 제원/호환성 페이지입니다. 부품번호, 규격, OEM 정보, 적용 차종, 연식, 호환 조건을 우선 추출하세요."
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
}

MODES = [
    "📷 수동 캡처(Vision) 입력",
    "🕵️ 데이터 검수 및 DB 이관",
    "🌐 다국어 번역 엔진",
    "⚙️ 시스템 프롬프트 관리",
    "🏥 실패 URL 관리 (DLQ)",
    "📊 통합 DB 추출 (CSV)",
]


@st.cache_data
def convert_df(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8-sig")


def init_state() -> None:
    if "logged_in" not in st.session_state:
        st.session_state["logged_in"] = False
    if "prompt_values" not in st.session_state:
        prompt_values, source = load_config_prompts(DEFAULT_PROMPTS)
        st.session_state["prompt_values"] = prompt_values
        st.session_state["prompt_source"] = source
    if "prompt_source" not in st.session_state:
        st.session_state["prompt_source"] = "defaults"
    if "last_vision_result" not in st.session_state:
        st.session_state["last_vision_result"] = None
    if "last_translation_results" not in st.session_state:
        st.session_state["last_translation_results"] = {}


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

    st.title("🔒 Weviko OS v4.0 Login")
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
            <div class="weviko-kicker">Weviko OS v4.0</div>
            <div class="weviko-title">Master Command Center</div>
            <p class="weviko-copy">
                수동 캡처, 검수 대기열, 다국어 번역, 프롬프트 운영, 실패 URL 관리, CSV 백업까지
                Supabase 중심 운영 흐름으로 통합했습니다.
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
        st.title("🌍 Weviko OS v4.0")
        mode = st.radio("작업 모드", MODES)
        st.divider()
        if st.button("🔌 로그아웃", use_container_width=True):
            st.session_state["logged_in"] = False
            st.rerun()
    return mode


def render_vision_input_mode() -> None:
    st.title("📷 수동 캡처 (GSW 우회 입력)")
    st.info("이미지를 업로드하면 AI가 분석하고 OEM 브랜드 정보와 함께 `pending_data` 검수 대기열로 전송합니다.")

    col1, col2, col3, col4 = st.columns(4)
    part_number = col1.text_input("부품 번호 (필수)")
    oem_brand = col2.text_input("OEM 브랜드", placeholder="HYUNDAI / KIA / MOBIS")
    document_type = col3.selectbox("문서 종류", ["정비 지침", "도해도/스펙", "회로도"])
    market = col4.selectbox("시장", ["GLOBAL", "VN", "KR", "US"])

    selected_label = st.selectbox(
        "수집 경로 및 종류 선택",
        options=list(WEVIKO_PATH_MAP.keys()) + ["직접 입력..."],
        help="수집하려는 정보의 종류에 맞는 경로를 선택하세요. AI 스키마가 자동으로 매핑됩니다.",
    )
    direct_path = ""
    if selected_label == "직접 입력...":
        direct_path = st.text_input("새로운 경로 패턴 입력", placeholder="/new/path/")
    schema_key, path_hint = resolve_path_selection(selected_label, direct_path)
    st.caption(f"📍 현재 활성화된 AI 스키마: `{schema_key}` | 탐색 경로: `{path_hint}`")

    prompt_text = st.text_area(
        "Vision 프롬프트",
        value=prompt_value(schema_key),
        height=120,
    )
    uploaded_file = st.file_uploader("스크린샷 업로드", type=["png", "jpg", "jpeg", "webp"])

    if uploaded_file is not None:
        st.image(uploaded_file, caption="업로드 미리보기", use_container_width=True)

    if uploaded_file and st.button("🚀 AI 분석 및 대기열 전송", type="primary"):
        if not part_number.strip():
            st.error("부품 번호는 필수입니다.")
            return

        with st.spinner("AI가 이미지를 분석하고 대기열에 저장하는 중입니다..."):
            analysis_result, analysis_storage = analyze_uploaded_image(
                file_bytes=uploaded_file.getvalue(),
                mime_type=uploaded_file.type or "image/png",
                part_number=part_number.strip(),
                oem_brand=oem_brand.strip(),
                schema_key=schema_key,
                source_path_hint=path_hint,
                document_type=document_type,
                prompt_text=f"{prompt_text}\n\nSelected schema key: {schema_key}\nSelected path hint: {path_hint}",
            )
            queue_result = enqueue_pending_vision_result(
                part_number=part_number.strip(),
                oem_brand=oem_brand.strip(),
                schema_key=schema_key,
                source_path_hint=path_hint,
                market=market,
                document_type=document_type,
                analysis_payload=analysis_result,
            )

        st.session_state["last_vision_result"] = analysis_result
        if queue_result["saved"]:
            st.success("분석 완료! 데이터가 `pending_data` 검수 대기열로 이동했습니다.")
        else:
            st.warning("분석은 완료됐지만 대기열 저장은 되지 않았습니다.")
        st.caption(analysis_storage["message"])
        st.caption(queue_result["message"])

    if st.session_state.get("last_vision_result") is not None:
        st.subheader("최근 Vision 분석 결과")
        st.json(st.session_state["last_vision_result"])


def render_review_mode() -> None:
    st.title("🕵️ 최종 데이터 검수소")
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

    st.write(f"### 부품 번호: `{item.get('part_number', 'Unknown')}`")
    st.caption(
        f"OEM 브랜드: {item.get('oem_brand', '-') or '-'} | "
        f"시장: {item.get('market', 'GLOBAL')} | "
        f"문서 종류: {item.get('document_type', '-')}"
    )

    raw_json = item.get("raw_json", {})
    edited_json = st.text_area(
        "AI 추출 원본 (수정 가능)",
        value=json.dumps(raw_json, indent=2, ensure_ascii=False),
        height=320,
    )

    col1, col2 = st.columns(2)
    if col1.button("✅ 승인 및 정식 DB 이관", type="primary", use_container_width=True):
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
            st.success("정식 DB로 이관되었습니다.")
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


def render_prompt_mode() -> None:
    st.title("⚙️ AI 프롬프트 중앙 관리")
    prompt_key = st.selectbox(
        "관리할 프롬프트",
        [
            "crawling_ecommerce",
            "vision_gsw",
            "translation_vn",
            "path_manual",
            "path_detail",
            "path_wiring",
            "path_community",
            "path_dtc",
        ],
    )
    current_value, source = get_config_prompt(prompt_key, DEFAULT_PROMPTS[prompt_key])
    new_value = st.text_area("프롬프트 내용", value=current_value, height=220)
    st.caption(f"현재 로드 소스: {source}")

    if st.button("💾 DB에 프롬프트 저장", type="primary"):
        result = save_config_prompt(prompt_key, new_value)
        refresh_prompts()
        if result["remote_saved"] or result["local_saved"]:
            st.success(result["message"])
        else:
            st.error(result["message"])


def render_dead_letter_mode() -> None:
    st.title("🏥 데드 레터 큐 (Dead Letter Queue)")
    if not supabase_available():
        st.error("Supabase 환경 변수가 설정되지 않았습니다.")
        return

    rows = fetch_dead_letters(limit=200)
    df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["url", "error_reason", "created_at"])
    st.dataframe(df, use_container_width=True, hide_index=True)
    if st.button("🔄 미해결 URL 전체 재수집 큐에 넣기"):
        st.info("재수집 워커는 아직 연결되지 않았습니다. 다음 단계에서 백그라운드 큐와 붙일 수 있습니다.")


def render_export_mode() -> None:
    st.title("📊 Weviko 글로벌 데이터베이스 백업")
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
    st.set_page_config(page_title="Weviko Master Command Center", page_icon="🌍", layout="wide")
    init_state()
    inject_styles()
    ensure_login()

    mode = render_sidebar()
    render_header()
    render_status()

    if mode == "📷 수동 캡처(Vision) 입력":
        render_vision_input_mode()
    elif mode == "🕵️ 데이터 검수 및 DB 이관":
        render_review_mode()
    elif mode == "🌐 다국어 번역 엔진":
        render_translation_mode()
    elif mode == "⚙️ 시스템 프롬프트 관리":
        render_prompt_mode()
    elif mode == "🏥 실패 URL 관리 (DLQ)":
        render_dead_letter_mode()
    else:
        render_export_mode()


if __name__ == "__main__":
    main()
