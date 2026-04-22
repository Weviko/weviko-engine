from __future__ import annotations

import json
import os
from datetime import datetime
from urllib.parse import urlparse

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from streamlit_services import (
    analyze_uploaded_image,
    llm_available,
    load_prompt_templates,
    persist_review_decision,
    reset_prompt_templates,
    save_prompt_template,
    supabase_available,
    translate_record,
)
from weviko_factory import run_factory


load_dotenv()

ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "weviko1234!")

MODE_SECTIONS = {
    "🛠️ Data Collection": [
        "🎯 단일 타겟 영점조준",
        "📷 수동 캡처(Vision) 분석",
        "🏭 대규모 양산 팩토리",
    ],
    "🧠 AI & Management": [
        "🕵️ 데이터 검수소 (H-i-t-L)",
        "🌐 다국어 번역 엔진",
        "⚙️ 시스템 프롬프트 튜닝",
        "🏥 실패 URL 에러 병원",
    ],
    "📈 Analytics": [
        "📊 통합 현황 및 백업(Export)",
    ],
}

DEFAULT_PROMPTS = {
    "도해도 팩트 추출 프롬프트": (
        "당신은 자동차 데이터 분석가입니다. 정비 도해도, 회로도, 체결 토크 표, "
        "서비스 매뉴얼 화면에서 수치화 가능한 팩트만 추출하고, "
        "저작권 보호를 위해 장문 원문을 재구성하지 마세요."
    ),
    "이커머스 호환성 파싱 프롬프트": (
        "자동차 부품 데이터에서 부품번호, 호환 차종, 연식, 규격, 토크, 중량, "
        "장착 조건을 추출하고 다국어 JSON으로 정리하세요."
    ),
}


@st.cache_data
def convert_df(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8-sig")


def init_state() -> None:
    if "password_correct" not in st.session_state:
        st.session_state["password_correct"] = False
    if "current_mode" not in st.session_state:
        st.session_state["current_mode"] = "🎯 단일 타겟 영점조준"
    if "last_run_result" not in st.session_state:
        st.session_state["last_run_result"] = None
    if "review_states" not in st.session_state:
        st.session_state["review_states"] = {}
    if "translation_results" not in st.session_state:
        st.session_state["translation_results"] = {}
    if "last_vision_result" not in st.session_state:
        st.session_state["last_vision_result"] = None
    if "prompt_templates" not in st.session_state:
        prompts, source = load_prompt_templates(DEFAULT_PROMPTS)
        st.session_state["prompt_templates"] = prompts
        st.session_state["prompt_source"] = source
    if "prompt_source" not in st.session_state:
        st.session_state["prompt_source"] = "defaults"


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
        .weviko-panel {
            background: var(--weviko-panel);
            border: 1px solid var(--weviko-line);
            border-radius: 22px;
            padding: 1rem 1.15rem;
            margin-bottom: 1rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def ensure_login() -> None:
    if st.session_state["password_correct"]:
        return

    st.title("🔒 Weviko Master Command Center")
    st.caption("관리자 암호를 입력해야 데이터 파이프라인 제어 화면에 접근할 수 있습니다.")
    password = st.text_input("접근 암호", type="password")
    if st.button("시스템 가동", type="primary", use_container_width=True):
        if password == ADMIN_PASSWORD:
            st.session_state["password_correct"] = True
            st.rerun()
        st.error("보안 경고: 암호가 일치하지 않습니다.")
    st.stop()


def render_hero() -> None:
    st.markdown(
        """
        <div class="weviko-hero">
            <div class="weviko-kicker">Weviko OS v3.0</div>
            <div class="weviko-title">Master Command Center</div>
            <p class="weviko-copy">
                공개 부품 정보 수집, 비전 분석, 경로 분류, 검수 승인, 번역 파이프라인, 운영 백업까지
                하나의 콘솔에서 제어합니다.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_system_status() -> None:
    llm_badge = "Gemini 연결됨" if llm_available() else "Gemini 미설정"
    db_badge = "Supabase 연결됨" if supabase_available() else "Supabase 미설정"
    prompt_badge = {
        "supabase": "프롬프트 저장: Supabase",
        "local_file": "프롬프트 저장: 로컬 파일",
        "defaults": "프롬프트 저장: 기본값",
    }.get(st.session_state.get("prompt_source", "defaults"), "프롬프트 저장: 기본값")

    st.markdown(
        f"""
        <div class="weviko-panel">
            <span class="weviko-status">🧠 {llm_badge}</span>
            <span class="weviko-status">🗄️ {db_badge}</span>
            <span class="weviko-status">💾 {prompt_badge}</span>
            <span class="weviko-status">🚀 Docker / Streamlit Web Service</span>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_sidebar() -> str:
    with st.sidebar:
        st.title("🌍 Weviko OS v3.0")
        st.caption("Global Automotive Data Pipeline")
        st.divider()

        for section, modes in MODE_SECTIONS.items():
            st.subheader(section)
            for mode in modes:
                is_active = st.session_state["current_mode"] == mode
                if st.button(
                    mode,
                    key=f"mode-{mode}",
                    type="primary" if is_active else "secondary",
                    use_container_width=True,
                ):
                    st.session_state["current_mode"] = mode
                    st.rerun()
            st.divider()

        if st.button("🔌 시스템 로그아웃", use_container_width=True):
            st.session_state["password_correct"] = False
            st.rerun()

    return st.session_state["current_mode"]


def safe_csv_list(raw_value: str) -> list[str]:
    return [item.strip() for item in raw_value.split(",") if item.strip()]


def infer_path_hint(target_url: str) -> str:
    path = urlparse(target_url).path.strip()
    if not path:
        return ""
    if path.count("/") >= 3:
        parent = path.rsplit("/", 1)[0]
        return parent or path
    return ""


def get_last_result():
    return st.session_state.get("last_run_result")


def get_last_rows() -> list[dict]:
    result = get_last_result()
    return [] if result is None else list(result.rows)


def get_last_df() -> pd.DataFrame:
    rows = get_last_rows()
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def get_content_rows() -> list[dict]:
    return [row for row in get_last_rows() if row.get("route_status") == "content_page"]


def store_result(result) -> None:
    st.session_state["last_run_result"] = result


def render_metric_cards(result) -> None:
    route_summary = result.route_status_counts
    cards = [
        ("Queued URLs", str(len(result.queued_urls)), "탐색 단계에서 큐에 적재된 주소 수"),
        ("Content Pages", str(route_summary.get("content_page", 0)), "실제 본문 추출 대상으로 처리된 페이지"),
        ("Auth Required", str(route_summary.get("auth_required", 0)), "로그인 게이트로 분류된 내부 업무 경로"),
        ("Broken Routes", str(route_summary.get("broken_public_route", 0)), "공개 경로이지만 현재 오류인 라우트"),
    ]
    columns = st.columns(4)
    for column, (label, value, copy) in zip(columns, cards):
        with column:
            st.markdown(
                f"""
                <div class="weviko-card">
                    <div class="weviko-card-label">{label}</div>
                    <div class="weviko-card-value">{value}</div>
                    <div class="weviko-card-copy">{copy}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )


def render_results_table(result) -> None:
    if not result.rows:
        st.warning("표시할 결과가 아직 없습니다.")
        return

    df = pd.DataFrame(result.rows)
    ordered_columns = [
        "url",
        "final_url",
        "status",
        "route_status",
        "route_reason",
        "http_status",
        "target_market",
        "part_number",
        "extracted_facts_count",
        "compressed_chars",
        "content_hash",
    ]
    visible_columns = [column for column in ordered_columns if column in df.columns]
    st.dataframe(df[visible_columns], use_container_width=True, hide_index=True)

    with st.expander("원본 결과 JSON 보기"):
        st.json(result.rows)


def execute_run(
    *,
    start_url: str,
    num_workers: int,
    target_market: str,
    product_path_hint: str,
    discovery_extra_path_hints: list[str],
    route_watch_hints: list[str],
    discovery_max_pages: int,
    discovery_max_matches: int,
    discovery_max_depth: int,
    max_urls: int,
) -> None:
    progress_bar = st.progress(0, text="워커 준비 중...")
    status_area = st.empty()
    log_area = st.empty()
    logs: list[str] = []
    stats = {"queued": 0, "processed": 0}

    def on_log(line: str) -> None:
        logs.append(line)
        if "[Spider] queued:" in line:
            stats["queued"] += 1
        if (
            "[Route]" in line
            or "processed successfully" in line
            or "model unavailable" in line
            or "extracted no useful text" in line
            or "failed after retries" in line
            or "[Cache Hit]" in line
        ):
            stats["processed"] += 1

        if stats["queued"] == 0:
            progress_value = 5
            status_text = "탐색 중..."
        else:
            ratio = min(stats["processed"] / max(stats["queued"], 1), 1.0)
            progress_value = 10 + int(ratio * 90)
            status_text = f"처리 중... ({stats['processed']}/{stats['queued']})"

        progress_bar.progress(progress_value, text=status_text)
        status_area.caption(line)
        log_area.code("\n".join(logs[-80:]), language="bash")

    result = run_factory(
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
        log_callback=on_log,
    )
    store_result(result)

    progress_bar.progress(100, text="완료")
    st.success(f"명령 완료: 총 {len(result.rows)}건이 처리되었습니다.")
    render_metric_cards(result)
    st.divider()
    render_results_table(result)


def render_single_target_mode() -> None:
    st.title("🎯 단일 타겟 영점조준")
    st.markdown("새로운 웹페이지 1건을 직접 겨냥해 구조와 경로 판정을 검증합니다.")

    target_url = st.text_input(
        "타겟 웹페이지 URL",
        value="https://www.weviko.com/community/parts-info/how-to-read-part-numbers",
    )
    col1, col2 = st.columns(2)
    with col1:
        target_market = st.selectbox("시장 사양", ["GLOBAL", "VN", "KR", "US"], index=0)
    with col2:
        category = st.selectbox("부품 카테고리", ["Brake", "Engine", "Suspension", "Body"])

    default_hint = infer_path_hint(target_url)
    product_path_hint = st.text_input(
        "경로 힌트",
        value=default_hint,
        help="비워두면 입력한 URL 자체를 직접 검사합니다.",
    )
    st.caption(f"선택된 카테고리: {category}")

    if st.button("🚀 단일 테스트 가동", type="primary", use_container_width=True):
        execute_run(
            start_url=target_url,
            num_workers=1,
            target_market=target_market,
            product_path_hint=product_path_hint,
            discovery_extra_path_hints=[],
            route_watch_hints=[],
            discovery_max_pages=1,
            discovery_max_matches=1,
            discovery_max_depth=0,
            max_urls=1,
        )


def render_vision_mode() -> None:
    st.title("📷 수동 캡처(Vision) 데이터 세탁소")
    st.info("GSW, 도해도, 토크 표처럼 크롤링 대신 수동 캡처가 필요한 화면을 Gemini Vision으로 구조화합니다.")

    col1, col2 = st.columns(2)
    with col1:
        part_number = st.text_input("부품 번호 (선택)")
    with col2:
        document_type = st.selectbox("문서 종류", ["정비/조립 순서", "회로도/배선", "체결 토크 표"])

    prompt_text = st.text_area(
        "Vision 분석 프롬프트",
        value=st.session_state["prompt_templates"]["도해도 팩트 추출 프롬프트"],
        height=120,
    )
    uploaded_file = st.file_uploader("스크린샷 업로드", type=["png", "jpg", "jpeg", "webp"])

    if uploaded_file is not None:
        st.image(uploaded_file, caption="업로드 미리보기", use_container_width=True)

    if st.button("✨ AI 시각 분석 시작", type="primary", use_container_width=True):
        if uploaded_file is None:
            st.warning("먼저 이미지를 업로드해 주세요.")
            return

        with st.spinner("Gemini Vision이 이미지에서 팩트 데이터를 추출하는 중입니다..."):
            result, storage = analyze_uploaded_image(
                file_bytes=uploaded_file.getvalue(),
                mime_type=uploaded_file.type or "image/png",
                part_number=part_number.strip(),
                document_type=document_type,
                prompt_text=prompt_text,
            )
        st.session_state["last_vision_result"] = result

        if result.get("analysis_mode") == "gemini":
            st.success("Vision 분석이 완료되었습니다.")
        else:
            st.warning("Gemini 설정이 없어 예시 기반 폴백 결과를 표시합니다.")
        st.caption(storage["message"])

    if st.session_state.get("last_vision_result"):
        st.subheader("Vision 결과")
        st.code(
            json.dumps(st.session_state["last_vision_result"], ensure_ascii=False, indent=2),
            language="json",
        )


def render_factory_mode() -> None:
    st.title("🏭 대규모 크롤링 양산 팩토리")
    st.warning("대량 작업은 프록시, Playwright, 외부 네트워크를 함께 사용하므로 처리 시간이 길어질 수 있습니다.")

    col1, col2 = st.columns(2)
    with col1:
        start_url = st.text_input("시작 카테고리 URL (Spider Start)", value="https://www.weviko.com/")
        target_market = st.selectbox("타겟 시장", ["GLOBAL", "VN", "KR", "US"], index=0, key="factory-market")
        product_path_hint = st.text_input("주요 경로 힌트", value="/community/parts-info")
    with col2:
        workers = st.slider("동시 워커(Worker) 수", 1, 10, 2)
        max_urls = st.number_input("최대 수집 URL 개수", min_value=1, max_value=50000, value=20)
        discovery_max_depth = st.slider("탐색 깊이", 0, 5, 3)

    with st.expander("고급 설정"):
        extra_path_hints = st.text_input("추가 콘텐츠 경로 힌트", value="")
        route_watch_hints = st.text_input("감시 경로 힌트", value="/parts,/dashboard")
        discovery_max_pages = st.slider("최대 탐색 페이지 수", 1, 50, 12)
        discovery_max_matches = st.slider("최대 매치 수", 1, 100, 20)

    if st.button("🔥 팩토리 풀가동 시작", type="primary", use_container_width=True):
        execute_run(
            start_url=start_url,
            num_workers=workers,
            target_market=target_market,
            product_path_hint=product_path_hint,
            discovery_extra_path_hints=safe_csv_list(extra_path_hints),
            route_watch_hints=safe_csv_list(route_watch_hints),
            discovery_max_pages=discovery_max_pages,
            discovery_max_matches=discovery_max_matches,
            discovery_max_depth=discovery_max_depth,
            max_urls=int(max_urls),
        )


def render_review_mode() -> None:
    st.title("🕵️ 데이터 검수소 (H-i-t-L)")
    st.markdown("AI가 수집한 결과를 사람이 마지막으로 승인하거나 보류합니다.")

    content_rows = get_content_rows()
    if not content_rows:
        st.info("아직 검수할 수집 결과가 없습니다. 먼저 단일 테스트나 대규모 팩토리를 실행해 주세요.")
        return

    by_url = {row["url"]: row for row in content_rows}
    selected_url = st.selectbox("검수 대상 URL", list(by_url.keys()))
    record = by_url[selected_url]
    current_state = st.session_state["review_states"].get(selected_url, "대기")
    notes = st.text_input("검수 메모", key=f"review-note-{selected_url}")
    edited_json = st.text_area(
        "AI 추출 결과 편집",
        value=json.dumps(record, ensure_ascii=False, indent=2),
        height=280,
        key=f"review-json-{selected_url}",
    )
    st.caption(f"현재 상태: {current_state}")

    col1, col2, col3 = st.columns(3)
    approve_clicked = col1.button("✅ 승인 (DB 이관)", type="primary", use_container_width=True)
    edit_approve_clicked = col2.button("✏️ 수정 후 승인", use_container_width=True)
    discard_clicked = col3.button("🗑️ 영구 폐기", type="secondary", use_container_width=True)

    action = None
    if approve_clicked:
        action = "approved"
    elif edit_approve_clicked:
        action = "edited_approved"
    elif discard_clicked:
        action = "discarded"

    if action is not None:
        try:
            reviewed_record = json.loads(edited_json)
            if not isinstance(reviewed_record, dict):
                raise ValueError("JSON 객체 형태여야 합니다.")
        except Exception as exc:
            st.error(f"편집된 JSON을 읽을 수 없습니다: {exc}")
            return

        persistence = persist_review_decision(
            original_record=record,
            reviewed_record=reviewed_record,
            decision=action,
            notes=notes,
        )
        label_map = {
            "approved": "승인됨",
            "edited_approved": "수정 승인",
            "discarded": "폐기됨",
        }
        st.session_state["review_states"][selected_url] = label_map[action]
        st.success(f"검수 상태가 `{label_map[action]}`으로 업데이트되었습니다.")
        st.caption(persistence["review_message"])
        st.caption(persistence["parts_message"])


def render_translation_mode() -> None:
    st.title("🌐 다국어 번역 엔진")
    st.info("수집된 구조화 데이터를 한국어, 영어, 베트남어 JSON으로 번역하고 선택적으로 Supabase에 저장합니다.")

    content_rows = get_content_rows()
    if not content_rows:
        st.info("번역할 수집 결과가 없습니다. 먼저 크롤링 결과를 만들어 주세요.")
        return

    prompt_text = st.text_area(
        "번역 프롬프트",
        value=st.session_state["prompt_templates"]["이커머스 호환성 파싱 프롬프트"],
        height=120,
    )
    mode = st.radio("번역 범위", ["단일 항목", "여러 항목"], horizontal=True)

    urls = [row["url"] for row in content_rows]
    if mode == "단일 항목":
        selected_urls = [st.selectbox("번역 대상 URL", urls)]
    else:
        selected_urls = st.multiselect("번역 대상 URL", urls, default=urls[: min(3, len(urls))])

    if st.button("🌍 번역 실행", type="primary", use_container_width=True):
        if not selected_urls:
            st.warning("번역할 항목을 하나 이상 선택해 주세요.")
            return

        lookup = {row["url"]: row for row in content_rows}
        progress = st.progress(0, text="번역 준비 중...")
        completed = 0
        for url in selected_urls:
            record = lookup[url]
            translation, storage = translate_record(record=record, prompt_text=prompt_text)
            st.session_state["translation_results"][url] = translation
            completed += 1
            progress.progress(int(completed / len(selected_urls) * 100), text=f"번역 중... ({completed}/{len(selected_urls)})")
            st.caption(f"{url} -> {storage['message']}")
        progress.progress(100, text="번역 완료")
        st.success(f"{len(selected_urls)}건 번역을 완료했습니다.")

    if st.session_state["translation_results"]:
        preview_url = st.selectbox(
            "번역 결과 미리보기",
            list(st.session_state["translation_results"].keys()),
            key="translation-preview",
        )
        st.json(st.session_state["translation_results"][preview_url])


def render_prompt_mode() -> None:
    st.title("⚙️ 시스템 프롬프트 튜닝")
    prompt_name = st.selectbox("수정할 프롬프트", list(st.session_state["prompt_templates"].keys()))
    new_text = st.text_area(
        "시스템 지시문",
        value=st.session_state["prompt_templates"][prompt_name],
        height=240,
    )

    col1, col2 = st.columns(2)
    if col1.button("💾 프롬프트 업데이트", type="primary", use_container_width=True):
        result = save_prompt_template(prompt_name, new_text)
        st.session_state["prompt_templates"][prompt_name] = new_text
        st.session_state["prompt_source"] = result["source"]
        st.success(result["message"])

    if col2.button("↺ 기본 프롬프트 복원", use_container_width=True):
        result = reset_prompt_templates(DEFAULT_PROMPTS)
        st.session_state["prompt_templates"] = dict(DEFAULT_PROMPTS)
        st.session_state["prompt_source"] = result["source"]
        st.success(result["message"])

    st.caption(f"현재 저장 소스: {st.session_state.get('prompt_source', 'defaults')}")


def render_error_hospital() -> None:
    st.title("🏥 실패 URL 에러 병원")
    st.error("공개 오류, 로그인 필요, 타임아웃 등 문제가 생긴 대상을 재점검하는 공간입니다.")

    df = get_last_df()
    if not df.empty and "route_status" in df.columns:
        error_df = df[df["route_status"] != "content_page"].copy()
    else:
        error_df = pd.DataFrame(
            {
                "url": ["/parts/hyundai/engine-x", "/dashboard/garage/parts/inquiries/new"],
                "route_status": ["broken_public_route", "auth_required"],
                "route_reason": ["http_status_500", "redirected_to_login"],
                "observed_at": [datetime.now().strftime("%Y-%m-%d")] * 2,
            }
        )

    if error_df.empty:
        st.success("현재 실패한 URL이 없습니다.")
        return

    st.dataframe(error_df, use_container_width=True, hide_index=True)
    st.button("🔄 프리미엄 프록시를 사용하여 재시도", type="primary", use_container_width=True)


def render_analytics_mode() -> None:
    st.title("📊 통합 현황 및 백업(Export)")

    df = get_last_df()
    total_rows = len(df)
    content_rows = int((df["route_status"] == "content_page").sum()) if "route_status" in df.columns else 0
    broken_rows = int((df["route_status"] == "broken_public_route").sum()) if "route_status" in df.columns else 0
    auth_rows = int((df["route_status"] == "auth_required").sum()) if "route_status" in df.columns else 0
    translated_rows = len(st.session_state.get("translation_results", {}))
    reviewed_rows = len(st.session_state.get("review_states", {}))
    vision_runs = 1 if st.session_state.get("last_vision_result") else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("총 수집 결과", f"{total_rows:,}")
    c2.metric("정상 콘텐츠 페이지", f"{content_rows:,}")
    c3.metric("깨진 공개 경로", f"{broken_rows:,}")
    c4.metric("로그인 필요 경로", f"{auth_rows:,}")

    c5, c6, c7 = st.columns(3)
    c5.metric("번역 완료 항목", f"{translated_rows:,}")
    c6.metric("검수 처리 항목", f"{reviewed_rows:,}")
    c7.metric("Vision 분석 실행", f"{vision_runs:,}")

    st.divider()
    st.subheader("💾 원클릭 데이터 추출 (CSV)")
    if df.empty:
        st.info("내보낼 수집 결과가 없습니다. 먼저 수집 작업을 실행해 주세요.")
        return

    csv_data = convert_df(df)
    st.download_button(
        label="📥 현재 결과 CSV 다운로드",
        data=csv_data,
        file_name=f"weviko_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
        type="primary",
    )
    st.dataframe(df, use_container_width=True, hide_index=True)


def main() -> None:
    st.set_page_config(page_title="Weviko Master Command Center", page_icon="🌍", layout="wide")
    init_state()
    inject_styles()
    ensure_login()

    current_mode = render_sidebar()
    render_hero()
    render_system_status()

    if current_mode == "🎯 단일 타겟 영점조준":
        render_single_target_mode()
    elif current_mode == "📷 수동 캡처(Vision) 분석":
        render_vision_mode()
    elif current_mode == "🏭 대규모 양산 팩토리":
        render_factory_mode()
    elif current_mode == "🕵️ 데이터 검수소 (H-i-t-L)":
        render_review_mode()
    elif current_mode == "🌐 다국어 번역 엔진":
        render_translation_mode()
    elif current_mode == "⚙️ 시스템 프롬프트 튜닝":
        render_prompt_mode()
    elif current_mode == "🏥 실패 URL 에러 병원":
        render_error_hospital()
    else:
        render_analytics_mode()


if __name__ == "__main__":
    main()
