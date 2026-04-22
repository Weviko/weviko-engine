from __future__ import annotations

import json
import os
from datetime import datetime
from urllib.parse import urlparse

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from weviko_factory import run_factory


load_dotenv()

ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "weviko1234!")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "").strip() or os.getenv("GEMINI_API_KEY", "").strip()
SUPABASE_URL = os.getenv("NEXT_PUBLIC_SUPABASE_URL", "").strip()
SUPABASE_KEY = (
    os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    or os.getenv("SUPABASE_SECRET_KEY", "").strip()
)

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
        "당신은 자동차 데이터 분석가입니다. 스크린샷과 정비 도해도에서 수치화 가능한 팩트만 추출하고, "
        "저작권 보호를 위해 원문 장문을 그대로 복원하지 마세요."
    ),
    "이커머스 호환성 파싱 프롬프트": (
        "자동차 부품 페이지에서 부품번호, 차종, 연식, 호환 조건, 수치 스펙을 추출해 JSON으로 정리하세요."
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
    if "prompt_templates" not in st.session_state:
        st.session_state["prompt_templates"] = dict(DEFAULT_PROMPTS)


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
    llm_badge = "Gemini 연결됨" if GOOGLE_API_KEY else "Gemini 미설정"
    db_badge = "Supabase 연결됨" if SUPABASE_URL and SUPABASE_KEY else "Supabase 미설정"
    render_mode = "Docker / Streamlit Web Service"

    st.markdown(
        f"""
        <div class="weviko-panel">
            <span class="weviko-status">🧠 {llm_badge}</span>
            <span class="weviko-status">🗄️ {db_badge}</span>
            <span class="weviko-status">🚀 {render_mode}</span>
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
    st.info("GSW, 도해도, 토크 표처럼 크롤링 대신 수동 캡처가 필요한 화면을 후처리하는 공간입니다.")

    col1, col2 = st.columns(2)
    with col1:
        part_number = st.text_input("부품 번호 (선택)")
    with col2:
        doc_type = st.selectbox("문서 종류", ["정비/조립 순서", "회로도/배선", "체결 토크 표"])

    uploaded_file = st.file_uploader("스크린샷 업로드", type=["png", "jpg", "jpeg", "webp"])
    if uploaded_file is not None:
        st.image(uploaded_file, caption="업로드 미리보기", use_container_width=True)

    if st.button("✨ AI 시각 분석 시작", type="primary", use_container_width=True):
        if uploaded_file is None:
            st.warning("먼저 이미지를 업로드해 주세요.")
            return
        if not GOOGLE_API_KEY:
            st.warning("현재 GOOGLE_API_KEY가 없어 실제 Gemini Vision 호출 대신 예시 결과를 표시합니다.")

        preview = {
            "part_number": part_number or "UNKNOWN",
            "document_type": doc_type,
            "extracted_facts": {
                "torque_nm": "25~30",
                "steps": ["배터리 탈거", "브래킷 고정 볼트 해제"],
            },
            "captured_at": datetime.now().isoformat(timespec="seconds"),
        }
        st.success("분석 요청 형식 준비 완료")
        st.code(json.dumps(preview, ensure_ascii=False, indent=2), language="json")


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

    df = get_last_df()
    if df.empty:
        st.info("아직 검수할 수집 결과가 없습니다. 먼저 단일 테스트나 대규모 팩토리를 실행해 주세요.")
        return

    content_df = df[df["route_status"] == "content_page"].copy() if "route_status" in df.columns else df
    if content_df.empty:
        st.info("현재 검수 가능한 본문 페이지 결과가 없습니다.")
        return

    selected_url = st.selectbox("검수 대상 URL", content_df["url"].tolist())
    record = content_df[content_df["url"] == selected_url].iloc[0].to_dict()
    current_state = st.session_state["review_states"].get(selected_url, "대기")

    st.caption(f"현재 상태: {current_state}")
    col1, col2 = st.columns([2, 1])
    with col1:
        st.text_area(
            "AI 추출 결과",
            value=json.dumps(record, ensure_ascii=False, indent=2),
            height=260,
        )
    with col2:
        if st.button("✅ 승인 (DB 이관)", type="primary", use_container_width=True):
            st.session_state["review_states"][selected_url] = "승인됨"
            st.rerun()
        if st.button("✏️ 수정 후 승인", use_container_width=True):
            st.session_state["review_states"][selected_url] = "수정 승인"
            st.rerun()
        if st.button("🗑️ 영구 폐기", type="secondary", use_container_width=True):
            st.session_state["review_states"][selected_url] = "폐기됨"
            st.rerun()


def render_translation_mode() -> None:
    st.title("🌐 다국어 번역 엔진")
    st.info("수집 결과를 한국어, 영어, 베트남어용 구조화 데이터로 확장하는 운영 패널입니다.")

    rows = get_last_rows()
    ready_count = len(rows)
    st.write(f"현재 번역 대기 데이터: **{ready_count}건**")
    if st.button("🌍 전체 자동 번역 가동", type="primary", use_container_width=True):
        if ready_count == 0:
            st.warning("먼저 수집 결과를 하나 이상 만들어 주세요.")
        elif not GOOGLE_API_KEY:
            st.warning("GOOGLE_API_KEY가 없어 실제 번역 호출 대신 미리보기만 제공합니다.")
        else:
            st.success(f"{ready_count}건 번역 작업을 큐에 올릴 준비가 되었습니다.")

    preview = {
        "ko": {"step_1": "브레이크 캘리퍼 볼트를 풉니다."},
        "en": {"step_1": "Loosen the brake caliper bolts."},
        "vn": {"step_1": "Nới lỏng các bu lông ngàm phanh."},
    }
    st.subheader("번역 결과 미리보기")
    st.json(preview)


def render_prompt_mode() -> None:
    st.title("⚙️ 시스템 프롬프트 튜닝")
    prompt_name = st.selectbox("수정할 프롬프트", list(st.session_state["prompt_templates"].keys()))
    new_text = st.text_area(
        "시스템 지시문",
        value=st.session_state["prompt_templates"][prompt_name],
        height=220,
    )
    if st.button("💾 프롬프트 업데이트", type="primary"):
        st.session_state["prompt_templates"][prompt_name] = new_text
        st.success("프롬프트가 세션에 저장되었습니다. 다음 실행부터 이 값을 기준으로 운영할 수 있습니다.")


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
    broken_rows = (
        int((df["route_status"] == "broken_public_route").sum())
        if "route_status" in df.columns
        else 0
    )
    auth_rows = int((df["route_status"] == "auth_required").sum()) if "route_status" in df.columns else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("총 수집 결과", f"{total_rows:,}")
    c2.metric("정상 콘텐츠 페이지", f"{content_rows:,}")
    c3.metric("깨진 공개 경로", f"{broken_rows:,}")
    c4.metric("로그인 필요 경로", f"{auth_rows:,}")

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
