from __future__ import annotations

import os
from urllib.parse import urlparse

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from weviko_factory import run_factory


load_dotenv()
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "weviko1234!")


def inject_styles() -> None:
    st.markdown(
        """
        <style>
        :root {
            --weviko-bg: #f4efe6;
            --weviko-surface: #fffaf1;
            --weviko-panel: #fffdf8;
            --weviko-line: #d7c9ad;
            --weviko-text: #1f2a20;
            --weviko-muted: #667161;
            --weviko-accent: #8a4b17;
            --weviko-accent-2: #205b47;
        }
        .stApp {
            background:
                radial-gradient(circle at top right, rgba(32, 91, 71, 0.10), transparent 30%),
                radial-gradient(circle at left bottom, rgba(138, 75, 23, 0.14), transparent 28%),
                linear-gradient(180deg, #f8f2e8 0%, var(--weviko-bg) 100%);
            color: var(--weviko-text);
        }
        .weviko-hero {
            background: linear-gradient(135deg, rgba(255,250,241,0.95), rgba(245,233,212,0.88));
            border: 1px solid var(--weviko-line);
            border-radius: 24px;
            padding: 1.4rem 1.6rem;
            box-shadow: 0 16px 40px rgba(77, 54, 20, 0.08);
            margin-bottom: 1rem;
        }
        .weviko-kicker {
            color: var(--weviko-accent-2);
            font-size: 0.8rem;
            letter-spacing: 0.14em;
            text-transform: uppercase;
            font-weight: 700;
        }
        .weviko-title {
            font-size: 2rem;
            font-weight: 800;
            margin: 0.3rem 0 0.2rem;
            color: var(--weviko-text);
        }
        .weviko-copy {
            color: var(--weviko-muted);
            margin: 0;
        }
        .weviko-card {
            background: rgba(255, 253, 248, 0.94);
            border: 1px solid var(--weviko-line);
            border-radius: 20px;
            padding: 1rem 1.1rem;
            min-height: 110px;
        }
        .weviko-card-label {
            color: var(--weviko-muted);
            font-size: 0.82rem;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            font-weight: 700;
        }
        .weviko-card-value {
            color: var(--weviko-text);
            font-size: 1.8rem;
            font-weight: 800;
            margin-top: 0.35rem;
        }
        .weviko-card-copy {
            color: var(--weviko-muted);
            font-size: 0.92rem;
            margin-top: 0.15rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_hero() -> None:
    st.markdown(
        """
        <div class="weviko-hero">
            <div class="weviko-kicker">Weviko Global Engine</div>
            <div class="weviko-title">Crawling Command Center</div>
            <p class="weviko-copy">
                공개 부품 정보 페이지를 탐색하고, 깨진 공개 경로와 로그인 필요 경로를 구분하며,
                추출 가능한 본문은 구조화된 결과로 정리합니다.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def ensure_login() -> None:
    if "password_correct" not in st.session_state:
        st.session_state["password_correct"] = False

    if st.session_state["password_correct"]:
        return

    st.title("Weviko 데이터 센터 로그인")
    st.caption("관리자 비밀번호를 입력해 크롤링 대시보드에 접속하세요.")
    password = st.text_input("관리자 비밀번호", type="password")
    if st.button("접속", type="primary", use_container_width=True):
        if password == ADMIN_PASSWORD:
            st.session_state["password_correct"] = True
            st.rerun()
        else:
            st.error("비밀번호가 일치하지 않습니다.")
    st.stop()


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


def render_metric_cards(result) -> None:
    route_summary = result.route_status_counts
    content_count = route_summary.get("content_page", 0)
    auth_count = route_summary.get("auth_required", 0)
    broken_count = route_summary.get("broken_public_route", 0)
    queued_count = len(result.queued_urls)

    col1, col2, col3, col4 = st.columns(4)
    cards = [
        (col1, "Queued URLs", str(queued_count), "탐색 단계에서 큐에 올라간 URL 수"),
        (col2, "Content Pages", str(content_count), "실제 본문 추출 대상으로 처리된 페이지"),
        (col3, "Auth Required", str(auth_count), "로그인이 필요한 내부 업무 경로"),
        (col4, "Broken Routes", str(broken_count), "공개 경로지만 현재 오류인 라우트"),
    ]
    for column, label, value, copy in cards:
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


def render_results(result) -> None:
    rows = result.rows
    if not rows:
        st.warning("표시할 결과가 아직 없습니다.")
        return

    df = pd.DataFrame(rows)
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
        st.json(rows)


def render_database_view() -> None:
    st.subheader("데이터베이스 현황")
    st.info(
        "현재 페이지는 Supabase가 연결되어 있지 않아 실시간 통계를 표시하지 않습니다. "
        "환경 변수를 채우면 실제 카운트 조회 화면으로 이어서 확장할 수 있습니다."
    )


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
    progress_bar = st.progress(0, text="엔진 준비 중...")
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
        log_area.code("\n".join(logs[-60:]), language="bash")

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

    progress_bar.progress(100, text="완료")
    st.success(f"크롤링이 완료되었습니다. 총 {len(result.rows)}건의 결과를 정리했습니다.")
    render_metric_cards(result)
    st.divider()
    render_results(result)


def main() -> None:
    st.set_page_config(page_title="Weviko Global Engine", page_icon="🚜", layout="wide")
    inject_styles()
    ensure_login()

    st.sidebar.title("Weviko Engine")
    st.sidebar.markdown("---")
    mode = st.sidebar.radio(
        "작업 모드 선택",
        ["단일 타겟 테스트", "대량 양산 공장", "데이터베이스 현황"],
    )
    st.sidebar.markdown("---")
    if st.sidebar.button("로그아웃", use_container_width=True):
        st.session_state["password_correct"] = False
        st.rerun()

    render_hero()

    if mode == "단일 타겟 테스트":
        st.subheader("단일 타겟 테스트")
        st.caption("특정 기사 또는 부품 페이지 한 건을 직접 검사해 추출 결과와 경로 판정을 확인합니다.")
        target_url = st.text_input(
            "타겟 URL",
            value="https://www.weviko.com/community/parts-info/how-to-read-part-numbers",
        )

        col1, col2 = st.columns(2)
        with col1:
            target_market = st.selectbox("시장 사양", ["GLOBAL", "VN", "KR", "US"], index=0)
        with col2:
            num_workers = st.slider("워커 수", 1, 3, 1)

        default_hint = infer_path_hint(target_url)
        product_path_hint = st.text_input(
            "경로 힌트",
            value=default_hint,
            help="비워두면 시작 URL 자체를 직접 검사합니다.",
        )

        if st.button("단일 테스트 시작", type="primary", use_container_width=True):
            execute_run(
                start_url=target_url,
                num_workers=num_workers,
                target_market=target_market,
                product_path_hint=product_path_hint,
                discovery_extra_path_hints=[],
                route_watch_hints=[],
                discovery_max_pages=1,
                discovery_max_matches=1,
                discovery_max_depth=0,
                max_urls=1,
            )

    elif mode == "대량 양산 공장":
        st.subheader("대량 양산 공장")
        st.caption("카테고리 시작점에서 상세 글, 공개 오류 경로, 로그인 필요 경로까지 함께 분류합니다.")

        col1, col2 = st.columns(2)
        with col1:
            start_url = st.text_input(
                "시작 카테고리 URL",
                value="https://www.weviko.com/",
            )
            target_market = st.selectbox("타겟 시장", ["GLOBAL", "VN", "KR", "US"], index=0)
            product_path_hint = st.text_input("주요 경로 힌트", value="/community/parts-info")
        with col2:
            num_workers = st.slider("동시 접속 브라우저 수", 1, 10, 3)
            max_urls = st.number_input("최대 수집 URL 수", min_value=1, max_value=50000, value=20)
            discovery_max_depth = st.slider("탐색 깊이", 0, 5, 3)

        with st.expander("고급 설정"):
            extra_path_hints = st.text_input(
                "추가 콘텐츠 경로 힌트",
                value="",
                help="쉼표로 구분합니다. 예: /community/parts-info,/catalog",
            )
            route_watch_hints = st.text_input(
                "감시 경로 힌트",
                value="/parts,/dashboard",
                help="공개 오류 또는 로그인 필요 여부를 확인할 경로 패턴입니다.",
            )
            discovery_max_pages = st.slider("탐색 최대 페이지 수", 1, 50, 12)
            discovery_max_matches = st.slider("탐색 최대 매치 수", 1, 100, 20)

        st.warning("실제 크롤링은 Playwright 브라우저와 외부 네트워크를 사용합니다.")

        if st.button("공장 가동", type="primary", use_container_width=True):
            execute_run(
                start_url=start_url,
                num_workers=num_workers,
                target_market=target_market,
                product_path_hint=product_path_hint,
                discovery_extra_path_hints=safe_csv_list(extra_path_hints),
                route_watch_hints=safe_csv_list(route_watch_hints),
                discovery_max_pages=discovery_max_pages,
                discovery_max_matches=discovery_max_matches,
                discovery_max_depth=discovery_max_depth,
                max_urls=int(max_urls),
            )

    else:
        render_database_view()


if __name__ == "__main__":
    main()
