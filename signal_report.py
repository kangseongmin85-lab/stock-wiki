"""
signal_report.py — 영웅문 CSV → Signal Evening 형식 리포트 자동 생성
======================================================================

사용법:
  python signal_report.py                        # "오늘의 관심종목/" 내 최신 .csv 자동 선택
  python signal_report.py --csv 오늘스크리너.csv  # CSV 파일 직접 지정 (오늘의 관심종목/ 안에서도 탐색)
  python signal_report.py --no-fetch             # 본문 크롤링 생략 (빠름)
  python signal_report.py --date 20260509        # 날짜 지정 (기본: 오늘)

영웅문 CSV 내보내기 방법:
  조건검색 → 결과화면 우클릭 → "파일로 저장" → CSV 선택
  파일을 "오늘의 관심종목/" 폴더에 저장하면 자동으로 인식

테마 지정 (선택):
  themes.txt 파일에 아래 형식으로 작성하면 테마별로 그루핑됨:
    가온전선,전력인프라
    현대오토에버,SDV/로봇
    현대모비스,현대차그룹
  없으면 메모 키워드로 자동 추론 → 거래대금 순 정렬

의존성:
  pip install requests beautifulsoup4
"""

import os
import sys
import re
import csv
import io
import json
import time
import glob
import argparse
import requests
import webbrowser
import threading
import socket
from http.server import BaseHTTPRequestHandler, HTTPServer
from datetime import datetime, timedelta
from collections import defaultdict

if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ──────────────────────────────────────
# 설정
# ──────────────────────────────────────

def _load_config() -> dict:
    path = os.path.join(BASE_DIR, "config.json")
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return {}


_CFG = _load_config()

NOTION_TOKEN  = os.environ.get("NOTION_TOKEN")  or _CFG.get("NOTION_TOKEN",  "")
NOTION_DB_ID  = os.environ.get("NOTION_DB_ID")  or _CFG.get("NOTION_DB_ID",  "")

# 2026-05-17 추가 ── 주간 DB 로테이션 (fetch_news.py 와 동일 플래그)
# 켜져 있으면 검색 시 최근 7일 윈도우에 걸린 모든 주차 DB 를 함께 조회.
WEEKLY_ROTATION = str(
    _CFG.get("WEEKLY_ROTATION", "") or os.environ.get("WEEKLY_ROTATION", "")
).lower() in ("1", "true", "yes")

NAVER_API = {
    "client_id":     os.environ.get("NAVER_CLIENT_ID")     or _CFG.get("NAVER_CLIENT_ID",     ""),
    "client_secret": os.environ.get("NAVER_CLIENT_SECRET") or _CFG.get("NAVER_CLIENT_SECRET", ""),
}

# 테마 자동 추론용 키워드 (메모 텍스트에서 매칭)
THEME_KEYWORDS = {
    "전력인프라":    ["전력", "변압기", "전선", "배전", "송전", "HVDC", "전력망"],
    "HBM/반도체":   ["HBM", "반도체", "파운드리", "DRAM", "낸드", "웨이퍼"],
    "SDV/로봇":     ["SDV", "로봇", "스마트팩토리", "자율주행", "소프트웨어중심차"],
    "현대차그룹":    ["현대차그룹", "현대모비스", "현대오토에버", "기아"],
    "바이오":       ["바이오", "신약", "임상", "제약", "의약", "치료제"],
    "방산":         ["방산", "방위", "무기", "미사일", "K-방산", "레이더"],
    "이차전지":     ["이차전지", "배터리", "양극재", "음극재", "전해질", "ESS"],
    "AI/데이터센터": ["AI", "데이터센터", "GPU", "인공지능", "엔비디아"],
    "광통신":       ["광통신", "광케이블", "광섬유", "AI인프라"],
    "수소/연료전지": ["수소", "연료전지", "SOFC", "PAFC", "수전해"],
    "디스플레이":   ["OLED", "LCD", "디스플레이", "패널"],
    "카메라/광학":  ["카메라", "광학", "렌즈", "이미지센서"],
}

# fetch_news.py 와 동일한 차단 키워드 — 제목에 포함 시 후보에서 제외
BLOCK_KEYWORDS = [
    # 스팸·찌라시
    "폭증", "불꽃", "랠리", "슈퍼개미", "수퍼개미",
    "부고", "부음", "폭탄", "무료", "인포스탁", "라씨로",
    # 증시 요약형 (여러 종목 나열 → 특정 종목 재료 없음)
    "증시요약", "오후장", "오전장", "종목이슈",
    "상한가 종목", "하한가 종목", "주요공시", "주간 코스닥", "주간 코스피",
    "기관 순매수", "기관 순매도", "주간 기관", "오늘의 증시", "마감 시황",
    "투자경고종목", "주가 급등→",
    # 기술적 지표 과열 나열 기사
    "이격도과열", "상대강도과열", "시간외단일가", "시간외Y", "시간외N",
    # 단순 주가 게시 / 공시 종합
    "장중 투자주의", "투자주의종목", "거래정지", "[공시]",
    # 테마 묶음 기사 (개별 재료 아님)
    "테마주 동향", "관련주 현황",
    # 기타 나열형
    "베스트&워스트", "급등락주", "코스닥 기관", "코스피 기관",
    "주간 특징주", "오늘의 특징주", "장중 특징주",
    "주가,", "원에 거래",
    # 기술적 지표 나열형 (예: "[골든크로스 종목] 오늘의...")
    "골든크로스 종목", "데드크로스 종목", "신고가 종목", "신저가 종목",
    # 공매도·수급 브리핑 나열형
    "공매도 브리핑", "공매도 현황", "공매도 top", "수급 브리핑",
]

# desc-only 매칭 허용 시, 기사 제목이 주식/사업 관련임을 확인하는 힌트 키워드
# "매점매석", 정치, 사회면 기사 등 무관 기사 필터용
STOCK_ARTICLE_HINTS = [
    "특징주", "주가", "증시", "%", "억원", "억",
    "수주", "계약", "실적", "영업이익", "매출",
    "배터리", "반도체", "로봇", "전력", "바이오",
    "상한가", "급등", "강세", "사업", "개발", "공급", "투자",
]


# ──────────────────────────────────────
# Step 1: 영웅문 CSV 파싱
# ──────────────────────────────────────

CSV_DIR = os.path.join(BASE_DIR, "오늘의 관심종목")


def find_latest_csv() -> str:
    """"오늘의 관심종목/" 폴더 내 가장 최근 수정된 .csv 파일 반환"""
    csvs = glob.glob(os.path.join(glob.escape(CSV_DIR), "*.csv"))
    if not csvs:
        return ""
    return max(csvs, key=os.path.getmtime)


def resolve_csv_path(arg_path: str) -> str:
    """--csv 인자 처리: 절대/상대 경로 그대로 → CSV_DIR 내부 → BASE_DIR 내부 순으로 탐색"""
    if not arg_path:
        return ""
    if os.path.isfile(arg_path):
        return arg_path
    cand = os.path.join(CSV_DIR, arg_path)
    if os.path.isfile(cand):
        return cand
    cand2 = os.path.join(BASE_DIR, arg_path)
    if os.path.isfile(cand2):
        return cand2
    return arg_path  # 존재하지 않으면 그대로 반환 → 호출부에서 에러 처리


def parse_heroesgate_csv(path: str) -> list:
    """
    영웅문 스크리너 CSV (EUC-KR) 파싱

    컬럼 v1 (10열): 분/종목명/L일봉H/현재가/대비/(공백)/등락률/거래량/메모/종목코드
    컬럼 v2 (11열): 분/종목명/L일봉H/현재가/대비/(공백)/등락률/거래량/메모/거래대금/종목코드
      → 거래대금 컬럼이 있으면 직접 사용 (백만원 단위 → //100 → 억원)
      → 없으면 거래량 × 현재가로 계산

    반환: [{name, code, ctrt, vol_eok, memo}]
    """
    if not os.path.exists(path):
        print(f"[오류] CSV 파일 없음: {path}")
        return []

    with open(path, encoding="euc-kr", errors="replace") as f:
        content = f.read()

    reader = csv.reader(io.StringIO(content))
    rows   = list(reader)

    # 헤더 찾기 + 거래대금 컬럼 위치 확인
    header_row = 0
    col_trd    = -1  # 거래대금 컬럼 인덱스 (-1이면 계산으로 대체)
    col_code   = 9   # 종목코드 기본 위치
    for i, row in enumerate(rows):
        if len(row) >= 2 and row[1].strip() == "종목명":
            header_row = i
            headers    = [c.strip() for c in row]
            if "거래대금" in headers:
                col_trd  = headers.index("거래대금")
                col_code = headers.index("종목코드") if "종목코드" in headers else col_trd + 1
            break

    stocks = []
    for row in rows[header_row + 1:]:
        if len(row) < 8:
            continue
        name = row[1].strip()
        if not name or name == "종목명":
            continue
        # 우선주 제외 (종목명이 "우" 또는 "우B"로 끝나는 경우)
        if re.search(r'우B?$', name):
            continue

        price_str = row[3].strip().replace(",", "")
        ctrt_str  = row[6].strip().replace(",", "")
        vol_str   = row[7].strip().replace(",", "")
        memo      = row[8].strip().lstrip("'") if len(row) > 8 else ""
        trd_str   = row[col_trd].strip().replace(",", "") if col_trd >= 0 and len(row) > col_trd else ""
        code      = row[col_code].strip().lstrip("'") if len(row) > col_code else ""

        try:
            ctrt = float(ctrt_str)
            if trd_str:
                # 거래대금 컬럼 있음 → 백만원 단위 → 억원 변환
                vol_eok = max(1, int(trd_str) // 100)
            else:
                # 거래대금 없음 → 거래량 × 현재가 계산
                price   = int(price_str)
                vol     = int(vol_str)
                vol_eok = max(1, round(vol * price / 100_000_000))
        except (ValueError, ZeroDivisionError):
            continue

        stocks.append({
            "name":    name,
            "code":    code,
            "ctrt":    ctrt,
            "vol_eok": vol_eok,
            "memo":    memo,
            "theme":   "",
        })

    print(f"[CSV] {os.path.basename(path)} → 종목 {len(stocks)}개 파싱 완료")
    return stocks


def load_themes_file() -> dict:
    """themes.txt 읽어서 {종목명: 테마명} 반환"""
    path = os.path.join(BASE_DIR, "themes.txt")
    result = {}
    if not os.path.exists(path):
        return result
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(",", 1)
            if len(parts) == 2:
                result[parts[0].strip()] = parts[1].strip()
    return result


def infer_theme(memo: str) -> str:
    """메모 텍스트에서 THEME_KEYWORDS 매칭으로 테마 추론"""
    if not memo:
        return "개별주"
    for theme, kws in THEME_KEYWORDS.items():
        if any(kw in memo for kw in kws):
            return theme
    return "개별주"


def assign_themes(stocks: list) -> list:
    """themes.txt 우선 → 메모 키워드 추론 순으로 테마 배정"""
    themes_map = load_themes_file()
    for s in stocks:
        if s["name"] in themes_map:
            s["theme"] = themes_map[s["name"]]
        else:
            s["theme"] = infer_theme(s["memo"])
    return stocks


# ──────────────────────────────────────
# Step 2: 뉴스 검색
# ──────────────────────────────────────

def _clean(text: str) -> str:
    """HTML 태그 및 엔티티 제거"""
    text = re.sub(r"<[^>]+>", "", text)
    for src, dst in [("&quot;", '"'), ("&amp;", "&"), ("&lt;", "<"),
                     ("&gt;", ">"), ("&nbsp;", " "), ("&#39;", "'")]:
        text = text.replace(src, dst)
    return re.sub(r"\s+", " ", text).strip()


def _parse_pub_date(pub: str) -> str:
    """Naver pubDate → 'YYYY.MM.DD'"""
    try:
        dt = datetime.strptime(pub, "%a, %d %b %Y %H:%M:%S +0900")
        return dt.strftime("%Y.%m.%d")
    except Exception:
        return ""


def _search_naver(query: str, display: int = 10) -> list:
    """Naver 뉴스 검색 API"""
    if not NAVER_API["client_id"]:
        print("[경고] NAVER_CLIENT_ID 없음 — config.json 확인")
        return []
    try:
        r = requests.get(
            "https://openapi.naver.com/v1/search/news.json",
            headers={
                "X-Naver-Client-Id":     NAVER_API["client_id"],
                "X-Naver-Client-Secret": NAVER_API["client_secret"],
            },
            params={"query": query, "display": display, "sort": "date"},
            timeout=5,
        )
        if r.status_code == 200:
            return r.json().get("items", [])
        return []
    except Exception:
        return []


# 2026-05-17 추가 ── 검색 대상 DB ID 캐시 (프로세스 1회 해석)
_NOTION_SEARCH_DB_IDS = None


def _get_notion_search_db_ids(today_str: str) -> list:
    """Notion 검색 대상 DB ID 목록.
    - WEEKLY_ROTATION OFF: [NOTION_DB_ID]
    - WEEKLY_ROTATION ON : 최근 7일 윈도우에 걸린 모든 주차 DB + 기본 DB (중복 제거)
    실패하면 기본 DB 하나로 폴백.
    """
    global _NOTION_SEARCH_DB_IDS
    if _NOTION_SEARCH_DB_IDS is not None:
        return _NOTION_SEARCH_DB_IDS

    ids = [NOTION_DB_ID] if NOTION_DB_ID else []
    if WEEKLY_ROTATION and NOTION_TOKEN and NOTION_DB_ID:
        try:
            from weekly_db import get_parent_page_id, get_db_ids_for_window
            parent = get_parent_page_id(NOTION_TOKEN, NOTION_DB_ID)
            if parent:
                today_d = datetime.strptime(today_str, "%Y.%m.%d")
                weekly_ids = get_db_ids_for_window(
                    NOTION_TOKEN, parent,
                    today_d - timedelta(days=7), today_d,
                )
                for wid in weekly_ids:
                    if wid not in ids:
                        ids.append(wid)
                if len(ids) > 1:
                    print(f"[weekly_db] Notion 검색 대상 DB {len(ids)}개 (윈도우 7일)")
        except Exception as e:
            print(f"[weekly_db] 검색 대상 DB 조회 실패 — 기본 DB 만 사용: {e}")

    _NOTION_SEARCH_DB_IDS = ids
    return ids


def _search_notion_db(variants: list, today_str: str) -> list:
    """Notion 텔레그램 아카이브 DB에서 종목명 포함 기사 검색.
    WEEKLY_ROTATION 켜져 있으면 윈도우 내 모든 주차 DB 를 순회한다.
    """
    if not NOTION_TOKEN or not NOTION_DB_ID:
        return []

    db_ids = _get_notion_search_db_ids(today_str)
    if not db_ids:
        return []

    since = (datetime.strptime(today_str, "%Y.%m.%d") - timedelta(days=7)).strftime("%Y-%m-%d")
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28",
    }

    seen = set()
    results = []
    for variant in variants:
        payload = {
            "filter": {
                "and": [
                    {"property": "날짜", "date": {"on_or_after": since}},
                    {"or": [
                        {"property": "제목",     "title":     {"contains": variant}},
                        {"property": "원문내용", "rich_text": {"contains": variant}},
                    ]},
                ]
            },
            "sorts": [{"property": "날짜", "direction": "descending"}],
            "page_size": 5,
        }
        for db_id in db_ids:
            try:
                r = requests.post(
                    f"https://api.notion.com/v1/databases/{db_id}/query",
                    headers=headers, json=payload, timeout=5,
                )
                if r.status_code != 200:
                    continue
                for page in r.json().get("results", []):
                    props    = page.get("properties", {})
                    title_a  = props.get("제목", {}).get("title", [])
                    title    = title_a[0]["plain_text"] if title_a else ""
                    url      = props.get("원문링크", {}).get("url", "") or ""
                    date_raw = (props.get("날짜", {}).get("date") or {}).get("start", "")
                    date_str = date_raw[:10].replace("-", ".") if date_raw else ""
                    cont_a   = props.get("원문내용", {}).get("rich_text", [])
                    content  = cont_a[0]["plain_text"] if cont_a else ""

                    key = url or title
                    if not title or key in seen:
                        continue
                    seen.add(key)
                    results.append({
                        "title":  title,
                        "url":    url,
                        "desc":   content[:300],
                        "date":   date_str,
                        "source": "notion",
                    })
            except Exception:
                pass
    return results


def _name_variants(name: str) -> list:
    """종목명 검색 변형 목록"""
    variants = [name]
    kor = re.sub(r"^[A-Za-z0-9&]+", "", name).strip()
    # 5글자 미만 단축형은 다른 종목명과 겹칠 위험이 높아 제외
    # 예: "로보틱스"(4) → 두산로보틱스 기사에 오매칭
    if kor and len(kor) >= 5 and kor != name:
        variants.append(kor)
    return variants


_NAV_KEYWORDS = (
    "로그인", "회원가입", "구독", "전체메뉴", "검색 기사검색",
    "정치 사회 경제", "공유 페이스북", "링크가 복사되었습니다",
    "PREMIUM", "글자크기",
)


def _looks_like_nav(text: str) -> bool:
    """첫 120자에 nav 키워드 1개 이상이면 nav/footer로 판단."""
    head = text[:120]
    return any(kw in head for kw in _NAV_KEYWORDS)


def fetch_article_body(url: str, max_chars: int = 500) -> str:
    """기사 본문 크롤링 (Naver 우선, 일반 도메인은 og:description 우선, 실패 시 빈 문자열)."""
    try:
        from bs4 import BeautifulSoup
        r = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
            timeout=6,
            allow_redirects=True,
        )
        r.encoding = r.apparent_encoding or "utf-8"
        soup = BeautifulSoup(r.text, "html.parser")

        # ① Naver 등 정확히 본문 selector 가 알려진 사이트 — 우선 시도
        for sel in ["#dic_area", "#articeBody", "#newsct_article", ".go_trans"]:
            el = soup.select_one(sel)
            if el:
                text = re.sub(r"\s+", " ", el.get_text(" ", strip=True))
                if len(text) > 50:
                    return text[:max_chars] + ("..." if len(text) > max_chars else "")

        # ② og:description / description meta — 일반 사이트는 이게 가장 깔끔
        meta = soup.find("meta", {"property": "og:description"}) \
            or soup.find("meta", {"name": "description"})
        og = (meta.get("content", "") if meta else "").strip()
        if len(og) >= 80:
            return og[:max_chars] + ("..." if len(og) > max_chars else "")

        # ③ 마지막 폴백: <article> 태그. nav/footer 섞일 위험 있어 nav 키워드 검사 후 사용
        el = soup.select_one("article")
        if el:
            text = re.sub(r"\s+", " ", el.get_text(" ", strip=True))
            if len(text) > 50 and not _looks_like_nav(text):
                return text[:max_chars] + ("..." if len(text) > max_chars else "")

        # ④ og 가 짧아도 있는 게 nav 보다 나음
        if og:
            return og[:max_chars]
        return ""
    except Exception:
        return ""


def _collect_candidates(stock: dict, today_str: str):
    """
    종목당 후보 기사 수집 + 점수 부여 (네트워크 호출 1회).

    점수 기준:
      +10  오늘 날짜
      +15  제목에 특징주 + 종목명 포함 (날짜 무관) — 콤보 보너스
      + 5  제목에 종목명 포함
      + 2  Notion DB 출처 보너스
    31일 이내 기사만 허용.

    반환: (candidates, variants)
      candidates — 점수 내림차순 정렬된 후보 dict 리스트
      variants   — 종목명 변형 토큰 리스트 (pick_best_article에서 재사용)
    """
    name     = stock["name"]
    variants = _name_variants(name)
    today_d  = datetime.strptime(today_str, "%Y.%m.%d")
    cutoff_d = today_d - timedelta(days=31)

    queries = [
        f"{name} 특징주",
        name,
    ]

    candidates = []
    seen_urls  = set()

    for query in queries:
        items = _search_naver(query, display=20)
        for item in items:
            url   = item.get("originallink") or item.get("link", "")
            title = _clean(item.get("title", ""))
            desc  = _clean(item.get("description", ""))
            pub   = item.get("pubDate", "")

            if not title or url in seen_urls:
                continue
            if any(kw in title for kw in BLOCK_KEYWORDS):
                continue

            seen_urls.add(url)
            pub_date = _parse_pub_date(pub)

            if pub_date:
                try:
                    if datetime.strptime(pub_date, "%Y.%m.%d") < cutoff_d:
                        continue
                except ValueError:
                    pass

            is_today = (pub_date == today_str)
            name_in  = any(v in title for v in variants)
            tokjing  = "특징주" in title

            score = 0
            if is_today:
                score += 10
            if name_in:
                score += 5
            if tokjing and name_in:
                score += 15

            candidates.append({
                "title":    title,
                "url":      url,
                "desc":     desc,
                "date":     pub_date,
                "score":    score,
                "is_today": is_today,
                "source":   "naver",
            })
        time.sleep(0.15)

    # ── Notion DB 검색 결과 추가 ──
    notion_hits = _search_notion_db(variants, today_str)
    for nr in notion_hits:
        key = nr["url"] or nr["title"]
        if key in seen_urls:
            continue
        if any(kw in nr["title"] for kw in BLOCK_KEYWORDS):
            continue
        seen_urls.add(key)
        pub_date = nr["date"]
        is_today = (pub_date == today_str)
        name_in  = any(v in nr["title"] for v in variants) or any(v in nr["desc"] for v in variants)
        score = 0
        if is_today:  score += 10
        if name_in:   score += 5
        score += 2
        candidates.append({
            "title":    nr["title"],
            "url":      nr["url"],
            "desc":     nr["desc"],
            "date":     pub_date,
            "score":    score,
            "is_today": is_today,
            "source":   "notion",
        })

    candidates.sort(key=lambda x: x["score"], reverse=True)
    return candidates, variants


def pick_best_article(candidates: list, variants: list, fetch_body: bool) -> dict:
    """수집된 후보 중 베스트 1건 선택 + 본문 보강 (find_best_article 기존 반환 형식 유지)."""
    if not candidates:
        return {}

    # ① title에 종목명 있는 후보 우선
    title_hits = [c for c in candidates if any(v in c["title"] for v in variants)]

    if title_hits:
        best = max(title_hits, key=lambda x: x["score"])
    else:
        # ② title에 없으면 desc 후보 — 단, 기사 제목이 주식/사업 관련이어야 함
        desc_hits = [
            c for c in candidates
            if any(v in c.get("desc", "") for v in variants)
            and any(h in c["title"] for h in STOCK_ARTICLE_HINTS)
        ]
        if not desc_hits:
            return {}
        best = max(desc_hits, key=lambda x: x["score"])

    body = ""
    if fetch_body and best.get("url"):
        body = fetch_article_body(best["url"])
        time.sleep(0.2)

    summary = (body or best["desc"])[:500]
    if len(body or best["desc"]) > 500:
        summary += "..."

    return {
        "title":    best["title"],
        "url":      best["url"],
        "summary":  summary,
        "date":     best["date"],
        "is_today": best["is_today"],
    }


def find_best_article(stock: dict, today_str: str, fetch_body: bool) -> dict:
    """(기존 호환) 후보 수집 → 베스트 선택 wrapper."""
    candidates, variants = _collect_candidates(stock, today_str)
    return pick_best_article(candidates, variants, fetch_body)


# ──────────────────────────────────────
# Step 3: 코스피/코스닥 지수 조회
# ──────────────────────────────────────

def fetch_index_info() -> dict:
    result = {}
    for key, code in [("kospi", "KOSPI"), ("kosdaq", "KOSDAQ")]:
        try:
            r = requests.get(
                f"https://m.stock.naver.com/api/index/{code}/basic",
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=4,
            )
            d     = r.json()
            price = d.get("closePrice", d.get("currentPrice", ""))
            rate  = d.get("fluctuationsRatio", "")
            sign  = "+" if float(rate or 0) > 0 else ""
            result[key] = f"{price}({sign}{rate}%)" if price else ""
        except Exception:
            result[key] = ""
    return result


# ──────────────────────────────────────
# Step 4: Signal Evening 형식 리포트
# ──────────────────────────────────────

# 2026-05-17 추가 ── 노션 푸시용 로컬 helper 서버 ─────────────────────────
# review HTML 의 "📤 노션에 푸시" 버튼이 localhost:<port>/push 로 POST 하면
# notion_pusher.push_reviewed() 로 종목재료정리 DB 에 즉시 upsert.

def _find_free_port(default: int = 8765) -> int:
    for port in [default, 8766, 8767, 8768, 0]:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(("127.0.0.1", port))
                return s.getsockname()[1]
        except OSError:
            continue
    return 0


# 푸시 작업 상태 (노션 upsert가 수십 초~수 분 걸려 브라우저 연결이 끊기므로
# 백그라운드 스레드 + /status 폴링 방식으로 처리)
_PUSH_STATE = {"status": "idle", "result": None, "error": None}
_PUSH_DONE  = threading.Event()   # 푸시 성공 → 메인 루프 자동 종료 신호
_PUSH_ACK   = threading.Event()   # 브라우저가 done 상태를 확인했음 → 서버 내려도 안전


def _run_push_job(payload):
    try:
        from notion_pusher import push_reviewed
        result = push_reviewed(payload)
        _PUSH_STATE.update(status="done", result=result, error=None)
        _PUSH_DONE.set()
    except Exception as e:
        # 실패 시 서버는 살려둠 — 브라우저에서 재시도 가능
        _PUSH_STATE.update(status="error", result=None, error=str(e))
        print(f"\n[helper] ❌ 노션 푸시 실패: {e} — 브라우저에서 재시도하거나 Ctrl+C 로 종료하세요.")


class _PushHandler(BaseHTTPRequestHandler):
    def _cors(self):
        self.send_header("Access-Control-Allow-Origin",  "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def do_OPTIONS(self):
        self.send_response(204); self._cors(); self.end_headers()

    def do_GET(self):
        if self.path != "/status":
            self.send_response(404); self._cors(); self.end_headers(); return
        self._reply(200, _PUSH_STATE)
        if _PUSH_STATE["status"] == "done":
            _PUSH_ACK.set()

    def do_POST(self):
        if self.path != "/push":
            self.send_response(404); self._cors(); self.end_headers(); return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            payload = json.loads(self.rfile.read(length).decode("utf-8"))
        except Exception as e:
            self._reply(400, {"error": f"invalid JSON: {e}"}); return
        if _PUSH_STATE["status"] == "running":
            self._reply(409, {"error": "이미 푸시 진행 중"}); return
        _PUSH_STATE.update(status="running", result=None, error=None)
        threading.Thread(target=_run_push_job, args=(payload,), daemon=True).start()
        self._reply(202, {"status": "running"})

    def _reply(self, code, obj):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        try:
            self.send_response(code)
            self._cors()
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (ConnectionAbortedError, ConnectionResetError, BrokenPipeError):
            pass  # 브라우저가 먼저 연결을 끊은 경우 — 무해하므로 조용히 무시

    def log_message(self, format, *args):
        # 콘솔 잡음 줄이기 — 푸시 결과만 직접 print
        pass


def _start_push_server():
    port = _find_free_port()
    if not port:
        print("[helper] 사용 가능한 포트 없음 — 자동 푸시 비활성화")
        return None, None
    server = HTTPServer(("127.0.0.1", port), _PushHandler)
    th = threading.Thread(target=server.serve_forever, daemon=True)
    th.start()
    return server, port


def build_review_html(review_data: list, date_str: str, push_endpoint: str = "") -> str:
    """검수용 단일 HTML 페이지 (JSON 데이터 임베드)."""
    import json as _json
    json_data = _json.dumps(review_data, ensure_ascii=False)
    json_data = json_data.replace("</", "<\\/")  # script 태그 안전 처리

    template = """<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="utf-8">
<title>기사 검수 - __DATE__</title>
<style>
* { box-sizing: border-box; }
body { font-family: -apple-system, "Apple SD Gothic Neo", "Malgun Gothic", sans-serif; max-width: 1100px; margin: 0 auto; padding: 20px; background: #f5f5f7; color: #222; }
.topbar { position: sticky; top: 10px; background: white; padding: 14px 18px; border-radius: 12px; display: flex; align-items: center; gap: 14px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); margin-bottom: 16px; z-index: 10; }
.topbar h2 { margin: 0; font-size: 17px; }
.progress { flex: 1; height: 8px; background: #eee; border-radius: 4px; overflow: hidden; }
.progress-bar { height: 100%; background: #4CAF50; transition: width 0.2s; }
.stock-card { background: white; padding: 18px; border-radius: 10px; margin-bottom: 12px; box-shadow: 0 1px 3px rgba(0,0,0,0.06); border-left: 4px solid transparent; }
.stock-card.approved { border-left-color: #4CAF50; background: #f1f8f3; }
.stock-card.rejected { border-left-color: #f44336; background: #fdecea; }
.stock-card.skipped  { border-left-color: #999; background: #f5f5f5; }
.stock-header { display: flex; align-items: center; gap: 10px; margin-bottom: 10px; flex-wrap: wrap; }
.stock-name { font-size: 17px; font-weight: 700; }
.stock-meta { color: #666; font-size: 13px; }
.ctrt-up   { color: #d32f2f; font-weight: 600; }
.ctrt-down { color: #1976d2; font-weight: 600; }
.theme-tag { background: #e3f2fd; color: #1976d2; padding: 2px 8px; border-radius: 4px; font-size: 12px; }
.memo { color: #888; font-size: 12px; margin-left: 4px; }
.article { background: #fafafa; padding: 12px; border-radius: 8px; margin: 8px 0; }
.article-meta { color: #666; font-size: 12px; margin-bottom: 4px; }
.article-title { font-weight: 600; margin: 4px 0; line-height: 1.4; }
.article-title a { color: #1565c0; text-decoration: none; }
.article-title a:hover { text-decoration: underline; }
.article-desc { color: #444; font-size: 13px; line-height: 1.5; }
.nav-bar { display: flex; align-items: center; gap: 8px; margin: 8px 0 4px; }
.actions { display: flex; gap: 8px; margin-top: 12px; flex-wrap: wrap; }
button { padding: 8px 14px; border: none; border-radius: 6px; cursor: pointer; font-weight: 600; font-size: 13px; }
button:disabled { opacity: 0.4; cursor: default; }
.btn-approve { background: #4CAF50; color: white; }
.btn-reject  { background: #f44336; color: white; }
.btn-skip    { background: #9e9e9e; color: white; }
.btn-nav     { background: #e0e0e0; color: #333; padding: 6px 10px; }
.btn-export  { background: #1976d2; color: white; }
.btn-reset   { background: white; color: #888; border: 1px solid #ddd; }
.badge-today { background: #4CAF50; color: white; font-size: 11px; padding: 2px 6px; border-radius: 3px; margin-right: 4px; }
.badge-old   { background: #ff9800; color: white; font-size: 11px; padding: 2px 6px; border-radius: 3px; margin-right: 4px; }
.badge-source { background: #7e57c2; color: white; font-size: 11px; padding: 2px 6px; border-radius: 3px; }
.empty { color: #aaa; font-style: italic; padding: 18px; text-align: center; background: #fafafa; border-radius: 8px; }
.decision-tag { margin-left: auto; font-size: 13px; font-weight: 600; }
</style>
</head>
<body>
<div class="topbar">
  <h2>기사 검수 - __DATE__</h2>
  <div class="progress"><div class="progress-bar" id="progressBar"></div></div>
  <span id="progressText" style="font-size:13px;color:#666;"></span>
  <button class="btn-export" onclick="pushToNotion()" id="btnPush">📤 노션에 푸시</button>
  <button class="btn-reset" onclick="resetAll()">초기화</button>
</div>

<div id="pushResult" style="margin:12px 0;padding:10px;border-radius:6px;display:none;font-size:14px;"></div>

<div id="cards"></div>

<script>
const REVIEW_DATE     = "__DATE__";
const STORAGE_KEY     = "review_" + REVIEW_DATE;
const PUSH_ENDPOINT   = "__PUSH_ENDPOINT__";  // 비어있으면 푸시 비활성, 다운로드만
const DATA            = __JSON_DATA__;

let state = JSON.parse(localStorage.getItem(STORAGE_KEY) || "{}");

function saveState() { localStorage.setItem(STORAGE_KEY, JSON.stringify(state)); }

function ensureState(name) {
  if (!state[name]) state[name] = { chosen_idx: 0, decision: null };
  return state[name];
}

function decide(name, decision) {
  const st = ensureState(name);
  st.decision = (st.decision === decision) ? null : decision;  // 같은 버튼 다시 누르면 해제
  saveState();
  render();
}

function navCandidate(name, delta) {
  const st = ensureState(name);
  const stock = DATA.find(s => s.name === name);
  const max = (stock.candidates || []).length - 1;
  let idx = (st.chosen_idx || 0) + delta;
  if (idx < 0) idx = 0;
  if (idx > max) idx = max;
  st.chosen_idx = idx;
  saveState();
  render();
}

function _buildPayload() {
  const results = DATA.map(s => {
    const st = state[s.name] || {};
    const idx = st.chosen_idx || 0;
    const cands = s.candidates || [];
    const cand = cands[idx] || null;
    // recent_articles: 대표 1건(chosen) + 나머지 후보들 (URL dedup)
    const seen = new Set();
    const recent = [];
    const tryAdd = (a) => {
      if (!a || !a.url || seen.has(a.url)) return;
      seen.add(a.url);
      recent.push(a);
    };
    tryAdd(cand);
    cands.forEach(tryAdd);
    return {
      name: s.name, code: s.code, ctrt: s.ctrt, vol_eok: s.vol_eok, theme: s.theme,
      decision: st.decision || null,
      chosen_article: (st.decision === "approved" && cand) ? cand : null,
      recent_articles: recent.slice(0, 5),
      candidates_seen: (st.chosen_idx || 0) + 1,
      total_candidates: cands.length
    };
  });
  return {
    date: REVIEW_DATE,
    exported_at: new Date().toISOString(),
    summary: {
      total: DATA.length,
      approved: results.filter(r => r.decision === "approved").length,
      rejected: results.filter(r => r.decision === "rejected").length,
      skipped:  results.filter(r => r.decision === "skipped").length,
      pending:  results.filter(r => !r.decision).length
    },
    results
  };
}

function _showPushResult(html, color) {
  const div = document.getElementById("pushResult");
  div.style.display = "block";
  div.style.background = color || "#f1f8f3";
  div.innerHTML = html;
}

async function pushToNotion() {
  const payload = _buildPayload();
  const approved = payload.summary.approved;
  if (approved === 0) {
    _showPushResult("⚠️ 승인된 종목이 없습니다. ✅ 승인 버튼을 먼저 누르세요.", "#fff3e0");
    return;
  }
  if (!PUSH_ENDPOINT) {
    _showPushResult("⚠️ 푸시 서버가 비활성화 — JSON 다운로드로 폴백합니다.", "#fff3e0");
    _downloadJson(payload);
    return;
  }
  const btn = document.getElementById("btnPush");
  btn.disabled = true;
  btn.textContent = "⏳ 푸시 중...";
  _showPushResult("⏳ 노션에 " + approved + "건 푸시 중...", "#e3f2fd");
  try {
    const res = await fetch(PUSH_ENDPOINT + "/push", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify(payload)
    });
    const ack = await res.json();
    if (!res.ok) throw new Error(ack.error || ("HTTP " + res.status));
    // 서버가 백그라운드로 푸시 진행 → /status 폴링으로 완료 대기
    let st, elapsed = 0;
    for (;;) {
      await new Promise(r => setTimeout(r, 1500));
      elapsed += 1.5;
      const sr = await fetch(PUSH_ENDPOINT + "/status");
      st = await sr.json();
      if (st.status === "done" || st.status === "error") break;
      _showPushResult("⏳ 노션에 " + approved + "건 푸시 중... (" + Math.round(elapsed) + "초 경과)", "#e3f2fd");
    }
    if (st.status === "error") throw new Error(st.error || "unknown");
    const data = st.result || {};
    let msg = "✅ 노션 푸시 완료 — 총 " + data.total + "건 / 신규 " + data.created + ", 업데이트 " + data.updated;
    let color = "#e8f5e9";
    if (data.errors && data.errors.length) {
      msg += "<br>⚠️ 실패 " + data.errors.length + "건: ";
      msg += data.errors.map(e => e.name + " (" + e.error + ")").join(", ");
      color = "#fff3e0";
    }
    msg += "<br>🖥️ cmd 창은 남은 단계(아웃컴·패턴·동기화)를 마친 뒤 자동으로 닫힙니다.";
    _showPushResult(msg, color);
  } catch (e) {
    _showPushResult("❌ 푸시 실패: " + e.message + "<br>JSON 다운로드로 폴백합니다.", "#ffebee");
    _downloadJson(payload);
  } finally {
    btn.disabled = false;
    btn.textContent = "📤 노션에 푸시";
  }
}

function _downloadJson(payload) {
  const blob = new Blob([JSON.stringify(payload, null, 2)], {type: "application/json"});
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "reviewed_" + REVIEW_DATE + ".json";
  document.body.appendChild(a); a.click(); a.remove();
  URL.revokeObjectURL(url);
}

function resetAll() {
  if (!confirm("모든 검수 결과를 초기화합니다.")) return;
  state = {}; saveState(); render();
}

function escapeHtml(s) {
  return String(s == null ? "" : s)
    .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
}

function decisionLabel(d) {
  return ({ approved: "✅ 승인", rejected: "❌ 거부", skipped: "⏭️ 스킵" })[d] || "";
}

function render() {
  const cards = document.getElementById("cards");
  cards.innerHTML = "";
  let done = 0;

  DATA.forEach((s, sIdx) => {
    const st = state[s.name] || { chosen_idx: 0, decision: null };
    if (st.decision) done++;

    const card = document.createElement("div");
    card.className = "stock-card" + (st.decision ? " " + st.decision : "");
    card.dataset.name = s.name;

    const ctrtClass = s.ctrt >= 0 ? "ctrt-up" : "ctrt-down";
    const ctrtSign  = s.ctrt >= 0 ? "+" : "";

    let html = "";
    html += '<div class="stock-header">';
    html += '<span class="stock-name">' + escapeHtml(s.name) + '</span>';
    html += '<span class="' + ctrtClass + '">' + ctrtSign + s.ctrt.toFixed(2) + '%</span>';
    html += '<span class="stock-meta">' + s.vol_eok.toLocaleString() + '억</span>';
    html += '<span class="theme-tag">' + escapeHtml(s.theme) + '</span>';
    if (s.memo) html += '<span class="memo">' + escapeHtml(s.memo) + '</span>';
    if (st.decision) html += '<span class="decision-tag">→ ' + decisionLabel(st.decision) + '</span>';
    html += '</div>';

    const candidates = s.candidates || [];
    if (candidates.length === 0) {
      html += '<div class="empty">관련 기사 없음</div>';
    } else {
      const idx = st.chosen_idx || 0;
      const cand = candidates[idx];
      html += '<div class="nav-bar">';
      html += '<button class="btn-nav" data-act="prev" data-name="' + escapeHtml(s.name) + '"' + (idx === 0 ? ' disabled' : '') + '>⬅️ 이전</button>';
      html += '<span style="font-size:13px;color:#666;">후보 ' + (idx+1) + ' / ' + candidates.length + '</span>';
      html += '<button class="btn-nav" data-act="next" data-name="' + escapeHtml(s.name) + '"' + (idx >= candidates.length-1 ? ' disabled' : '') + '>다음 ➡️</button>';
      html += '<span style="margin-left:auto;font-size:12px;color:#999;">점수 ' + cand.score + '</span>';
      html += '</div>';
      html += '<div class="article">';
      html += '<div class="article-meta">';
      if (cand.date) html += '<span>' + escapeHtml(cand.date) + '</span> ';
      if (cand.is_today) html += '<span class="badge-today">오늘</span>';
      else if (cand.date) html += '<span class="badge-old">이전</span>';
      html += '<span class="badge-source">' + escapeHtml(cand.source || "naver") + '</span>';
      html += '</div>';
      html += '<div class="article-title"><a href="' + escapeHtml(cand.url) + '" target="_blank" rel="noopener">' + escapeHtml(cand.title) + '</a></div>';
      if (cand.desc) html += '<div class="article-desc">' + escapeHtml(cand.desc) + '</div>';
      html += '</div>';
    }

    html += '<div class="actions">';
    html += '<button class="btn-approve" data-act="approved" data-name="' + escapeHtml(s.name) + '">✅ 승인</button>';
    html += '<button class="btn-reject"  data-act="rejected" data-name="' + escapeHtml(s.name) + '">❌ 거부</button>';
    html += '<button class="btn-skip"    data-act="skipped"  data-name="' + escapeHtml(s.name) + '">⏭️ 스킵</button>';
    html += '</div>';

    card.innerHTML = html;
    cards.appendChild(card);
  });

  document.getElementById("progressBar").style.width = (done / DATA.length * 100) + "%";
  document.getElementById("progressText").textContent = done + " / " + DATA.length + " 검토";
}

document.addEventListener("click", (e) => {
  const btn = e.target.closest("button[data-act]");
  if (!btn) return;
  const name = btn.dataset.name;
  const act  = btn.dataset.act;
  if (act === "prev")      navCandidate(name, -1);
  else if (act === "next") navCandidate(name, 1);
  else                     decide(name, act);
});

render();
</script>
</body>
</html>
"""

    return (template
            .replace("__DATE__", date_str)
            .replace("__PUSH_ENDPOINT__", push_endpoint or "")
            .replace("__JSON_DATA__", json_data))


# ──────────────────────────────────────
# 메인
# ──────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="영웅문 CSV → Signal Evening 리포트")
    ap.add_argument("--csv",      default="",
                    help="영웅문 스크리너 CSV 파일 경로 (기본: 폴더 내 최신 .csv)")
    ap.add_argument("--date",     default=datetime.today().strftime("%Y%m%d"),
                    help="날짜 YYYYMMDD (기본: 오늘)")
    ap.add_argument("--no-fetch", action="store_true",
                    help="기사 본문 크롤링 생략 (빠름, 요약만)")
    ap.add_argument("--dry-run",  action="store_true",
                    help="뉴스 검색 없이 CSV 파싱만 테스트")
    ap.add_argument("--no-open",  action="store_true",
                    help="검수 HTML 자동 열기 비활성화")
    args = ap.parse_args()

    csv_path = resolve_csv_path(args.csv) if args.csv else find_latest_csv()
    if not csv_path:
        print("[오류] CSV 파일을 찾을 수 없습니다.")
        print("       영웅문 스크리너 결과를 이 폴더에 CSV로 저장하거나")
        print("       --csv 옵션으로 직접 지정하세요.")
        raise SystemExit(1)

    stocks = parse_heroesgate_csv(csv_path)
    if not stocks:
        raise SystemExit(1)

    stocks = assign_themes(stocks)

    theme_summary = defaultdict(int)
    for s in stocks:
        theme_summary[s["theme"]] += 1
    print("[테마] " + " | ".join(f"{t}:{n}" for t, n in
          sorted(theme_summary.items(), key=lambda x: -x[1])))

    # daily_picks_tracker — vault 에 daily signal 페이지 생성 + 재등장 알림
    # 메인 흐름과 분리: 실패해도 검수/푸시는 계속 진행.
    try:
        from daily_picks_tracker import process_csv as _dp_process_csv
        from pathlib import Path as _Path
        _dp = _dp_process_csv(_Path(csv_path))
        print(f"[daily_signal] vault 생성: {_dp['page_path'].name if _dp['page_path'] else '-'} "
              f"| 종목 {_dp['picks_count']}개 | 재등장 {_dp['reappear_count']}개")
    except Exception as _e:
        print(f"[daily_signal] 처리 실패 (메인 흐름은 계속): {_e}")

    today_str = datetime.strptime(args.date, "%Y%m%d").strftime("%Y.%m.%d")

    news_map: dict = {}
    review_data: list = []
    if not args.dry_run:
        print(f"\n[뉴스] {len(stocks)}개 종목 기사 검색 시작 (오늘: {today_str})...")
        for i, s in enumerate(stocks, 1):
            name = s["name"]
            print(f"  [{i:2d}/{len(stocks)}] {name}...", end="", flush=True)
            candidates, variants = _collect_candidates(s, today_str)
            art = pick_best_article(candidates, variants, fetch_body=not args.no_fetch)
            if art:
                today_flag = "✅" if art.get("is_today") else "⚠️"
                print(f" {today_flag} {art['date']} [{art['title'][:30]}]")
            else:
                print(" ❌ 기사 없음")
            news_map[name] = art if art else None
            # 검수용 데이터 누적 (상위 8개 후보)
            review_data.append({
                "name":    name,
                "code":    s.get("code", ""),
                "ctrt":    s["ctrt"],
                "vol_eok": s["vol_eok"],
                "theme":   s["theme"],
                "memo":    (s.get("memo") or "")[:80],
                "candidates": [
                    {
                        "title":    c["title"],
                        "url":      c["url"],
                        "desc":     (c.get("desc") or "")[:300],
                        "date":     c.get("date", ""),
                        "score":    c.get("score", 0),
                        "is_today": c.get("is_today", False),
                        "source":   c.get("source", "naver"),
                    }
                    for c in candidates[:8]
                ],
                "best_url": art.get("url", "") if art else "",
            })
    else:
        print("[dry-run] 뉴스 검색 생략")
        news_map = {s["name"]: None for s in stocks}
        review_data = [
            {"name": s["name"], "code": s.get("code",""), "ctrt": s["ctrt"],
             "vol_eok": s["vol_eok"], "theme": s["theme"], "memo": (s.get("memo") or "")[:80],
             "candidates": [], "best_url": ""}
            for s in stocks
        ]

    index_info = {}
    if not args.dry_run:
        print("\n[지수] 코스피/코스닥 조회...", end="", flush=True)
        index_info = fetch_index_info()
        print(f" {index_info.get('kospi','?')} / {index_info.get('kosdaq','?')}")

    # 노션 자동 푸시용 로컬 helper 서버 (백그라운드)
    push_server, push_port = (None, None)
    if not args.dry_run:
        push_server, push_port = _start_push_server()
        if push_port:
            print(f"[helper] 노션 푸시 서버 가동: http://127.0.0.1:{push_port}")
    push_endpoint = f"http://127.0.0.1:{push_port}" if push_port else ""

    # 검수 페이지 (HTML) — reports/review_YYYYMMDD.html
    reports_dir = os.path.join(BASE_DIR, "reports")
    os.makedirs(reports_dir, exist_ok=True)
    review_html = build_review_html(review_data, args.date, push_endpoint=push_endpoint)
    review_path = os.path.join(reports_dir, f"review_{args.date}.html")
    with open(review_path, "w", encoding="utf-8") as f:
        f.write(review_html)

    today_count = sum(1 for a in news_map.values() if a and a.get("is_today"))
    print(f"\n✅ 검수 페이지: reports/review_{args.date}.html")
    print(f"   종목 {len(stocks)}개 | 당일 기사 {today_count}/{len(stocks)}건")

    # 검수 HTML 자동 오픈 (--no-open 으로 끌 수 있음)
    if not args.no_open:
        try:
            webbrowser.open(f"file://{os.path.abspath(review_path)}")
            print(f"   🌐 브라우저에서 검수 페이지 열림")
        except Exception as e:
            print(f"   ⚠️ 브라우저 자동 열기 실패: {e}")

    # helper 서버 가동 중이면 푸시 완료까지 대기 → 완료 시 자동 종료
    if push_server:
        print("\n[helper] '📤 노션에 푸시' 버튼을 누르면 종목재료정리 DB 에 즉시 반영됩니다.")
        print("[helper] 푸시가 완료되면 이 단계는 자동 종료됩니다. (푸시 안 할 거면 Ctrl+C)")
        try:
            _PUSH_DONE.wait()
            # 브라우저가 완료 상태를 확인할 시간을 줌 (최대 10초)
            _PUSH_ACK.wait(timeout=10)
            r = _PUSH_STATE.get("result") or {}
            print(f"\n[helper] ✅ 노션 푸시 완료 — 총 {r.get('total', 0)}건 "
                  f"/ 신규 {r.get('created', 0)}, 업데이트 {r.get('updated', 0)}"
                  + (f", 실패 {len(r['errors'])}건" if r.get("errors") else ""))
            print("[helper] 서버 자동 종료 — 다음 단계로 진행합니다.")
        except KeyboardInterrupt:
            print("\n[helper] 종료.")
        push_server.shutdown()


if __name__ == "__main__":
    main()
