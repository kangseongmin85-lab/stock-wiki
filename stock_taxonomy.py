#!/usr/bin/env python3
"""
stock_taxonomy.py — 종목재료정리 DB → 종목명별 (카테고리·테마) 룩업 테이블

사용자가 Notion '종목재료정리' DB에 이미 분류해 둔 종목명·카테고리·관련테마를
로컬 JSON 캐시로 만든다. fetch_market_window.py 가 import해서 종목 식별·산업 분류에 사용.

캐시 위치: _cache/stock_taxonomy.json
캐시 TTL: 기본 7일. 강제 갱신은 --refresh.

사용법:
  python stock_taxonomy.py               # 캐시 빌드 or 재사용
  python stock_taxonomy.py --refresh     # 강제 재페치
  python stock_taxonomy.py --stats       # 캐시 통계만 출력
"""

import os, sys, json, argparse
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path
from collections import Counter

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

BASE_DIR        = Path(__file__).parent
CONFIG_PATH     = BASE_DIR / "config.json"
TAXONOMY_CACHE  = BASE_DIR / "_cache" / "stock_taxonomy.json"
TAXONOMY_TTL    = timedelta(days=7)

# 종목재료정리 DB (ingest_all.py 의 DB_ID 와 동일 — 사용자가 분류해 둔 본진)
JAEROO_DB_ID = "2cbffbf4-6173-80e3-8d07-e8b5e59e36c4"


def _load_config():
    if CONFIG_PATH.exists():
        try:
            return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


_CFG = _load_config()
NOTION_TOKEN = _CFG.get("NOTION_TOKEN", "") or os.getenv("NOTION_TOKEN", "")


def _prop_text(prop):
    if not prop:
        return ""
    t = prop.get("type", "")
    if t == "title":
        return "".join(p.get("plain_text", "") for p in prop.get("title", []))
    if t == "rich_text":
        return "".join(p.get("plain_text", "") for p in prop.get("rich_text", []))
    if t == "select":
        s = prop.get("select")
        return s.get("name", "") if s else ""
    if t == "multi_select":
        return ", ".join(s.get("name", "") for s in prop.get("multi_select", []))
    return ""


def _prop_multiselect(prop):
    """관련테마 같은 multi_select 컬럼은 리스트로 그대로."""
    if not prop:
        return []
    if prop.get("type") != "multi_select":
        return []
    return [s.get("name", "").strip() for s in prop.get("multi_select", []) if s.get("name")]


def fetch_full_jaeroo_db():
    """종목재료정리 DB 전체 페이지 페치."""
    if not NOTION_TOKEN:
        raise RuntimeError("NOTION_TOKEN 없음 — config.json 또는 환경변수 확인")
    pages, cursor = [], None
    for _ in range(200):  # 최대 20,000 pages 가드
        body = {"page_size": 100}
        if cursor:
            body["start_cursor"] = cursor
        req = urllib.request.Request(
            f"https://api.notion.com/v1/databases/{JAEROO_DB_ID}/query",
            data=json.dumps(body).encode("utf-8"),
            headers={
                "Authorization":  f"Bearer {NOTION_TOKEN}",
                "Notion-Version": "2022-06-28",
                "Content-Type":   "application/json",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=30) as r:
            data = json.loads(r.read())
        pages.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
    return pages


def build_taxonomy(pages):
    """종목명 → {category, themes, last_seen} 룩업 dict."""
    taxonomy = {}
    for page in pages:
        props = page.get("properties", {})
        name = _prop_text(props.get("종목명")).strip()
        if not name:
            continue
        cat    = (_prop_text(props.get("카테고리")) or "기타").strip()
        themes = _prop_multiselect(props.get("관련테마"))
        last_edit = page.get("last_edited_time", "")

        # 같은 종목이 여러 행이면 카테고리·테마 합치기 (가장 최근 우선)
        if name in taxonomy:
            existing = taxonomy[name]
            # 테마는 union
            merged_themes = list(dict.fromkeys(existing["themes"] + themes))
            # 카테고리는 최신 페이지 값 우선
            if last_edit > existing.get("last_seen", ""):
                taxonomy[name] = {
                    "category":  cat,
                    "themes":    merged_themes,
                    "last_seen": last_edit,
                }
            else:
                existing["themes"] = merged_themes
        else:
            taxonomy[name] = {
                "category":  cat,
                "themes":    themes,
                "last_seen": last_edit,
            }
    return taxonomy


def load_taxonomy(force_refresh=False):
    """캐시 우선. TTL 초과 또는 force_refresh 면 재페치."""
    if not force_refresh and TAXONOMY_CACHE.exists():
        age = datetime.now() - datetime.fromtimestamp(TAXONOMY_CACHE.stat().st_mtime)
        if age < TAXONOMY_TTL:
            data = json.loads(TAXONOMY_CACHE.read_text(encoding="utf-8"))
            print(f"  [taxonomy] 캐시 사용 ({len(data['stocks'])}종목, {age.days}일 전)")
            return data

    print(f"  [taxonomy] Notion 종목재료정리 DB 전체 페치...")
    pages = fetch_full_jaeroo_db()
    print(f"  [taxonomy] {len(pages)} pages 수신")
    stocks = build_taxonomy(pages)

    data = {
        "built_at": datetime.now().isoformat(),
        "source":   f"notion_db:{JAEROO_DB_ID}",
        "n_stocks": len(stocks),
        "stocks":   stocks,
    }
    TAXONOMY_CACHE.parent.mkdir(parents=True, exist_ok=True)
    TAXONOMY_CACHE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"  [taxonomy] 캐시 저장: {TAXONOMY_CACHE} ({len(stocks)}종목)")
    return data


def print_stats(data):
    stocks = data["stocks"]
    print(f"\n═══════════════════════════════════════════════")
    print(f"  종목 분류 통계 (총 {len(stocks)}개 종목)")
    print(f"  빌드: {data.get('built_at', '?')[:19]}")
    print(f"═══════════════════════════════════════════════")

    # 카테고리 분포
    cats = Counter(s["category"] for s in stocks.values())
    print(f"\n── 카테고리 분포 (상위 20) ──")
    for cat, n in cats.most_common(20):
        print(f"  {cat:<20} {n:>5}개")

    # 테마 분포
    theme_counter = Counter()
    for s in stocks.values():
        for t in s["themes"]:
            theme_counter[t] += 1
    print(f"\n── 관련테마 분포 (상위 30, 총 {len(theme_counter)}개 테마) ──")
    for theme, n in theme_counter.most_common(30):
        print(f"  {theme:<25} {n:>4}개 종목")

    # 테마 없는 종목 비율
    no_theme = sum(1 for s in stocks.values() if not s["themes"])
    print(f"\n── 분류 품질 ──")
    print(f"  테마 없는 종목: {no_theme}개 ({no_theme/len(stocks)*100:.1f}%)")
    multi_theme = sum(1 for s in stocks.values() if len(s["themes"]) >= 3)
    print(f"  테마 3개 이상: {multi_theme}개 ({multi_theme/len(stocks)*100:.1f}%)")


def main():
    parser = argparse.ArgumentParser(description="종목 분류 룩업 테이블 빌더")
    parser.add_argument("--refresh", action="store_true", help="캐시 무시하고 강제 재페치")
    parser.add_argument("--stats",   action="store_true", help="통계만 출력")
    args = parser.parse_args()

    if args.stats and not args.refresh and TAXONOMY_CACHE.exists():
        data = json.loads(TAXONOMY_CACHE.read_text(encoding="utf-8"))
    else:
        data = load_taxonomy(force_refresh=args.refresh)

    print_stats(data)




# ══════════════════════════════════════════════════════════════════════
#  산업 묶음 매핑 (시그널 리포트 스타일)
# ══════════════════════════════════════════════════════════════════════
# 룰: 종목의 카테고리·테마 모두 검사. 아래 순서대로 첫 매치 채택.
#     매칭 안 되면 "🗂 기타". "정치" 카테고리는 INDUSTRY_GROUPS 에 없으므로
#     종목이 가진 다른 테마(예: 이재명+건설) 의 산업 키워드로 자동 분류됨.

INDUSTRY_GROUPS = [
    # specific-first: ESS 가 "전력저장장치" 라서 전력 키워드와 충돌. 이차전지를 먼저 검사.
    ("🔋 이차전지/ESS", {
        "categories": [],
        "theme_keywords": ["ESS", "2차전지", "이차전지", "배터리"],
    }),
    ("🔌 전력/에너지", {
        "categories": ["에너지", "원자재"],
        "theme_keywords": ["전력기기", "태양광", "풍력", "수소", "발전", "원자력"],
    }),
    ("💾 반/디플", {
        "categories": ["반도체"],
        "theme_keywords": ["반도체", "HBM", "유리기판", "낸드", "DRAM", "파운드리"],
    }),
    ("🚢 조선/방산", {
        "categories": ["조선/해운"],
        "theme_keywords": ["조선", "방산", "함정", "무기"],
    }),
    ("🤖 AI/로봇", {
        "categories": ["AI/데이터센터/로봇"],
        "theme_keywords": ["로봇", "AI", "휴머노이드", "자율주행"],
    }),
    ("💊 바이오/의료", {
        "categories": ["바이오/의료AI", "전염병/질병", "바이오/의료AI, 전염병/질병"],
        "theme_keywords": ["면역항암", "비만치료", "의료AI", "유전자치료", "임플란트", "코로나"],
    }),
    ("🚗 자동차", {
        "categories": ["자동차"],
        "theme_keywords": ["자동차", "전기차"],
    }),
    ("🛰️ 우주/항공", {
        "categories": ["우주/항공"],
        "theme_keywords": ["우주", "위성", "항공"],
    }),
    ("🎬 IP/엔터", {
        "categories": ["IP/엔터"],
        "theme_keywords": ["게임", "엔터", "미디어", "콘텐츠"],
    }),
    ("🏗️ 건설/인프라", {
        "categories": ["건설/인프라"],
        "theme_keywords": ["건설", "인프라", "토목"],
    }),
    ("💰 금융", {
        "categories": ["금융"],
        "theme_keywords": ["밸류업", "저PBR", "증권", "은행", "보험"],
    }),
]


def map_to_industry(category: str, themes: list) -> str:
    """종목의 카테고리·테마 → 산업 묶음 (시그널 스타일).
    매칭 안 되면 '🗂 기타'.
    """
    cat_norm = (category or "").strip()
    themes_lower = " ".join(themes).lower()

    for group_name, rule in INDUSTRY_GROUPS:
        # 카테고리 정확 매치
        if cat_norm in rule["categories"]:
            return group_name
        # 카테고리에 콤마 구분된 복합값(예: "바이오/의료AI, 전염병/질병")
        cat_parts = [c.strip() for c in cat_norm.split(",")]
        if any(c in rule["categories"] for c in cat_parts):
            return group_name
        # 테마 키워드 매치 (테마명에 키워드 포함)
        for kw in rule["theme_keywords"]:
            if kw.lower() in themes_lower:
                return group_name
    return "🗂 기타"


# ══════════════════════════════════════════════════════════════════════
#  종목명 추출 (기사 제목·내용 → 매칭된 종목명 리스트)
# ══════════════════════════════════════════════════════════════════════

def build_stock_name_index(taxonomy: dict) -> list:
    """taxonomy 의 모든 종목명을 길이 내림차순으로 정렬해 반환.
    긴 종목명을 먼저 매칭해야 'LG' 가 'LG에너지솔루션' 보다 먼저 매칭되는 오류 방지.
    """
    names = list(taxonomy.keys())
    # 너무 짧은 종목명(1자) 제외 — false positive 너무 큼
    names = [n for n in names if len(n) >= 2]
    names.sort(key=len, reverse=True)
    return names


def _is_hangul(ch: str) -> bool:
    return "가" <= ch <= "힣"


# 종목명 뒤에 자주 붙는 한국어 조사 시작 글자
# 예: 레이는, 기아가, 레이의, 대상도 등은 정상 매칭으로 인정
_PARTICLE_STARTS = frozenset("은는이가을를의도만와과에로")


def _find_with_boundary(text: str, name: str) -> list:
    """좌·우가 한글이 아닐 때만 매칭 위치 반환.
    한글 2자 종목명이 합성어/외래어(클레이, 디스플레이, 레이저, 기아자동차 등)
    안에서 substring 매칭되는 false positive 방지.
    우측이 한국어 조사로 시작하면 매칭 허용(레이는, 기아가 등).
    """
    n = len(name)
    positions = []
    i = 0
    while i <= len(text) - n:
        if text[i:i + n] == name:
            left_ok = (i == 0) or not _is_hangul(text[i - 1])
            if not left_ok:
                i += 1
                continue
            if i + n == len(text):
                right_ok = True
            else:
                right_ch = text[i + n]
                right_ok = (not _is_hangul(right_ch)) or (right_ch in _PARTICLE_STARTS)
            if right_ok:
                positions.append(i)
                i += n
                continue
        i += 1
    return positions


def extract_stocks(text: str, stock_names_sorted: list) -> list:
    """text 에서 매칭된 종목명 리스트(중복 포함, 같은 종목 N번 언급되면 N개).
    매칭 후 마스킹해서 부분 문자열 false positive 방지.
    한글 2자 종목명은 좌·우 한글 경계 확인(합성어 false positive 방지).
    """
    if not text:
        return []
    masked = list(text)
    found = []
    for name in stock_names_sorted:
        n = len(name)
        joined = "".join(masked)
        # 한글 2자 종목명만 단어 경계 검사 (3자 이상은 false positive 위험 작음)
        if n <= 2 and all(_is_hangul(c) for c in name):
            positions = _find_with_boundary(joined, name)
        else:
            positions = []
            start = 0
            while True:
                idx = joined.find(name, start)
                if idx == -1:
                    break
                positions.append(idx)
                start = idx + n
        for pos in positions:
            found.append(name)
            for j in range(n):
                masked[pos + j] = "·"
    return found


if __name__ == "__main__":
    main()
