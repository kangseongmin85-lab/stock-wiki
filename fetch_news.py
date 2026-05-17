#!/usr/bin/env python3
# Last patched: 2026-05-17 (2026-05-17T05:26:02.285753) — dedup wiki fallback + pubDate skip + 네이버 dt 필터
"""
fetch_news.py — 주식 뉴스 모니터링 + 텔레그램 알림

소스:
  1. 네이버 금융 뉴스 (m.stock.naver.com, 비공식 API)
  2. 네이버 검색 API (공식, 활성 테마 검색어 기반)
  3. DART 공시 (당일 발행분)
  4. RSS: 한경/매경/이데일리/인베스팅/연합/아경/서경/파이낸셜/이투데이/뉴스토마토/뉴스팜 (11개 매체 30개 URL)
  5. YouTube RSS (채널별 신규 영상 알림)

출력:
  - 텔레그램 알림
  - Notion DB (35bffbf4617381d2a19bf264d5616563) 저장
  - wiki/news/뉴스_YYYY-MM-DD.md 누적 저장

실행:
  python fetch_news.py              # 일반 실행
  python fetch_news.py --dry-run    # 텔레그램 전송 없이 콘솔 출력만
  python fetch_news.py --digest     # 일일 요약 강제 발송
  python fetch_news.py --test       # 텔레그램 연결 테스트 (무조건 1건 전송)
"""

import os, json, re, time, argparse, urllib.request, urllib.parse, urllib.error, hashlib
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

TOKEN    = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID  = os.getenv("TELEGRAM_CHAT_ID", "")
DART_KEY = os.getenv("DART_API_KEY", "")
TG_API_ID   = int(os.getenv("TELEGRAM_API_ID", "0") or "0")
TG_API_HASH = os.getenv("TELEGRAM_API_HASH", "")
TG_SESSION  = os.getenv("SESSION_STRING", "")

BASE_DIR   = Path(__file__).parent
SEEN_FILE  = BASE_DIR / "wiki" / "news" / "seen_ids.json"
NEWS_DIR   = BASE_DIR / "wiki" / "news"
STOCKS_DIR = BASE_DIR / "wiki" / "stocks"
THEMES_DIR = BASE_DIR / "wiki" / "themes"

HDR = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer":    "https://m.stock.naver.com/",
}

# ── RSS 소스 정의 (2026-05-10 검증, 11개 매체 / 30개 URL) ────────────────────
RSS_SOURCES = {
    "한국경제": [
        "https://www.hankyung.com/feed/all-news",
        "https://www.hankyung.com/feed/economy",
        "https://www.hankyung.com/feed/finance",
        "https://www.hankyung.com/feed/it",
    ],
    "매일경제": [
        "https://www.mk.co.kr/rss/30000001/",
        "https://www.mk.co.kr/rss/30100041/",
        "https://www.mk.co.kr/rss/40300001/",
    ],
    "이데일리": [
        "http://rss.edaily.co.kr/edaily_news.xml",
    ],
    "인베스팅닷컴": [
        "https://kr.investing.com/rss/news_25.rss",
        "https://kr.investing.com/rss/news_14.rss",
    ],
    "연합뉴스": [
        "https://www.yna.co.kr/rss/economy.xml",
    ],
    "아시아경제": [
        "https://www.asiae.co.kr/rss/economy.htm",
    ],
    "서울경제": [
        "https://www.sedaily.com/rss/economy",
        "https://www.sedaily.com/rss/finance",
        "https://www.sedaily.com/rss/newsall",
    ],
    "파이낸셜뉴스": [
        "https://www.fnnews.com/rss/r20/fn_realnews_stock.xml",
        "https://www.fnnews.com/rss/r20/fn_realnews_finance.xml",
        "https://www.fnnews.com/rss/r20/fn_realnews_economy.xml",
    ],
    "이투데이": [
        "https://rss.etoday.co.kr/eto/etoday_news_all.xml",
        "https://rss.etoday.co.kr/eto/market_news.xml",
        "https://rss.etoday.co.kr/eto/finance_news.xml",
        "https://rss.etoday.co.kr/eto/economy_news.xml",
    ],
    "뉴스토마토": [
        "https://www.newstomato.com/rss",
    ],
    "뉴스팜": [
        "https://www.newsfarm.co.kr/rss/allArticle.xml",
    ],
}
# 제거됨 (2026-05-10): 머니투데이(403 정책차단), 헤럴드경제(홈리다이렉트),
# 조선비즈(SPA), 인포스탁(SPA), 조세일보(없음), RSShub 텔레그램(403)

# ── YouTube RSS 채널 (채널 ID 기반) ──────────────────────────────────────────
# 추가하려면: {"채널명": "채널ID"} 형식으로 입력
# 채널 ID는 유튜브 채널 페이지 소스에서 "channelId" 검색
YOUTUBE_CHANNELS = {
    # 예시 (주석 해제 후 실제 채널 ID로 변경):
    # "삼프로TV": "UC3Bk9OVSbBhKqCvFgIWNGpg",
    # "한국경제TV": "UCCwZqKgkfSDYnAZvLM4cGDg",
}

# ── 텔레그램 공개 채널 목록 (username 기준) ───────────────────────────────────
TELEGRAM_CHANNELS = {
    "FastStockNewsUSA":  "미국증시",
    "FastStockNews":     "한국증시",
    "YeouidoStory2":     "리포트/뉴스",
    "bornlupin":         "투자정보",
    "itechkorea":        "미국증시",
    "FS_public_channel": "한국증시",
    "bdragon0808":       "바이오/제약",
    "pharmbiohana":      "바이오/제약",
    "huhpharm":          "바이오/제약",
}


# ─── Notion / 네이버 검색 API 설정 (2026-05-10 추가) ─────────────────────────
def _load_config():
    """config.json에서 키 로드 (.env 미지정시 fallback)"""
    cfg_path = BASE_DIR / "config.json"
    if cfg_path.exists():
        try:
            return json.loads(cfg_path.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}

_CFG = _load_config()
# config.json을 진실의 원천(SoT)으로 사용 — .env에 잘못된/오래된 토큰이 있어도 config.json이 우선
NOTION_TOKEN     = _CFG.get("NOTION_TOKEN", "")     or os.getenv("NOTION_TOKEN", "")
NOTION_DB_ID     = _CFG.get("NOTION_DB_ID", "")     or os.getenv("NOTION_DB_ID", "")
# 2026-05-17 추가 ── 주간 DB 로테이션 (옵트인) ─────────────────────────────
# config.json/.env 에 WEEKLY_ROTATION=true 면 활성화. 현재 주차 DB 자동 생성/전환.
WEEKLY_ROTATION  = str(_CFG.get("WEEKLY_ROTATION", "") or os.getenv("WEEKLY_ROTATION", "")).lower() in ("1", "true", "yes")
if WEEKLY_ROTATION and NOTION_TOKEN and NOTION_DB_ID:
    try:
        from weekly_db import resolve_active_db_id
        _resolved = resolve_active_db_id(NOTION_TOKEN, NOTION_DB_ID)
        if _resolved and _resolved != NOTION_DB_ID:
            print(f"[weekly_db] 활성 DB 전환: {NOTION_DB_ID[:8]}.. -> {_resolved[:8]}..")
            NOTION_DB_ID = _resolved
    except Exception as _e:
        print(f"[weekly_db] 로테이션 실패 — 기존 DB 유지: {_e}")
NAVER_CLIENT_ID  = _CFG.get("NAVER_CLIENT_ID", "")  or os.getenv("NAVER_CLIENT_ID", "")
NAVER_CLIENT_SEC = _CFG.get("NAVER_CLIENT_SECRET", "") or os.getenv("NAVER_CLIENT_SECRET", "")

# 활성 테마 (wiki/index.md "활성 🔥" 기준 — 네이버 검색 API 검색어로 사용)
# 2026-05-17 수정: "조선"→"조선업"(북한/드라마 노이즈 차단), "원전" 사용자 요청으로 복원
# 참고: "원전"·"원자력" 중복 매칭은 dedup이 잡음 (fallback 제거로 제목 매칭 필수)
NAVER_SEARCH_QUERIES = [
    "HBM", "AI데이터센터", "방산", "로봇", "원자력", "원전", "이차전지",
    "자율주행", "휴머노이드", "조선업",
]

def fetch_naver_search(keywords, cutoff_hours=0.25):
    """네이버 검색 API: 활성 테마별 최신 뉴스 (15분 윈도우)
    2026-05-17 강화: pubDate 시간 필터 추가 (실시간만)
    """
    if not (NAVER_CLIENT_ID and NAVER_CLIENT_SEC):
        return []
    articles = []
    cutoff = datetime.now(timezone.utc) - timedelta(hours=cutoff_hours)
    for q in NAVER_SEARCH_QUERIES:
        try:
            qenc = urllib.parse.quote(q)
            url  = f"https://openapi.naver.com/v1/search/news.json?query={qenc}&display=10&sort=date"
            req  = urllib.request.Request(url, headers={
                "X-Naver-Client-Id":     NAVER_CLIENT_ID,
                "X-Naver-Client-Secret": NAVER_CLIENT_SEC,
            })
            with urllib.request.urlopen(req, timeout=10) as r:
                data = json.loads(r.read())
            for item in data.get("items", []):
                # 2026-05-17 추가: pubDate cutoff
                pub_raw = item.get("pubDate", "").strip()
                if not pub_raw:
                    continue
                pub_dt = None
                try:
                    from email.utils import parsedate_to_datetime
                    pub_dt = parsedate_to_datetime(pub_raw)
                except Exception:
                    pass
                if pub_dt is None:
                    continue
                if pub_dt.tzinfo is None:
                    pub_dt = pub_dt.replace(tzinfo=timezone.utc)
                if pub_dt < cutoff:
                    continue
                title = re.sub(r"<[^>]+>", "", item.get("title", "")).replace("&quot;", '"').replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">").replace("&apos;", "'").strip()
                link  = item.get("originallink") or item.get("link", "")
                desc  = re.sub(r"<[^>]+>", "", item.get("description", ""))[:200]
                if not title or is_blocked(title):
                    continue
                matched = [kw for kw in keywords if kw in title]
                # 2026-05-17: fallback 제거. 제목에 키워드 없으면 skip
                if not matched:
                    continue
                articles.append({
                    "id":      f"link_{stable_id(link or title)}",
                    "title":   title,
                    "link":    link,
                    "desc":    desc,
                    "matched": matched[:3],
                    "source":  f"네이버검색({q})",
                    "dart":    False,
                })
        except Exception as e:
            print(f"[네이버검색 오류] {q}: {e}")
    return articles

def save_to_notion(article):
    """기사 1건을 Notion DB에 저장 (bot.py와 동일 스키마)"""
    if not (NOTION_TOKEN and NOTION_DB_ID):
        return
    title    = article["title"][:100] or "(제목 없음)"
    link     = article["link"]
    source   = article["source"]
    matched  = article["matched"]
    is_dart  = article["dart"]
    desc     = article.get("desc", "")
    now_iso  = datetime.now().isoformat()
    importance = {
        "🚨 긴급": "높음", "🔥 중요": "높음", "📋 공시": "높음",
        "📰 주목": "보통", "📄 일반": "낮음",
    }.get(urgency(article["title"], is_dart), "보통")
    if "DART" in source:
        category = "공시"
    elif "네이버" in source:
        category = "네이버"
    elif source in ("한국경제", "매일경제", "이데일리", "인베스팅닷컴", "연합뉴스",
                    "아시아경제", "서울경제", "파이낸셜뉴스", "이투데이",
                    "뉴스토마토", "뉴스팜"):
        category = "RSS"
    else:
        category = "기타"
    properties = {
        "제목":     {"title": [{"text": {"content": title}}]},
        "채널":     {"select": {"name": source[:100]}},
        "카테고리": {"select": {"name": category}},
        "테마태그": {"multi_select": [{"name": m[:100]} for m in matched[:5]]},
        "중요도":   {"select": {"name": importance}},
        "날짜":     {"date": {"start": now_iso}},
        "원문내용": {"rich_text": [{"text": {"content": (desc or title)[:2000]}}]},
    }
    if link:
        properties["원문링크"] = {"url": link}
    payload = json.dumps({
        "parent":     {"database_id": NOTION_DB_ID},
        "properties": properties,
    }).encode("utf-8")
    req = urllib.request.Request(
        "https://api.notion.com/v1/pages",
        data=payload,
        headers={
            "Authorization":   f"Bearer {NOTION_TOKEN}",
            "Content-Type":    "application/json",
            "Notion-Version":  "2022-06-28",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            r.read()
    except Exception as e:
        print(f"[Notion 저장 오류] {title[:40]}: {e}")

# ─── 안정적 ID 생성 (Python hash() 랜덤 시드 회피) ──────────────────────────
def stable_id(s):
    """결정론적 hash: 같은 입력 → 항상 같은 출력 (Python 재시작 무관)"""
    return hashlib.md5((s or "").encode("utf-8")).hexdigest()[:16]

def url_key(url):
    """URL 정규화 후 hash - 같은 기사가 다른 도메인/모바일/https 로 들어와도 dedup
    2026-05-17 추가: 네이버 미러, 모바일 URL, http/https 차이 흡수
    """
    if not url:
        return ""
    try:
        from urllib.parse import urlparse
        parsed = urlparse(url.strip().lower())
        path = parsed.path.rstrip("/")
        for ext in (".html", ".htm", ".do", ".jsp", ".aspx"):
            if path.endswith(ext):
                path = path[:-len(ext)]
        return f"urlpath_{stable_id(path)}"
    except Exception:
        return f"urlpath_{stable_id(url)}"

def title_key(t):
    """제목 정규화 후 hash — 같은 기사가 다른 source/URL/말머리 로 들어와도 dedup
    2026-05-17 강화: [속보]/[단독]/(긴급) 등 접두사 제거 + 구두점/특수문자 제거
    """
    norm = (t or "").strip().lower()
    # 말머리: [속보], [단독], (긴급), <기획> 등 — 시작 위치 또는 본문 중간
    norm = re.sub(r"[\[\(<\u3008\uff3b\uff08][^\]\)>\u3009\uff3d\uff09]{1,15}[\]\)>\u3009\uff3d\uff09]", "", norm)
    norm = re.sub(r"\s+", "", norm)               # 공백 제거
    norm = re.sub(r"[\"'\.,?!…·•\-_~/\\]", "", norm)  # 구두점/기호 제거
    return f"title_{stable_id(norm)}"


# ── 키워드 로드 ───────────────────────────────────────────────────────────────
# 노이즈 키워드 제외 리스트 — 너무 광범위하게 매칭되어 false positive 유발
# 위키 파일은 보존하되 매칭 키워드에서만 제외
EXCLUDE_KEYWORDS = {
    "금", "은",   # 1글자 테마 — 일반 기사에 빈번히 등장
    "남성",       # 종목명이지만 일반 기사("30대 남성", "남성복" 등)에 빈번히 등장
    "DSR",        # 종목명이지만 일반 기사(부동산 DSR 규제 등)에 빈번히 등장 — DSR제강은 보존
}

def load_keywords():
    keywords = set()
    if STOCKS_DIR.exists():
        for f in STOCKS_DIR.glob("*.md"):
            if f.name != "_TEMPLATE.md":
                keywords.add(f.stem)
    if THEMES_DIR.exists():
        for f in THEMES_DIR.glob("*.md"):
            if f.name == "_TEMPLATE.md":
                continue
            # 신규상장 분기별 노이즈 테마 제외
            if "신규상장" in f.stem:
                continue
            keywords.add(f.stem)
    keywords.update([
        "코스피", "코스닥", "나스닥", "S&P", "반도체", "금리", "환율",
        "외국인", "기관", "공매도", "수급", "FOMC", "엔비디아", "HBM",
        "이차전지", "방산", "바이오", "AI", "로봇", "자율주행",
        "급등", "상한가", "하한가", "서킷브레이커", "사이드카",
        "실적", "수주", "계약", "인수", "합병", "FDA", "임상",
        "특징주",
    ])
    # 노이즈 키워드 제거
    keywords -= EXCLUDE_KEYWORDS
    return keywords

# ── seen_ids 관리 ─────────────────────────────────────────────────────────────
def load_seen():
    """Notion DB 의 최근 2시간 내 항목에서 URL+제목 을 seen set 으로 로드.
    GitHub Actions cache 의존성 제거 (캐시가 신뢰 불가). Notion 자체가 ground truth.
    실패 시 로컬 seen_ids.json 으로 fallback (로컬 dry-run 등 토큰 없는 경우 포함)."""
    # 1순위: Notion query
    if NOTION_TOKEN and NOTION_DB_ID:
        try:
            # 2시간 윈도우 — RSS cutoff 와 일치. cron 12회분 (10분 간격) 커버.
            # timeout=30s × 10 페이지 = 최대 5분, 실제는 보통 20~40초.
            cutoff_dt = datetime.now() - timedelta(hours=6)  # 2026-05-17: 2h->6h dedup 강화
            cutoff = cutoff_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
            seen = set()
            cursor = None
            for _ in range(10):  # 최대 1000건 (stocknews 정지 시 충분)
                body = {
                    "page_size": 100,
                    "filter": {"timestamp": "created_time", "created_time": {"after": cutoff}},
                }
                if cursor:
                    body["start_cursor"] = cursor
                req = urllib.request.Request(
                    f"https://api.notion.com/v1/databases/{NOTION_DB_ID}/query",
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
                for pg in data.get("results", []):
                    pp = pg["properties"]
                    url = (pp.get("원문링크") or {}).get("url") or ""
                    if url:
                        seen.add(f"link_{stable_id(url)}")
                        uk = url_key(url)
                        if uk:
                            seen.add(uk)
                    title_blocks = (pp.get("제목") or {}).get("title", [])
                    title = "".join(t.get("plain_text", "") for t in title_blocks)
                    if title:
                        seen.add(title_key(title))
                if not data.get("has_more"):
                    break
                cursor = data.get("next_cursor")
            print(f"  [seen] Notion 최근 2h: {len(seen)} entry 로드", flush=True)
            return seen
        except Exception as e:
            print(f"  [seen] Notion 로드 실패 ({e}) — 로컬 파일 fallback", flush=True)
    # 2순위: wiki/news/뉴스_*.md 파일 파싱 (2026-05-17 추가)
    # Notion 실패 + 로컬 비어있어도 매 run wiki 파일에서 URL/제목 재구성
    seen = set()
    today = datetime.now().strftime("%Y-%m-%d")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    for date_str in (today, yesterday):
        wiki_path = NEWS_DIR / f"뉴스_{date_str}.md"
        if wiki_path.exists():
            try:
                content = wiki_path.read_text(encoding="utf-8")
                for url in re.findall(r'href="([^"]+)"', content):
                    seen.add(f"link_{stable_id(url)}")
                    uk = url_key(url)
                    if uk:
                        seen.add(uk)
                for title in re.findall(r'<a [^>]+>([^<]+)</a>', content):
                    seen.add(title_key(title))
            except Exception:
                pass
    if seen:
        print(f"  [seen] wiki fallback: {len(seen)} entry 로드", flush=True)

    # 3순위: 로컬 seen_ids.json (로컬 dry-run 또는 Notion 다운 시)
    SEEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    if SEEN_FILE.exists():
        try:
            seen.update(json.loads(SEEN_FILE.read_text(encoding="utf-8")))
        except Exception:
            pass
    return seen

def save_seen(seen):
    lst = list(seen)[-5000:]
    SEEN_FILE.write_text(json.dumps(lst, ensure_ascii=False), encoding="utf-8")

# ── 텔레그램 전송 ─────────────────────────────────────────────────────────────
def tg_send(text, dry_run=False):
    if dry_run:
        print(f"\n[DRY-RUN] {text}\n")
        return
    if not TOKEN or not CHAT_ID:
        print("[경고] 텔레그램 토큰/채팅ID 없음")
        return
    url  = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    data = urllib.parse.urlencode({
        "chat_id":    CHAT_ID,
        "text":       text[:4000],
        "parse_mode": "HTML",
    }).encode()
    for attempt in range(3):
        try:
            with urllib.request.urlopen(url, data=data, timeout=10) as r:
                resp = json.loads(r.read())
                if resp.get("ok"):
                    return
                print(f"[텔레그램 실패] {resp}")
                return
        except urllib.error.HTTPError as e:
            if e.code == 429:
                retry_after = int(e.headers.get("Retry-After", 30))
                print(f"[텔레그램 429] {retry_after}초 대기 후 재시도 (attempt {attempt+1}/3)")
                time.sleep(retry_after)
            else:
                print(f"[텔레그램 오류] HTTP {e.code}: {e}")
                return
        except Exception as e:
            print(f"[텔레그램 오류] {e}")
            return

# ── 긴급도 계산 ───────────────────────────────────────────────────────────────
# ── 차단 키워드 (제목에 포함 시 수집/전송/저장 모두 제외) ─────────────────────
BLOCK_KEYWORDS = [
    "폭증", "불꽃", "랠리", "슈퍼개미", "수퍼개미",
    "부고", "부음", "폭탄", "무료", "인포스탁",
    "증시요약", "테마", "종목이슈", "오후장", "오전장", "라씨로",
    # 농수산/공공기관 노이즈 (제목에 등장 시 차단)
    "산림청", "산림교육원", "산림", "농산물", "가락시장",
    "농협", "축협",
    # 2026-05-17 추가: 라이프스타일/연예/기상 노이즈 차단
    "사주", "운세", "MBTI",
    "드라마", "예능", "KPOP",
    "날씨", "장마", "폭염",
]

URGENT_KEYWORDS = ["유상증자", "무상증자", "CB", "BW", "상장폐지", "감사의견",
                   "횡령", "배임", "대규모", "공급계약", "수주", "FDA", "임상",
                   "상한가", "하한가", "서킷브레이커", "사이드카"]
HIGH_KEYWORDS   = ["실적", "영업이익", "매출", "수출", "수주", "협약", "MOU",
                   "인수", "합병", "지분", "공시", "특허", "급등", "급락",
                   "계약", "선정", "승인", "허가"]

def is_blocked(title):
    """차단 키워드가 제목에 포함되면 True"""
    return any(kw in title for kw in BLOCK_KEYWORDS)

def urgency(title, is_dart=False):
    if is_dart:
        for kw in URGENT_KEYWORDS:
            if kw in title:
                return "🚨 긴급"
        return "📋 공시"
    for kw in URGENT_KEYWORDS:
        if kw in title:
            return "🔥 중요"
    for kw in HIGH_KEYWORDS:
        if kw in title:
            return "📰 주목"
    return "📄 일반"

def urgency_score(level):
    return {"🚨 긴급": 4, "🔥 중요": 3, "📋 공시": 3, "📰 주목": 2, "📄 일반": 1}.get(level, 1)

# ── 네이버 금융 뉴스 ──────────────────────────────────────────────────────────
def fetch_naver_news(keywords, cutoff_hours=0.25):
    """네이버 금융 뉴스 API (15분 윈도우)
    2026-05-17 강화: dt 필드 (KST YYYYMMDDHHMMSS) cutoff 적용 — 실시간만
    """
    articles = []
    KST = timezone(timedelta(hours=9))
    cutoff = datetime.now(timezone.utc) - timedelta(hours=cutoff_hours)
    try:
        for page in range(1, 4):
            url = f"https://m.stock.naver.com/api/news/list?page={page}&pageSize=20"
            req = urllib.request.Request(url, headers=HDR)
            with urllib.request.urlopen(req, timeout=10) as r:
                data = json.loads(r.read())
            for item in data:
                if item.get("type") != 1:
                    continue
                # 2026-05-17 추가: dt cutoff
                dt_str = item.get("dt", "")
                if not dt_str or len(dt_str) < 14:
                    continue
                try:
                    pub_dt = datetime.strptime(dt_str[:14], "%Y%m%d%H%M%S").replace(tzinfo=KST)
                except Exception:
                    continue
                if pub_dt < cutoff:
                    continue
                title = item.get("tit", "")
                oid   = item.get("oid", "")
                aid   = item.get("aid", "")
                if not title or not aid:
                    continue
                link    = f"https://n.news.naver.com/mnews/article/{oid}/{aid}"
                if is_blocked(title):
                    continue
                matched = [kw for kw in keywords if kw in title]
                if not matched:
                    continue
                articles.append({
                    "id":      f"link_{stable_id(link)}",
                    "title":   title,
                    "link":    link,
                    "desc":    "",
                    "matched": matched[:3],
                    "source":  f"네이버({item.get('ohnm','금융')})",
                    "dart":    False,
                })
    except Exception as e:
        print(f"[네이버뉴스 오류] {e}")
    return articles

# ── RSS 수집 (공통) ───────────────────────────────────────────────────────────
def fetch_rss(source_name, feed_url, keywords, cutoff_hours=0.25):
    """RSS 피드에서 최근 cutoff_hours 시간 내 기사 수집
    2026-05-17 변경: 기본값 2h → 0.25h(15분) — cron 10분 간격 + 안전 마진 5분
    """
    articles = []
    try:
        req = urllib.request.Request(feed_url, headers={
            "User-Agent": "Mozilla/5.0 (compatible; StockNewsBot/1.0)",
        })
        with urllib.request.urlopen(req, timeout=10) as r:
            raw = r.read()

        root = ET.fromstring(raw)
        ns = {"atom": "http://www.w3.org/2005/Atom"}

        # RSS 2.0 형식
        items = root.findall(".//item")
        # Atom 형식 fallback
        if not items:
            items = root.findall(".//atom:entry", ns) or root.findall(".//entry")

        # 2026-05-17 버그 수정: tz-aware UTC 비교 (이전: naive datetime.now() → KST/UTC 9시간 오차)
        cutoff = datetime.now(timezone.utc) - timedelta(hours=cutoff_hours)

        for item in items[:30]:
            # 제목
            title_el = item.find("title")
            if title_el is None:
                title_el = item.find("atom:title", ns)
            title = ""
            if title_el is not None:
                title = title_el.text or ""
            title = re.sub(r"<[^>]+>", "", title).strip()
            if not title:
                continue

            # 링크
            link_el = item.find("link")
            if link_el is None:
                link_el = item.find("atom:link", ns)
            link = ""
            if link_el is not None:
                link = link_el.text or link_el.get("href", "")
            link = link.strip()

            # 고유 ID
            guid_el = item.find("guid")
            if guid_el is None:
                guid_el = item.find("id")
            if guid_el is None:
                guid_el = item.find("atom:id", ns)
            item_id = guid_el.text if guid_el is not None else link
            if not item_id:
                item_id = f"{source_name}_{hash(title)}"

            # 2026-05-17 추가: pubDate 파싱 + cutoff 비교 (진짜 시간 필터)
            # RSS 2.0: <pubDate>Sat, 17 May 2026 01:13:00 +0900</pubDate>
            # Atom: <published>2026-05-17T01:13:00+09:00</published>
            date_el = item.find("pubDate") or item.find("published")
            if date_el is None:
                date_el = item.find("atom:published", ns) or item.find("atom:updated", ns)
            # 2026-05-17 강화: pubDate 없거나 파싱 실패 시 안전하게 skip
            # (실시간만 받기 위해 pubDate가 분명한 기사만 통과)
            if date_el is None or not date_el.text:
                continue
            pub_dt = None
            try:
                from email.utils import parsedate_to_datetime
                pub_dt = parsedate_to_datetime(date_el.text.strip())
            except Exception:
                try:
                    pub_dt = datetime.fromisoformat(date_el.text.strip().replace("Z", "+00:00"))
                except Exception:
                    pass
            if pub_dt is None:
                continue  # 파싱 실패 시도 skip
            if pub_dt.tzinfo is None:
                pub_dt = pub_dt.replace(tzinfo=timezone.utc)  # naive면 UTC 가정
            if pub_dt < cutoff:
                continue  # cutoff 이전 기사 skip

            # 요약 (description)
            desc_el = item.find("description")
            if desc_el is None:
                desc_el = item.find("summary")
            if desc_el is None:
                desc_el = item.find("atom:summary", ns)
            desc = ""
            if desc_el is not None and desc_el.text:
                desc = re.sub(r"<[^>]+>", "", desc_el.text).strip()
                desc = re.sub(r"\s+", " ", desc)[:200]

            # 차단 키워드 필터
            if is_blocked(title):
                continue

            # 키워드 매칭
            matched = [kw for kw in keywords if kw in title]
            if not matched:
                continue

            articles.append({
                "id":      f"link_{stable_id(item_id or link)}",
                "title":   title,
                "link":    link,
                "desc":    desc,
                "matched": matched[:3],
                "source":  source_name,
                "dart":    False,
            })

    except Exception as e:
        print(f"[RSS 오류] {source_name} ({feed_url}): {e}")
    return articles

def fetch_all_rss(keywords):
    articles = []
    for source_name, urls in RSS_SOURCES.items():
        for url in urls:
            fetched = fetch_rss(source_name, url, keywords)
            articles.extend(fetched)
            if fetched:
                print(f"  {source_name}: {len(fetched)}건 매칭")
    return articles

# ── YouTube RSS 수집 ──────────────────────────────────────────────────────────
def fetch_youtube_rss(keywords):
    articles = []
    for channel_name, channel_id in YOUTUBE_CHANNELS.items():
        url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=10) as r:
                raw = r.read()
            root = ET.fromstring(raw)
            ns = {
                "atom":  "http://www.w3.org/2005/Atom",
                "media": "http://search.yahoo.com/mrss/",
                "yt":    "http://www.youtube.com/xml/schemas/2015",
            }
            for entry in root.findall("atom:entry", ns)[:10]:
                title_el = entry.find("atom:title", ns)
                title = title_el.text if title_el is not None else ""
                link_el = entry.find("atom:link", ns)
                link = link_el.get("href", "") if link_el is not None else ""
                vid_id_el = entry.find("yt:videoId", ns)
                vid_id = vid_id_el.text if vid_id_el is not None else abs(hash(title))

                matched = [kw for kw in keywords if kw in title]
                # YouTube는 키워드 매칭 없어도 채널 자체로 알림 (선택적)
                # if not matched: continue

                articles.append({
                    "id":      f"yt_{channel_id}_{vid_id}",
                    "title":   f"▶ {title}",
                    "link":    link,
                    "desc":    "",
                    "matched": matched[:3] if matched else [channel_name],
                    "source":  f"YouTube({channel_name})",
                    "dart":    False,
                })
        except Exception as e:
            print(f"[YouTube RSS 오류] {channel_name}: {e}")
    return articles

# ── DART 공시 ─────────────────────────────────────────────────────────────────
def fetch_dart(keywords):
    articles = []
    if not DART_KEY:
        return articles
    try:
        today = datetime.now().strftime("%Y%m%d")
        url = (f"https://opendart.fss.or.kr/api/list.json"
               f"?crtfc_key={DART_KEY}&bgn_de={today}&end_de={today}"
               f"&last_reprt_at=N&page_no=1&page_count=40")
        with urllib.request.urlopen(url, timeout=10) as r:
            data = json.loads(r.read())
        for item in data.get("list", []):
            title  = item.get("report_nm", "")
            corp   = item.get("corp_name", "")
            rcept  = item.get("rcept_no", "")
            link   = f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept}"
            full   = f"[{corp}] {title}"
            if is_blocked(full):
                continue
            matched = [kw for kw in keywords if kw in corp or kw in title]
            urg = urgency(title, is_dart=True)
            if not matched and urg == "📄 일반":
                continue
            articles.append({
                "id":      f"dart_{rcept}",
                "title":   full,
                "link":    link,
                "desc":    "",
                "matched": matched[:3] if matched else [corp],
                "source":  "DART공시",
                "dart":    True,
            })
    except Exception as e:
        print(f"[DART 오류] {e}")
    return articles

# ── 텔레그램 채널 수집 (Telethon) ────────────────────────────────────────────
async def _tg_fetch_async(keywords, cutoff_hours=0.25):  # 2026-05-17: 15분 윈도우
    from telethon import TelegramClient
    from telethon.sessions import StringSession

    cutoff   = datetime.now(timezone.utc) - timedelta(hours=cutoff_hours)
    articles = []

    async with TelegramClient(StringSession(TG_SESSION), TG_API_ID, TG_API_HASH) as client:
        for ch_name in TELEGRAM_CHANNELS:
            try:
                ch_count = 0
                async for msg in client.iter_messages(ch_name, limit=50):
                    if not msg.date:
                        continue
                    msg_date = msg.date if msg.date.tzinfo else msg.date.replace(tzinfo=timezone.utc)
                    if msg_date < cutoff:
                        break
                    text = msg.text or msg.message or ""
                    if len(text.strip()) < 15:
                        continue
                    first_line = text.strip().split("\n")[0]
                    if is_blocked(first_line):
                        continue
                    matched = [kw for kw in keywords if kw in text]
                    if not matched:
                        continue
                    url = f"https://t.me/{ch_name}/{msg.id}"
                    articles.append({
                        "id":      f"link_{stable_id(url)}",
                        "title":   first_line[:100],
                        "link":    url,
                        "desc":    text[:200],
                        "matched": matched[:3],
                        "source":  f"TG:{ch_name}",
                        "dart":    False,
                    })
                    ch_count += 1
                if ch_count:
                    print(f"  TG:{ch_name}: {ch_count}건 매칭")
            except Exception as e:
                print(f"[텔레그램 채널 오류] {ch_name}: {e}")
    return articles


def fetch_telegram_channels(keywords):
    """Telethon으로 텔레그램 채널 최근 2h 메시지 수집 (동기 래퍼)"""
    if not (TG_API_ID and TG_API_HASH and TG_SESSION):
        return []
    try:
        import asyncio
        return asyncio.run(_tg_fetch_async(keywords))
    except ImportError:
        print("[텔레그램 채널] telethon 미설치 — 스킵")
        return []
    except Exception as e:
        print(f"[텔레그램 채널 수집 오류] {e}")
        return []


# ── 저장 & 포맷 ──────────────────────────────────────────────────────────────
def save_news_md(articles, date_str):
    NEWS_DIR.mkdir(parents=True, exist_ok=True)
    path = NEWS_DIR / f"뉴스_{date_str}.md"
    existing = path.read_text(encoding="utf-8") if path.exists() else f"# 뉴스 {date_str}\n\n"
    now = datetime.now().strftime("%H:%M")
    lines = [f"\n## {now} 업데이트 ({len(articles)}건)\n"]
    for a in articles:
        urg = urgency(a["title"], a["dart"])
        tag = " ".join(f"#{m}" for m in a["matched"])
        link = f'<a href="{a["link"]}">{a["title"]}</a>' if a["link"] else a["title"]
        lines.append(f"- {urg} [{a['source']}] {link} {tag}")
    path.write_text(existing + "\n".join(lines), encoding="utf-8")

def format_msg(article):
    urg   = urgency(article["title"], article["dart"])
    tags  = " ".join(f"#{m}" for m in article["matched"])
    src   = article["source"]
    link  = article["link"]
    title = article["title"]
    now   = datetime.now().strftime("%H:%M")
    # URL을 일반 텍스트로 보내면 텔레그램이 자동으로 이미지+제목+설명 미리보기 생성
    if link:
        return f"{urg} [{src}] {now}\n{title}\n{link}\n{tags}"
    return f"{urg} [{src}] {now}\n{title}\n{tags}"

# ── 메인 ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--digest",  action="store_true")
    parser.add_argument("--test",    action="store_true", help="텔레그램 연결 테스트")
    args = parser.parse_args()

    # 연결 테스트 모드
    if args.test:
        tg_send("✅ <b>뉴스 모니터링 연결 테스트</b>\nGitHub Actions → 텔레그램 연결 정상!", dry_run=args.dry_run)
        print("[테스트] 텔레그램 메시지 전송 완료")
        return

    date_str = datetime.now().strftime("%Y-%m-%d")
    keywords = load_keywords()
    seen     = load_seen()
    new_count = 0

    print(f"[{datetime.now().strftime('%H:%M')}] 뉴스 수집 시작 (키워드 {len(keywords)}개)")

    # 수집
    articles = (
        fetch_naver_news(keywords) +
        fetch_naver_search(keywords) +
        fetch_all_rss(keywords) +
        fetch_youtube_rss(keywords) +
        fetch_dart(keywords) +
        fetch_telegram_channels(keywords)
    )

    # 신규 필터 & 정렬 (이전 run 중복 + 같은 run 내 중복 제거)
    # ID 기반 + 제목 기반 이중 dedup — 같은 기사가 RSS/네이버검색/네이버금융에서
    # 다른 URL/ID 로 들어와도 제목으로 한 번 더 걸러냄
    seen_in_run = set()
    new_articles = []
    for a in articles:
        tkey = title_key(a["title"])
        ukey = url_key(a.get("link", ""))  # 2026-05-17: URL 정규화 key
        if a["id"] in seen or tkey in seen or (ukey and ukey in seen):
            continue  # 이전 run 에서 본 것
        if a["id"] in seen_in_run or tkey in seen_in_run or (ukey and ukey in seen_in_run):
            continue  # 같은 run 내 중복
        seen_in_run.add(a["id"])
        seen_in_run.add(tkey)
        if ukey:
            seen_in_run.add(ukey)
        new_articles.append(a)
    new_articles.sort(key=lambda x: -urgency_score(urgency(x["title"], x["dart"])))

    print(f"  수집: {len(articles)}건 / 신규: {len(new_articles)}건")

    # 2026-05-17 추가: per-run 메시지 cap — Telegram flood wait 방지
    # 평일 5분 cron 기준 정상 패턴은 1run당 5~30건. 50건 초과 시 spam wave로 간주.
    MAX_PER_RUN = 50
    if len(new_articles) > MAX_PER_RUN:
        print(f"  ⚠️ {len(new_articles)}건 > {MAX_PER_RUN} cap. 상위 {MAX_PER_RUN}건만 전송, 나머지는 다음 run에서 처리")
        new_articles = new_articles[:MAX_PER_RUN]

    # 전송 — 1건씩 별도 메시지로 전송 (URL 미리보기 카드 각각 표시)
    # 2026-05-17 변경: 5건 묶음 → 1건씩 (사용자 요청)
    TG_BATCH = 1
    sent = 0
    saved_notion = 0
    for i in range(0, len(new_articles), TG_BATCH):
        batch = new_articles[i:i + TG_BATCH]
        parts = []
        for a in batch:
            parts.append(format_msg(a))
            if not args.dry_run:
                save_to_notion(a)
                saved_notion += 1
            seen.add(a["id"])
            seen.add(title_key(a["title"]))
            uk = url_key(a.get("link", ""))
            if uk:
                seen.add(uk)
            new_count += 1
        tg_send("\n\n".join(parts), dry_run=args.dry_run)
        sent += len(batch)
        time.sleep(1.5)  # 2026-05-17: 1건씩 전송 변경에 맞춰 안전 간격 확보

    print(f"  텔레그램 전송: {sent}건 ({(sent + TG_BATCH - 1) // TG_BATCH}개 메시지) / Notion 저장: {saved_notion}건")

    if new_articles:
        save_news_md(new_articles, date_str)

    save_seen(seen)

    # 일일 요약
    now_h, now_m = datetime.now().hour, datetime.now().minute
    if args.digest or (now_h == 8 and 50 <= now_m <= 59):
        path = NEWS_DIR / f"뉴스_{date_str}.md"
        if path.exists():
            lines = path.read_text(encoding="utf-8").splitlines()
            count = sum(1 for l in lines if l.startswith("- "))
            tg_send(
                f"📋 <b>오늘의 뉴스 요약</b> ({date_str})\n"
                f"총 {count}건 수집\n"
                f"소스: 네이버·한경·매경·머니투데이·이데일리·DART",
                dry_run=args.dry_run
            )

    print(f"  완료: 신규 {new_count}건 처리")

if __name__ == "__main__":
    main()
