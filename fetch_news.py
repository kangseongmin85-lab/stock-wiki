#!/usr/bin/env python3
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

import os, json, re, time, argparse, urllib.request, urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

TOKEN    = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID  = os.getenv("TELEGRAM_CHAT_ID", "")
DART_KEY = os.getenv("DART_API_KEY", "")

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
NAVER_CLIENT_ID  = _CFG.get("NAVER_CLIENT_ID", "")  or os.getenv("NAVER_CLIENT_ID", "")
NAVER_CLIENT_SEC = _CFG.get("NAVER_CLIENT_SECRET", "") or os.getenv("NAVER_CLIENT_SECRET", "")

# 활성 테마 (wiki/index.md "활성 🔥" 기준 — 네이버 검색 API 검색어로 사용)
NAVER_SEARCH_QUERIES = [
    "HBM", "AI데이터센터", "방산", "로봇", "원자력", "이차전지",
    "자율주행", "휴머노이드", "조선", "원전",
]

def fetch_naver_search(keywords):
    """네이버 검색 API: 활성 테마별 최신 뉴스 (옵션 1)"""
    if not (NAVER_CLIENT_ID and NAVER_CLIENT_SEC):
        return []
    articles = []
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
                title = re.sub(r"<[^>]+>", "", item.get("title", "")).replace("&quot;", '"').replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">").replace("&apos;", "'").strip()
                link  = item.get("originallink") or item.get("link", "")
                desc  = re.sub(r"<[^>]+>", "", item.get("description", ""))[:200]
                if not title or is_blocked(title):
                    continue
                matched = [kw for kw in keywords if kw in title]
                if not matched:
                    matched = [q]  # 검색어 자체가 매칭 키워드
                articles.append({
                    "id":      f"naverapi_{q}_{abs(hash(link or title))}",
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

# ── 키워드 로드 ───────────────────────────────────────────────────────────────
# 노이즈 키워드 제외 리스트 — 너무 광범위하게 매칭되어 false positive 유발
# 위키 파일은 보존하되 매칭 키워드에서만 제외
EXCLUDE_KEYWORDS = {
    "금", "은",   # 1글자 테마 — 일반 기사에 빈번히 등장
    "남성",       # 종목명이지만 일반 기사("30대 남성", "남성복" 등)에 빈번히 등장
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
    SEEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    if SEEN_FILE.exists():
        try:
            return set(json.loads(SEEN_FILE.read_text(encoding="utf-8")))
        except Exception:
            pass
    return set()

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
    try:
        with urllib.request.urlopen(url, data=data, timeout=10) as r:
            resp = json.loads(r.read())
            if not resp.get("ok"):
                print(f"[텔레그램 실패] {resp}")
    except Exception as e:
        print(f"[텔레그램 오류] {e}")

# ── 긴급도 계산 ───────────────────────────────────────────────────────────────
# ── 차단 키워드 (제목에 포함 시 수집/전송/저장 모두 제외) ─────────────────────
BLOCK_KEYWORDS = [
    "폭증", "불꽃", "랠리", "슈퍼개미", "수퍼개미",
    "부고", "부음", "폭탄", "무료", "인포스탁",
    "증시요약", "테마", "종목이슈", "오후장", "오전장", "라씨로",
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
def fetch_naver_news(keywords):
    articles = []
    try:
        for page in range(1, 4):
            url = f"https://m.stock.naver.com/api/news/list?page={page}&pageSize=20"
            req = urllib.request.Request(url, headers=HDR)
            with urllib.request.urlopen(req, timeout=10) as r:
                data = json.loads(r.read())
            for item in data:
                if item.get("type") != 1:
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
                    "id":      f"naver_{oid}_{aid}",
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
def fetch_rss(source_name, feed_url, keywords, cutoff_hours=2):
    """RSS 피드에서 최근 cutoff_hours 시간 내 기사 수집"""
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

        cutoff = datetime.now() - timedelta(hours=cutoff_hours)

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
                "id":      f"rss_{source_name}_{abs(hash(item_id))}",
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
        fetch_dart(keywords)
    )

    # 신규 필터 & 정렬
    new_articles = [a for a in articles if a["id"] not in seen]
    new_articles.sort(key=lambda x: -urgency_score(urgency(x["title"], x["dart"])))

    print(f"  수집: {len(articles)}건 / 신규: {len(new_articles)}건")

    # 전송 (키워드 매칭된 모든 기사 전송 — 긴급도 필터 없음)
    sent = 0
    saved_notion = 0
    for a in new_articles:
        tg_send(format_msg(a), dry_run=args.dry_run)
        if not args.dry_run:
            save_to_notion(a)
            saved_notion += 1
        time.sleep(0.3)
        seen.add(a["id"])
        new_count += 1
        sent += 1

    print(f"  텔레그램 전송: {sent}건 / Notion 저장: {saved_notion}건")

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
