#!/usr/bin/env python3
"""
fetch_news.py — 주식 뉴스 모니터링 + 텔레그램 알림

소스:
  1. 네이버 금융 뉴스 (주요 언론 통합)
  2. DART 공시 (당일 발행분)

실행:
  python fetch_news.py              # 일반 실행
  python fetch_news.py --dry-run    # 텔레그램 전송 없이 콘솔 출력만
  python fetch_news.py --digest     # 일일 요약 강제 발송
"""

import os, json, re, time, argparse, urllib.request, urllib.parse
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

HDR = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Referer": "https://m.stock.naver.com/",
}

# ── 키워드 로드 (wiki/stocks 종목명 + 주요 테마) ─────────────────────────────
def load_keywords():
    keywords = set()
    # 종목명 (파일명 = 종목명)
    if STOCKS_DIR.exists():
        for f in STOCKS_DIR.glob("*.md"):
            if f.name != "_TEMPLATE.md":
                keywords.add(f.stem)
    # 고정 시황 키워드
    keywords.update([
        "코스피", "코스닥", "나스닥", "S&P", "반도체", "금리", "환율",
        "외국인", "기관", "공매도", "수급", "FOMC", "엔비디아", "HBM",
        "이차전지", "방산", "바이오", "AI", "로봇", "자율주행",
    ])
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
    # 최대 5000개 유지 (오래된 것 제거)
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
        "disable_web_page_preview": "true",
    }).encode()
    try:
        with urllib.request.urlopen(url, data=data, timeout=10) as r:
            json.loads(r.read())
    except Exception as e:
        print(f"[텔레그램 오류] {e}")

# ── 긴급도 계산 ───────────────────────────────────────────────────────────────
URGENT_KEYWORDS = ["유상증자", "무상증자", "CB", "BW", "상장폐지", "감사의견",
                   "횡령", "배임", "대규모", "공급계약", "수주", "FDA", "임상"]
HIGH_KEYWORDS   = ["실적", "영업이익", "매출", "수출", "수주", "협약", "MOU",
                   "인수", "합병", "지분", "공시", "특허"]

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

# ── 네이버 금융 뉴스 수집 ─────────────────────────────────────────────────────
def fetch_naver_news(keywords):
    """네이버 금융뉴스 수집 (type=1: 주식 관련 기사)
    필드: tit=제목, oid+aid=고유ID, ohnm=언론사
    링크: https://n.news.naver.com/mnews/article/{oid}/{aid}
    """
    articles = []
    try:
        for page in range(1, 4):   # 최대 3페이지 (60건)
            url = f"https://m.stock.naver.com/api/news/list?page={page}&pageSize=20"
            req = urllib.request.Request(url, headers=HDR)
            with urllib.request.urlopen(req, timeout=10) as r:
                data = json.loads(r.read())

            for item in data:
                if item.get("type") != 1:   # 주식 관련 기사만
                    continue
                title = item.get("tit", "")
                oid   = item.get("oid", "")
                aid   = item.get("aid", "")
                if not title or not aid:
                    continue
                link = f"https://n.news.naver.com/mnews/article/{oid}/{aid}"
                matched = [kw for kw in keywords if kw in title]
                if not matched:
                    continue
                articles.append({
                    "id":      f"naver_{oid}_{aid}",
                    "title":   title,
                    "link":    link,
                    "matched": matched[:3],
                    "source":  f"네이버({item.get('ohnm','금융')})",
                    "dart":    False,
                })
    except Exception as e:
        print(f"[네이버뉴스 오류] {e}")
    return articles

# ── DART 공시 수집 ────────────────────────────────────────────────────────────
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
            title   = item.get("report_nm", "")
            corp    = item.get("corp_name", "")
            rcept   = item.get("rcept_no", "")
            link    = f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept}"
            full    = f"[{corp}] {title}"
            matched = [kw for kw in keywords if kw in corp or kw in title]
            # DART는 관련 키워드 없어도 긴급 공시면 포함
            urg = urgency(title, is_dart=True)
            if not matched and urg == "📄 일반":
                continue
            articles.append({
                "id":      f"dart_{rcept}",
                "title":   full,
                "link":    link,
                "matched": matched[:3] if matched else [corp],
                "source":  "DART공시",
                "dart":    True,
            })
    except Exception as e:
        print(f"[DART 오류] {e}")
    return articles

# ── 뉴스 저장 (wiki/news/뉴스_DATE.md) ──────────────────────────────────────
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
        lines.append(f"- {urg} {link} {tag}")
    path.write_text(existing + "\n".join(lines), encoding="utf-8")

# ── 텔레그램 메시지 포맷 ─────────────────────────────────────────────────────
def format_msg(article):
    urg  = urgency(article["title"], article["dart"])
    tags = " ".join(f"#{m}" for m in article["matched"])
    src  = article["source"]
    link = article["link"]
    title = article["title"]
    now  = datetime.now().strftime("%H:%M")
    if link:
        return f'{urg} [{src}] {now}\n<a href="{link}">{title}</a>\n{tags}'
    return f"{urg} [{src}] {now}\n{title}\n{tags}"

# ── 메인 ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--digest",  action="store_true", help="일일 요약 강제 발송")
    args = parser.parse_args()

    date_str = datetime.now().strftime("%Y-%m-%d")
    keywords = load_keywords()
    seen     = load_seen()
    new_count = 0

    print(f"[{datetime.now().strftime('%H:%M')}] 뉴스 수집 시작 (키워드 {len(keywords)}개)")

    # 수집
    articles = fetch_naver_news(keywords) + fetch_dart(keywords)

    # 신규 필터
    new_articles = [a for a in articles if a["id"] not in seen]
    new_articles.sort(key=lambda x: -urgency_score(urgency(x["title"], x["dart"])))

    print(f"  수집: {len(articles)}건 / 신규: {len(new_articles)}건")

    # 전송 (긴급도 2 이상만 즉시 전송)
    for a in new_articles:
        urg = urgency(a["title"], a["dart"])
        if urgency_score(urg) >= 2:
            tg_send(format_msg(a), dry_run=args.dry_run)
            time.sleep(0.3)
        seen.add(a["id"])
        new_count += 1

    # wiki 저장
    if new_articles:
        save_news_md(new_articles, date_str)

    save_seen(seen)

    # 일일 요약 (--digest 또는 오전 8:50~9:00)
    now_h, now_m = datetime.now().hour, datetime.now().minute
    if args.digest or (now_h == 8 and 50 <= now_m <= 59):
        path = NEWS_DIR / f"뉴스_{date_str}.md"
        if path.exists():
            lines = path.read_text(encoding="utf-8").splitlines()
            count = sum(1 for l in lines if l.startswith("- "))
            tg_send(
                f"📋 <b>오늘의 뉴스 요약</b> ({date_str})\n"
                f"총 {count}건 수집\n"
                f"wiki/news/뉴스_{date_str}.md 저장 완료",
                dry_run=args.dry_run
            )

    print(f"  완료: 신규 {new_count}건 처리")

if __name__ == "__main__":
    main()
