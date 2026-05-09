#!/usr/bin/env python3
"""
fetch_briefing.py — 장 마감 브리핑

흐름:
  1. change_rate CSV에서 등락률 ≥ 3% 종목 추출
  2. Naver API로 거래대금 조회 → 150억 이상 필터
  3. 당일 뉴스(wiki/news/뉴스_YYYY-MM-DD.md)에서 종목 관련 기사 매칭
  4. 텔레그램으로 브리핑 전송

실행:
  python fetch_briefing.py                    # 오늘 날짜 자동
  python fetch_briefing.py --date 2026-05-09  # 날짜 지정
  python fetch_briefing.py --dry-run          # 콘솔 출력만
"""

import os, csv, json, re, time, argparse, urllib.request
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

BASE_DIR   = Path(__file__).parent
NEWS_DIR   = BASE_DIR / "wiki" / "news"
WIKI_DIR   = BASE_DIR / "wiki"

# ── 필터 기준 ─────────────────────────────────────────────────────────────────
RATE_MIN       = 3.0          # 등락률 ≥ 3%
TRADE_VAL_MIN  = 15_000_000_000  # 거래대금 ≥ 150억

HDR = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

# ── Naver API ─────────────────────────────────────────────────────────────────
def naver_get(url, timeout=8):
    req = urllib.request.Request(url, headers=HDR)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())

def fetch_trade_val(code):
    """당일 거래대금(원) 조회"""
    try:
        url = f"https://m.stock.naver.com/api/stock/{code}/integration"
        data = naver_get(url)
        val_str = data.get("dealTradingVolume") or \
                  data.get("accumulatedTradingValue") or "0"
        val_str = re.sub(r"[^\d]", "", str(val_str))
        return int(val_str) if val_str else 0
    except Exception:
        return 0

# ── CSV 로드 ──────────────────────────────────────────────────────────────────
def load_candidates(date_str):
    """change_rate CSV에서 등락률 ≥ RATE_MIN 종목 로드"""
    csv_path = WIKI_DIR / f"change_rate_{date_str}.csv"
    if not csv_path.exists():
        # 최신 CSV 자동 탐색
        candidates_csv = sorted(WIKI_DIR.glob("change_rate_*.csv"))
        if not candidates_csv:
            print(f"[오류] change_rate CSV 없음: {csv_path}")
            return []
        csv_path = candidates_csv[-1]
        print(f"[안내] CSV 자동 선택: {csv_path.name}")

    result = []
    with open(csv_path, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            try:
                rate = float(row["등락률(%)"].replace("+", ""))
                if rate >= RATE_MIN:
                    result.append({
                        "name":   row["종목명"],
                        "code":   row["종목코드"],
                        "rate":   rate,
                        "market": row.get("시장", ""),
                    })
            except Exception:
                pass
    return result

# ── 거래대금 필터 ─────────────────────────────────────────────────────────────
def filter_by_trade_val(candidates):
    result = []
    total = len(candidates)
    for i, c in enumerate(candidates, 1):
        print(f"\r  거래대금 조회 {i}/{total}...", end="", flush=True)
        val = fetch_trade_val(c["code"])
        if val >= TRADE_VAL_MIN:
            c["trade_val"] = val
            result.append(c)
        time.sleep(0.05)
    print()
    result.sort(key=lambda x: -x["trade_val"])
    return result

# ── 뉴스 매칭 ─────────────────────────────────────────────────────────────────
def load_news_lines(date_str):
    """당일 뉴스 md에서 기사 목록 로드"""
    path = NEWS_DIR / f"뉴스_{date_str}.md"
    if not path.exists():
        return []
    lines = path.read_text(encoding="utf-8").splitlines()
    return [l for l in lines if l.startswith("- ")]

def match_news(stock_name, news_lines):
    """종목명이 포함된 뉴스 라인 반환 (최대 3건)"""
    matched = [l for l in news_lines if stock_name in l]
    return matched[:3]

# ── 텔레그램 전송 ─────────────────────────────────────────────────────────────
def tg_send(text, dry_run=False):
    if dry_run:
        print(f"\n{'='*50}\n{text}\n{'='*50}")
        return
    if not TOKEN or not CHAT_ID:
        print("[경고] 텔레그램 환경변수 없음")
        return
    import urllib.parse
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

# ── wiki 저장 ────────────────────────────────────────────────────────────────
def save_briefing_md(stocks, news_lines, date_str):
    """wiki/analysis/브리핑_YYYY-MM-DD.md 저장"""
    analysis_dir = BASE_DIR / "wiki" / "analysis"
    analysis_dir.mkdir(parents=True, exist_ok=True)
    path = analysis_dir / f"브리핑_{date_str}.md"

    lines = [
        f"# 장 마감 브리핑 {date_str}",
        f"> 거래대금 150억↑ · 등락률 3%↑ · {len(stocks)}종목",
        f"> 생성: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        "",
        "| 순위 | 종목명 | 등락률 | 거래대금 |",
        "|------|--------|--------|----------|",
    ]

    for i, s in enumerate(stocks, 1):
        val_str  = f"{s['trade_val'] / 100_000_000:.0f}억"
        rate_str = f"+{s['rate']:.1f}%" if s['rate'] > 0 else f"{s['rate']:.1f}%"
        lines.append(f"| {i} | {s['name']} | {rate_str} | {val_str} |")

    lines += ["", "## 종목별 관련 뉴스", ""]

    for s in stocks:
        rate_str = f"+{s['rate']:.1f}%" if s['rate'] > 0 else f"{s['rate']:.1f}%"
        val_str  = f"{s['trade_val'] / 100_000_000:.0f}억"
        lines.append(f"### {s['name']} ({rate_str} | {val_str})")
        matched = match_news(s["name"], news_lines)
        if matched:
            for m in matched:
                title = re.sub(r"^- [^\[]+\[[^\]]+\]\s*", "", m)
                title = re.sub(r"#\S+", "", title).strip()
                lines.append(f"- {title}")
        else:
            lines.append("- 관련 뉴스 없음")
        lines.append("")

    path.write_text("
".join(lines), encoding="utf-8")
    print(f"  wiki 저장: {path.name}")

# ── 포맷 ─────────────────────────────────────────────────────────────────────
def format_briefing(stocks, news_lines, date_str):
    now = datetime.now().strftime("%H:%M")
    lines = [
        f"📊 <b>장 마감 브리핑</b> {date_str}",
        f"거래대금 150억↑ · 등락률 3%↑ · {len(stocks)}종목",
        "",
    ]

    for i, s in enumerate(stocks[:15], 1):  # 최대 15종목
        val_str = f"{s['trade_val'] / 100_000_000:.0f}억"
        rate_str = f"+{s['rate']:.1f}%" if s['rate'] > 0 else f"{s['rate']:.1f}%"
        lines.append(f"<b>{i}. {s['name']}</b> {rate_str} | {val_str}")

        matched = match_news(s["name"], news_lines)
        if matched:
            for m in matched:
                # 뉴스 라인에서 제목만 추출 (이모지·태그 제거)
                title = re.sub(r"^- [^\[]+\[[^\]]+\]\s*", "", m)
                title = re.sub(r"#\S+", "", title).strip()
                title = title[:60] + "…" if len(title) > 60 else title
                lines.append(f"  └ {title}")
        else:
            lines.append("  └ 관련 뉴스 없음")
        lines.append("")

    if len(stocks) > 15:
        lines.append(f"… 외 {len(stocks)-15}종목")

    return "\n".join(lines)

# ── 메인 ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date",    default=datetime.now().strftime("%Y-%m-%d"))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    date_str = args.date
    print(f"[브리핑] {date_str} 시작")

    # 1. 후보 로드
    candidates = load_candidates(date_str)
    print(f"  등락률 3%↑: {len(candidates)}종목")
    if not candidates:
        tg_send(f"📊 {date_str} 브리핑: 조건 충족 종목 없음", dry_run=args.dry_run)
        return

    # 2. 거래대금 필터
    stocks = filter_by_trade_val(candidates)
    print(f"  거래대금 150억↑: {len(stocks)}종목")
    if not stocks:
        tg_send(f"📊 {date_str} 브리핑: 거래대금 150억↑ 종목 없음", dry_run=args.dry_run)
        return

    # 3. 뉴스 로드
    news_lines = load_news_lines(date_str)
    print(f"  당일 뉴스: {len(news_lines)}건")

    # 4. 브리핑 포맷 & 전송 & wiki 저장
    msg = format_briefing(stocks, news_lines, date_str)
    tg_send(msg, dry_run=args.dry_run)
    save_briefing_md(stocks, news_lines, date_str)
    print(f"  완료: {len(stocks)}종목 브리핑 전송 + wiki 저장")

if __name__ == "__main__":
    main()
