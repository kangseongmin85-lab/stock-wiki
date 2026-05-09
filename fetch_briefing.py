#!/usr/bin/env python3
"""
fetch_briefing.py — 장 마감 브리핑 (GitHub Actions 완전 자동화)

흐름:
  1. Naver 상승률 랭킹 API → 등락률 3%↑ 종목 수집 (KOSPI + KOSDAQ)
  2. 거래대금 150억↑ 필터
  3. 당일 뉴스(wiki/news/뉴스_YYYY-MM-DD.md)에서 관련 기사 매칭
  4. 텔레그램 전송 + wiki/analysis/브리핑_YYYY-MM-DD.md 저장

실행:
  python fetch_briefing.py                    # 오늘 날짜 자동
  python fetch_briefing.py --date 2026-05-09  # 날짜 지정
  python fetch_briefing.py --dry-run          # 콘솔 출력만
"""

import os, json, re, time, argparse, urllib.request, urllib.parse
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

BASE_DIR = Path(__file__).parent
NEWS_DIR = BASE_DIR / "wiki" / "news"

# ── 필터 기준 ─────────────────────────────────────────────────────────────────
RATE_MIN      = 3.0               # 등락률 ≥ 3%
TRADE_VAL_MIN = 15_000_000_000    # 거래대금 ≥ 150억

HDR = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Referer":    "https://m.stock.naver.com/",
}

# ── Naver API ─────────────────────────────────────────────────────────────────
def naver_get(url, timeout=10):
    req = urllib.request.Request(url, headers=HDR)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())

def parse_val(s):
    """문자열 거래대금 → int(원)"""
    if not s:
        return 0
    return int(re.sub(r"[^\d]", "", str(s)) or 0)

# ── 종목 수집: Naver 상승률 랭킹 ─────────────────────────────────────────────
def fetch_rising_stocks():
    """KOSPI + KOSDAQ 상승률 순위에서 등락률 3%↑ 종목 수집"""
    stocks = []
    for market in ("KOSPI", "KOSDAQ"):
        for page in range(1, 4):   # 최대 3페이지 (300종목)
            try:
                url = (f"https://m.stock.naver.com/api/stocks/up"
                       f"?market={market}&page={page}&pageSize=100")
                data = naver_get(url)
                items = data if isinstance(data, list) else data.get("stocks", [])
                if not items:
                    break
                for item in items:
                    rate = float(item.get("fluctuationsRatio") or 0)
                    if rate < RATE_MIN:
                        break   # 정렬되어 있으므로 이후는 더 낮음
                    trade_val = parse_val(item.get("accumulatedTradingValue") or
                                          item.get("dealTradingVolume") or 0)
                    stocks.append({
                        "name":      item.get("stockName", ""),
                        "code":      item.get("stockCode", ""),
                        "rate":      rate,
                        "trade_val": trade_val,
                        "market":    market,
                    })
                time.sleep(0.1)
            except Exception as e:
                print(f"  [랭킹 오류] {market} p{page}: {e}")
                break
    return stocks

# ── 거래대금 보완 조회 (랭킹 API에 없을 때) ──────────────────────────────────
def fill_trade_val(stocks):
    """trade_val = 0 인 종목만 개별 API로 보완"""
    for s in stocks:
        if s["trade_val"] > 0:
            continue
        try:
            url  = f"https://m.stock.naver.com/api/stock/{s['code']}/integration"
            data = naver_get(url)
            s["trade_val"] = parse_val(
                data.get("accumulatedTradingValue") or
                data.get("dealTradingVolume") or 0
            )
            time.sleep(0.05)
        except Exception:
            pass
    return stocks

# ── 뉴스 매칭 ─────────────────────────────────────────────────────────────────
def load_news_lines(date_str):
    path = NEWS_DIR / f"뉴스_{date_str}.md"
    if not path.exists():
        return []
    return [l for l in path.read_text(encoding="utf-8").splitlines()
            if l.startswith("- ")]

def match_news(stock_name, news_lines):
    matched = [l for l in news_lines if stock_name in l]
    return matched[:3]

def clean_news_line(line):
    """뉴스 라인에서 제목만 추출"""
    title = re.sub(r"^-\s*[^\[]+\[[^\]]+\]\s*", "", line)
    title = re.sub(r"#\S+", "", title).strip()
    return (title[:60] + "…") if len(title) > 60 else title

# ── 텔레그램 전송 ─────────────────────────────────────────────────────────────
def tg_send(text, dry_run=False):
    if dry_run:
        print(f"\n{'='*60}\n{text}\n{'='*60}")
        return
    if not TOKEN or not CHAT_ID:
        print("[경고] 텔레그램 환경변수 없음")
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

# ── wiki 저장 ─────────────────────────────────────────────────────────────────
def save_briefing_md(stocks, news_lines, date_str):
    analysis_dir = BASE_DIR / "wiki" / "analysis"
    analysis_dir.mkdir(parents=True, exist_ok=True)
    path = analysis_dir / f"브리핑_{date_str}.md"

    lines = [
        f"# 장 마감 브리핑 {date_str}",
        f"> 거래대금 150억↑ · 등락률 3%↑ · {len(stocks)}종목",
        f"> 생성: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        "",
        "| 순위 | 종목명 | 등락률 | 거래대금 | 시장 |",
        "|------|--------|--------|----------|------|",
    ]
    for i, s in enumerate(stocks, 1):
        val_str  = f"{s['trade_val'] / 100_000_000:.0f}억"
        rate_str = f"+{s['rate']:.1f}%"
        lines.append(f"| {i} | {s['name']} | {rate_str} | {val_str} | {s['market']} |")

    lines += ["", "## 종목별 관련 뉴스", ""]
    for s in stocks:
        rate_str = f"+{s['rate']:.1f}%"
        val_str  = f"{s['trade_val'] / 100_000_000:.0f}억"
        lines.append(f"### {s['name']} ({rate_str} | {val_str})")
        matched = match_news(s["name"], news_lines)
        if matched:
            for m in matched:
                lines.append(f"- {clean_news_line(m)}")
        else:
            lines.append("- 관련 뉴스 없음")
        lines.append("")

    path.write_text("\n".join(lines), encoding="utf-8")
    print(f"  wiki 저장: {path.name}")

# ── 텔레그램 포맷 ─────────────────────────────────────────────────────────────
def format_briefing(stocks, news_lines, date_str):
    lines = [
        f"📊 <b>장 마감 브리핑</b> {date_str}",
        f"거래대금 150억↑ · 등락률 3%↑ · {len(stocks)}종목",
        "",
    ]
    for i, s in enumerate(stocks[:15], 1):
        val_str  = f"{s['trade_val'] / 100_000_000:.0f}억"
        rate_str = f"+{s['rate']:.1f}%"
        lines.append(f"<b>{i}. {s['name']}</b> {rate_str} | {val_str}")
        matched = match_news(s["name"], news_lines)
        if matched:
            for m in matched:
                lines.append(f"  └ {clean_news_line(m)}")
        else:
            lines.append("  └ 관련 뉴스 없음")
        lines.append("")
    if len(stocks) > 15:
        lines.append(f"… 외 {len(stocks)-15}종목 (wiki 저장 완료)")
    return "\n".join(lines)

# ── 메인 ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date",    default=datetime.now().strftime("%Y-%m-%d"))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    date_str = args.date
    print(f"[브리핑] {date_str} 시작")

    # 1. 상승 종목 수집
    print("  Naver 상승률 랭킹 조회 중...")
    stocks = fetch_rising_stocks()
    print(f"  등락률 {RATE_MIN}%↑: {len(stocks)}종목")

    if not stocks:
        tg_send(f"📊 {date_str} 브리핑: 조건 충족 종목 없음", dry_run=args.dry_run)
        return

    # 2. 거래대금 보완 후 필터
    stocks = fill_trade_val(stocks)
    stocks = [s for s in stocks if s["trade_val"] >= TRADE_VAL_MIN]
    stocks.sort(key=lambda x: -x["trade_val"])
    print(f"  거래대금 150억↑: {len(stocks)}종목")

    if not stocks:
        tg_send(f"📊 {date_str} 브리핑: 거래대금 150억↑ 종목 없음", dry_run=args.dry_run)
        return

    # 3. 뉴스 매칭
    news_lines = load_news_lines(date_str)
    print(f"  당일 뉴스: {len(news_lines)}건")

    # 4. 전송 + wiki 저장
    msg = format_briefing(stocks, news_lines, date_str)
    tg_send(msg, dry_run=args.dry_run)
    save_briefing_md(stocks, news_lines, date_str)
    print(f"  완료: {len(stocks)}종목 브리핑 전송 + wiki 저장")

if __name__ == "__main__":
    main()
