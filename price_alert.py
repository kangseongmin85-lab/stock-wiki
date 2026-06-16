#!/usr/bin/env python3
"""
price_alert.py — 장중 가격 트리거 텔레그램 알림 (뉴스봇과 별도 봇)
====================================================================
감시 대상 2층 구조:
  1) 관심종목 (진입 신호) : "오늘의 관심종목/" 최근 14일 CSV 누적 종목
       - 🚀 전고점 돌파 : 현재가 > 60일 고점(당일 제외) AND 거래량 1.5배 이상
       - 🔥 거래량 급증 : 장중 환산 거래량이 20일 평균 대비 3배 이상 AND 상승 중
  2) 보유종목 (리스크 신호) : 보유종목.csv (종목명,종목코드,평단가,손절선,메모)
       - ⛔ 손절선 이탈 : 현재가 <= 손절선
       - 📉 20일선 이탈 : 전일 종가는 20일선 위, 현재가는 아래 (신규 이탈만)
       - 🩸 급락 경고   : 당일 등락률 -5% 이하

텔레그램: 뉴스봇과 분리된 전용 봇 사용
  .env 에 ALERT_BOT_TOKEN / ALERT_CHAT_ID 필요 (없으면 콘솔 출력만)

사용:
  python price_alert.py                # 1회 스캔 (장중에만 동작) → 가격알림_실행.bat 루프용
  python price_alert.py --force        # 장외 시간에도 강제 스캔 (테스트)
  python price_alert.py --dry-run      # 텔레그램 전송 없이 콘솔만
  python price_alert.py --test         # 알림봇 연결 테스트 (1건 전송)
  python price_alert.py --get-chat-id  # 새 봇에게 말을 건 뒤 chat_id 확인

설계 원칙:
  - 기준선(60일 고점·20일 평균거래량·20일선)은 하루 1회 네이버 일봉에서 캐시
  - 같은 날 같은 종목·같은 조건은 1회만 알림 (_cache/price_alert_sent.json)
  - daily_picks_tracker 의 CSV 파서 재사용 (단일 소스)
"""

import os
import sys
import csv
import json
import re
import time
import argparse
import urllib.request
import urllib.parse
from datetime import datetime, timedelta, timezone
from pathlib import Path

KST = timezone(timedelta(hours=9))  # GitHub Actions 러너는 UTC — 항상 KST 기준으로 판단

if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import daily_picks_tracker as dpt

BASE_DIR      = Path(__file__).parent
PICKS_DIR     = BASE_DIR / "오늘의 관심종목"
HOLDINGS_CSV  = BASE_DIR / "보유종목.csv"
CACHE_DIR     = BASE_DIR / "_cache"
BASELINE_PATH = CACHE_DIR / "price_alert_baseline.json"
SENT_PATH     = CACHE_DIR / "price_alert_sent.json"

ALERT_TOKEN   = os.getenv("ALERT_BOT_TOKEN", "")
ALERT_CHAT_ID = os.getenv("ALERT_CHAT_ID", "")

WATCH_DAYS      = 14     # 관심종목 CSV 누적 기간
MAX_WATCH       = 120    # 감시 종목 상한 (최신 등장순)
BREAKOUT_VOLX   = 1.5    # 전고점 돌파 시 최소 거래량 배수
SURGE_VOLX      = 3.0    # 거래량 급증 기준 배수
CRASH_PCT       = -5.0   # 보유종목 급락 경고 기준
SESSION_MINUTES = 390    # 09:00 ~ 15:30
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}


# ──────────────────────────────────────
# 유틸
# ──────────────────────────────────────

def now():
    return datetime.now(KST)


def today_str():
    return now().strftime("%Y-%m-%d")


def market_open(t=None):
    """평일 09:00~15:40 (마감 직후 여유 10분)."""
    t = t or now()
    if t.weekday() >= 5:
        return False
    hm = t.hour * 60 + t.minute
    return 9 * 60 <= hm <= 15 * 60 + 40


def elapsed_ratio(t=None):
    """장 시작 후 경과 비율 (0.15 하한 — 개장 직후 과대평가 방지)."""
    t = t or now()
    minutes = (t.hour * 60 + t.minute) - 9 * 60
    minutes = max(0, min(minutes, SESSION_MINUTES))
    return max(0.15, minutes / SESSION_MINUTES)


def fmt(n):
    return f"{n:,.0f}"


def load_json(path, default):
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            pass
    return default


def save_json(path, obj):
    CACHE_DIR.mkdir(exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=1), encoding="utf-8")


def http_get(url, timeout=10):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", errors="replace")


# ──────────────────────────────────────
# 텔레그램 (알림 전용 봇 — 뉴스봇과 분리)
# ──────────────────────────────────────

def tg_send(text, dry_run=False):
    if dry_run:
        print(f"\n[DRY-RUN]\n{text}\n")
        return True
    if not ALERT_TOKEN or not ALERT_CHAT_ID:
        print("[경고] ALERT_BOT_TOKEN / ALERT_CHAT_ID 미설정 — 콘솔 출력만")
        print(text)
        return False
    url = f"https://api.telegram.org/bot{ALERT_TOKEN}/sendMessage"
    data = urllib.parse.urlencode({
        "chat_id": ALERT_CHAT_ID,
        "text": text[:4000],
        "disable_web_page_preview": "true",
    }).encode()
    try:
        req = urllib.request.Request(url, data=data)
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read()).get("ok", False)
    except Exception as e:
        print(f"[오류] 텔레그램 전송 실패: {e}")
        return False


def get_chat_id():
    """새 봇에게 아무 메시지나 보낸 뒤 실행하면 chat_id 출력."""
    if not ALERT_TOKEN:
        print("[오류] .env 에 ALERT_BOT_TOKEN 을 먼저 넣어주세요.")
        return
    try:
        raw = http_get(f"https://api.telegram.org/bot{ALERT_TOKEN}/getUpdates")
        updates = json.loads(raw).get("result", [])
    except Exception as e:
        print(f"[오류] getUpdates 실패: {e}")
        return
    if not updates:
        print("수신 메시지 없음 — 텔레그램에서 새 봇에게 아무 말이나 보낸 뒤 다시 실행하세요.")
        return
    seen = set()
    for u in updates:
        chat = (u.get("message") or u.get("channel_post") or {}).get("chat", {})
        cid = chat.get("id")
        if cid and cid not in seen:
            seen.add(cid)
            print(f"chat_id: {cid}  ({chat.get('type')}, {chat.get('title') or chat.get('first_name')})")
    print("\n→ 위 chat_id 를 .env 의 ALERT_CHAT_ID 에 넣으세요.")


# ──────────────────────────────────────
# 감시 목록 로드
# ──────────────────────────────────────

def load_watchlist():
    """최근 WATCH_DAYS 일 CSV 누적 → {code: {name, last_seen}} (최신 등장 우선).

    클라우드(GitHub Actions)에서는 ALERT_WATCHLIST Secret(종목명,종목코드,등록일)을 읽음
    — 공개 저장소에 스크리너 CSV를 올리지 않기 위한 경로. sync_watchlist_github.py가 갱신.
    """
    raw = os.getenv("ALERT_WATCHLIST", "").lstrip("﻿").strip()
    if raw:
        watch = {}
        for ln in raw.splitlines():
            parts = [p.strip() for p in ln.split(",")]
            if len(parts) < 3 or not parts[1].isdigit():
                continue  # 헤더·빈 줄
            watch[parts[1].zfill(6)] = {"name": parts[0], "last_seen": parts[2]}
        items = sorted(watch.items(), key=lambda kv: kv[1]["last_seen"], reverse=True)[:MAX_WATCH]
        return dict(items)

    cutoff = (now() - timedelta(days=WATCH_DAYS)).strftime("%Y-%m-%d")
    files = []
    for p in sorted(PICKS_DIR.glob("*.csv")):
        d = dpt.parse_csv_date(p.name)
        if d and d >= cutoff:
            files.append((d, p))
    watch = {}
    for d, p in sorted(files):  # 날짜 오름차순 → 같은 종목은 최신 정보로 덮임
        try:
            for row in dpt.parse_picks_csv(p):
                code = row["code"].zfill(6)
                watch[code] = {"name": row["name"], "last_seen": d}
        except Exception as e:
            print(f"[경고] CSV 파싱 실패 {p.name}: {e}")
    # 상한: 최신 등장순으로 자름
    items = sorted(watch.items(), key=lambda kv: kv[1]["last_seen"], reverse=True)[:MAX_WATCH]
    return dict(items)


def load_holdings():
    """보유종목 → {code: {name, avg_price, stop_price, memo}}.

    우선순위: ALERT_HOLDINGS 환경변수(GitHub Secret — 공개 저장소에 평단/손절 노출 방지)
              → 로컬 보유종목.csv → 없으면 템플릿 생성.
    """
    raw = os.getenv("ALERT_HOLDINGS", "").lstrip("﻿").strip()
    if raw:
        lines = [ln for ln in raw.splitlines() if not ln.lstrip().startswith("#")]
    elif HOLDINGS_CSV.exists():
        with open(HOLDINGS_CSV, "r", encoding="utf-8-sig", errors="replace") as f:
            lines = [ln for ln in f if not ln.lstrip().startswith("#")]
    else:
        HOLDINGS_CSV.write_text(
            "종목명,종목코드,평단가,손절선,메모\n"
            "# 위 헤더 아래에 한 줄씩 추가. 손절선 비우면 손절 알림 생략 (예시: 삼성전자,005930,75000,72000,반도체)\n",
            encoding="utf-8-sig",
        )
        print(f"[안내] {HOLDINGS_CSV.name} 템플릿 생성 — 보유종목을 채워주세요.")
        return {}
    holdings = {}
    for row in csv.DictReader(lines):
        name = (row.get("종목명") or "").strip()
        code = (row.get("종목코드") or "").lstrip("'").strip().zfill(6)
        if not name or not code or code == "000000":
            continue
        holdings[code] = {
            "name": name,
            "avg_price": dpt._to_float(row.get("평단가") or ""),
            "stop_price": dpt._to_float(row.get("손절선") or ""),
            "memo": (row.get("메모") or "").strip(),
        }
    return holdings


# ──────────────────────────────────────
# 기준선 (일봉) — 하루 1회 캐시
# ──────────────────────────────────────

def fetch_daily_candles(code, count=75):
    """네이버 fchart 일봉 → [(date, open, high, low, close, volume)] 오름차순."""
    url = (f"https://fchart.stock.naver.com/sise.nhn?symbol={code}"
           f"&timeframe=day&count={count}&requestType=0")
    raw = http_get(url)
    out = []
    for m in re.finditer(r'data="([^"]+)"', raw):
        parts = m.group(1).split("|")
        if len(parts) != 6:
            continue
        try:
            out.append((parts[0], float(parts[1]), float(parts[2]),
                        float(parts[3]), float(parts[4]), float(parts[5])))
        except ValueError:
            continue
    return out


def build_baseline(codes, force=False):
    """{code: {high60, avg_vol20, ma20, prev_close}} — built 날짜가 오늘이면 재사용."""
    cache = load_json(BASELINE_PATH, {})
    if not force and cache.get("built") == today_str():
        missing = [c for c in codes if c not in cache.get("stocks", {})]
        if not missing:
            return cache["stocks"]
        codes = missing  # 새로 추가된 종목만 보충
        stocks = cache.get("stocks", {})
    else:
        stocks = {}
    today_ymd = now().strftime("%Y%m%d")
    print(f"[기준선] {len(codes)}종목 일봉 수집...")
    for i, code in enumerate(codes, 1):
        try:
            candles = fetch_daily_candles(code)
            # 장중이면 오늘 캔들이 포함됨 → 기준선에서 제외
            candles = [c for c in candles if c[0] != today_ymd]
            if len(candles) < 21:
                continue
            highs = [c[2] for c in candles[-60:]]
            vols = [c[5] for c in candles[-20:]]
            closes = [c[4] for c in candles[-20:]]
            stocks[code] = {
                "high60": max(highs),
                "avg_vol20": sum(vols) / len(vols),
                "ma20": sum(closes) / len(closes),
                "prev_close": candles[-1][4],
            }
        except Exception as e:
            print(f"  [경고] {code} 일봉 실패: {e}")
        if i % 25 == 0:
            print(f"  ...{i}/{len(codes)}")
    cache = {"built": today_str(), "stocks": stocks}
    save_json(BASELINE_PATH, cache)
    return stocks


# ──────────────────────────────────────
# 실시간 시세
# ──────────────────────────────────────

def fetch_quotes(codes):
    """네이버 폴링 API 배치 조회 (20개씩) → {code: {price, chg_pct, acc_volume}}.

    종목당 개별 요청 대신 묶음 조회 — 90종목 스캔이 5회 요청·수 초에 끝나
    1분 루프 모드에서도 네이버 부하·차단 위험이 낮다.
    """
    out = {}
    for i in range(0, len(codes), 20):
        chunk = codes[i:i + 20]
        url = ("https://polling.finance.naver.com/api/realtime/domestic/stock/"
               + ",".join(chunk))
        try:
            data = json.loads(http_get(url))
            for d in data.get("datas") or []:
                out[d.get("itemCode", "")] = {
                    "price": dpt._to_float(d.get("closePrice", "")),
                    "chg_pct": dpt._to_float(d.get("fluctuationsRatio", "")),
                    "acc_volume": dpt._to_float(d.get("accumulatedTradingVolume", "")),
                }
        except Exception as e:
            print(f"  [경고] 시세 배치 조회 실패 ({chunk[0]}~): {e}")
    return out


# ──────────────────────────────────────
# 조건 평가
# ──────────────────────────────────────

def check_entry_signals(code, info, q, base, ratio):
    """관심종목 진입 신호 → [(cond_key, message)]."""
    alerts = []
    volx = 0.0
    if base["avg_vol20"] > 0:
        volx = q["acc_volume"] / (base["avg_vol20"] * ratio)
    name = info["name"]
    head = f"{name} ({code})\n현재가 {fmt(q['price'])} ({q['chg_pct']:+.1f}%)"

    if q["price"] > base["high60"] and volx >= BREAKOUT_VOLX:
        alerts.append((
            "breakout",
            f"🚀 [전고점 돌파] {head}\n"
            f"60일 고점 {fmt(base['high60'])} 돌파 · 거래량 20일평균 대비 {volx:.1f}배\n"
            f"(관심종목 등록일 {info['last_seen']})"
        ))
    if volx >= SURGE_VOLX and q["chg_pct"] > 0:
        alerts.append((
            "volume_surge",
            f"🔥 [거래량 급증] {head}\n"
            f"장중 환산 거래량 20일평균 대비 {volx:.1f}배\n"
            f"(관심종목 등록일 {info['last_seen']})"
        ))
    return alerts


def check_risk_signals(code, info, q, base):
    """보유종목 리스크 신호 → [(cond_key, message)]."""
    alerts = []
    name = info["name"]
    pnl = ""
    if info["avg_price"] > 0:
        pnl_pct = (q["price"] / info["avg_price"] - 1) * 100
        pnl = f"\n평단 {fmt(info['avg_price'])} 대비 {pnl_pct:+.1f}%"
    head = f"{name} ({code})\n현재가 {fmt(q['price'])} ({q['chg_pct']:+.1f}%){pnl}"

    if info["stop_price"] > 0 and q["price"] <= info["stop_price"]:
        alerts.append((
            "stop_loss",
            f"⛔ [손절선 이탈] {head}\n손절선 {fmt(info['stop_price'])} 하향"
        ))
    if base and base["prev_close"] >= base["ma20"] > q["price"] > 0:
        alerts.append((
            "ma20_break",
            f"📉 [20일선 이탈] {head}\n20일선 {fmt(base['ma20'])} 신규 하향 이탈"
        ))
    if q["chg_pct"] <= CRASH_PCT:
        alerts.append((
            "crash",
            f"🩸 [급락 경고] {head}\n당일 {q['chg_pct']:+.1f}% (기준 {CRASH_PCT}%)"
        ))
    return alerts


# ──────────────────────────────────────
# 메인 스캔
# ──────────────────────────────────────

def scan(dry_run=False, force=False):
    if not force and not market_open():
        print(f"[{now():%H:%M}] 장외 시간 — 스캔 생략 (--force 로 강제 실행 가능)")
        return

    watch = load_watchlist()
    holdings = load_holdings()
    all_codes = sorted(set(watch) | set(holdings))
    if not all_codes:
        print("[안내] 감시 대상 없음 (관심종목 CSV / 보유종목.csv 확인)")
        return
    print(f"[{now():%H:%M}] 감시: 관심 {len(watch)} + 보유 {len(holdings)} (합집합 {len(all_codes)})")

    baseline = build_baseline(all_codes)
    sent = load_json(SENT_PATH, {})
    ratio = elapsed_ratio()
    today = today_str()
    n_alert = 0

    quotes = fetch_quotes(all_codes)
    for code in all_codes:
        q = quotes.get(code)
        if not q or q["price"] <= 0:
            continue
        base = baseline.get(code)
        alerts = []
        if code in watch and base:
            alerts += check_entry_signals(code, watch[code], q, base, ratio)
        if code in holdings:
            alerts += check_risk_signals(code, holdings[code], q, base)
        for cond, msg in alerts:
            key = f"{today}|{code}|{cond}"
            if key in sent:
                continue
            n_alert += 1
            # dry-run 은 dedup 기록 안 함 (실전송 성공 시에만 기록)
            if not dry_run and tg_send(msg):
                sent[key] = now().strftime("%H:%M")
            elif dry_run:
                tg_send(msg, dry_run=True)

    if not dry_run:
        # 오늘 이전 dedup 키 정리
        sent = {k: v for k, v in sent.items() if k.startswith(today)}
        save_json(SENT_PATH, sent)
    print(f"[{now():%H:%M}] 스캔 완료 — 신규 알림 {n_alert}건")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="텔레그램 전송 없이 콘솔 출력")
    ap.add_argument("--force", action="store_true", help="장외 시간에도 강제 스캔")
    ap.add_argument("--test", action="store_true", help="알림봇 연결 테스트")
    ap.add_argument("--get-chat-id", action="store_true", help="새 봇 chat_id 확인")
    ap.add_argument("--loop", type=int, default=0, metavar="분",
                    help="N분 동안 내부 루프 스캔 (클라우드 1분 주기용. 0=1회 스캔)")
    ap.add_argument("--interval", type=int, default=60, metavar="초",
                    help="루프 모드 스캔 간격 (기본 60초)")
    args = ap.parse_args()

    if args.get_chat_id:
        get_chat_id()
        return
    if args.test:
        ok = tg_send(f"✅ 가격알림 봇 연결 테스트 ({now():%Y-%m-%d %H:%M})")
        print("전송 성공" if ok else "전송 실패 — .env 의 ALERT_BOT_TOKEN/ALERT_CHAT_ID 확인")
        return

    if args.loop > 0:
        # 루프 모드: 서버 부팅 1회로 N분간 1분 주기 스캔 → 알림 지연 최소화
        end = now() + timedelta(minutes=args.loop)
        while now() < end:
            if not args.force and not market_open():
                print(f"[{now():%H:%M}] 장 마감 — 루프 종료")
                break
            t0 = time.time()
            try:
                scan(dry_run=args.dry_run, force=args.force)
            except Exception as e:
                print(f"[오류] 스캔 실패 (다음 주기 재시도): {e}")
            wait = args.interval - (time.time() - t0)
            if wait > 0 and now() < end:
                time.sleep(wait)
    else:
        scan(dry_run=args.dry_run, force=args.force)


if __name__ == "__main__":
    main()
