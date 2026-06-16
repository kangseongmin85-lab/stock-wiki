#!/usr/bin/env python3
"""fetch_levels.py — 시황분석용 종목 가격 레벨·손익비 산출
=================================================================
시황 분석(market-analyst)이 "진입존·손절·목표·손익비"를 추상적 코멘트가 아니라
실제 숫자로 쓸 수 있도록, 종목별 핵심 레벨을 한 번에 뽑아 캐시한다.

산출 레벨 (네이버 일봉 + 실시간 시세):
  - 현재가(장중) / 전일종가
  - 5일선 · 20일선 · 60일선
  - 60일 전고점 · 20일 최저(지지 후보)
  - 20일선 이격도(%)  ← 과열 정량화
  - 연속 상승일(종가 기준)  ← 과열 정량화
  - 거래량 배수 (당일/전일 대비 20일 평균)
  - 손절 후보 (보유종목 손절선 명시값 / 그 외 20일선·최근 저점)
  - 스윙 R/R (기계계산: 5일선 진입·20일선 손절·전고점 목표)

종목명→코드 해결: 관심종목 14일 누적 CSV + 보유종목.csv 인덱스 → 미해결분만 DART 폴백.

사용:
  python fetch_levels.py --names "원익IPS,HPSP,한미반도체"
  python fetch_levels.py --watchlist          # 관심종목 14일 누적 전체
  python fetch_levels.py --holdings           # 보유종목.csv (평단/손절/손익률 포함)
  python fetch_levels.py --all                # 관심 + 보유 합집합
  python fetch_levels.py --names "..." --codes "신규종목:123456"   # 코드 직접 보완
  python fetch_levels.py --names "..." --quiet                     # 표 출력 생략(저장만)

산출물: _cache/levels_YYYY-MM-DD.json  (market-analyst 가 읽음) + stdout 마크다운 표
"""

import os
import sys
import csv
import json
import re
import argparse
from datetime import timedelta
from pathlib import Path

if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import daily_picks_tracker as dpt
from price_alert import (
    fetch_daily_candles, fetch_quotes, now, today_str, market_open,
    PICKS_DIR, HOLDINGS_CSV, CACHE_DIR, WATCH_DAYS,
)

DART_API_KEY = os.getenv("DART_API_KEY", "")
_dart_corps = None  # DART corp list 1회 다운로드 캐시 (미해결분 있을 때만)


# ──────────────────────────────────────
# 종목명 → 코드 해결
# ──────────────────────────────────────

def build_name_index():
    """관심종목 14일 누적 + 보유종목 → {정규화이름: code}."""
    idx = {}
    # 관심종목 (최근 WATCH_DAYS 일)
    cutoff = (now() - timedelta(days=WATCH_DAYS)).strftime("%Y-%m-%d")
    for p in sorted(PICKS_DIR.glob("*.csv")):
        d = dpt.parse_csv_date(p.name)
        if not d or d < cutoff:
            continue
        try:
            for row in dpt.parse_picks_csv(p):
                idx[_norm(row["name"])] = row["code"].zfill(6)
        except Exception:
            pass
    # 보유종목
    for name, code in _read_holdings_codes().items():
        idx[_norm(name)] = code
    return idx


def _norm(name):
    """공백·대소문자 무시 매칭용 정규화."""
    return re.sub(r"\s+", "", (name or "")).upper()


def _read_holdings_codes():
    out = {}
    if not HOLDINGS_CSV.exists():
        return out
    with open(HOLDINGS_CSV, "r", encoding="utf-8-sig", errors="replace") as f:
        lines = [ln for ln in f if not ln.lstrip().startswith("#")]
    for row in csv.DictReader(lines):
        name = (row.get("종목명") or "").strip()
        code = (row.get("종목코드") or "").lstrip("'").strip().zfill(6)
        if name and code and code != "000000":
            out[name] = code
    return out


def resolve_dart(name):
    """DART corp list 로 종목명 → 6자리 코드. 실패/키없음 시 None."""
    global _dart_corps
    if not DART_API_KEY:
        return None
    try:
        if _dart_corps is None:
            import dart_fss as dart
            dart.set_api_key(DART_API_KEY)
            _dart_corps = dart.get_corp_list()
        res = (_dart_corps.find_by_corp_name(name, exactly=True)
               or _dart_corps.find_by_corp_name(name, exactly=False))
        if not res:
            return None
        corp = res[0] if isinstance(res, list) else res
        code = (getattr(corp, "stock_code", "") or "").strip()
        return code.zfill(6) if code else None
    except Exception as e:
        print(f"  [경고] DART 코드해결 실패 ({name}): {e}")
        return None


def resolve_codes(names, extra=None):
    """{name: code|None}. extra: {name: code} 수동 지정 우선."""
    extra = extra or {}
    idx = build_name_index()
    out = {}
    for name in names:
        if name in extra:
            out[name] = extra[name].zfill(6)
            continue
        code = idx.get(_norm(name))
        if not code:
            code = resolve_dart(name)
        out[name] = code
    return out


# ──────────────────────────────────────
# 레벨 계산
# ──────────────────────────────────────

def ma(closes, n):
    return sum(closes[-n:]) / n if len(closes) >= n else None


def up_streak(closes):
    """종가 기준 연속 상승일 (마지막 캔들부터 거꾸로)."""
    s = 0
    for i in range(len(closes) - 1, 0, -1):
        if closes[i] > closes[i - 1]:
            s += 1
        else:
            break
    return s


def compute_levels(code):
    """일봉으로 레벨 dict 산출. 데이터 부족/실패 시 None."""
    today_ymd = now().strftime("%Y%m%d")
    candles = fetch_daily_candles(code, count=75)
    # 장중이면 오늘 캔들 포함 → 기준선에서 제외 (전일 종가 기준)
    candles = [c for c in candles if c[0] != today_ymd]
    if len(candles) < 21:
        return None
    closes = [c[4] for c in candles]
    highs = [c[2] for c in candles]
    lows = [c[3] for c in candles]
    vols = [c[5] for c in candles]
    prev_close = closes[-1]
    lv = {
        "prev_close": round(prev_close),
        "ma5": round(ma(closes, 5)) if ma(closes, 5) else None,
        "ma20": round(ma(closes, 20)) if ma(closes, 20) else None,
        "ma60": round(ma(closes, 60)) if ma(closes, 60) else None,
        "high60": round(max(highs[-60:])),
        "high20": round(max(highs[-20:])),
        "low5": round(min(lows[-5:])),
        "low20": round(min(lows[-20:])),
        "avg_vol20": round(sum(vols[-20:]) / 20),
        "last_vol": round(vols[-1]),
        "up_streak": up_streak(closes),
    }
    return lv


def enrich(lv, quote):
    """실시간 시세 결합 → 현재가·이격도·거래량배수·스윙RR."""
    price = quote["price"] if quote and quote.get("price", 0) > 0 else lv["prev_close"]
    # 장중에만 '실시간' — 장외/주말엔 quote 가 직전 세션 종가라 라벨을 직전세션으로
    intraday = bool(market_open() and quote and quote.get("price", 0) > 0
                    and quote.get("acc_volume", 0) > 0)
    lv["price"] = round(price)
    lv["chg_pct"] = round(quote["chg_pct"], 2) if quote else None
    lv["intraday"] = intraday

    if lv.get("ma20"):
        lv["gap_ma20_pct"] = round((price / lv["ma20"] - 1) * 100, 1)
    if lv.get("ma5"):
        lv["gap_ma5_pct"] = round((price / lv["ma5"] - 1) * 100, 1)

    cur_vol = quote.get("acc_volume", 0) if intraday else lv["last_vol"]
    if lv["avg_vol20"] > 0:
        lv["vol_x"] = round(cur_vol / lv["avg_vol20"], 1)
        lv["vol_x_basis"] = "장중누적" if intraday else "직전세션"

    # 스윙 R/R 기계계산: 5일선 진입 · 20일선 손절 · 60일전고 목표
    entry, stop, target = lv.get("ma5"), lv.get("ma20"), lv.get("high60")
    if entry and stop and target and entry > stop and target > entry:
        lv["swing_rr"] = round((target - entry) / (entry - stop), 1)
        lv["swing_entry"], lv["swing_stop"], lv["swing_target"] = entry, stop, target
    else:
        lv["swing_rr"] = None
    return lv


# ──────────────────────────────────────
# 메인
# ──────────────────────────────────────

def gather_names(args):
    names, holdings_meta = [], {}
    if args.names:
        names += [n.strip() for n in args.names.split(",") if n.strip()]
    if args.watchlist or args.all:
        for name, _code in _watchlist_names().items():
            names.append(name)
    if args.holdings or args.all:
        h = _read_holdings_meta()
        holdings_meta.update(h)
        names += list(h.keys())
    # 중복 제거(입력 순서 보존)
    seen, uniq = set(), []
    for n in names:
        if _norm(n) not in seen:
            seen.add(_norm(n)); uniq.append(n)
    return uniq, holdings_meta


def _watchlist_names():
    idx = {}
    cutoff = (now() - timedelta(days=WATCH_DAYS)).strftime("%Y-%m-%d")
    for p in sorted(PICKS_DIR.glob("*.csv")):
        d = dpt.parse_csv_date(p.name)
        if not d or d < cutoff:
            continue
        try:
            for row in dpt.parse_picks_csv(p):
                idx[row["name"]] = row["code"].zfill(6)
        except Exception:
            pass
    return idx


def _read_holdings_meta():
    """보유종목 → {name: {code, avg_price, stop_price, memo}}."""
    out = {}
    if not HOLDINGS_CSV.exists():
        return out
    with open(HOLDINGS_CSV, "r", encoding="utf-8-sig", errors="replace") as f:
        lines = [ln for ln in f if not ln.lstrip().startswith("#")]
    for row in csv.DictReader(lines):
        name = (row.get("종목명") or "").strip()
        code = (row.get("종목코드") or "").lstrip("'").strip().zfill(6)
        if not name or not code or code == "000000":
            continue
        out[name] = {
            "code": code,
            "avg_price": dpt._to_float(row.get("평단가") or ""),
            "stop_price": dpt._to_float(row.get("손절선") or ""),
            "memo": (row.get("메모") or "").strip(),
        }
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--names", help="쉼표구분 종목명")
    ap.add_argument("--watchlist", action="store_true", help="관심종목 14일 누적")
    ap.add_argument("--holdings", action="store_true", help="보유종목.csv")
    ap.add_argument("--all", action="store_true", help="관심+보유 합집합")
    ap.add_argument("--codes", help="수동 코드 보완 (이름:코드,이름:코드)")
    ap.add_argument("--quiet", action="store_true", help="표 출력 생략(저장만)")
    args = ap.parse_args()

    extra = {}
    if args.codes:
        for pair in args.codes.split(","):
            if ":" in pair:
                nm, cd = pair.split(":", 1)
                extra[nm.strip()] = cd.strip()

    names, holdings_meta = gather_names(args)
    if not names:
        print("[안내] 대상 종목 없음 — --names / --watchlist / --holdings / --all 중 하나 지정")
        return

    code_map = resolve_codes(names, extra)
    codes = [c for c in code_map.values() if c]
    print(f"[레벨] {len(names)}종목 (코드해결 {len(codes)}) 일봉·시세 수집...")
    quotes = fetch_quotes(codes) if codes else {}

    result = {"date": today_str(), "generated": now().strftime("%Y-%m-%d %H:%M"), "stocks": {}}
    unresolved = []
    for name in names:
        code = code_map.get(name)
        if not code:
            unresolved.append(name)
            continue
        lv = compute_levels(code)
        if not lv:
            result["stocks"][name] = {"code": code, "error": "일봉부족/실패"}
            continue
        enrich(lv, quotes.get(code))
        lv["code"] = code
        if name in holdings_meta:
            hm = holdings_meta[name]
            lv["holding"] = True
            lv["avg_price"] = round(hm["avg_price"]) if hm["avg_price"] else None
            lv["stop_price"] = round(hm["stop_price"]) if hm["stop_price"] else None
            if hm["avg_price"] > 0 and lv.get("price"):
                lv["pnl_pct"] = round((lv["price"] / hm["avg_price"] - 1) * 100, 1)
        result["stocks"][name] = lv

    if unresolved:
        result["unresolved"] = unresolved

    CACHE_DIR.mkdir(exist_ok=True)
    out_path = CACHE_DIR / f"levels_{today_str()}.json"
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=1), encoding="utf-8")

    if not args.quiet:
        print_table(result)
    print(f"\n저장: {out_path}")
    if unresolved:
        print(f"코드 미해결({len(unresolved)}): {', '.join(unresolved)}"
              f"  → --codes \"이름:코드\" 로 보완 가능")


def print_table(result):
    rows = result["stocks"]
    def g(d, k, suf=""):
        v = d.get(k)
        return f"{v:,}{suf}" if isinstance(v, (int, float)) and k not in ("up_streak",) else (
            f"{v}{suf}" if v is not None else "-")
    print("\n| 종목 | 현재가 | 5일선 | 20일선 | 60일전고 | 20일이격 | 연속↑ | 거래량x | 스윙RR | 보유 |")
    print("|------|-------:|------:|-------:|--------:|--------:|------:|-------:|------:|------|")
    for name, d in rows.items():
        if d.get("error"):
            print(f"| {name} | (오류: {d['error']}) | | | | | | | | |")
            continue
        gap = f"{d.get('gap_ma20_pct'):+.1f}%" if d.get("gap_ma20_pct") is not None else "-"
        volx = f"{d.get('vol_x')}x" if d.get("vol_x") is not None else "-"
        rr = f"1:{d.get('swing_rr')}" if d.get("swing_rr") else "-"
        hold = ""
        if d.get("holding"):
            pnl = f"{d.get('pnl_pct'):+.1f}%" if d.get("pnl_pct") is not None else ""
            hold = f"평단{d.get('avg_price'):,}/{pnl}" if d.get("avg_price") else "보유"
        print(f"| {name} | {g(d,'price')} | {g(d,'ma5')} | {g(d,'ma20')} | "
              f"{g(d,'high60')} | {gap} | {d.get('up_streak','-')}일 | {volx} | {rr} | {hold} |")


if __name__ == "__main__":
    main()
