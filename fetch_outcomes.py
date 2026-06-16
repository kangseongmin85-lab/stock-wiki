#!/usr/bin/env python3
"""
fetch_outcomes.py — daily picks 사후 성과 라벨 + 거래일별 매크로 (패턴 엔진 Phase 1)
====================================================================================
종목이 잡힌 뒤 주가가 어떻게 됐는가(T+1~10 종가 수익률)를 자동 라벨링 + 일자별 지수·수급.

입력 : _cache/daily_picks_history.json
출력 : _cache/outcomes.json , _cache/macro.json

견고화(2026-06-02): 종목당 네트워크 타임아웃(무한 멈춤 방지) + 중간 저장(중단해도 진행분 보존).

사용:
  python fetch_outcomes.py            # 전체 백필 + 미완성 보충 + 매크로
  python fetch_outcomes.py --date 2026-05-29
  python fetch_outcomes.py --stock 005930
  python fetch_outcomes.py --macro-only
  python fetch_outcomes.py --dry-run
"""

import os, sys, json, argparse, urllib.request, urllib.parse, threading, time
from datetime import datetime, timedelta
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

BASE_DIR   = Path(__file__).parent
CACHE_DIR  = BASE_DIR / "_cache"
HISTORY    = CACHE_DIR / "daily_picks_history.json"
OUTCOMES   = CACHE_DIR / "outcomes.json"
MACRO      = CACHE_DIR / "macro.json"
HORIZONS   = list(range(1, 11))          # T+1 ~ T+10 (거래일)
FINALIZE_AFTER_DAYS = 25
TIMEOUT_SEC = 12                          # 종목당 네트워크 한도 — 넘으면 건너뜀
SAVE_EVERY  = 15                          # N종목마다 중간 저장


def load_json(path):
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_json(path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _call_with_timeout(fn, *args, timeout=TIMEOUT_SEC):
    """fn(*args) 를 데몬 스레드로 실행. timeout 초 넘으면 None 반환(포기)."""
    box = {}
    def run():
        try:
            box["v"] = fn(*args)
        except Exception as e:
            box["e"] = e
    t = threading.Thread(target=run, daemon=True)
    t.start()
    t.join(timeout)
    if t.is_alive():
        return None
    if "e" in box:
        raise box["e"]
    return box.get("v")


# ── 주가 OHLCV (pykrx 우선, FDR 폴백, 각 호출 타임아웃) ────────────────────
def _ohlcv_pykrx(code, start, end):
    from pykrx import stock
    df = stock.get_market_ohlcv(start, end, code)
    if df is None or df.empty:
        return []
    out = []
    for idx, row in df.iterrows():
        d = idx.strftime("%Y-%m-%d") if hasattr(idx, "strftime") else str(idx)[:10]
        out.append((d, float(row["종가"]), float(row["고가"]), float(row["저가"])))
    return out

def _ohlcv_fdr(code, start, end):
    import FinanceDataReader as fdr
    df = fdr.DataReader(code, start.replace("-", ""), end.replace("-", ""))
    if df is None or df.empty:
        return []
    out = []
    for idx, row in df.iterrows():
        d = idx.strftime("%Y-%m-%d")
        out.append((d, float(row["Close"]), float(row["High"]), float(row["Low"])))
    return out

def get_ohlcv(code, flag_date):
    d0  = datetime.strptime(flag_date, "%Y-%m-%d")
    end = (d0 + timedelta(days=35)).strftime("%Y%m%d")
    start = d0.strftime("%Y%m%d")
    for fetch in (_ohlcv_pykrx, _ohlcv_fdr):
        try:
            rows = _call_with_timeout(fetch, code, start, end)
            if rows:
                return sorted(rows, key=lambda r: r[0])
        except Exception:
            continue
    return []


# ── 성과 라벨 계산 ─────────────────────────────────────────────────────────
def compute_outcome(code, flag_date, name=""):
    rows = get_ohlcv(code, flag_date)
    rows = [r for r in rows if r[0] >= flag_date]
    if not rows or rows[0][0] != flag_date:
        return None
    base = rows[0][1]
    if base <= 0:
        return None
    closes = [r[1] for r in rows]
    highs  = [r[2] for r in rows]
    lows   = [r[3] for r in rows]

    fwd = {}
    for h in HORIZONS:
        if len(closes) > h and closes[h] > 0:
            fwd[f"t{h}"] = round((closes[h] / base - 1) * 100, 2)
        else:
            fwd[f"t{h}"] = None

    def mfe(n):
        w = [x for x in highs[1:n + 1] if x > 0]
        return round((max(w) / base - 1) * 100, 2) if w else None
    def mae(n):
        w = [x for x in lows[1:n + 1] if x > 0]
        return round((min(w) / base - 1) * 100, 2) if w else None

    elapsed = (datetime.now() - datetime.strptime(flag_date, "%Y-%m-%d")).days
    complete = all(fwd[f"t{h}"] is not None for h in HORIZONS)
    status = "complete" if complete else ("final" if elapsed > FINALIZE_AFTER_DAYS else "partial")

    return {
        "name": name, "flag_close": base, "fwd": fwd,
        "mfe_t5": mfe(5), "mae_t5": mae(5), "mfe_t10": mfe(10), "mae_t10": mae(10),
        "status": status, "computed_at": datetime.now().isoformat(timespec="seconds"),
    }


def needs_compute(entry):
    if not entry:
        return True
    return entry.get("status") not in ("complete", "final")


# ── 매크로: 지수 (Naver 일별 히스토리) ─────────────────────────────────────
def _naver_index_history(code, pages=2, size=30):
    rows = {}
    for page in range(1, pages + 1):
        url = f"https://m.stock.naver.com/api/index/{code}/price?pageSize={size}&page={page}"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        try:
            with urllib.request.urlopen(req, timeout=6) as r:
                data = json.loads(r.read().decode("utf-8"))
        except Exception:
            break
        if not data:
            break
        for x in data:
            d = (x.get("localTradedAt") or "")[:10]
            if not d:
                continue
            try:
                close = float(str(x.get("closePrice", "")).replace(",", ""))
                chg   = float(str(x.get("fluctuationsRatio", "")).replace(",", ""))
            except (TypeError, ValueError):
                continue
            rows[d] = {"close": close, "chg": chg}
    return rows

def fetch_index_history():
    return {"kospi": _naver_index_history("KOSPI"), "kosdaq": _naver_index_history("KOSDAQ")}


# ── 매크로: 수급 (pykrx, 타임아웃) ─────────────────────────────────────────
def fetch_supply(date):
    ymd = date.replace("-", "")
    def _q():
        from pykrx import stock
        return stock.get_market_trading_value_by_date(ymd, ymd, "KOSPI")
    try:
        df = _call_with_timeout(_q)
        if df is None or df.empty:
            return None
        row = df.iloc[-1]
        def g(*names):
            for n in names:
                if n in df.columns:
                    try:    return int(row[n])
                    except (TypeError, ValueError): return None
            return None
        return {"foreign": g("외국인합계", "외국인"),
                "inst":    g("기관합계", "기관"),
                "indi":    g("개인")}
    except Exception:
        return None


# ── 백필 ───────────────────────────────────────────────────────────────────
def run_outcomes(only_date=None, only_stock=None, dry_run=False):
    history  = load_json(HISTORY)
    outcomes = load_json(OUTCOMES)
    if not history:
        print(f"[경고] {HISTORY.name} 없음 — daily_picks_tracker 먼저 실행 필요"); return outcomes

    todo = []
    for code, picks in history.items():
        if only_stock and code != only_stock:
            continue
        for p in picks:
            fdate = p.get("date")
            if not fdate or (only_date and fdate != only_date):
                continue
            todo.append((code, fdate, p.get("name", "")))

    total = len(todo)
    done = skipped = failed = 0
    for i, (code, fdate, name) in enumerate(todo, 1):
        key = f"{code}|{fdate}"
        if not needs_compute(outcomes.get(key)):
            skipped += 1; continue
        res = compute_outcome(code, fdate, name)
        if res is None:
            failed += 1
            print(f"  [{i}/{total}] {name}({code}) {fdate} - 건너뜀(데이터없음/타임아웃)", flush=True)
            continue
        outcomes[key] = res
        done += 1
        f = res["fwd"]
        print(f"  [{i}/{total}] {name}({code}) {fdate} | T+1 {f['t1']} T+3 {f['t3']} T+5 {f['t5']} T+10 {f['t10']} [{res['status']}]", flush=True)
        if done % SAVE_EVERY == 0 and not dry_run:
            save_json(OUTCOMES, outcomes)
            print(f"      ...중간 저장 ({done}건)", flush=True)
        time.sleep(0.15)

    print(f"\n[outcomes] 갱신 {done} / skip(완료) {skipped} / 건너뜀 {failed} / 총 {total}")
    if not dry_run:
        save_json(OUTCOMES, outcomes)
        print(f"[저장] {OUTCOMES}")
    return outcomes


def run_macro(only_date=None, dry_run=False):
    history = load_json(HISTORY)
    macro   = load_json(MACRO)
    dates = sorted({p["date"] for picks in history.values() for p in picks if p.get("date")})
    if only_date:
        dates = [d for d in dates if d == only_date]
    if not dates:
        print("[macro] 대상 날짜 없음"); return macro

    idx = fetch_index_history()
    for d in dates:
        ks, kq = idx["kospi"].get(d), idx["kosdaq"].get(d)
        macro[d] = {"kospi": ks, "kosdaq": kq, "supply": fetch_supply(d)}
        ks_s = f"{ks['close']:.2f}({ks['chg']:+.2f}%)" if ks else "—"
        kq_s = f"{kq['close']:.2f}({kq['chg']:+.2f}%)" if kq else "—"
        sup  = macro[d]["supply"]
        sup_s = "수급 미수집" if sup is None else f"외{sup.get('foreign')}/기{sup.get('inst')}/개{sup.get('indi')}"
        print(f"  {d} | KOSPI {ks_s} | KOSDAQ {kq_s} | {sup_s}", flush=True)

    if not dry_run:
        save_json(MACRO, macro)
        print(f"[저장] {MACRO}")
    return macro


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date")
    ap.add_argument("--stock")
    ap.add_argument("--macro-only", action="store_true")
    ap.add_argument("--no-macro", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    CACHE_DIR.mkdir(exist_ok=True)
    if not args.macro_only:
        print("=== 성과 라벨 (T+1~10, 종가) ===")
        run_outcomes(args.date, args.stock, args.dry_run)
    if not args.no_macro and not args.stock:
        print("\n=== 거래일별 매크로 (지수·수급) ===")
        run_macro(args.date, args.dry_run)


if __name__ == "__main__":
    main()
