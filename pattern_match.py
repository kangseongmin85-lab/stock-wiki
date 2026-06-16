#!/usr/bin/env python3
"""
pattern_match.py — 유사국면 검색 + daily_signal 페이지 표시 (패턴 엔진 Phase 2)
기준일의 테마 벡터와 과거일들을 코사인 유사도로 비교해 가장 닮은 날을 찾고,
그 날 종목들이 '이후' 어떻게 됐는지(outcomes)를 붙인다.
--annotate 로 daily_signal 페이지의 🤖 마커 사이에 결과를 써넣는다(🧠 섹션 불침범).

입력 : _cache/situations.json, _cache/outcomes.json, _cache/macro.json
사용 :
  python pattern_match.py                 # 최신일 콘솔 출력
  python pattern_match.py --date 2026-05-29
  python pattern_match.py --past-only     # 기준일 이전만 매칭
  python pattern_match.py --annotate      # 기준일 페이지에 표시
  python pattern_match.py --annotate-all  # 모든 페이지에 표시
"""
import sys, json, math, argparse
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

BASE_DIR = Path(__file__).parent
SIT     = BASE_DIR / "_cache" / "situations.json"
OUT     = BASE_DIR / "_cache" / "outcomes.json"
MACRO   = BASE_DIR / "_cache" / "macro.json"
SIGNALS = BASE_DIR / "wiki" / "daily_signals"

BEGIN = "<!-- PATTERN:BEGIN (자동 생성 — 직접 수정 금지, 본인 분석은 아래 🧠 섹션에) -->"
END   = "<!-- PATTERN:END -->"


def load(path):
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def cosine(a, b):
    keys = set(a) | set(b)
    if not keys:
        return 0.0
    dot = sum(a.get(k, 0) * b.get(k, 0) for k in keys)
    na = math.sqrt(sum(v * v for v in a.values()))
    nb = math.sqrt(sum(v * v for v in b.values()))
    return dot / (na * nb) if na and nb else 0.0


def day_outcomes(outcomes, date):
    rows = [v for k, v in outcomes.items() if k.endswith("|" + date)]
    res = {}
    for h in ("t1", "t3", "t5"):
        vals = [r["fwd"][h] for r in rows if r.get("fwd", {}).get(h) is not None]
        res[h] = round(sum(vals) / len(vals), 2) if vals else None
    t3 = [r["fwd"]["t3"] for r in rows if r.get("fwd", {}).get("t3") is not None]
    res["win_t3"] = round(sum(1 for x in t3 if x > 0) / len(t3), 2) if t3 else None
    res["n"] = len(rows)
    return res


def similar_days(sits, target, topk=3, past_only=False):
    tv = sits[target]["theme_vector"]
    cands = []
    for d, snap in sits.items():
        if d == target:
            continue
        if past_only and d >= target:
            continue
        cands.append((cosine(tv, snap["theme_vector"]), d))
    cands.sort(reverse=True)
    return cands[:topk]


def _pct(v):
    return f"{v:+.1f}%" if isinstance(v, (int, float)) else "-"


def build_block(date, sits, outcomes, macro):
    L = [BEGIN, "", "## 🤖 패턴 엔진 (자동)", ""]
    m = macro.get(date, {})
    ks, kq, sup = m.get("kospi"), m.get("kosdaq"), m.get("supply")
    def idx(o):
        return f"{o['close']:.2f}({o['chg']:+.2f}%)" if o else "—"
    sup_s = "수급 미수집" if not sup else f"외 {sup.get('foreign')} / 기 {sup.get('inst')} / 개 {sup.get('indi')}"
    L += [f"**매크로** — KOSPI {idx(ks)} · KOSDAQ {idx(kq)} · {sup_s}", ""]

    name2fwd = {v["name"]: v.get("fwd", {}) for k, v in outcomes.items() if k.endswith("|" + date)}
    leaders = sits.get(date, {}).get("leaders", [])
    if leaders:
        L += ["**이 날 종목 사후 성과** (거래대금 상위)", "",
              "| 종목 | T+1 | T+3 | T+5 |", "|------|-----|-----|-----|"]
        for l in leaders:
            f = name2fwd.get(l["name"], {})
            L.append(f"| {l['name']} | {_pct(f.get('t1'))} | {_pct(f.get('t3'))} | {_pct(f.get('t5'))} |")
        L.append("")

    if sits.get(date, {}).get("theme_vector"):
        L += ["**유사 과거 국면** (테마 벡터 코사인)", "",
              "| 닮은 날 | 유사도 | 주도 테마 | 그날 이후평균 T+1/T+3/T+5 | T+3 승률(n) |",
              "|---------|--------|-----------|----------------------------|-------------|"]
        for sim, d in similar_days(sits, date, topk=3):
            dtop = ", ".join(t for t, _ in sorted(sits[d]["theme_vector"].items(), key=lambda x: -x[1])[:3])
            oc = day_outcomes(outcomes, d)
            win = f"{int(oc['win_t3'] * 100)}%" if oc["win_t3"] is not None else "-"
            avg = f"{_pct(oc['t1'])} / {_pct(oc['t3'])} / {_pct(oc['t5'])}"
            L.append(f"| [[daily_signals/{d}]] | {sim:.2f} | {dtop} | {avg} | {win} (n={oc['n']}) |")
        L += ["", "> ⚠️ 공부일 표본 적음 — 통계 참고용. 누적될수록 신뢰도 상승."]

    L += ["", END]
    return "\n".join(L)


def upsert_block(text, block):
    if BEGIN in text and END in text:
        return text[:text.index(BEGIN)] + block + text[text.index(END) + len(END):]
    anchor = "## 🧠 본인 분석"
    if anchor in text:
        i = text.index(anchor)
        return text[:i] + block + "\n\n" + text[i:]
    return text.rstrip() + "\n\n" + block + "\n"


def annotate(date, sits, outcomes, macro):
    page = SIGNALS / f"{date}.md"
    if not page.exists():
        print(f"  [skip] {date}.md 없음"); return
    page.write_text(upsert_block(page.read_text(encoding="utf-8"),
                                 build_block(date, sits, outcomes, macro)), encoding="utf-8")
    print(f"  [표시] {date}.md")


def print_query(target, sits, outcomes, past_only):
    tv = sits[target]["theme_vector"]
    if not tv:
        print(f"[경고] {target} 테마 벡터 비어있음"); return
    ttop = ", ".join(t for t, _ in sorted(tv.items(), key=lambda x: -x[1])[:4])
    print(f"[기준일 {target}] 주도 테마: {ttop}\n== 닮은 과거 국면 ==")
    for sim, d in similar_days(sits, target, 3, past_only):
        dtop = ", ".join(t for t, _ in sorted(sits[d]["theme_vector"].items(), key=lambda x: -x[1])[:4])
        oc = day_outcomes(outcomes, d)
        win = f"{int(oc['win_t3'] * 100)}%" if oc["win_t3"] is not None else "-"
        print(f"  {d} (유사도 {sim:.2f}) | {dtop}")
        print(f"      이후평균 T+1 {oc['t1']} / T+3 {oc['t3']} / T+5 {oc['t5']} | 승률 {win} (n={oc['n']})")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date")
    ap.add_argument("--past-only", action="store_true")
    ap.add_argument("--annotate", action="store_true")
    ap.add_argument("--annotate-all", action="store_true")
    args = ap.parse_args()

    sits, outcomes, macro = load(SIT), load(OUT), load(MACRO)
    if not sits:
        print("[경고] situations.json 없음 — situation_index.py 먼저 실행"); return

    if args.annotate_all:
        print("== daily_signal 표시 (전체) ==")
        for d in sorted(sits):
            annotate(d, sits, outcomes, macro)
        return

    target = args.date or max(sits)
    if target not in sits:
        print(f"[경고] {target} 스냅샷 없음"); return
    if args.annotate:
        annotate(target, sits, outcomes, macro)
    else:
        print_query(target, sits, outcomes, args.past_only)


if __name__ == "__main__":
    main()
