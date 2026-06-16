# -*- coding: utf-8 -*-
"""
AIDC 순환 트래킹 — 분야·종목별 상대강도(rs) 추세로 가열/냉각 감지.
교차검증(aidc_crosscheck.py)이 '오늘 스냅샷'이면, 이건 '며칠 흐름'.
메모리 [[theme-rotation-workflow]] 패턴 + AIDC 한정.

rs(상대강도) = 종목 등락률 − AIDC 그룹 중앙값(그날). 시장 영향 제거하고 '돈이 콕 집어 들어온 정도'.
가열 = 최근 rs가 직전 평균보다 상승 / 냉각 = 하락.

사용: python aidc_rotation.py          (최근 5일 자동)
      python aidc_rotation.py 7        (최근 7일)
산출: wiki/analysis/AIDC_순환트래킹.md (최신 덮어쓰기)
"""
import csv, sys, os, glob, datetime
from aidc_crosscheck import AIDC, parse_num

BASE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(BASE, "data", "themes")
OUT = os.path.join(BASE, "wiki", "analysis")

def load_day(date):
    """그날 AIDC 종목 → {name: {chg, value}} (테마중복시 최대 거래대금)."""
    path = os.path.join(DATA, f"{date}_stocks.csv")
    if not os.path.exists(path): return None
    real = {}
    with open(path, encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            name = r["stock"].strip()
            if name not in AIDC: continue
            val = parse_num(r["value_mil"])
            if name not in real or val > real[name]["value"]:
                real[name] = {"chg": parse_num(r["chg"]), "value": val}
    # 그룹 중앙값 → rs
    chgs = sorted(d["chg"] for d in real.values())
    n = len(chgs)
    med = chgs[n//2] if n%2 else (chgs[n//2-1]+chgs[n//2])/2 if n else 0
    for d in real.values():
        d["rs"] = d["chg"] - med
    return {"real": real, "median": med, "n": n}

def main():
    ndays = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1].isdigit() else 5
    dates = sorted(os.path.basename(p).split("_")[0]
                   for p in glob.glob(os.path.join(DATA, "*_stocks.csv")))
    dates = dates[-ndays:]
    if len(dates) < 2: sys.exit("최소 2일치 stocks csv 필요 — fetch_themes.py 더 누적")

    days = {d: load_day(d) for d in dates}
    days = {d: v for d, v in days.items() if v}
    # 주말·휴장 중복 제거: 직전일과 등락률 스냅샷이 동일하면 비거래일로 보고 드롭
    dedup, prev_sig = {}, None
    for d in sorted(days):
        sig = tuple(sorted((n, round(x["chg"],2)) for n,x in days[d]["real"].items()))
        if sig != prev_sig:
            dedup[d] = days[d]; prev_sig = sig
    days = dedup
    dlist = sorted(days)
    latest = dlist[-1]

    # ── 종목별 rs 시계열 ──
    series = {}  # name -> {date: rs}
    valseries = {}  # name -> {date: value}
    for name in AIDC:
        series[name] = {d: days[d]["real"].get(name, {}).get("rs") for d in dlist}
        valseries[name] = {d: days[d]["real"].get(name, {}).get("value") for d in dlist}

    # 가열/냉각 = 최신 rs − 직전 며칠 rs 평균
    def momentum(name):
        s = [series[name][d] for d in dlist if series[name][d] is not None]
        if len(s) < 2: return None
        return s[-1] - (sum(s[:-1]) / len(s[:-1]))

    # ── 분야별 집계 (그날 분야평균 rs) ──
    fields = sorted(set(f for *_, f in AIDC.values()))
    field_rs = {f: {} for f in fields}
    for f in fields:
        members = [n for n,(*_, ff) in AIDC.items() if ff == f]
        for d in dlist:
            vals = [series[n][d] for n in members if series[n][d] is not None]
            field_rs[f][d] = round(sum(vals)/len(vals), 1) if vals else None
    def field_mom(f):
        s = [field_rs[f][d] for d in dlist if field_rs[f][d] is not None]
        if len(s) < 2: return None
        return round(s[-1] - sum(s[:-1])/len(s[:-1]), 1)

    # ── 신호 분류 ──
    heating, cooling, persistent = [], [], []
    for name in AIDC:
        m = momentum(name)
        cur = series[name].get(latest)
        if m is None or cur is None: continue
        if m >= 3:   heating.append((name, m, cur))
        if m <= -3:  cooling.append((name, m, cur))
        s = [series[name][d] for d in dlist if series[name][d] is not None]
        if len(s) >= 3 and all(x >= 3 for x in s[-3:]):
            persistent.append((name, cur))
    heating.sort(key=lambda x:-x[1]); cooling.sort(key=lambda x:x[1])

    grade = {n: g for n,(g,*_) in AIDC.items()}
    fieldof = {n: f for n,(*_, f) in AIDC.items()}
    def rsfmt(v): return f"{v:+.1f}" if v is not None else " · "

    L = []
    L.append("---")
    L.append("type: analysis")
    L.append("title: AIDC 순환 트래킹 (rs 추세)")
    L.append(f"date: {latest}")
    L.append("tags: [AI데이터센터, 순환, 상대강도]")
    L.append("related: [[analysis/AIDC_매매교차검증_" + latest + "]], [[analysis/AIDC_카탈리스트_캘린더]]")
    L.append("status: 활성")
    L.append("---\n")
    L.append("# AIDC 순환 트래킹 (상대강도 rs 추세)\n")
    L.append(f"> 기간 {dlist[0]} ~ {latest} ({len(dlist)}일). rs = 종목등락 − AIDC그룹중앙값(시장영향 제거).")
    L.append("> 가열 = 최신 rs가 직전평균 대비 +3↑(자금 새로 유입), 냉각 = −3↓(자금 이탈). **분야 로테이션 = 어느 하위분야로 돈이 도는가.**\n")

    L.append("## 🔄 분야 로테이션 (어느 하위분야로 자금이 도나)")
    L.append("> 분야별 구성종목 평균 rs. ↑가열 ↓냉각 →유지.\n")
    header = "| 분야 | " + " | ".join(dlist) + " | 모멘텀 |"
    L.append(header)
    L.append("|------|" + "------:|"*len(dlist) + ":---:|")
    frows = sorted(fields, key=lambda f:-(field_mom(f) if field_mom(f) is not None else -99))
    for f in frows:
        cells = " | ".join(rsfmt(field_rs[f][d]) for d in dlist)
        m = field_mom(f)
        arrow = "🔥↑" if (m or 0) >= 2 else ("❄️↓" if (m or 0) <= -2 else "→")
        L.append(f"| {f} | {cells} | {rsfmt(m)} {arrow} |")
    L.append("")

    L.append("## 🔥 가열 종목 (rs 상승전환 — 자금 새로 유입)")
    if heating:
        for name, m, cur in heating:
            L.append(f"- **{name}** ({grade[name]} · {fieldof[name]}) rs {cur:+.1f} / 모멘텀 {m:+.1f} ↑")
    else:
        L.append("- 뚜렷한 가열 종목 없음.")
    L.append("")

    L.append("## ❄️ 냉각 종목 (rs 하락전환 — 자금 이탈, 보유시 주의)")
    if cooling:
        for name, m, cur in cooling:
            L.append(f"- **{name}** ({grade[name]} · {fieldof[name]}) rs {cur:+.1f} / 모멘텀 {m:+.1f} ↓")
    else:
        L.append("- 뚜렷한 냉각 종목 없음.")
    L.append("")

    if persistent:
        L.append("## 💪 지속 강자 (최근 3일 연속 rs≥3 — 추세 강건)")
        for name, cur in persistent:
            L.append(f"- **{name}** ({grade[name]} · {fieldof[name]}) 현재 rs {cur:+.1f}")
        L.append("")

    # ── 종목별 rs 히트맵 (최신 rs 순) ──
    L.append("## 📊 종목별 rs 히트맵 (최신 상대강도 순)")
    L.append("> 빈칸 = 그날 테마 미노출(수급 약함).\n")
    L.append("| 종목 | 익스포저 | 분야 | " + " | ".join(dlist) + " |")
    L.append("|------|:---:|----|" + "------:|"*len(dlist))
    order = sorted(AIDC, key=lambda n:-(series[n][latest] if series[n][latest] is not None else -99))
    for name in order:
        cells = " | ".join(rsfmt(series[name][d]) for d in dlist)
        L.append(f"| {name} | {grade[name]} | {fieldof[name]} | {cells} |")
    L.append("")

    L.append("---")
    L.append("## 매매 해석 가이드")
    L.append("- **분야 로테이션 🔥↑** = 그 하위분야로 자금이 도는 중 → 해당분야 [공시]검증·코어주 우선 관찰.")
    L.append("- **가열+코어** = 가장 신뢰. **가열+주변** = 단기 회전매매(추격주의, 캘린더 임박트리거 확인).")
    L.append("- **냉각 종목 보유 중**이면 분할익절·손절 점검. 특히 코어주 냉각은 체인 전체 신호일 수 있음.")
    L.append("- 절대 등락률 아닌 rs 기준이므로, 지수 급락일에도 '진짜 강한 종목'이 보인다.")
    L.append(f"\n## 마지막 업데이트\n{datetime.date.today()} | aidc_rotation.py | {dlist[0]}~{latest}")

    os.makedirs(OUT, exist_ok=True)
    outpath = os.path.join(OUT, "AIDC_순환트래킹.md")
    with open(outpath, "w", encoding="utf-8") as f:
        f.write("\n".join(L))
    print(f"OK -> {outpath}")
    print(f"기간 {dlist[0]}~{latest} {len(dlist)}일 | 가열 {len(heating)} 냉각 {len(cooling)} 지속강자 {len(persistent)}")
    if frows:
        top = frows[0]
        print(f"최고 가열분야: {top} (모멘텀 {field_mom(top)})")

if __name__ == "__main__":
    main()
