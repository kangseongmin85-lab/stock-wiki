# -*- coding: utf-8 -*-
"""
AIDC 매매 교차검증 — 협업체인(검증완료) × 네이버 실거래(거래대금·등락률)
출처 지도: wiki/analysis/AIDC_밸류체인_2026-06-09.md (PART 2-C 협업검증 / PART 2-D 익스포저)
실거래: data/themes/날짜_stocks.csv (fetch_themes.py 산출)

산출: wiki/analysis/AIDC_매매교차검증_날짜.md
- 거래대금 순 + 등락률 순 정렬
- 익스포저등급(코어/중간/주변) × 실거래 수급 매트릭스
- [⚠️모순] = 협업 강링크인데 실거래 식음 / 가장자리인데 과열

사용: python aidc_crosscheck.py            (최신 날짜 자동)
      python aidc_crosscheck.py 2026-06-09 (날짜 지정)
"""
import csv, sys, os, glob, datetime

BASE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(BASE, "data", "themes")
OUT = os.path.join(BASE, "wiki", "analysis")

# ── AIDC 검증종목 메타 (PART 2-C 협업검증 + 2-D 익스포저 등급) ──────────────
# grade: 🟢코어 / 🟡중간 / 🔴주변 / ⚫산정불가
# anchor: 협업체인 종착 앵커(누구 덕에 먹고사나)  link: 검증강도 [공시]/[기사]/[논의]
AIDC = {
    # ===== 앵커 =====
    "한미반도체":    ("🟢코어","80~95%","SK하이닉스→엔비디아","[공시]","연산"),
    "SK하이닉스":    ("🟢코어","50~70%","엔비디아 HBM","[기사]","연산"),
    "삼성전자":      ("🟡중간","10~20%","엔비디아 HBM4 퀄","[기사]","연산"),
    "이수페타시스":  ("🟢코어","40~60%","엔비디아·구글 MLB","[기사]","연산"),
    "삼성전기":      ("🟡중간","10~25%","엔비디아·AMD·브로드컴 FC-BGA","[기사]","연산"),
    "오이솔루션":    ("🟡중간","20~40%","엔비디아 공급망·NTT 광트랜시버","[기사]","네트워크"),
    "HD현대일렉트릭":("🟡중간","25~40%","미국송전망 765kV","[공시]","전력"),
    "효성중공업":    ("🟡중간","15~30%","북미 전력망 변압기","[기사]","전력"),
    "산일전기":      ("🟢코어","40~60%","블룸에너지 변압기 503억","[공시]","전력"),
    "LS":            ("🔴주변","5~15%","빅테크 AIDC 버스덕트 5천억","[공시]","전력"),
    "대한전선":      ("🟡중간","10~25%","유럽전력망 초고압케이블","[기사]","전력"),
    "두산에너빌리티":("🟡중간","15~30%","뉴스케일·엑스에너지 SMR/가스터빈","[기사]","발전"),
    "한전기술":      ("🔴주변","5~15%","두산 동반 원전설계","[기사]","발전"),
    "GST":           ("🔴주변","5~15%","LG유플러스 액침냉각 첫납품","[기사]","냉각"),
    "케이엔솔":      ("🟡중간","20~40%","Submer 기술제휴 액침냉각","[기사]","냉각"),
    # ===== 1차 확장 =====
    "ISC":           ("🟡중간","30~45%","SK·엔비디아 HBM테스트소켓","[기사]","테스트"),
    "리노공업":      ("🟡중간","20~30%","엔비디아·TSMC 테스트핀","[기사]","테스트"),
    "파이버프로":    ("🟡중간","10~25%","산텍 광검사장비 45.6억","[공시]","네트워크"),
    "빛과전자":      ("🟡중간","10~25%","엔비디아 광동맹(미확정)","[논의]","네트워크"),
    "DB하이텍":      ("🔴주변","5~10%","전력반도체 파운드리(본양산27)","[기사]","전력반도체"),
    "퀄리타스반도체":("⚫적자","적자","베리실리콘 IP·삼성파운드리","[기사]","네트워크"),
    "KEC":           ("🔴주변","0~5%","차량 SiC(AIDC 거의무관)","[기사]","전력반도체"),
    # ===== 2차 확장 =====
    "제룡전기":      ("🟡중간","30~50%","PSE&G 배전변압기 531억","[공시]","전력"),
    "코리아써키트":  ("🟡중간","20~35%","브로드컴 장기공급·엔비디아","[공시]","기판"),
    "심텍":          ("🟡중간","20~35%","SK·삼성·마이크론 HBM4기판","[기사]","기판"),
    "대덕전자":      ("🟡중간","15~30%","AMD MLB승인임박·SK GDDR7","[기사]","기판"),
    "일진전기":      ("🟡중간","15~30%","미국송전망 변압기 4318억","[공시]","전력"),
    "유니셈":        ("🔴주변","0~5%","팹 공정냉각(DC직접아님)","[기사]","냉각"),
    "한중엔시에스":  ("🔴주변","0~5%","ESS배터리수냉(BESS경유간접)","[기사]","냉각"),
    # ===== 3차 확장 =====
    "케이아이엔엑스":("🟢코어","40~60%","AWS·Azure CloudHub 직결","[기사]","IDC"),
    "두산테스나":    ("🟡중간","20~35%","엔비디아 그록3 LPU 테스트","[기사]","테스트"),
    "네패스":        ("🟡중간","15~30%","삼성 엑시노스AI·PMIC","[기사]","테스트"),
    "하나마이크론":  ("🟡중간","15~30%","SK하이닉스 OSAT 외주이관","[기사]","OSAT"),
    "NAVER":         ("🟡중간","10~25%","엔비디아 DSX AI팩토리 동맹","[기사]","SW/IDC"),
    "현대건설":      ("🔴주변","5~15%","안산DC 8074억 수주","[공시]","건설"),
    "GS건설":        ("🔴주변","5~10%","DC EPC·클린룸","[기사]","건설"),
    "에치에프알":    ("🔴주변","0~10%","5G통신망위주(DC제한적)","[기사]","네트워크"),
    "우리넷":        ("🔴주변","0~5%","AIDC전송 국책R&D(매출무)","[논의]","네트워크"),
}

def parse_num(s):
    try: return float(str(s).replace(",", "").replace("%", "").replace("+", ""))
    except: return 0.0

def latest_date():
    files = sorted(glob.glob(os.path.join(DATA, "*_stocks.csv")))
    if not files: sys.exit("stocks csv 없음 — fetch_themes.py 먼저 실행")
    return os.path.basename(files[-1]).split("_")[0]

def main():
    date = sys.argv[1] if len(sys.argv) > 1 else latest_date()
    path = os.path.join(DATA, f"{date}_stocks.csv")
    if not os.path.exists(path): sys.exit(f"{path} 없음")

    # 종목명 → 실거래(여러 테마 중복 → 최대 거래대금 1개 채택)
    real = {}
    with open(path, encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            name = r["stock"].strip()
            if name not in AIDC: continue
            val = parse_num(r["value_mil"])
            if name not in real or val > real[name]["value"]:
                real[name] = {"price": r["price"], "chg": parse_num(r["chg"]),
                              "value": val, "theme": r["theme"]}

    rows = []
    for name,(grade,op,anchor,link,field) in AIDC.items():
        d = real.get(name)
        rows.append({"name":name,"grade":grade,"op":op,"anchor":anchor,"link":link,
                     "field":field,"chg":d["chg"] if d else None,
                     "value":d["value"] if d else 0,"price":d["price"] if d else "-",
                     "theme":d["theme"] if d else "-"})

    found = [r for r in rows if r["chg"] is not None]
    missing = [r["name"] for r in rows if r["chg"] is None]

    by_value = sorted(found, key=lambda x:-x["value"])
    by_chg   = sorted(found, key=lambda x:-x["chg"])

    # ── 그룹 중앙값 = 시장/AIDC 국면 기준선 (절대등락 아닌 상대강도로 판단) ──
    chgs = sorted(r["chg"] for r in found)
    n = len(chgs)
    median = chgs[n//2] if n%2 else (chgs[n//2-1]+chgs[n//2])/2
    avg = sum(chgs)/n if n else 0
    # 각 종목 상대강도 = 종목등락 - 그룹중앙값
    for r in found:
        r["rs"] = r["chg"] - median
    if median <= -1.5:   regime = f"위험회피(그룹 중앙값 {median:+.1f}%) — 역행강자 위주로 압축"
    elif median >= 1.5:  regime = f"위험선호(그룹 중앙값 {median:+.1f}%) — 대장 추세추종 유효"
    else:                regime = f"중립/혼조(그룹 중앙값 {median:+.1f}%)"

    # ── 신호 = 상대강도 기준 (그룹 대비) ──
    # 역행강자: 그룹 중앙값 대비 +3%p↑ → 종목 고유 강세(돈이 콕 집어 들어옴)
    # 역행약세: 코어/공시강링크인데 그룹 중앙값 대비 -3%p↓ → 종목 고유 악재 의심
    rs_strong, rs_weak, edge_hot_sig = [], [], []
    for r in found:
        strong = r["grade"].startswith("🟢") or r["link"]=="[공시]"
        edge = r["grade"].startswith("🔴") or r["link"]=="[논의]"
        if r["rs"] >= 3:
            rs_strong.append(r)
        if strong and r["rs"] <= -3:
            rs_weak.append(r)
        if edge and r["rs"] >= 5 and r["chg"] >= 5:
            edge_hot_sig.append(r)
    rs_strong.sort(key=lambda x:-x["rs"])
    rs_weak.sort(key=lambda x:x["rs"])

    # ── 매매 액션 등급 = 익스포저 × 검증 × 상대강도 × 거래대금 결합 ──
    # prio: 정렬 우선순위(작을수록 위)
    def action(r):
        g, link, rs, val, chg = r["grade"], r["link"], r["rs"], r["value"], r["chg"]
        core = g.startswith("🟢"); mid = g.startswith("🟡")
        edge = g.startswith("🔴") or g.startswith("⚫")
        gongsi = link=="[공시]"; nonono = link=="[논의]"
        # 추격금지: 실익스포저 작은데 과열(가장자리 + 그룹대비 +5%p & 절대 +5%↑)
        if edge and rs>=5 and chg>=5:
            return (4,"🚫 추격금지","실익스포저 작은데 과열 — 재료소멸 빠름, 매수금지")
        # 1순위: 코어 or 공시검증 + 상대강세 + 거래대금 동반
        if (core or gongsi) and not edge and rs>=0 and val>=30000:
            return (0,"🟢 1순위 매수후보","실적·수급·검증 3정합 — 차트 눌림서 분할매수")
        if (core or gongsi) and not edge and rs>=0:
            return (1,"🟢 1순위(거래대금↓)","검증·상대강세 정합이나 거래대금 미흡 — 수급 확인후")
        # 눌림목 대기: 본질 강링크(코어 or 중간+공시)인데 그룹보다 약함 → 본질좋은데 눌림 (주변주 제외)
        if (core or (mid and gongsi)) and rs<=-2:
            return (2,"🔵 눌림목 대기","본질 강링크 눌림 — 지지선 반등·거래량 확인후 진입")
        # 스윙 관찰: 중간 익스포저 상대강세
        if mid and rs>=0:
            return (3,"🟡 스윙 관찰","본업+AIDC 혼재(완충) — 상대강세 유지시 스윙")
        # 주변 단기모멘텀
        if edge and rs>=0:
            return (5,"⚪ 단기모멘텀(주변)","테마편승 — 매매시 짧게, 분할익절")
        return (6,"⚪ 관망","뚜렷한 수급우위 없음")
    for r in found:
        r["prio"], r["act"], r["act_why"] = action(r)

    def fmt_chg(c): return f"{c:+.2f}%" if c is not None else "-"
    def vmil(v): return f"{v/100:.0f}억" if v else "-"   # 백만원→억

    L = []
    L.append("---")
    L.append("type: analysis")
    L.append(f"title: AIDC 매매 교차검증 ({date})")
    L.append(f"date: {date}")
    L.append("tags: [AI데이터센터, 교차검증, 수급]")
    L.append("related: [[analysis/AIDC_밸류체인_2026-06-09]]")
    L.append("---\n")
    L.append(f"# AIDC 매매 교차검증 — {date}\n")
    L.append("> 협업체인(공시/기사 검증완료) × 네이버 실거래(거래대금·등락률) 대조.")
    L.append("> 거래대금 = 여러 테마 중복 노출 시 최대값 1개 채택. **추세(며칠 변화)가 절대값보다 정확.**")
    L.append(f"> 실거래 매칭 {len(found)}/{len(AIDC)}종목. 출처: data/themes/{date}_stocks.csv\n")

    L.append(f"## 🧭 AIDC 그룹 국면\n")
    L.append(f"- **{regime}** | 그룹 평균 {avg:+.1f}% / 중앙값 {median:+.1f}%")
    L.append(f"- 해석: 등락률을 절대값이 아니라 **그룹 중앙값 대비 상대강도(rs)**로 본다. 시장 전체가 빠진 날엔 '덜 빠진/오른' 종목이 진짜 수급 강자.\n")

    # ── 매매 액션 등급표 (최상단 결론) ──
    by_act = sorted(found, key=lambda x:(x["prio"], -x["value"]))
    L.append("## 🚦 매매 액션 등급 (결론 — 이것만 봐도 됨)")
    L.append("> 익스포저(코어/중간/주변) × 검증([공시]/[기사]/[논의]) × 상대강도 × 거래대금 결합 자동산정.")
    L.append("> ⚠️ 시나리오 가이드일 뿐 매수신호 아님 — 차트·거래량 직접 확인 필수.\n")
    L.append("| 액션 | 종목 | 등락률 | rs | 거래대금 | 익스포저 | 검증 | 근거 |")
    L.append("|------|------|------:|------:|------:|:---:|:---:|------|")
    for r in by_act:
        L.append(f"| {r['act']} | {r['name']} | {fmt_chg(r['chg'])} | {r['rs']:+.1f} | {vmil(r['value'])} | {r['grade']} | {r['link']} | {r['act_why']} |")
    L.append("")

    L.append("## ⭐ 역행강자 (그룹 중앙값 대비 +3%p↑ — 돈이 콕 집어 들어온 종목)")
    if rs_strong:
        for r in rs_strong:
            L.append(f"- **{r['name']}** ({r['grade']} {r['link']} · {r['field']}) {fmt_chg(r['chg'])} / 상대강도 {r['rs']:+.1f}%p / 거래대금 {vmil(r['value'])} — 앵커: {r['anchor']}")
    else:
        L.append("- 그룹 대비 뚜렷한 역행강자 없음(그룹 동조 흐름).")
    L.append("")

    L.append("## 🔻 역행약세 (강링크인데 그룹보다 더 빠짐 — 종목 고유 악재 의심·검증요)")
    if rs_weak:
        for r in rs_weak:
            L.append(f"- **{r['name']}** ({r['grade']} {r['link']} · {r['field']}) {fmt_chg(r['chg'])} / 상대강도 {r['rs']:+.1f}%p — 앵커: {r['anchor']}")
    else:
        L.append("- 강링크 종목 중 그룹 대비 이탈 종목 없음.")
    L.append("")

    if edge_hot_sig:
        L.append("## ⚠️ 가장자리 과열 (실익스포저 작은데 급등 — 추격금지·재료소멸리스크)")
        for r in edge_hot_sig:
            L.append(f"- **{r['name']}** ({r['grade']} {r['link']}) {fmt_chg(r['chg'])} / 상대강도 {r['rs']:+.1f}%p — {r['anchor']}")
        L.append("")

    L.append("## 💰 거래대금 순 (실제 자금집중 = 수급 대장)")
    L.append("| 순 | 종목 | 등락률 | 거래대금 | 익스포저 | 검증 | 분야 | 협업앵커 |")
    L.append("|---:|------|------:|------:|:---:|:---:|----|------|")
    for i,r in enumerate(by_value,1):
        L.append(f"| {i} | {r['name']} | {fmt_chg(r['chg'])} | {vmil(r['value'])} | {r['grade']} | {r['link']} | {r['field']} | {r['anchor']} |")
    L.append("")

    L.append("## 📈 등락률 순 (오늘의 모멘텀)")
    L.append("| 순 | 종목 | 등락률 | 거래대금 | 익스포저 | 검증 | 분야 |")
    L.append("|---:|------|------:|------:|:---:|:---:|----|")
    for i,r in enumerate(by_chg,1):
        L.append(f"| {i} | {r['name']} | {fmt_chg(r['chg'])} | {vmil(r['value'])} | {r['grade']} | {r['link']} | {r['field']} |")
    L.append("")

    # 익스포저 × 상대강도 매트릭스 (그룹 대비 강세 = rs>=0 이면서 거래대금 동반)
    hot = [r for r in found if r["rs"] >= 0 and r["value"] >= 30000]  # 그룹대비 강세 + 거래대금300억↑
    hot.sort(key=lambda x:-x["rs"])
    core_hot = [r for r in hot if r["grade"].startswith("🟢")]
    mid_hot  = [r for r in hot if r["grade"].startswith("🟡")]
    edge_hot = [r for r in hot if r["grade"].startswith("🔴")]
    L.append("## 🎯 익스포저 × 상대강도 매트릭스 (그룹 대비 강세 + 거래대금 동반)")
    L.append("> 기준: 상대강도 ≥ 0(그룹 중앙값 이상) AND 거래대금 300억↑ (= 빠지는 날에도 버틴 수급)\n")
    L.append(f"- 🟢 **코어 + 상대강세** (실적·수급 정합, 1순위): {', '.join(r['name'] for r in core_hot) or '없음'}")
    L.append(f"- 🟡 **중간 + 상대강세** (본업완충, 스윙): {', '.join(r['name'] for r in mid_hot) or '없음'}")
    L.append(f"- 🔴 **주변 + 상대강세** (테마 단기모멘텀, 추격주의): {', '.join(r['name'] for r in edge_hot) or '없음'}")
    L.append("")

    if missing:
        L.append(f"## 📋 실거래 미매칭 ({len(missing)}종목)")
        L.append("> 네이버 테마 구성종목에 미노출 = 오늘 해당테마 수급 약하거나 종목명 표기차이.")
        L.append(f"- {', '.join(missing)}\n")

    L.append("---")
    L.append("## 매매 해석 가이드")
    L.append("- **거래대금 순 = 실전 대장주**(펀더멘털 ⭐와 다를 수 있음). 1~3위에 코어주가 있으면 정석 흐름.")
    L.append("- **[공시] 검증 + 거래대금 상위 + 등락 양봉** 3박자 = 가장 신뢰도 높은 후보.")
    L.append("- **🔴주변 + 급등**은 재료 소멸 빠름 → 추격 금지, 차트 눌림 확인.")
    L.append("- 차트·거래량 패턴은 직접 확인(전고돌파·20일선·거래량 N배).")
    L.append(f"\n## 마지막 업데이트\n{datetime.date.today()} | aidc_crosscheck.py | 실거래 {date}")

    os.makedirs(OUT, exist_ok=True)
    outpath = os.path.join(OUT, f"AIDC_매매교차검증_{date}.md")
    with open(outpath, "w", encoding="utf-8") as f:
        f.write("\n".join(L))
    print(f"OK → {outpath}")
    print(f"매칭 {len(found)}/{len(AIDC)} | 국면 {regime.split('(')[0]} | 역행강자 {len(rs_strong)} 역행약세 {len(rs_weak)}")
    print(f"거래대금 1위: {by_value[0]['name']} {vmil(by_value[0]['value'])}" if by_value else "데이터없음")

if __name__ == "__main__":
    main()
