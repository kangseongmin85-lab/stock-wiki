# -*- coding: utf-8 -*-
"""
track_rotation.py — 누적된 테마 시세 CSV로 순환테마(로테이션) 추적

입력: data/themes/YYYY-MM-DD_themes.csv (fetch_themes.py가 매일 적재)
출력: wiki/analysis/순환테마_트래킹.md (옵시디언 누적 대시보드, 항상 최신)

판정:
  🔼 가열   — 등락률 상승 추세 + 거래대금 최근평균 대비 증가
  🔽 냉각   — 등락률 하락 전환 + 거래대금 감소
  🆕 신규   — 거래대금 순위가 최근 대비 크게 상승
  ➡️ 유지   — 큰 변화 없음

사용: python track_rotation.py [--days N] [--date YYYY-MM-DD]
"""
import csv, os, glob, re, sys, io, datetime, argparse

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
BASE = os.path.dirname(os.path.abspath(__file__))
DDIR = os.path.join(BASE, 'data', 'themes')


def fpct(s):
    s = re.sub(r'[^0-9.\-]', '', (s or '').replace('+', ''))
    try:
        return float(s) if s not in ('', '-', '.') else 0.0
    except ValueError:
        return 0.0


def load_all():
    """{date: {theme: {chg, chg3d, value_eok}}}"""
    data = {}
    for f in sorted(glob.glob(os.path.join(DDIR, '*_themes.csv'))):
        d = os.path.basename(f)[:10]
        rows = {}
        with open(f, encoding='utf-8-sig') as fh:
            for r in csv.DictReader(fh):
                rows[r['theme']] = {
                    'chg': fpct(r.get('chg', '')),
                    'chg3d': fpct(r.get('chg_3d', '')),
                    'value': float(r.get('value_eok', 0) or 0),
                }
        if rows:
            data[d] = rows
    return data


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--days', type=int, default=7, help='비교 기간(일)')
    ap.add_argument('--date', default=datetime.date.today().isoformat())
    args = ap.parse_args()

    data = load_all()
    dates = sorted(data.keys())
    if not dates:
        print("[순환트래킹] 수집된 데이터 없음. 먼저 fetch_themes.py 실행 필요.")
        return

    dates = dates[-args.days:]
    today = dates[-1]
    L = [f"# 순환테마 트래킹 — 기준일 {today}\n",
         f"> data/themes 누적 CSV {len(dates)}일치 기반 | 거래대금=억원 | "
         "자금이 어느 테마로 들어오고 빠지는지 추적\n"]

    if len(dates) < 2:
        L += ["## ⚠️ 데이터 누적 중",
              f"현재 **{len(dates)}일치**만 수집됨 (순환 비교는 최소 2일 필요).",
              "리포트_실행.bat을 며칠 더 돌리면 아래 분석이 자동 활성화됩니다.\n",
              "## 오늘 거래대금 상위 테마 (스냅샷)\n",
              "| 순위 | 테마 | 거래대금 | 당일 |", "|---|---|---:|---:|"]
        snap = sorted(data[today].items(), key=lambda x: x[1]['value'], reverse=True)[:20]
        for i, (th, v) in enumerate(snap, 1):
            L.append(f"| {i} | {th} | {v['value']:,.0f}억 | {v['chg']:+.2f}% |")
        _write(L, today)
        return

    prev = dates[-2]
    cur, pv = data[today], data[prev]
    # 거래대금 순위(오늘/직전)
    rank_cur = {th: i for i, (th, _) in enumerate(
        sorted(cur.items(), key=lambda x: x[1]['value'], reverse=True), 1)}
    rank_pv = {th: i for i, (th, _) in enumerate(
        sorted(pv.items(), key=lambda x: x[1]['value'], reverse=True), 1)}
    # 거래대금 최근평균(오늘 제외)
    hist = dates[:-1]

    def avg_value(th):
        vals = [data[d][th]['value'] for d in hist if th in data[d]]
        return sum(vals) / len(vals) if vals else 0

    rows = []
    for th, v in cur.items():
        av = avg_value(th)
        vchg = (v['value'] / av - 1) * 100 if av else 0          # 거래대금 변화율
        chg_y = pv.get(th, {}).get('chg', 0)                     # 어제 등락률
        rk_up = rank_pv.get(th, 999) - rank_cur.get(th, 999)     # 순위 상승폭(+면 상승)
        # 판정
        if rank_cur.get(th, 999) <= 40 and rk_up >= 15:
            tag = '🆕 신규'
        elif v['chg'] > 0 and chg_y <= v['chg'] and vchg > 25:
            tag = '🔼 가열'
        elif v['chg'] < chg_y and vchg < -15:
            tag = '🔽 냉각'
        else:
            tag = '➡️ 유지'
        rows.append((th, v, av, vchg, chg_y, rk_up, tag))

    def section(title, tag, key, rev=True, n=12):
        sub = [r for r in rows if r[6] == tag]
        sub.sort(key=key, reverse=rev)
        L.append(f"\n## {title}\n")
        if not sub:
            L.append("_해당 없음_")
            return
        L.append("| 테마 | 당일 | 어제 | 거래대금 | 최근평균比 | 순위변동 |")
        L.append("|---|---:|---:|---:|---:|---:|")
        for th, v, av, vchg, chg_y, rk_up, _ in sub[:n]:
            mv = f"▲{rk_up}" if rk_up > 0 else (f"▼{-rk_up}" if rk_up < 0 else "-")
            L.append(f"| {th} | {v['chg']:+.2f}% | {chg_y:+.2f}% | {v['value']:,.0f}억 "
                     f"| {vchg:+.0f}% | {mv} |")

    section("🔼 가열 — 자금 유입 + 상승 (신규 진입 후보)", '🔼 가열', lambda r: r[3])
    section("🆕 신규 부각 — 거래대금 순위 급상승", '🆕 신규', lambda r: r[5])
    section("🔽 냉각 — 자금 이탈 + 하락 (차익실현 주의)", '🔽 냉각', lambda r: r[3], rev=False)

    # 거래대금 상위 테마 N일 등락률 매트릭스
    top = [th for th, _ in sorted(cur.items(), key=lambda x: x[1]['value'], reverse=True)[:15]]
    L += ["\n## 📈 주요 테마 일별 등락률 매트릭스 (거래대금 상위 15)\n",
          "| 테마 | " + " | ".join(d[5:] for d in dates) + " | 거래대금 |",
          "|---|" + "---:|" * (len(dates) + 1)]
    for th in top:
        cells = []
        for d in dates:
            c = data[d].get(th, {}).get('chg')
            cells.append(f"{c:+.1f}" if c is not None and th in data[d] else "-")
        L.append(f"| {th} | " + " | ".join(cells) + f" | {cur[th]['value']:,.0f}억 |")

    _write(L, today)


def _write(L, today):
    apath = os.path.join(BASE, 'wiki', 'analysis', '순환테마_트래킹.md')
    open(apath, 'w', encoding='utf-8').write("\n".join(L))
    print("[순환트래킹] 저장:", apath)
    # 날짜 스냅샷도 누적 보관
    snap = os.path.join(BASE, 'wiki', 'analysis', f'순환테마_{today}.md')
    open(snap, 'w', encoding='utf-8').write("\n".join(L))


if __name__ == '__main__':
    main()
