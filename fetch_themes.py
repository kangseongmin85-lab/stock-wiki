# -*- coding: utf-8 -*-
"""
fetch_themes.py — 네이버 금융 테마별 시세 수집 → 시계열 적재 + 오늘의 시황 리포트 생성

수집 내용:
  1) 테마 목록(강세순): 테마명, 당일 등락률, 최근 3일 등락률
  2) 각 테마 상세: 구성 종목별 현재가/등락률/거래량/거래대금/편입사유
  3) 테마 거래대금 = 구성 종목 거래대금 합산 (단위: 백만원 → 리포트는 억원)

산출물:
  - data/themes/YYYY-MM-DD_themes.csv   (테마 레벨 시계열 — 순환테마 트래킹용)
  - data/themes/YYYY-MM-DD_stocks.csv   (종목 레벨 상세)
  - wiki/analysis/시황_YYYY-MM-DD.md     (강세순 + 거래대금 + 위키 주목종목 매칭 리포트)

사용: python fetch_themes.py [--date YYYY-MM-DD] [--top N]
"""
import urllib.request, re, csv, os, glob, sys, io, time, datetime, argparse

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
BASE = os.path.dirname(os.path.abspath(__file__))
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")
WATCH_DAYS = 14   # 최근 N일 내 위키 업데이트된 종목 = 주목종목(⭐)

strip = lambda s: re.sub(r'<.*?>', '', s).replace('&amp;', '&').replace('&nbsp;', '').strip()


def fetch(url, retry=3):
    for i in range(retry):
        try:
            req = urllib.request.Request(url, headers={'User-Agent': UA})
            return urllib.request.urlopen(req, timeout=15).read().decode('euc-kr', 'replace')
        except Exception as e:
            if i == retry - 1:
                raise
            time.sleep(1)


def num(s):
    """'112,364' -> 112364, 빈 값/문자 -> 0"""
    s = re.sub(r'[^0-9.\-]', '', s or '')
    try:
        return float(s) if s not in ('', '-', '.') else 0.0
    except ValueError:
        return 0.0


def pct(s):
    m = re.search(r'[-+]?\d+\.?\d*%', s or '')
    return m.group(0) if m else ''


def fetch_theme_list():
    """강세순 정렬 테마 목록 전체 페이지 수집"""
    themes = []
    seen = set()
    for page in range(1, 30):
        url = (f"https://finance.naver.com/sise/theme.naver"
               f"?field=updown&ordering=desc&page={page}")
        html = fetch(url)
        rows = re.findall(r'<td class="col_type1">(.*?)</tr>', html, re.S)
        added = 0
        for b in rows:
            m = re.search(r'type=theme&no=(\d+)">(.*?)</a>', b)
            if not m:
                continue
            no = m.group(1)
            if no in seen:
                continue
            seen.add(no)
            nums = [strip(x) for x in re.findall(r'class="number[^"]*">(.*?)</td>', b, re.S)]
            themes.append({
                'no': no, 'name': strip(m.group(2)),
                'chg': nums[0] if nums else '',
                'd3': nums[1] if len(nums) > 1 else '',
            })
            added += 1
        if added == 0:
            break
        time.sleep(0.2)
    return themes


def fetch_theme_detail(no):
    """테마 상세 → 구성 종목 리스트"""
    html = fetch(f"https://finance.naver.com/sise/sise_group_detail.naver?type=theme&no={no}")
    stocks = []
    for tr in re.findall(r'<tr[^>]*>(\s*<td class="name">.*?)</tr>', html, re.S):
        tds = re.findall(r'<td[^>]*>(.*?)</td>', tr, re.S)
        if len(tds) < 10:
            continue
        m = re.search(r'code=\d+">(.*?)</a>', tds[0])
        name = strip(m.group(1)) if m else strip(tds[0]).rstrip(' *')
        stocks.append({
            'name': name,
            'reason': strip(tds[1]).split('\n', 1)[-1].strip() if tds[1] else '',
            'price': strip(tds[2]),
            'chg': pct(strip(tds[4])),
            'volume': int(num(tds[7])),
            'value': int(num(tds[8])),  # 거래대금(백만원)
        })
    return stocks


def load_watch():
    """최근 WATCH_DAYS일 내 업데이트된 위키 종목 = 주목종목"""
    cutoff = (datetime.date.today() - datetime.timedelta(days=WATCH_DAYS)).isoformat()
    watch = {}
    for f in glob.glob(os.path.join(BASE, 'wiki', 'stocks', '*.md')):
        base = os.path.splitext(os.path.basename(f))[0]
        if base.startswith('_'):
            continue
        try:
            head = open(f, encoding='utf-8').read(600)
        except Exception:
            continue
        m = re.search(r'last_updated:\s*([0-9]{4}-[0-9]{2}-[0-9]{2})', head)
        if m and m.group(1) >= cutoff:
            for part in base.split(','):
                p = part.strip()
                if p:
                    watch[p] = m.group(1)
    return watch


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--date', default=datetime.date.today().isoformat())
    ap.add_argument('--top', type=int, default=15, help='리포트 상세에 노출할 강세 테마 수')
    args = ap.parse_args()
    today = args.date

    print("[1/4] 테마 목록 수집 중...")
    themes = fetch_theme_list()
    print(f"      {len(themes)}개 테마")

    print("[2/4] 테마별 구성 종목 + 거래대금 수집 중...")
    stock_rows = []
    for i, t in enumerate(themes, 1):
        stocks = fetch_theme_detail(t['no'])
        t['stocks'] = stocks
        t['value'] = sum(s['value'] for s in stocks)      # 거래대금 합(백만원)
        t['count'] = len(stocks)
        for s in stocks:
            stock_rows.append([today, t['name'], s['name'], s['price'], s['chg'],
                               s['volume'], s['value'], s['reason']])
        if i % 20 == 0:
            print(f"      {i}/{len(themes)}")
        time.sleep(0.15)

    watch = load_watch()
    print(f"[3/4] 위키 주목종목 {len(watch)}개 (최근 {WATCH_DAYS}일 업데이트) 매칭")

    # --- CSV 적재 ---
    ddir = os.path.join(BASE, 'data', 'themes')
    os.makedirs(ddir, exist_ok=True)
    tpath = os.path.join(ddir, f'{today}_themes.csv')
    with open(tpath, 'w', newline='', encoding='utf-8-sig') as f:
        w = csv.writer(f)
        w.writerow(['date', 'theme', 'chg', 'chg_3d', 'value_mil', 'value_eok', 'count'])
        for t in themes:
            w.writerow([today, t['name'], t['chg'], t['d3'],
                        t['value'], round(t['value'] / 100, 1), t['count']])
    spath = os.path.join(ddir, f'{today}_stocks.csv')
    with open(spath, 'w', newline='', encoding='utf-8-sig') as f:
        w = csv.writer(f)
        w.writerow(['date', 'theme', 'stock', 'price', 'chg', 'volume', 'value_mil', 'reason'])
        w.writerows(stock_rows)

    # --- 리포트 ---
    print("[4/4] 리포트 생성 중...")
    eok = lambda v: f"{v/100:,.0f}억"
    L = [f"# 오늘의 시황 — {today}\n",
         "> 출처: 네이버 금융 테마별 시세 | ⭐ = 최근 "
         f"{WATCH_DAYS}일 내 위키 업데이트된 주목 종목 | 거래대금 = 구성 종목 합산\n",
         "## 🔥 강세 테마 순위 (당일 등락률)\n",
         "| 순위 | 테마 | 당일 | 3일 | 거래대금 | 종목수 | 주목종목⭐ |",
         "|---|---|---:|---:|---:|---:|---|"]
    for i, t in enumerate(themes[:args.top], 1):
        hits = [s['name'] for s in t['stocks'] if s['name'] in watch]
        L.append(f"| {i} | {t['name']} | {t['chg']} | {t['d3']} | {eok(t['value'])} "
                 f"| {t['count']} | {', '.join(hits) if hits else '-'} |")

    # 거래대금 순위 (돈이 실제로 몰린 테마)
    by_val = sorted(themes, key=lambda x: x['value'], reverse=True)[:args.top]
    L += ["\n## 💰 거래대금 순위 (실제 자금 집중)\n",
          "| 순위 | 테마 | 거래대금 | 당일 | 3일 |",
          "|---|---|---:|---:|---:|"]
    for i, t in enumerate(by_val, 1):
        L.append(f"| {i} | {t['name']} | {eok(t['value'])} | {t['chg']} | {t['d3']} |")

    # 주목종목 포함 강세 테마 상세
    L.append("\n## 🎯 주목 종목이 포함된 강세 테마 상세\n")
    for t in themes:
        hits = [s for s in t['stocks'] if s['name'] in watch]
        if not hits:
            continue
        L.append(f"### {t['name']}  (당일 {t['chg']} / 3일 {t['d3']} / 거래대금 {eok(t['value'])})")
        for s in hits:
            L.append(f"- ⭐ **{s['name']}** {s['chg']}  (위키 {watch[s['name']]})")
        top = sorted(t['stocks'], key=lambda x: num(x['chg']), reverse=True)[:3]
        L.append("  - 테마 내 상위: " + ", ".join(f"{s['name']} {s['chg']}" for s in top) + "\n")

    apath = os.path.join(BASE, 'wiki', 'analysis', f'시황_{today}.md')
    open(apath, 'w', encoding='utf-8').write("\n".join(L))

    print("\n=== 완료 ===")
    print("테마 CSV :", tpath)
    print("종목 CSV :", spath)
    print("리포트   :", apath)


if __name__ == '__main__':
    main()
