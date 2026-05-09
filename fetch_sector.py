#!/usr/bin/env python3
"""
fetch_sector.py — 업종별 등락률 + 외국인/기관 순매수 수집기
  - 네이버 금융 API : 업종별 등락률, 지수
  - 한국투자증권 API: 외국인/기관 순매수 (시가총액 상위 N종목 기준)

사용법:
  python fetch_sector.py                   # wiki/analysis/ 에 저장
  python fetch_sector.py --print-only      # stdout 출력만
  python fetch_sector.py --universe 50     # 분석 대상 종목 수 확대
"""

import json, os, sys, time, argparse, urllib.request, urllib.error
from datetime import datetime

# ── 설정 ────────────────────────────────────────────────────────────────────
KIS_APP_KEY    = os.environ.get('KIS_APP_KEY',    'PSMXYuNXVXwrgkA9soJlDvCMAnrOXDGarql7')
KIS_APP_SECRET = os.environ.get('KIS_APP_SECRET', 'v6uaPRGDsydG2/lhiwGzuX6BtRAG6536cLbnz2bgjfVCIwYpFB/dK0YGwsvsD4mXZIdacvf543qWml3h/ASJKOmidrh9qDN4TW2lE7uitW13izzOKTg6qO7B9vHAliD6msl04GL6Is5rF9VZvw1ZnQg0XRTlvsoZDrrSAFeHCMv3VXOx96k=')
KIS_BASE    = 'https://openapi.koreainvestment.com:9443'
NAVER_BASE  = 'https://m.stock.naver.com/api'
TOKEN_CACHE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'kis_token.json')

NAVER_HDR = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
    'Referer': 'https://m.stock.naver.com/',
}


# ── KIS 토큰 ─────────────────────────────────────────────────────────────────

def get_token() -> str:
    if os.path.exists(TOKEN_CACHE):
        with open(TOKEN_CACHE) as f:
            d = json.load(f)
        if datetime.strptime(d['expires'], '%Y-%m-%d %H:%M:%S') > datetime.now():
            return d['token']
    body = json.dumps({'grant_type': 'client_credentials',
                       'appkey': KIS_APP_KEY, 'appsecret': KIS_APP_SECRET}).encode()
    req = urllib.request.Request(KIS_BASE + '/oauth2/tokenP', data=body,
                                 headers={'Content-Type': 'application/json'}, method='POST')
    with urllib.request.urlopen(req, timeout=10) as r:
        resp = json.loads(r.read().decode())
    with open(TOKEN_CACHE, 'w') as f:
        json.dump({'token': resp['access_token'],
                   'expires': resp['access_token_token_expired']}, f)
    return resp['access_token']


# ── HTTP helpers ─────────────────────────────────────────────────────────────

def naver_get(path: str) -> dict:
    req = urllib.request.Request(NAVER_BASE + path, headers=NAVER_HDR)
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read().decode())


def kis_get(token, tr_id, path, params, retry=2) -> dict:
    headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'authorization': f'Bearer {token}',
        'appkey': KIS_APP_KEY, 'appsecret': KIS_APP_SECRET,
        'tr_id': tr_id, 'custtype': 'P',
    }
    for attempt in range(retry + 1):
        try:
            req = urllib.request.Request(KIS_BASE + path + '?' + params, headers=headers)
            with urllib.request.urlopen(req, timeout=10) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 500 and attempt < retry:
                time.sleep(0.3 * (attempt + 1))
                continue
            raise
    return {}


# ── 1. 업종별 등락률 ─────────────────────────────────────────────────────────

def get_industries(top_n=20) -> list:
    data = naver_get('/stocks/industry?page=1&pageSize=100')
    groups = data.get('groups', [])
    for g in groups:
        try:    g['_rate'] = float(g.get('changeRate', 0))
        except: g['_rate'] = 0.0
    return sorted(groups, key=lambda x: x['_rate'], reverse=True)[:top_n]


def fmt_industry(rows) -> str:
    lines = ['| 순위 | 업종명 | 등락률 | 상승 | 하락 |',
             '|------|--------|--------|------|------|']
    for i, g in enumerate(rows, 1):
        r = g['_rate']
        lines.append(f"| {i} | {g.get('name','-')} | {'+'if r>=0 else''}{r:.2f}% "
                     f"| {g.get('riseCount','-')} | {g.get('fallCount','-')} |")
    return '\n'.join(lines)


# ── 2. 지수 ──────────────────────────────────────────────────────────────────

def get_index_lines() -> str:
    lines = []
    for sym, label in [('KOSPI','코스피'), ('KOSDAQ','코스닥')]:
        try:
            d = naver_get(f'/index/{sym}/basic')
            r = float(d.get('fluctuationsRatio', 0))
            lines.append(f"- **{label}**: {d.get('closePrice','?')} ({'+'if r>=0 else''}{r:.2f}%)")
        except Exception:
            lines.append(f'- **{label}**: 조회 실패')
    return '\n'.join(lines)


# ── 3. 시가총액 상위 종목 코드 (네이버) ──────────────────────────────────────

def get_top_codes(market='KOSPI', n=40) -> list:
    data = naver_get(f'/stocks/marketValue?market={market}'
                     f'&sortType=MARKET_VALUE&page=1&pageSize={n}')
    return [(s['itemCode'], s['stockName']) for s in data.get('stocks', [])]


# ── 4. KIS: 종목별 투자자 순매수 ─────────────────────────────────────────────

def get_investor(token, code) -> dict:
    r = kis_get(token, 'FHKST01010900',
                '/uapi/domestic-stock/v1/quotations/inquire-investor',
                f'FID_COND_MRKT_DIV_CODE=J&FID_INPUT_ISCD={code}')
    if r.get('rt_cd') == '0' and r.get('output'):
        row = r['output'][0]
        return {
            'frgn': int(row.get('frgn_ntby_tr_pbmn', 0)),
            'orgn': int(row.get('orgn_ntby_tr_pbmn', 0)),
        }
    return {'frgn': 0, 'orgn': 0}


def collect_investor_data(token, universe=40) -> list:
    """KOSPI + KOSDAQ 상위 universe개 중복 제거 후 투자자 데이터 수집"""
    seen = {}  # code → {name, frgn, orgn}
    for market in ['KOSPI', 'KOSDAQ']:
        codes = get_top_codes(market, universe)
        for code, name in codes:
            if code in seen:
                continue
            try:
                inv = get_investor(token, code)
                seen[code] = {'name': name, **inv}
                time.sleep(0.12)
            except Exception as e:
                print(f'  SKIP {name}({code}): {e}', file=sys.stderr)
    return list(seen.values())


def fmt_investor_table(rows, key, label, top_n=10) -> str:
    sorted_rows = sorted(rows, key=lambda x: x[key], reverse=True)[:top_n]
    lines = [f'| 순위 | 종목명 | {label} 순매수 (억원) |',
             '|------|--------|---------------------|']
    for i, row in enumerate(sorted_rows, 1):
        v = row[key] // 100  # 백만원 → 억원
        lines.append(f"| {i} | {row['name']} | {'+'if v>=0 else ''}{v:,} |")
    return '\n'.join(lines)


# ── 메인 ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--date',       default=datetime.now().strftime('%Y-%m-%d'))
    parser.add_argument('--out',        default='wiki/analysis')
    parser.add_argument('--top',        type=int, default=20)
    parser.add_argument('--stocks',     type=int, default=10)
    parser.add_argument('--universe',   type=int, default=40)
    parser.add_argument('--print-only', action='store_true')
    args = parser.parse_args()

    print('[1/5] KIS 토큰...', file=sys.stderr)
    token = get_token()

    print('[2/5] 지수 현황...', file=sys.stderr)
    idx = get_index_lines()

    print('[3/5] 업종별 등락률...', file=sys.stderr)
    industries = get_industries(args.top)

    print(f'[4/5] 투자자 데이터 수집 (시가총액 상위 {args.universe}종목)...', file=sys.stderr)
    rows = collect_investor_data(token, args.universe)
    print(f'  → {len(rows)}종목 수집 완료', file=sys.stderr)

    print('[5/5] 마크다운 생성...', file=sys.stderr)
    ts  = datetime.now().strftime('%Y-%m-%d %H:%M')
    md  = f"""## 섹터 수급 분석 — {args.date}
> 수집 시각: {ts} KST | 출처: 네이버 금융 + 한국투자증권 API

---

### 지수 현황
{idx}

---

### 업종별 등락률 TOP {args.top}

{fmt_industry(industries)}

---

### 외국인 순매수 상위 {args.stocks}
> 시가총액 상위 {args.universe}종목 기준

{fmt_investor_table(rows, 'frgn', '외국인', args.stocks)}

---

### 기관 순매수 상위 {args.stocks}
> 시가총액 상위 {args.universe}종목 기준

{fmt_investor_table(rows, 'orgn', '기관', args.stocks)}
"""

    print(md)

    if not args.print_only:
        os.makedirs(args.out, exist_ok=True)
        out = os.path.join(args.out, f'섹터수급_{args.date}.md')
        with open(out, 'w', encoding='utf-8') as f:
            f.write(md)
        print(f'\n[완료] {out} 저장됨', file=sys.stderr)


if __name__ == '__main__':
    main()
