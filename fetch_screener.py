#!/usr/bin/env python3
"""
fetch_screener.py - 당일 조건 스크리닝

조건 (전부 AND):
  F: 저가 대비 고가 변동폭  >= 15%
  C: 시가 대비 종가 등락률  >= 9%
  D: 전일종가 대비 고가     >= 15%
  E: 거래대금               >= 50,000백만원 (500억)

사용법:
  python fetch_screener.py
  python fetch_screener.py --print-only
  python fetch_screener.py --date 2026-05-04
"""

import csv, json, os, sys, time, argparse, urllib.request
from datetime import datetime, timedelta

NAVER_HDR = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
    'Referer': 'https://m.stock.naver.com/',
}

TRADE_VALUE_MIN = 50_000_000_000   # 50,000백만원 = 500억원
RATE_F_MIN  = 0.15
RATE_C_MIN  = 0.09
RATE_D_MIN  = 0.15
PRE_FILTER  = 9.0   # 사전 필터: 등락률 >= 9%


def naver_get(url, timeout=10):
    req = urllib.request.Request(url, headers=NAVER_HDR)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def to_int(s):
    return int(str(s).replace(',', '').strip())


def parse_trade_value(s):
    """'672,208백만' -> 원 단위 정수"""
    s = str(s).replace(',', '').strip()
    if '백만' in s:
        return int(s.replace('백만', '').strip()) * 1_000_000
    return 0


def fetch_stock_data(code):
    """네이버 /integration 으로 OHLCV + 거래대금 한번에 조회"""
    try:
        url = 'https://m.stock.naver.com/api/stock/{}/integration'.format(code)
        data = naver_get(url)
        info = {item['code']: item['value'] for item in data.get('totalInfos', [])}

        return {
            'prev_close': to_int(info.get('lastClosePrice', '0')),
            'open':       to_int(info.get('openPrice', '0')),
            'high':       to_int(info.get('highPrice', '0')),
            'low':        to_int(info.get('lowPrice', '0')),
            'trade_val':  parse_trade_value(info.get('accumulatedTradingValue', '0')),
        }
    except Exception:
        return None


def load_candidates(csv_path):
    result = []
    with open(csv_path, encoding='utf-8-sig') as f:
        for row in csv.DictReader(f):
            try:
                rate = float(row['등락률(%)'])
                if rate >= PRE_FILTER:
                    result.append({
                        'name':      row['종목명'],
                        'code':      row['종목코드'],
                        'close_raw': to_int(row['현재가']),
                        'day_rate':  rate,
                    })
            except Exception:
                pass
    return result


def fetch_20d_avg_trade_val(code):
    """직전 20 거래일 평균 거래대금 (원) = 거래량(주) × 종가 근사값
    같은 종목 내 시계열 비교이므로 배율 계산에 충분히 유효함."""
    try:
        end_dt   = datetime.now() - timedelta(days=1)
        start_dt = datetime.now() - timedelta(days=45)   # 40일 = 영업일 약 28일 확보
        start_s  = start_dt.strftime('%Y%m%d') + '000000'
        end_s    = end_dt.strftime('%Y%m%d')   + '235959'
        url = ('https://api.stock.naver.com/chart/domestic/item/{}/day'
               '?startDateTime={}&endDateTime={}').format(code, start_s, end_s)
        data = naver_get(url)
        if not data:
            return None
        recent = data[-20:] if len(data) >= 20 else data
        vals = [
            d['accumulatedTradingVolume'] * d['closePrice']
            for d in recent
            if d.get('accumulatedTradingVolume') and d.get('closePrice')
        ]
        return sum(vals) / len(vals) if vals else None
    except Exception:
        return None


def screen(candidates):
    passed = []
    total = len(candidates)
    for i, c in enumerate(candidates, 1):
        print('  [{}/{}] {}...'.format(i, total, c['name']), end='\r')
        d = fetch_stock_data(c['code'])
        avg_val = fetch_20d_avg_trade_val(c['code'])
        time.sleep(0.05)
        if not d:
            continue

        close     = c['close_raw']
        open_     = d['open']
        high      = d['high']
        low       = d['low']
        prev_c    = d['prev_close']
        trade_val = d['trade_val']

        if low <= 0 or open_ <= 0 or prev_c <= 0:
            continue

        cond_F = (high / low - 1) >= RATE_F_MIN
        cond_C = (close / open_ - 1) >= RATE_C_MIN
        cond_D = (high / prev_c - 1) >= RATE_D_MIN
        cond_E = trade_val >= TRADE_VALUE_MIN

        if cond_F and cond_C and cond_D and cond_E:
            # 거래대금 배율: 오늘 거래대금 / 직전 20일 평균 (주 × 종가 근사)
            today_approx = close * d.get('high', close)  # 오늘 volume 없으므로 trade_val 사용
            ratio = round(trade_val / avg_val, 2) if avg_val and avg_val > 0 else 0
            leader_score = round(c['day_rate'] * ratio, 2) if ratio else 0
            passed.append({
                'name':          c['name'],
                'code':          c['code'],
                'close':         close,
                'open':          open_,
                'high':          high,
                'low':           low,
                'prev_c':        prev_c,
                'rate_C':        round((close / open_ - 1) * 100, 2),
                'rate_D':        round((high / prev_c - 1) * 100, 2),
                'rate_F':        round((high / low - 1) * 100, 2),
                'trade_val':     trade_val,
                'avg_trade_val': round(avg_val) if avg_val else 0,
                'trade_ratio':   ratio,
                'leader_score':  leader_score,
                'day_rate':      c['day_rate'],
            })

    print()
    passed.sort(key=lambda x: -x['trade_val'])
    return passed


def fmt_won(v):
    if v >= 1_000_000_000_000:
        return '{:.1f}조'.format(v / 1_000_000_000_000)
    return '{:.0f}억'.format(v / 100_000_000)


def save_md(results, date_str, path):
    lines = [
        '---',
        'date: {}'.format(date_str),
        'type: 스크리닝',
        '---',
        '',
        '# 📋 스크리닝 결과 — {}'.format(date_str),
        '',
        '## 적용 조건 (전부 AND)',
        '',
        '| 조건 | 기준 |',
        '|------|------|',
        '| F | 저가 대비 고가 변동폭 ≥ 15% |',
        '| C | 시가 대비 종가 ≥ +9% (강한 마감) |',
        '| D | 전일종가 대비 고가 ≥ +15% |',
        '| E | 거래대금 ≥ 500억원 |',
        '',
        '## 결과 ({} 종목)'.format(len(results)),
        '',
        '| 종목 | 종가 | 등락률 | 거래대금 | 배율 | leader_score |',
        '|------|------|--------|----------|------|------------|',
    ]
    for r in results:
        lines.append('| **{}**({}) | {:,} | +{:.1f}% | +{:.1f}% | +{:.1f}% | +{:.1f}% | {} |'.format(
            r['name'], r['code'], r['close'], r['day_rate'],
            r['rate_C'], r['rate_F'], r['rate_D'], fmt_won(r['trade_val'])
        ))
    lines += ['', '---', '> 출처: 네이버 금융 | 수집: {}'.format(datetime.now().strftime('%Y-%m-%d %H:%M'))]

    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    print('저장: {}'.format(path))



def update_recent_breakout(results, wiki_dir, date_str):
    """스크리닝 통과 종목의 wiki/stocks/{종목명}.md frontmatter에 recent_breakout 업데이트"""
    stocks_dir = os.path.join(wiki_dir, 'stocks')
    updated = []
    skipped = []

    for r in results:
        name = r['name']
        # 파일명 안전 처리 (슬래시 등 특수문자 → 하이픈)
        safe_name = re.sub(r'[/\\:*?"<>|]', '-', name)
        fpath = os.path.join(stocks_dir, safe_name + '.md')

        if not os.path.exists(fpath):
            skipped.append(name)
            continue

        with open(fpath, encoding='utf-8') as f:
            content = f.read()

        # frontmatter가 있으면 recent_breakout 필드 업데이트
        if content.startswith('---'):
            parts = content.split('---', 2)
            if len(parts) >= 3:
                fm = parts[1]
                body = parts[2]
                score = r.get('leader_score', 0)
                if 'recent_breakout:' in fm:
                    fm = re.sub(r'recent_breakout:.*', 'recent_breakout: {}'.format(date_str), fm)
                else:
                    fm = fm.rstrip() + '\nrecent_breakout: {}\n'.format(date_str)
                if 'leader_score:' in fm:
                    fm = re.sub(r'leader_score:.*', 'leader_score: {}'.format(score), fm)
                else:
                    fm = fm.rstrip() + '\nleader_score: {}\n'.format(score)
                new_content = '---{}---{}'.format(fm, body)
                with open(fpath, 'w', encoding='utf-8') as f:
                    f.write(new_content)
                updated.append(name)
            else:
                skipped.append(name)
        else:
            skipped.append(name)

    print('recent_breakout 업데이트: {}종목'.format(len(updated)))
    if skipped:
        print('  스킵 (파일 없음 또는 frontmatter 없음): {}'.format(', '.join(skipped)))
    return updated

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--date',       default=datetime.now().strftime('%Y-%m-%d'))
    parser.add_argument('--wiki-dir',   default='wiki')
    parser.add_argument('--print-only', action='store_true')
    args = parser.parse_args()

    csv_path = os.path.join(args.wiki_dir, 'change_rate_{}.csv'.format(args.date))
    if not os.path.exists(csv_path):
        print('[오류] CSV 없음 (fetch_change_rate.py 먼저 실행): {}'.format(csv_path))
        sys.exit(1)

    print('[1/2] 사전 필터링 (등락률 >= {}%)...'.format(PRE_FILTER))
    candidates = load_candidates(csv_path)
    print('  -> {}종목 후보'.format(len(candidates)))

    print('[2/2] OHLCV + 거래대금 조회 + 조건 검사...')
    results = screen(candidates)
    print('  -> {}종목 최종 통과'.format(len(results)))

    out_path = os.path.join(args.wiki_dir, 'analysis', '스크리닝_{}.md'.format(args.date))

    if not args.print_only:
        save_md(results, args.date, out_path)
        update_recent_breakout(results, args.wiki_dir, args.date)

    print()
    print('=' * 55)
    if results:
        print('  {:<16} {:>8}  {:>6}  {:>8}'.format('종목', '종가', '등락률', '거래대금'))
        print('  ' + '-' * 50)
        for r in results:
            print('  {:<16} {:>8,}  {:>+6.1f}%  {:>8}'.format(
                r['name'] + '(' + r['code'] + ')',
                r['close'], r['day_rate'], fmt_won(r['trade_val'])))
    else:
        print('  조건 충족 종목 없음')
    print('=' * 55)


if __name__ == '__main__':
    main()
