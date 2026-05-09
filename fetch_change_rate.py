#!/usr/bin/env python3
import os, re, csv, sys, argparse
from datetime import datetime

try:
    import FinanceDataReader as fdr
except ImportError:
    print("pip install finance-datareader --break-system-packages")
    sys.exit(1)

NAME_ALIAS = {
    '엔씨소프트':     'NC',
    'LIG넥스원':      'LIG디펜스앤에어로스페이스',
    'HD현대건설기계': 'HD건설기계',
    'JYP Ent':        'JYP Ent.',
}

def fetch_all_stocks():
    print('[1/3] KOSPI + KOSDAQ collecting...', file=sys.stderr)
    kospi  = fdr.StockListing('KOSPI')
    kosdaq = fdr.StockListing('KOSDAQ')
    result = {}
    for df, market in [(kospi, 'KOSPI'), (kosdaq, 'KOSDAQ')]:
        for _, row in df.iterrows():
            name = str(row['Name']).strip()
            if not name:
                continue
            try:
                rate_f = float(row.get('ChagesRatio', 0))
            except (ValueError, TypeError):
                rate_f = 0.0
            result[name] = {
                'code':   str(row['Code']),
                'close':  str(row['Close']),
                'rate':   rate_f,
                'market': market,
            }
    print('  -> {} stocks'.format(len(result)), file=sys.stderr)
    return result

def save_csv(stocks, path):
    with open(path, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        writer.writerow(['종목명', '종목코드', '현재가', '등락률(%)', '시장'])
        for name, d in sorted(stocks.items()):
            sign = '+' if d['rate'] >= 0 else ''
            writer.writerow([name, d['code'], d['close'],
                             '{}{:.2f}'.format(sign, d['rate']), d['market']])
    print('[2/3] CSV: {} ({} rows)'.format(path, len(stocks)), file=sys.stderr)

def set_field(text, key, value):
    pattern = '^' + re.escape(key) + ':.*$'
    new_line = '{}: {}'.format(key, value)
    if re.search(pattern, text, re.MULTILINE):
        return re.sub(pattern, new_line, text, flags=re.MULTILINE)
    if 'last_updated:' in text:
        return re.sub(r'(last_updated:[^\n]*)', r'\1\n' + new_line, text, count=1)
    return text.rstrip('\n') + '\n' + new_line

def update_md_files(stocks, stocks_dir, date_str):
    md_files = [f for f in os.listdir(stocks_dir)
                if f.endswith('.md') and f != '_TEMPLATE.md']
    updated, not_found = 0, []
    for fname in md_files:
        name = fname[:-3]
        lookup = NAME_ALIAS.get(name, name)
        info = stocks.get(lookup)
        if not info:
            not_found.append(name)
            continue
        fpath = os.path.join(stocks_dir, fname)
        with open(fpath, 'r', encoding='utf-8') as f:
            content = f.read()
        if not content.startswith('---'):
            continue
        end_fm = content.find('\n---', 3)
        if end_fm == -1:
            continue
        fm   = content[3:end_fm]
        body = content[end_fm + 4:]
        sign     = '+' if info['rate'] >= 0 else ''
        rate_str = '{}{:.2f}%'.format(sign, info['rate'])
        fm = set_field(fm, 'change_rate',  rate_str)
        fm = set_field(fm, 'close_price',  info['close'])
        fm = set_field(fm, 'rate_updated', date_str)
        new_content = '---' + fm + '\n---' + body
        if new_content != content:
            with open(fpath, 'w', encoding='utf-8') as f:
                f.write(new_content)
            updated += 1
    return updated, not_found

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--date',     default=datetime.now().strftime('%Y-%m-%d'))
    parser.add_argument('--wiki-dir', default='wiki')
    parser.add_argument('--csv-only', action='store_true')
    args = parser.parse_args()

    stocks     = fetch_all_stocks()
    stocks_dir = os.path.join(args.wiki_dir, 'stocks')
    csv_path   = os.path.join(args.wiki_dir, 'change_rate_{}.csv'.format(args.date))

    os.makedirs(args.wiki_dir, exist_ok=True)
    save_csv(stocks, csv_path)

    if args.csv_only:
        print('[done] CSV only')
        return

    print('[3/3] updating md files...', file=sys.stderr)
    updated, not_found = update_md_files(stocks, stocks_dir, args.date)

    print('\n[done]')
    print('  CSV: {}'.format(csv_path))
    print('  updated: {}'.format(updated))
    print('  unmatched: {} files'.format(len(not_found)))
    if not_found[:10]:
        print('  examples: {}'.format(not_found[:10]))

if __name__ == '__main__':
    main()
