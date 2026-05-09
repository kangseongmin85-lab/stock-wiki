#!/usr/bin/env python3
"""
run_lint.py — 위키 정합성 검사 및 오류 탐지

검사 항목:
  1. stock_code 누락
  2. frontmatter 형식 오류 (--- 없음, 필드 파싱 실패)
  3. tags 공백 포함 (하이픈으로 치환 필요)
  4. recent_breakout 날짜 형식 오류
  5. leader_score 음수 또는 비정상값
  6. [[themes/X]] 링크 대상 파일 없음 (broken link)
  7. 테마 파일에 관련 종목이 0개
  8. [stale: YYYY-MM-DD] 항목 중 90일 이상 경과
  9. 타임라인 항목 형식 위반
 10. 빈 파일 (내용 없는 stocks/*.md)

사용법:
  python run_lint.py
  python run_lint.py --wiki-dir wiki
  python run_lint.py --fix   (자동 수정 가능한 항목만)
"""

import os, re, sys, argparse
from datetime import datetime, timedelta
from pathlib import Path

TODAY = datetime.now().date()
STALE_DAYS = 90


def parse_frontmatter(text):
    """frontmatter dict 반환. 실패 시 None."""
    if not text.startswith('---'):
        return None
    parts = text.split('---', 2)
    if len(parts) < 3:
        return None
    fm = {}
    for line in parts[1].splitlines():
        if ':' in line:
            k, _, v = line.partition(':')
            fm[k.strip()] = v.strip()
    return fm


def check_stocks(stocks_dir, fix=False):
    issues = []
    files = [f for f in stocks_dir.glob('*.md') if f.name != '_TEMPLATE.md']

    for fpath in files:
        name = fpath.stem
        text = fpath.read_text(encoding='utf-8')

        # 1. 빈 파일
        if len(text.strip()) < 10:
            issues.append(('EMPTY', name, '내용 없음'))
            continue

        # 2. frontmatter 없음
        fm = parse_frontmatter(text)
        if fm is None:
            issues.append(('NO_FM', name, 'frontmatter(---) 없음'))
            continue

        # 3. stock_code 누락
        code = fm.get('stock_code', '').strip()
        if not code:
            issues.append(('NO_CODE', name, 'stock_code 비어있음'))

        # 4. tags 공백 포함
        tags_raw = fm.get('tags', '')
        if re.search(r'\b\w+ \w+', tags_raw):
            issues.append(('TAG_SPACE', name, f'tags 공백 포함: {tags_raw[:60]}'))
            if fix:
                fixed = re.sub(r'^tags:.*$',
                    'tags: ' + re.sub(r' (?=\w)', '-', tags_raw),
                    text, flags=re.MULTILINE)
                fpath.write_text(fixed, encoding='utf-8')

        # 5. recent_breakout 형식
        rb = fm.get('recent_breakout', '').strip()
        if rb:
            try:
                datetime.strptime(rb, '%Y-%m-%d')
            except ValueError:
                issues.append(('BAD_DATE', name, f'recent_breakout 형식 오류: {rb}'))

        # 6. leader_score 비정상
        ls = fm.get('leader_score', '').strip()
        if ls:
            try:
                v = float(ls)
                if v < 0:
                    issues.append(('BAD_SCORE', name, f'leader_score 음수: {v}'))
            except ValueError:
                issues.append(('BAD_SCORE', name, f'leader_score 파싱 실패: {ls}'))

        # 7. broken [[themes/X]] 링크
        theme_links = re.findall(r'\[\[themes/([^\]]+)\]\]', text)
        for t in theme_links:
            tpath = stocks_dir.parent / 'themes' / (t + '.md')
            if not tpath.exists():
                issues.append(('BROKEN_LINK', name, f'[[themes/{t}]] 파일 없음'))

        # 8. stale 항목 90일 이상
        for m in re.finditer(r'\[stale:\s*(\d{4}-\d{2}-\d{2})\]', text):
            try:
                d = datetime.strptime(m.group(1), '%Y-%m-%d').date()
                if (TODAY - d).days > STALE_DAYS:
                    issues.append(('STALE', name, f'[stale: {m.group(1)}] {(TODAY-d).days}일 경과'))
            except ValueError:
                pass

        # 9. 타임라인 형식 위반 — 섹션 안에서만 검사
        in_timeline = False
        for line in text.splitlines():
            if line.startswith('## 최근 재료 타임라인'):
                in_timeline = True
                continue
            if in_timeline and line.startswith('## '):
                break
            if in_timeline and line.startswith('- ') and len(line) > 10:
                if not re.match(r'- \[\d{4}-\d{2}-\d{2}\]', line):
                    issues.append(('BAD_TIMELINE', name, f'타임라인 날짜 누락: {line[:60]}'))
                    break

    return issues


def check_themes(themes_dir, stocks_dir):
    issues = []
    files = [f for f in themes_dir.glob('*.md') if f.name != '_TEMPLATE.md']

    # 전체 stocks 파일의 tags 수집
    tag_map = {}  # tag → [종목명]
    for sf in stocks_dir.glob('*.md'):
        if sf.name == '_TEMPLATE.md':
            continue
        text = sf.read_text(encoding='utf-8')
        fm = parse_frontmatter(text)
        if not fm:
            continue
        tags_raw = fm.get('tags', '')
        for t in re.findall(r'[\w가-힣\-\.]+', tags_raw):
            tag_map.setdefault(t, []).append(sf.stem)

    for fpath in files:
        tname = fpath.stem
        linked = tag_map.get(tname, [])
        if len(linked) == 0:
            issues.append(('ORPHAN_THEME', tname, '관련 종목 0개 (태그 연결 없음)'))

    return issues


def save_report(all_issues, wiki_dir):
    date_str = TODAY.strftime('%Y-%m-%d')
    out_path = wiki_dir / 'analysis' / f'lint_{date_str}.md'
    out_path.parent.mkdir(exist_ok=True)

    by_type = {}
    for typ, name, msg in all_issues:
        by_type.setdefault(typ, []).append((name, msg))

    type_labels = {
        'EMPTY':        '빈 파일',
        'NO_FM':        'Frontmatter 없음',
        'NO_CODE':      'stock_code 누락',
        'TAG_SPACE':    'Tags 공백',
        'BAD_DATE':     'recent_breakout 형식 오류',
        'BAD_SCORE':    'leader_score 오류',
        'BROKEN_LINK':  'Broken 테마 링크',
        'STALE':        'Stale 항목 (90일+)',
        'BAD_TIMELINE': '타임라인 형식 위반',
        'ORPHAN_THEME': '고아 테마 파일',
    }

    lines = [
        f'---\ndate: {date_str}\ntype: lint\n---\n',
        f'# 🔍 위키 Lint 리포트 — {date_str}\n',
        f'총 이슈: **{len(all_issues)}건**\n',
        '| 유형 | 건수 |',
        '|------|------|',
    ]
    for typ, items in sorted(by_type.items(), key=lambda x: -len(x[1])):
        lines.append(f'| {type_labels.get(typ, typ)} | {len(items)} |')

    for typ, items in sorted(by_type.items(), key=lambda x: -len(x[1])):
        lines.append(f'\n## {type_labels.get(typ, typ)} ({len(items)}건)\n')
        for name, msg in items[:50]:  # 최대 50개만 출력
            lines.append(f'- `{name}` — {msg}')
        if len(items) > 50:
            lines.append(f'- ... 외 {len(items)-50}건')

    out_path.write_text('\n'.join(lines), encoding='utf-8')
    return out_path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--wiki-dir', default='wiki')
    parser.add_argument('--fix', action='store_true', help='자동 수정 가능한 항목 수정')
    args = parser.parse_args()

    wiki_dir   = Path(args.wiki_dir)
    stocks_dir = wiki_dir / 'stocks'
    themes_dir = wiki_dir / 'themes'

    print(f'[Lint] stocks/ 검사 중...')
    stock_issues = check_stocks(stocks_dir, fix=args.fix)
    print(f'  → {len(stock_issues)}건')

    print(f'[Lint] themes/ 검사 중...')
    theme_issues = check_themes(themes_dir, stocks_dir)
    print(f'  → {len(theme_issues)}건')

    all_issues = stock_issues + theme_issues
    total = len(all_issues)

    if total == 0:
        print('[Lint] ✅ 이슈 없음')
        return 0

    out = save_report(all_issues, wiki_dir)
    print(f'\n[Lint] 총 {total}건 발견 → {out}')

    # 콘솔 요약 (상위 유형만)
    from collections import Counter
    cnt = Counter(t for t, _, _ in all_issues)
    type_labels = {
        'NO_CODE': 'stock_code 누락', 'BROKEN_LINK': 'Broken 링크',
        'STALE': 'Stale 90일+', 'ORPHAN_THEME': '고아 테마',
        'BAD_DATE': '날짜 오류', 'TAG_SPACE': 'Tags 공백',
        'BAD_TIMELINE': '타임라인 형식', 'NO_FM': 'FM 없음',
    }
    print()
    for typ, n in cnt.most_common():
        print(f'  {type_labels.get(typ, typ):20s} {n:4d}건')

    return 1 if total > 0 else 0


if __name__ == '__main__':
    sys.exit(main())
