#!/usr/bin/env python3
"""
fill_stock_codes.py — wiki/stocks/*.md 파일에 stock_code 일괄 추가
────────────────────────────────────────────────────────────────────
Notion API 호출 없음. dart-fss 1회 다운로드 후 캐시 활용.

사용법:
  python fill_stock_codes.py             # 전체 실행
  python fill_stock_codes.py --dry-run   # 파일 수정 없이 미리보기
"""

import re, sys, argparse
from pathlib import Path

BASE = Path(__file__).parent
WIKI = BASE / "wiki" / "stocks"
SKIP = {"_TEMPLATE.md"}

try:
    import dart_fss as dart
except ImportError:
    print("[ERROR] dart-fss 미설치.\n  pip install dart-fss")
    sys.exit(1)


def build_code_map() -> dict:
    """dart-fss로 전체 종목 코드 맵 생성 (1회 다운로드, 캐시)."""
    print("[INFO] dart-fss 코드 목록 로딩 중 (최초 1회, 약 10~30초)...")
    corps = dart.get_corp_list()
    print("[INFO] 로딩 완료\n")
    code_map = {}
    for corp in corps:
        name = getattr(corp, 'corp_name', None)
        code = getattr(corp, 'stock_code', None)
        if name and code and code.strip():
            if name not in code_map:
                code_map[name] = code.strip()
    return code_map


def get_code(name: str, code_map: dict) -> str:
    if name in code_map:
        return code_map[name]
    for corp_name, code in code_map.items():
        if name in corp_name or corp_name in name:
            return code
    return ""


def update_frontmatter(file: Path, stock_name: str, code_map: dict, dry_run: bool) -> str:
    text = file.read_text(encoding="utf-8")
    m = re.search(r'^stock_code:\s*(.*)$', text, re.MULTILINE)
    if m and m.group(1).strip():
        return "already"

    code = get_code(stock_name, code_map)
    if not code:
        return "notfound"

    if dry_run:
        return code

    if m:
        new_text = text[:m.start()] + f"stock_code: {code}" + text[m.end():]
    else:
        new_text = re.sub(
            r'(^theme:.*$)',
            r'\1\nstock_code: ' + code,
            text, count=1, flags=re.MULTILINE
        )
    file.write_text(new_text, encoding="utf-8")
    return code


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    mode = "DRY-RUN" if args.dry_run else "실행"
    print(f"{'='*50}")
    print(f"  stock_code 일괄 추가 ({mode})")
    print(f"{'='*50}\n")

    code_map = build_code_map()
    files = sorted(f for f in WIKI.glob("*.md") if f.name not in SKIP)
    print(f"대상 파일: {len(files)}개\n")

    updated, already, notfound = [], [], []
    for f in files:
        name = f.stem
        result = update_frontmatter(f, name, code_map, args.dry_run)
        if result == "already":
            already.append(name)
        elif result == "notfound":
            notfound.append(name)
        else:
            updated.append((name, result))
            tag = "[DRY]" if args.dry_run else "[OK]"
            print(f"  {tag} {name} → {result}")

    print(f"\n{'='*50}")
    print(f"업데이트: {len(updated)}개 | 기존 존재: {len(already)}개 | 미발견: {len(notfound)}개")
    if notfound:
        print(f"\n⚠️  코드 미발견 종목 ({len(notfound)}개):")
        for n in notfound:
            print(f"   - {n}")

if __name__ == "__main__":
    main()
