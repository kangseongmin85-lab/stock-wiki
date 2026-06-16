#!/usr/bin/env python3
"""
taxonomy_backfill.py — 종목재료정리 DB의 빈 카테고리/관련테마/키워드를
'이미 저장된 옵션 어휘' 안에서만 골라 채운다 (fill-empty-only).

설계 원칙
─────────
· 어떤 값을 고를지(분류 판단)는 **Claude 세션에서** 수행한다. 이 스크립트는
  ① 빈 종목 탐지(--list) 와 ② 검증된 기록(--apply) 만 담당. **LLM API 미사용.**
· 각 컬럼은 그 컬럼에 **이미 저장된 옵션 어휘** 안에서만 채운다.
  어휘에 없는 새 단어가 들어오면 거부한다 → Notion 옵션 오염(신규 옵션 생성) 방지.
· 이미 값이 있는 컬럼은 절대 덮어쓰지 않는다(빈 칸만 채움).

사용법
──────
  # 1) 빈 종목 + 분류용 텍스트 + 현재 옵션 어휘 덤프
  python taxonomy_backfill.py --list                  # 전체 빈 종목
  python taxonomy_backfill.py --list --limit 20       # 앞 20건만
  python taxonomy_backfill.py --list --names HPSP,주성엔지니어링
  python taxonomy_backfill.py --options               # 옵션 어휘만 갱신/출력

  # 2) Claude 가 고른 분류를 기록 (빈 칸만, 어휘 검증)
  python taxonomy_backfill.py --apply picks.json
  echo '[{"name":"HPSP","category":["반도체"],"themes":["#반도체 장비","#온디바이스AI"],"keywords":["반도체 장비","온디바이스AI"]}]' | python taxonomy_backfill.py --apply -

picks.json 형식: [{"name": "...", "category": [...], "themes": [...], "keywords": [...]}, ...]
  · page_id 를 직접 줘도 됨(name 대신/병행). category/themes/keywords 는 생략 가능(빈 배열).
"""

import os
import sys
import json
import argparse
import urllib.request
import urllib.error
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

NOTION_VERSION = "2022-06-28"
JAEROO_DB_ID = "2cbffbf4617380e38d07e8b5e59e36c4"  # 종목재료정리
TAX_FIELDS = ["카테고리", "관련테마", "키워드"]      # 채울 대상 multi_select 컬럼

BASE = Path(__file__).parent
OPTIONS_CACHE = BASE / "_cache" / "taxonomy_options.json"


# ── config / token ──────────────────────────────────────────────────────────
def _load_token():
    cfg = {}
    p = BASE / "config.json"
    if p.exists():
        try:
            cfg = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            cfg = {}
    return os.environ.get("NOTION_TOKEN") or cfg.get("NOTION_TOKEN", "")


NOTION_TOKEN = _load_token()


def _req(method, url, body=None, timeout=30):
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(
        url, data=data,
        headers={
            "Authorization":  f"Bearer {NOTION_TOKEN}",
            "Notion-Version": NOTION_VERSION,
            "Content-Type":   "application/json",
        },
        method=method,
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


# ── 옵션 어휘 (각 컬럼에 저장된 multi_select 값들) ────────────────────────────
def fetch_options(refresh=False):
    """DB 스키마에서 카테고리/관련테마/키워드 옵션명 리스트 추출 + 캐시.
    반환: {"카테고리": [...], "관련테마": [...], "키워드": [...]}"""
    if not refresh and OPTIONS_CACHE.exists():
        try:
            return json.loads(OPTIONS_CACHE.read_text(encoding="utf-8"))["options"]
        except Exception:
            pass
    db = _req("GET", f"https://api.notion.com/v1/databases/{JAEROO_DB_ID}")
    props = db.get("properties", {})
    options = {}
    for f in TAX_FIELDS:
        ms = props.get(f, {}).get("multi_select", {})
        options[f] = [o["name"] for o in ms.get("options", [])]
    OPTIONS_CACHE.parent.mkdir(parents=True, exist_ok=True)
    OPTIONS_CACHE.write_text(
        json.dumps({"db": JAEROO_DB_ID, "options": options}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return options


# ── 프로퍼티 헬퍼 ─────────────────────────────────────────────────────────────
def _title(prop):
    return "".join(p.get("plain_text", "") for p in (prop or {}).get("title", [])).strip()


def _rich(prop):
    return "".join(p.get("plain_text", "") for p in (prop or {}).get("rich_text", [])).strip()


def _multi(prop):
    return [s.get("name", "") for s in (prop or {}).get("multi_select", [])]


def _date(prop):
    d = (prop or {}).get("date") or {}
    return (d.get("start") or "")[:10]


# ── 빈 종목 탐지 ──────────────────────────────────────────────────────────────
def iter_pages():
    cursor = None
    for _ in range(400):  # 최대 40,000 page 가드
        body = {"page_size": 100}
        if cursor:
            body["start_cursor"] = cursor
        data = _req("POST", f"https://api.notion.com/v1/databases/{JAEROO_DB_ID}/query", body)
        for pg in data.get("results", []):
            yield pg
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")


def find_empty(names=None, limit=None, updated_on=None):
    """카테고리/관련테마/키워드 중 하나라도 비어 있는 종목 수집.
    names 지정 시 해당 종목만(빈 칸 없어도 텍스트 제공).
    updated_on('YYYY-MM-DD') 지정 시 그날 최근업데이트된 종목만(=그날 푸시분)."""
    want = set(n.strip() for n in names) if names else None
    out = []
    for pg in iter_pages():
        props = pg.get("properties", {})
        name = _title(props.get("종목명"))
        if not name:
            continue
        if want is not None and name not in want:
            continue
        if updated_on and _date(props.get("최근업데이트")) != updated_on:
            continue
        empties = [f for f in TAX_FIELDS if not _multi(props.get(f))]
        if want is None and not empties:
            continue
        out.append({
            "name": name,
            "page_id": pg["id"],
            "empty_fields": empties,
            "current": {f: _multi(props.get(f)) for f in TAX_FIELDS},
            # 분류 판단용 텍스트 (페이지 본문은 필요 시 notion-fetch 로 개별 조회)
            "summary": _rich(props.get("종목 재료 요약")),
            "today_news": _rich(props.get("오늘 기사 내용"))[:600],
            "change_pct": _rich(props.get("등락률 (%)")),
        })
        if limit and len(out) >= limit and want is None:
            break
    return out


# ── 검증 기록 (빈 칸만, 어휘 검증) ────────────────────────────────────────────
def apply_picks(items, options=None, dry_run=False):
    options = options or fetch_options()
    field_map = {"category": "카테고리", "themes": "관련테마", "keywords": "키워드"}
    results = []

    for it in items:
        name = (it.get("name") or "").strip()
        page_id = it.get("page_id")
        if not page_id and name:
            res = _req("POST", f"https://api.notion.com/v1/databases/{JAEROO_DB_ID}/query",
                       {"filter": {"property": "종목명", "title": {"equals": name}}, "page_size": 1})
            hits = res.get("results", [])
            page_id = hits[0]["id"] if hits else None
        if not page_id:
            results.append({"name": name, "status": "error", "error": "page not found"})
            continue

        # 현재 값 재확인 → 이미 채워진 컬럼은 건너뜀(절대 덮어쓰기 금지)
        page = _req("GET", f"https://api.notion.com/v1/pages/{page_id}")
        props = page.get("properties", {})

        to_set, dropped, skipped = {}, [], []
        for key, field in field_map.items():
            vals = it.get(key) or []
            if not vals:
                continue
            if _multi(props.get(field)):           # 이미 값 있음 → 보존(덮어쓰기 금지)
                skipped.append(field)
                continue
            vocab = set(options.get(field, []))
            valid = [v for v in vals if v in vocab]
            bad   = [v for v in vals if v not in vocab]   # 어휘에 없는 새 단어 → 드롭
            if bad:
                dropped.append({field: bad})
            if valid:
                to_set[field] = {"multi_select": [{"name": v} for v in valid]}

        extra = {}
        if dropped:
            extra["dropped"] = dropped            # 저장 안 된 어휘 외 값(참고용)
        if skipped:
            extra["skipped_nonempty"] = skipped

        if not to_set:
            results.append({"name": name, "page_id": page_id, "status": "noop", **extra})
            continue
        if dry_run:
            results.append({"name": name, "page_id": page_id, "status": "dry-run",
                            "would_set": {f: _multi(v) for f, v in to_set.items()}, **extra})
            continue
        try:
            _req("PATCH", f"https://api.notion.com/v1/pages/{page_id}", {"properties": to_set})
            results.append({"name": name, "page_id": page_id, "status": "filled",
                            "set": {f: _multi(v) for f, v in to_set.items()}, **extra})
        except urllib.error.HTTPError as e:
            err = e.read().decode("utf-8", errors="replace")[:300]
            results.append({"name": name, "page_id": page_id, "status": "error", "error": f"HTTP {e.code}: {err}"})
        except Exception as e:
            results.append({"name": name, "page_id": page_id, "status": "error", "error": str(e)})

    return results


# ── CLI ───────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="빈 카테고리/관련테마/키워드 백필 (fill-empty-only)")
    ap.add_argument("--list", action="store_true", help="빈 종목 + 텍스트 + 옵션 어휘 출력(JSON)")
    ap.add_argument("--options", action="store_true", help="옵션 어휘만 갱신/출력")
    ap.add_argument("--apply", metavar="PICKS_JSON", help="분류 기록(파일경로 또는 - 로 stdin)")
    ap.add_argument("--names", help="콤마구분 종목명만 대상(--list)")
    ap.add_argument("--updated-on", help="해당 날짜(YYYY-MM-DD) 최근업데이트분만(=그날 푸시분)")
    ap.add_argument("--today", action="store_true", help="오늘 최근업데이트된 빈 종목만(리포트 푸시 직후용)")
    ap.add_argument("--limit", type=int, help="--list 최대 건수")
    ap.add_argument("--refresh", action="store_true", help="옵션 어휘 캐시 강제 갱신")
    ap.add_argument("--dry-run", action="store_true", help="--apply 시 실제 기록 없이 미리보기")
    ap.add_argument("--out", help="--list 결과를 파일로도 저장")
    args = ap.parse_args()

    if not NOTION_TOKEN:
        print(json.dumps({"error": "NOTION_TOKEN 없음 — config.json 또는 환경변수 확인"}, ensure_ascii=False))
        sys.exit(1)

    if args.options:
        opts = fetch_options(refresh=True)
        print(json.dumps({f: len(v) for f, v in opts.items()}, ensure_ascii=False, indent=2))
        return

    if args.apply:
        raw = sys.stdin.read() if args.apply == "-" else Path(args.apply).read_text(encoding="utf-8")
        items = json.loads(raw)
        if isinstance(items, dict):
            items = items.get("picks") or items.get("items") or [items]
        results = apply_picks(items, dry_run=args.dry_run)
        print(json.dumps({"results": results}, ensure_ascii=False, indent=2))
        return

    # 기본: --list
    options = fetch_options(refresh=args.refresh)
    names = args.names.split(",") if args.names else None
    updated_on = args.updated_on
    if args.today:
        from datetime import date as _date_cls
        updated_on = _date_cls.today().isoformat()
    empties = find_empty(names=names, limit=args.limit, updated_on=updated_on)
    payload = {
        "db": JAEROO_DB_ID,
        "updated_on": updated_on,
        "options_counts": {f: len(v) for f, v in options.items()},
        "options": options,
        "empty_count": len(empties),
        "stocks": empties,
    }
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    if args.out:
        Path(args.out).write_text(text, encoding="utf-8")
        print(f"[backfill] 빈 종목 {len(empties)}건 → {args.out}")
    else:
        print(text)


if __name__ == "__main__":
    main()
