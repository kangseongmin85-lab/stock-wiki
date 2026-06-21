#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
schedule_push.py — Claude가 고른 일정(picks.json)을 노션 🗓️ 주식 일정 DB에
                    upsert + wiki/일정.md 재생성. taxonomy_backfill --apply 패턴.

사용:
  python schedule_push.py --apply picks.json --dry-run   # 미리보기(노션/위키 미변경)
  python schedule_push.py --apply picks.json             # 실제 입력 + 위키 재생성
  python schedule_push.py --sync-wiki                     # 노션 현재 상태로 위키만 재생성

picks.json 형식 (Claude 작성):
  [
    {"name":"SK하이닉스 2분기 실적발표", "date":"2026-07-24", "tags":["실적발표"], "note":"..."},
    {"name":"아이엠바이오 신규상장", "date":"2026-06-25", "end":"", "tags":["IPO/상장"]}
  ]

규칙:
  - dedup: 노션 기존 (이름+날짜) 동일하면 생성 안 함(스킵). 같은 이름·다른 날짜는 신규.
  - 태그 어휘: schedule_scan.TAG_VOCAB 권장(자유값도 허용 — multi_select 자동 생성).
  - 위키는 항상 노션 현재 상태(또는 dry-run 시 기존+picks 병합)에서 재생성 = 단일 진실원천.
"""

import os
import re
import sys
import json
import argparse
import urllib.request
from datetime import datetime, date, timedelta

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

BASE = os.path.dirname(os.path.abspath(__file__))
SCHEDULE_DB_ID = "20dffbf4617380a99df6c90c5de6fdc9"  # 🗓️ 주식 일정
NOTION_VERSION = "2022-06-28"
WIKI_PATH = os.path.join(BASE, "wiki", "일정.md")
CACHE_PATH = os.path.join(BASE, "_cache", "schedule.json")
TAG_VOCAB = ["실적발표", "IPO/상장", "락업해제", "거시/정책", "지정학", "종목공시"]


def _load_config():
    p = os.path.join(BASE, "config.json")
    if os.path.exists(p):
        try:
            return json.load(open(p, encoding="utf-8"))
        except Exception:
            pass
    return {}


_CFG = _load_config()
NOTION_TOKEN = os.environ.get("NOTION_TOKEN") or _CFG.get("NOTION_TOKEN", "")


def _headers():
    return {
        "Authorization": "Bearer " + NOTION_TOKEN,
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


def query_all():
    """노션 🗓️ 주식 일정 DB 전체 → [{name,date,end,tags,page_id}]"""
    out, cursor = [], None
    while True:
        body = {"page_size": 100}
        if cursor:
            body["start_cursor"] = cursor
        req = urllib.request.Request(
            f"https://api.notion.com/v1/databases/{SCHEDULE_DB_ID}/query",
            data=json.dumps(body).encode("utf-8"), headers=_headers(), method="POST")
        r = json.load(urllib.request.urlopen(req, timeout=20))
        for p in r.get("results", []):
            pr = p.get("properties", {})
            name = "".join(t.get("plain_text", "") for t in pr.get("이름", {}).get("title", []))
            dd = pr.get("날짜", {}).get("date") or {}
            tags = [o["name"] for o in pr.get("태그", {}).get("multi_select", [])]
            out.append({"name": name.strip(), "date": dd.get("start", ""),
                        "end": dd.get("end"), "tags": tags, "page_id": p.get("id", "")})
        if r.get("has_more"):
            cursor = r.get("next_cursor")
        else:
            break
    return out


def create_event(name, start, end=None, tags=None):
    props = {
        "이름": {"title": [{"text": {"content": name[:1900]}}]},
        "날짜": {"date": {"start": start, **({"end": end} if end else {})}},
    }
    if tags:
        props["태그"] = {"multi_select": [{"name": t} for t in tags]}
    body = {"parent": {"database_id": SCHEDULE_DB_ID}, "properties": props}
    req = urllib.request.Request("https://api.notion.com/v1/pages",
                                 data=json.dumps(body).encode("utf-8"),
                                 headers=_headers(), method="POST")
    r = json.load(urllib.request.urlopen(req, timeout=20))
    return r.get("id", "")


def _norm(s):
    return re.sub(r"\s+", " ", (s or "")).strip().lower()


def _dkey(start):
    return (start or "")[:10]


def apply_picks(picks, existing, dry_run):
    """picks → 노션 생성(중복 스킵). 반환: (created, skipped, merged_events)"""
    seen = {(_norm(e["name"]), _dkey(e["date"])) for e in existing}
    created, skipped = [], []
    merged = list(existing)
    for p in picks:
        name = (p.get("name") or "").strip()
        start = (p.get("date") or p.get("start") or "").strip()[:10]
        end = (p.get("end") or "").strip() or None
        tags = p.get("tags") or []
        if not name or not re.match(r"\d{4}-\d{2}-\d{2}", start):
            skipped.append({"name": name, "reason": "이름/날짜(YYYY-MM-DD) 누락"})
            continue
        k = (_norm(name), start)
        if k in seen:
            skipped.append({"name": name, "date": start, "reason": "중복(기존 존재)"})
            continue
        seen.add(k)
        if dry_run:
            created.append({"name": name, "date": start, "tags": tags, "page_id": "(dry-run)"})
        else:
            pid = create_event(name, start, end, tags)
            created.append({"name": name, "date": start, "tags": tags, "page_id": pid})
        merged.append({"name": name, "date": start, "end": end, "tags": tags, "page_id": "(new)"})
    return created, skipped, merged


def _tag_emoji(tags):
    m = {"실적발표": "📊", "IPO/상장": "🆕", "락업해제": "🔓",
         "거시/정책": "🏛️", "지정학": "🌍", "종목공시": "📢"}
    for t in tags:
        if t in m:
            return m[t]
    return "•"


def write_wiki(events, today_iso):
    """events 전체 → wiki/일정.md (다가오는 일정 시간순 + 지난 일정 접기)."""
    ty = datetime.strptime(today_iso, "%Y-%m-%d").date()
    week_end = ty + timedelta(days=(6 - ty.weekday()))     # 이번 주 일요일
    month_end = (ty.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)

    def parse(e):
        try:
            return datetime.strptime((e.get("date") or "")[:10], "%Y-%m-%d").date()
        except Exception:
            return None

    rows = [(parse(e), e) for e in events if parse(e)]
    rows.sort(key=lambda x: x[0])

    upcoming = [(d, e) for d, e in rows if d >= ty]
    past = [(d, e) for d, e in rows if d < ty]

    def fmt_row(d, e):
        dd = (d - ty).days
        dlabel = "D-DAY" if dd == 0 else (f"D-{dd}" if dd > 0 else f"D+{-dd}")
        tags = e.get("tags") or []
        tagtxt = " ".join(tags) if tags else "-"
        emo = _tag_emoji(tags)
        end = e.get("end")
        datetxt = d.isoformat() + (f"~{end[:10]}" if end else "")
        name = re.sub(r"\s+", " ", e.get("name", "")).strip()
        return f"| {datetxt} | {dlabel} | {emo} {name} | {tagtxt} |"

    L = []
    L.append("---")
    L.append("type: reference")
    L.append("title: 주식 일정 (노션 자동 동기화)")
    L.append(f"date: {today_iso}")
    L.append("tags: [일정, 캘린더]")
    L.append("source: Notion 🗓️ 주식 일정 DB")
    L.append("---")
    L.append("")
    L.append("# 🗓️ 주식 일정")
    L.append("")
    L.append("> 노션 **🗓️ 주식 일정** DB와 자동 동기화. 갱신: `리포트_실행.bat` → 세션에서 \"일정 업데이트해줘\".")
    L.append(f"> 마지막 동기화: {datetime.now().isoformat(timespec='minutes')} · 총 {len(rows)}건 (다가오는 {len(upcoming)} / 지난 {len(past)})")
    L.append("> 태그: 📊실적발표 🆕IPO/상장 🔓락업해제 🏛️거시/정책 🌍지정학 📢종목공시")
    L.append("")

    def section(title, items):
        L.append(f"### {title} ({len(items)})")
        if not items:
            L.append("_해당 없음_")
            L.append("")
            return
        L.append("| 날짜 | D- | 이벤트 | 태그 |")
        L.append("|------|----|--------|------|")
        for d, e in items:
            L.append(fmt_row(d, e))
        L.append("")

    L.append("## 📌 다가오는 일정")
    L.append("")
    section(f"이번 주 (~{week_end.isoformat()})", [x for x in upcoming if x[0] <= week_end])
    section(f"이번 달 (~{month_end.isoformat()})",
            [x for x in upcoming if week_end < x[0] <= month_end])
    section("그 이후", [x for x in upcoming if x[0] > month_end])

    L.append("---")
    L.append("")
    recent_past = [x for x in past if (ty - x[0]).days <= 30]
    L.append("<details>")
    L.append(f"<summary>지난 일정 (최근 30일, {len(recent_past)}건)</summary>")
    L.append("")
    L.append("| 날짜 | D- | 이벤트 | 태그 |")
    L.append("|------|----|--------|------|")
    for d, e in recent_past:
        L.append(fmt_row(d, e))
    L.append("")
    L.append("</details>")
    L.append("")

    os.makedirs(os.path.dirname(WIKI_PATH), exist_ok=True)
    with open(WIKI_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(L))
    return len(upcoming), len(past)


def save_cache(events, today_iso):
    os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)
    json.dump({"synced": datetime.now().isoformat(timespec="seconds"),
               "today": today_iso, "events": events},
              open(CACHE_PATH, "w", encoding="utf-8"), ensure_ascii=False, indent=2)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", help="picks.json 경로")
    ap.add_argument("--dry-run", action="store_true", help="노션/위키 미변경, 계획만 출력")
    ap.add_argument("--sync-wiki", action="store_true", help="노션 현재 상태로 위키만 재생성")
    args = ap.parse_args()

    if not NOTION_TOKEN:
        print("[ERROR] NOTION_TOKEN 없음 (config.json 확인)")
        sys.exit(1)

    today = date.today().isoformat()

    if args.sync_wiki and not args.apply:
        events = query_all()
        u, p = write_wiki(events, today)
        save_cache(events, today)
        print(f"[sync-wiki] 노션 {len(events)}건 → wiki/일정.md (다가오는 {u} / 지난 {p})")
        return

    if not args.apply:
        print("사용법: --apply picks.json [--dry-run]  또는  --sync-wiki")
        sys.exit(1)

    picks_path = args.apply if os.path.isabs(args.apply) else os.path.join(BASE, args.apply)
    picks = json.load(open(picks_path, encoding="utf-8"))
    if isinstance(picks, dict) and "picks" in picks:
        picks = picks["picks"]

    existing = query_all()
    created, skipped, merged = apply_picks(picks, existing, args.dry_run)

    mode = "DRY-RUN" if args.dry_run else "APPLY"
    print(f"[{mode}] picks={len(picks)} | 생성={len(created)} | 스킵={len(skipped)} | 기존={len(existing)}")
    for c in created:
        print(f"  + {c['date']} {c['name']}  {c['tags']}")
    for s in skipped:
        print(f"  - SKIP {s.get('date','')} {s['name']} ({s['reason']})")

    if not args.dry_run:
        # 실제 생성 후 노션 재조회로 위키 재생성 (단일 진실원천)
        events = query_all()
        u, p = write_wiki(events, today)
        save_cache(events, today)
        print(f"  위키 갱신: wiki/일정.md (다가오는 {u} / 지난 {p})")
    else:
        # dry-run: 병합본으로 위키 미리보기 생성 (reports/일정_preview.md)
        global WIKI_PATH
        preview = os.path.join(BASE, "reports", "일정_preview.md")
        _orig = WIKI_PATH
        WIKI_PATH = preview
        u, p = write_wiki(merged, today)
        WIKI_PATH = _orig
        print(f"  미리보기: reports/일정_preview.md (병합 후 다가오는 {u} / 지난 {p}) — 노션/위키 미변경")


if __name__ == "__main__":
    main()
