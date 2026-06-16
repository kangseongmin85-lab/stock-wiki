#!/usr/bin/env python3
"""theme_rollup.py — daily picks 누적 → 테마 페이지 & overview.md 자동 강세 기록 (2·3차)

설계:
  - daily_picks_tracker 의 CSV 파서/테마 집계 재사용 (단일 소스)
  - 각 테마 페이지의 '## 🤖 자동 — 최근 강세 기록' 섹션만 in-place upsert.
    🧠 수동 섹션(수급 상태/대장주 순위/재료 히스토리)은 절대 건드리지 않음.
  - overview.md 의 '## 🤖 자동 — 최근 강세 테마' 블록만 upsert.
    🧠 시황 분석 영역은 보존 (밤 9시 LLM 스케줄 소유).
  - 기존 테마 파일이 없는 테마는 건너뜀(파일 폭증 방지) + 리포트.
"""
import sys
import re
from pathlib import Path
from datetime import datetime

if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import daily_picks_tracker as dpt

BASE = Path(__file__).parent
THEMES_DIR = BASE / "wiki" / "themes"
OVERVIEW = BASE / "wiki" / "overview.md"

RECENT_DAYS = 10   # 테마 페이지 표에 보여줄 최근 강세일 수
MIN_COUNT = 2      # 강세 테마 기준 (1차와 동일: 2종목 이상)
OVERVIEW_TOP = 20  # overview 에 보여줄 최근 활동 테마 수

THEME_SECTION = "🤖 자동 — 최근 강세 기록"
OVERVIEW_SECTION = "🤖 자동 — 최근 강세 테마"


def theme_filename(theme: str) -> str:
    """노션 태그 → 실제 테마 파일명 슬러그 ('/' → '-')."""
    return theme.replace("/", "-")


def build_history() -> dict:
    """모든 CSV 누적 → {theme: [{date,count,avg,sum_amt,leader,members}, ...]} (날짜 오름차순)."""
    csvs = sorted(dpt.PICKS_DIR.glob("*.csv"), key=lambda p: dpt.parse_csv_date(p.name))
    hist: dict = {}
    for c in csvs:
        date = dpt.parse_csv_date(c.name)
        if not date:
            continue
        picks = dpt.parse_picks_csv(c)
        if not picks:
            continue
        ranked = dpt.aggregate_themes(picks, [])  # reappear 정보는 여기선 불필요
        for t in ranked:
            if t["theme"] == "미분류" or t["count"] < MIN_COUNT:
                continue
            hist.setdefault(t["theme"], []).append({
                "date":    date,
                "count":   t["count"],
                "avg":     t["avg_change"],
                "sum_amt": t["sum_amount"],
                "leader":  t["leader"],
                "members": t["members"],
            })
    return hist


def upsert_section(text: str, header: str, body: str) -> str:
    """'## {header}' 블록(다음 '## ' 직전까지)을 body 로 교체. 없으면 적절히 삽입."""
    block = f"## {header}\n\n{body}\n"
    pat = re.compile(rf"^## {re.escape(header)}\n.*?(?=^## |\Z)", re.S | re.M)
    if pat.search(text):
        return pat.sub(lambda m: block, text)
    # 신규 삽입: '## 마지막 업데이트' 앞, 없으면 파일 끝
    m = re.search(r"^## 마지막 업데이트", text, re.M)
    if m:
        return text[:m.start()] + block + "\n" + text[m.start():]
    return text.rstrip() + "\n\n---\n\n" + block


def render_theme_body(entries: list) -> str:
    rows = [
        "> daily_picks_tracker 자동 기록. 강세일 = 관심종목 CSV 에서 2종목 이상 집계된 날. 🧠 수동 섹션 불침범.",
        "",
        "| 날짜 | 강세 종목수 | 평균 등락률 | 거래대금 합(백만) | 대장(수급) |",
        "|------|-------------|-------------|-------------------|------------|",
    ]
    for e in entries[-RECENT_DAYS:][::-1]:
        rows.append(
            f"| {e['date']} | {e['count']} | {e['avg']:+.2f}% | "
            f"{int(e['sum_amt']):,} | [[stocks/{e['leader']}]] |"
        )
    last = entries[-1]
    rows += [
        "",
        f"최근 강세일: **{last['date']}** · 누적 강세일 {len(entries)}회 · 최근 대장 [[stocks/{last['leader']}]]",
    ]
    return "\n".join(rows)


def update_theme_pages(hist: dict):
    updated, missing = [], []
    for theme, entries in hist.items():
        fp = THEMES_DIR / f"{theme_filename(theme)}.md"
        if not fp.exists():
            missing.append(theme)
            continue
        try:
            text = fp.read_text(encoding="utf-8")
        except Exception:
            continue
        new = upsert_section(text, THEME_SECTION, render_theme_body(entries))
        if new != text:
            fp.write_text(new, encoding="utf-8")
            updated.append(theme)
    return updated, missing


def render_overview_auto(hist: dict) -> str:
    rows = []
    for theme, entries in hist.items():
        last = entries[-1]
        rows.append({
            "theme":  theme,
            "ldate":  last["date"],
            "cum":    len(entries),
            "cnt":    last["count"],
            "avg":    last["avg"],
            "leader": last["leader"],
        })
    # 최근 강세일 desc, 누적 강세일 desc
    rows.sort(key=lambda r: (r["ldate"], r["cum"]), reverse=True)
    out = [
        f"> daily_picks_tracker 자동 집계 · 갱신 {datetime.now().strftime('%Y-%m-%d %H:%M')} · "
        "🧠 시황 분석 섹션은 별도(밤 9시 스케줄/본인 소유, 불침범).",
        "",
        "| 테마 | 최근 강세일 | 누적 강세일 | 최근 종목수 | 최근 평균 | 최근 대장 |",
        "|------|-------------|-------------|-------------|-----------|-----------|",
    ]
    for r in rows[:OVERVIEW_TOP]:
        fp = THEMES_DIR / f"{theme_filename(r['theme'])}.md"
        tcell = f"[[themes/{theme_filename(r['theme'])}|{r['theme']}]]" if fp.exists() else r["theme"]
        out.append(
            f"| {tcell} | {r['ldate']} | {r['cum']} | {r['cnt']} | "
            f"{r['avg']:+.2f}% | [[stocks/{r['leader']}]] |"
        )
    return "\n".join(out)


OVERVIEW_SKELETON = """---
type: overview
last_updated: {date}
---

# 🗺️ 시황 overview

{auto}

## 🧠 시황 분석 (본인/밤 9시 스케줄)

> 이 영역은 자동화가 건드리지 않습니다. 지수·수급·선물·국면 판단을 여기에 기록.

### 현재 국면
- 

### 주도 섹터/테마
- 

### 수급 메모
- 

## 마지막 업데이트
{date} | theme_rollup.py 자동(🤖) + 본인/스케줄(🧠)
"""


def update_overview(hist: dict) -> str:
    auto_body = render_overview_auto(hist)
    if OVERVIEW.exists():
        text = OVERVIEW.read_text(encoding="utf-8")
        new = upsert_section(text, OVERVIEW_SECTION, auto_body)
        # 마지막 업데이트 날짜 갱신(있으면)
        new = re.sub(r"(## 마지막 업데이트\n).*", rf"\g<1>{datetime.now().strftime('%Y-%m-%d')} | theme_rollup.py 자동(🤖) + 본인/스케줄(🧠)", new, flags=re.S)
        OVERVIEW.write_text(new, encoding="utf-8")
        return "updated"
    else:
        OVERVIEW.parent.mkdir(parents=True, exist_ok=True)
        content = OVERVIEW_SKELETON.format(
            date=datetime.now().strftime("%Y-%m-%d"),
            auto=f"## {OVERVIEW_SECTION}\n\n{auto_body}\n",
        )
        OVERVIEW.write_text(content, encoding="utf-8")
        return "created"


def run():
    hist = build_history()
    updated, missing = update_theme_pages(hist)
    ov = update_overview(hist)
    print(f"[theme_rollup] 강세 테마 {len(hist)}개 집계")
    print(f"[theme_rollup] 테마 페이지 갱신: {len(updated)}개" + (f" / 파일없어 스킵: {len(missing)}개" if missing else ""))
    if missing:
        print(f"[theme_rollup] 스킵된 테마(파일 없음): {', '.join(sorted(missing)[:20])}")
    print(f"[theme_rollup] overview.md {ov}")
    return {"themes": len(hist), "updated": len(updated), "missing": missing, "overview": ov}


if __name__ == "__main__":
    run()
