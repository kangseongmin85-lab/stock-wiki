#!/usr/bin/env python3
"""
daily_picks_tracker.py — 오늘의 관심종목 CSV → 누적 이력 + daily signal 페이지

신호 종류:
  1) 재등장 종목: 같은 종목이 N일 만에 다시 잡혔는가
  2) 일일 시그널 페이지: wiki/daily_signals/YYYY-MM-DD.md

사용법:
  python daily_picks_tracker.py                      # 오늘의 관심종목/ 내 최신 CSV
  python daily_picks_tracker.py --csv 26.05.22.csv   # 특정 CSV
  python daily_picks_tracker.py --all-csvs           # 폴더 내 전체 CSV (retrospective)

설계 원칙:
  - stocks/*.md 페이지는 건드리지 않음 (ingest_all 충돌 방지)
  - 누적 이력은 _cache/daily_picks_history.json 에 저장
  - daily signal 페이지에 자동/수동 섹션 명확 분리
"""

import os
import sys
import csv
import json
import argparse
import glob
import re
from datetime import datetime, date
from pathlib import Path

if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

BASE_DIR = Path(__file__).parent
PICKS_DIR = BASE_DIR / "오늘의 관심종목"
SIGNALS_DIR = BASE_DIR / "wiki" / "daily_signals"
STOCKS_DIR = BASE_DIR / "wiki" / "stocks"
CACHE_DIR = BASE_DIR / "_cache"
HISTORY_PATH = CACHE_DIR / "daily_picks_history.json"

# CSV 파일명 형식: YY.MM.DD.csv (예: 26.05.22.csv)
CSV_DATE_RE = re.compile(r"(\d{2})\.(\d{2})\.(\d{2})\.csv$")


# ──────────────────────────────────────
# CSV 파싱
# ──────────────────────────────────────

def parse_csv_date(filename: str) -> str:
    """파일명에서 YYYY-MM-DD 추출. 매칭 안 되면 빈 문자열."""
    m = CSV_DATE_RE.search(filename)
    if not m:
        return ""
    yy, mm, dd = m.groups()
    return f"20{yy}-{mm}-{dd}"


def _clean_code(raw: str) -> str:
    """종목코드 앞의 ' 따옴표 제거 + strip."""
    return (raw or "").lstrip("'").strip()


def _to_float(s: str) -> float:
    """쉼표·% 제거 후 float 변환. 실패 시 0.0."""
    if not s:
        return 0.0
    try:
        return float(s.replace(",", "").replace("%", "").strip())
    except (ValueError, AttributeError):
        return 0.0


def parse_picks_csv(csv_path: Path) -> list:
    """CSV → [{code, name, change_rate, volume, trade_amount, memo}, ...]

    컬럼 변화 흡수 (5/17 은 거래대금 없음, 5/19+ 는 있음).
    빈 헤더 컬럼은 무시.
    """
    rows = []
    with open(csv_path, "r", encoding="cp949", errors="replace") as f:
        reader = csv.DictReader(f)
        # 빈 헤더 (5번째 칼럼) 가 None 또는 '' 키로 잡힐 수 있음 — 무시
        for row in reader:
            name = (row.get("종목명") or "").strip()
            code = _clean_code(row.get("종목코드") or "")
            if not name or not code:
                continue
            rows.append({
                "code":         code,
                "name":         name,
                "change_rate":  _to_float(row.get("등락률") or ""),
                "volume":       _to_float(row.get("거래량") or ""),
                "trade_amount": _to_float(row.get("거래대금") or ""),  # 5/17 은 0
                "memo":         (row.get("메모") or "").strip(),
            })
    return rows


# ──────────────────────────────────────
# 누적 이력
# ──────────────────────────────────────

def load_history() -> dict:
    """{종목코드: [{date, name, change_rate, memo_snippet}, ...]}"""
    if not HISTORY_PATH.exists():
        return {}
    try:
        return json.loads(HISTORY_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[WARN] history 로드 실패: {e} — 빈 상태로 시작")
        return {}


def save_history(history: dict) -> None:
    CACHE_DIR.mkdir(exist_ok=True)
    HISTORY_PATH.write_text(
        json.dumps(history, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def update_history(history: dict, date_str: str, picks: list) -> None:
    """In-place 갱신. 같은 (날짜+종목) 조합은 덮어쓰기 (재실행 안전)."""
    for p in picks:
        entries = history.setdefault(p["code"], [])
        # 같은 날짜 항목 제거 후 추가 (idempotent)
        entries[:] = [e for e in entries if e.get("date") != date_str]
        entries.append({
            "date":          date_str,
            "name":          p["name"],
            "change_rate":   p["change_rate"],
            "memo_snippet":  p["memo"][:120],
        })
        entries.sort(key=lambda e: e.get("date", ""))


def detect_reappearance(history: dict, date_str: str, picks: list) -> list:
    """오늘 잡힌 종목 중 history 에 이전 등장 이력이 있는 것만 리스트로.
    반환: [{code, name, days_gap, prev_date, prev_change_rate, today_change_rate, prev_count}, ...]
    """
    reappear = []
    today = datetime.strptime(date_str, "%Y-%m-%d").date()
    for p in picks:
        entries = history.get(p["code"], [])
        # 오늘 항목 빼고 이전 항목만
        prev = [e for e in entries if e.get("date") < date_str]
        if not prev:
            continue
        last = max(prev, key=lambda e: e.get("date", ""))
        try:
            last_date = datetime.strptime(last["date"], "%Y-%m-%d").date()
            days_gap = (today - last_date).days
        except Exception:
            days_gap = -1
        reappear.append({
            "code":               p["code"],
            "name":               p["name"],
            "days_gap":           days_gap,
            "prev_date":          last["date"],
            "prev_change_rate":   last.get("change_rate", 0.0),
            "today_change_rate":  p["change_rate"],
            "prev_count":         len(prev),
        })
    # 경과일 짧은 순 (= 가장 강한 재등장 신호)
    reappear.sort(key=lambda r: (r["days_gap"] if r["days_gap"] >= 0 else 9999))
    return reappear


# ──────────────────────────────────────
# 테마 집계 (종목페이지 frontmatter join)
# ──────────────────────────────────────

_THEME_CACHE: dict = {}


def _read_stock_themes(name: str) -> list:
    """wiki/stocks/[name].md frontmatter 의 theme 리스트 반환.
    theme(노션 관련테마) 우선, 없으면 sector(노션 카테고리) 폴백, 둘 다 없으면 빈 리스트.
    """
    if name in _THEME_CACHE:
        return _THEME_CACHE[name]
    path = STOCKS_DIR / f"{name}.md"
    themes: list = []
    if path.exists():
        try:
            text = path.read_text(encoding="utf-8")
        except Exception:
            text = ""
        # frontmatter 블록만 추출
        fm = ""
        if text.startswith("---"):
            end = text.find("\n---", 3)
            if end != -1:
                fm = text[3:end]
        theme_line, sector_val = "", ""
        for line in fm.splitlines():
            s = line.strip()
            if s.startswith("theme:"):
                theme_line = s[len("theme:"):].strip()
            elif s.startswith("sector:"):
                sector_val = s[len("sector:"):].strip().strip('"').strip("'")
        # theme: ["#A", "#B"]  또는  [#A, #B]  파싱
        if theme_line.startswith("["):
            rb = theme_line.rfind("]")
            inner = theme_line[1:rb] if rb != -1 else theme_line[1:]
            for tok in inner.split(","):
                t = tok.strip().strip('"').strip("'").lstrip("#").strip()
                if t:
                    themes.append(t)
        if not themes and sector_val and sector_val not in ("", "[]"):
            themes.append(sector_val)
    _THEME_CACHE[name] = themes
    return themes


def aggregate_themes(picks: list, reappear: list) -> list:
    """오늘 picks 를 theme 별로 묶어 집계.
    한 종목이 여러 테마에 속하면 각 테마에 중복 집계된다.
    반환: [{theme, count, avg_change, sum_amount, leader, members, reappear_names}, ...]
    """
    reappear_codes = {r["code"] for r in reappear}
    buckets: dict = {}
    for p in picks:
        themes = _read_stock_themes(p["name"]) or ["미분류"]
        for t in themes:
            b = buckets.setdefault(t, {"members": [], "sum_amount": 0.0,
                                       "sum_change": 0.0, "reappear_names": []})
            b["members"].append(p)
            b["sum_amount"] += p["trade_amount"]
            b["sum_change"] += p["change_rate"]
            if p["code"] in reappear_codes:
                b["reappear_names"].append(p["name"])
    result = []
    for t, b in buckets.items():
        n = len(b["members"])
        leader = max(b["members"], key=lambda m: (m["trade_amount"], m["change_rate"]))
        result.append({
            "theme":          t,
            "count":          n,
            "avg_change":     b["sum_change"] / n if n else 0.0,
            "sum_amount":     b["sum_amount"],
            "leader":         leader["name"],
            "members":        [m["name"] for m in sorted(b["members"], key=lambda m: -m["trade_amount"])],
            "reappear_names": b["reappear_names"],
        })
    # 강세 집중 = 종목 수 우선, 수급(거래대금 합) 보조. 미분류는 항상 맨 뒤.
    result.sort(key=lambda x: (x["theme"] == "미분류", -x["count"], -x["sum_amount"]))
    return result


# ──────────────────────────────────────
# daily signal 페이지 생성
# ──────────────────────────────────────

def _truncate_memo(memo: str, limit: int = 200) -> str:
    memo = memo.strip()
    if len(memo) <= limit:
        return memo
    return memo[:limit].rstrip() + "…"


def render_signal_page(date_str: str, picks: list, reappear: list) -> str:
    """daily signal markdown 생성."""
    # 거래대금 + 등락률 기준 랭킹 (등락률 우선, 거래대금 보조)
    ranked = sorted(picks, key=lambda p: (-p["change_rate"], -p["trade_amount"]))

    lines = [
        "---",
        f"date: {date_str}",
        f"picks_count: {len(picks)}",
        f"reappear_count: {len(reappear)}",
        "type: daily_signal",
        "---",
        "",
        f"# 📊 일일 시그널 — {date_str}",
        "",
        f"> 출처: `오늘의 관심종목/{_csv_filename_for_date(date_str)}` · 자동 생성: {datetime.now().isoformat(timespec='seconds')}",
        "",
    ]

    # ── 재등장 섹션 ──
    lines.append("## ⭐ 재등장 종목")
    if not reappear:
        lines.append("")
        lines.append("_이번이 처음 잡힌 종목들이거나 누적 이력이 없습니다._")
    else:
        lines.append("")
        lines.append("| 종목 | 경과일 | 이전 등장 | 이전 등락률 | 오늘 등락률 | 누적 등장 |")
        lines.append("|------|--------|-----------|-------------|-------------|-----------|")
        for r in reappear:
            gap_label = f"{r['days_gap']}일" if r["days_gap"] >= 0 else "—"
            lines.append(
                f"| [[stocks/{r['name']}]] | {gap_label} | {r['prev_date']} | "
                f"{r['prev_change_rate']:+.2f}% | {r['today_change_rate']:+.2f}% | {r['prev_count']+1}회 |"
            )
    lines.append("")

    # ── 자동 추출 종목 랭킹 ──
    lines.append("## 🤖 자동 추출 — 종목 랭킹 (등락률 순)")
    lines.append("")
    lines.append("| # | 종목 | 등락률 | 거래대금(백만) | 메모 |")
    lines.append("|---|------|--------|----------------|------|")
    for i, p in enumerate(ranked, 1):
        memo = _truncate_memo(p["memo"]).replace("\n", " ").replace("|", "/")
        lines.append(
            f"| {i} | [[stocks/{p['name']}]] | {p['change_rate']:+.2f}% | "
            f"{int(p['trade_amount']):,} | {memo} |"
        )
    lines.append("")

    # ── 자동 추출 강세 테마 랭킹 ──
    theme_rank = aggregate_themes(picks, reappear)
    multi = [t for t in theme_rank if t["count"] >= 2]
    singles = [t for t in theme_rank if t["count"] == 1 and t["theme"] != "미분류"]

    lines.append("## 🤖 오늘 강세 테마 랭킹")
    lines.append("")
    lines.append("> 종목페이지 frontmatter(노션 관련테마/카테고리) 기준 자동 집계. "
                 "한 종목이 여러 테마에 중복 집계될 수 있음. 대장은 거래대금(수급) 기준.")
    lines.append("")
    if not multi:
        lines.append("_2종목 이상 몰린 테마 없음 (오늘은 강세가 분산됨)._")
        lines.append("")
    else:
        lines.append("| # | 테마 | 강세 종목수 | 평균 등락률 | 거래대금 합(백만) | 대장(수급) | 재등장 |")
        lines.append("|---|------|-------------|-------------|-------------------|------------|--------|")
        for i, t in enumerate(multi, 1):
            _slug = t["theme"].replace("/", "-")
            theme_cell = t["theme"] if t["theme"] == "미분류" else (
                f"[[themes/{_slug}|{t['theme']}]]" if _slug != t["theme"] else f"[[themes/{t['theme']}]]"
            )
            reapp = ", ".join(t["reappear_names"]) if t["reappear_names"] else "-"
            lines.append(
                f"| {i} | {theme_cell} | {t['count']} | {t['avg_change']:+.2f}% | "
                f"{int(t['sum_amount']):,} | [[stocks/{t['leader']}]] | {reapp} |"
            )
        lines.append("")
        lines.append("<details><summary>테마별 구성 종목 펼치기</summary>")
        lines.append("")
        for t in multi:
            members = " · ".join(f"[[stocks/{m}]]" for m in t["members"])
            lines.append(f"- **{t['theme']}** ({t['count']}): {members}")
        lines.append("")
        lines.append("</details>")
        lines.append("")
    if singles:
        names = ", ".join(t["theme"] for t in singles)
        lines.append(f"> 단독 강세(1종목) 테마: {names}")
        lines.append("")

    # ── 본인 분석 섹션 (빈 상태로 둠 — 카파시 wiki 핵심) ──
    lines.append("## 🧠 본인 분석")
    lines.append("")
    lines.append("_이 섹션은 본인이 직접 채우는 영역. 자동 추출 데이터를 보고 그날의 시장 해석/주도 테마/관전 종목을 정리._")
    lines.append("")
    lines.append("### 주도 테마")
    lines.append("- ")
    lines.append("")
    lines.append("### 관전 종목")
    lines.append("- ")
    lines.append("")
    lines.append("### 매매 시나리오")
    lines.append("- ")
    lines.append("")

    return "\n".join(lines)


def _csv_filename_for_date(date_str: str) -> str:
    """2026-05-22 → 26.05.22.csv"""
    yy = date_str[2:4]
    mm = date_str[5:7]
    dd = date_str[8:10]
    return f"{yy}.{mm}.{dd}.csv"


def write_signal_page(date_str: str, content: str) -> Path:
    SIGNALS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = SIGNALS_DIR / f"{date_str}.md"
    out_path.write_text(content, encoding="utf-8")
    return out_path


# ──────────────────────────────────────
# 메인 처리
# ──────────────────────────────────────

def process_csv(csv_path: Path) -> dict:
    """단일 CSV 처리. 반환: {date, picks_count, reappear_count, page_path}"""
    date_str = parse_csv_date(csv_path.name)
    if not date_str:
        raise ValueError(f"CSV 파일명에서 날짜 추출 실패: {csv_path.name}")

    picks = parse_picks_csv(csv_path)
    if not picks:
        print(f"  [WARN] {csv_path.name} — 종목 추출 실패 (빈 결과)")
        return {"date": date_str, "picks_count": 0, "reappear_count": 0, "page_path": None}

    history = load_history()
    reappear = detect_reappearance(history, date_str, picks)
    update_history(history, date_str, picks)
    save_history(history)

    content = render_signal_page(date_str, picks, reappear)
    page_path = write_signal_page(date_str, content)

    return {
        "date":           date_str,
        "picks_count":    len(picks),
        "reappear_count": len(reappear),
        "page_path":      page_path,
    }


def main():
    parser = argparse.ArgumentParser(description="오늘의 관심종목 CSV → daily signal + 재등장 트래커")
    parser.add_argument("--csv", type=str, default="",
                        help="특정 CSV 파일명 (생략 시 폴더 내 최신)")
    parser.add_argument("--all-csvs", action="store_true",
                        help="폴더 내 전체 CSV 를 날짜순으로 일괄 처리 (retrospective)")
    args = parser.parse_args()

    if not PICKS_DIR.exists():
        print(f"[ERROR] {PICKS_DIR} 폴더가 없습니다.")
        sys.exit(1)

    if args.all_csvs:
        csv_files = sorted(PICKS_DIR.glob("*.csv"), key=lambda p: parse_csv_date(p.name))
        if not csv_files:
            print(f"[ERROR] CSV 파일이 없습니다: {PICKS_DIR}")
            sys.exit(1)
    elif args.csv:
        csv_path = PICKS_DIR / args.csv if not args.csv.startswith("/") else Path(args.csv)
        if not csv_path.exists():
            print(f"[ERROR] CSV 파일 없음: {csv_path}")
            sys.exit(1)
        csv_files = [csv_path]
    else:
        csv_files = sorted(PICKS_DIR.glob("*.csv"), key=lambda p: parse_csv_date(p.name))
        if not csv_files:
            print(f"[ERROR] CSV 파일이 없습니다: {PICKS_DIR}")
            sys.exit(1)
        csv_files = [csv_files[-1]]

    print(f"[daily_picks_tracker] 처리 대상 {len(csv_files)}개 CSV")
    print("=" * 60)

    for csv_path in csv_files:
        print(f"\n[처리] {csv_path.name}")
        try:
            result = process_csv(csv_path)
            print(f"  → 날짜: {result['date']}")
            print(f"  → 종목 수: {result['picks_count']}")
            print(f"  → 재등장: {result['reappear_count']}개")
            if result['page_path']:
                print(f"  → 페이지: {result['page_path'].relative_to(BASE_DIR)}")
        except Exception as e:
            print(f"  [ERROR] {e}")

    # 2·3차: 누적 테마 강세 → 테마 페이지 & overview 자동 반영
    try:
        import theme_rollup
        theme_rollup.run()
    except Exception as e:
        print(f"  [WARN] theme_rollup 실패: {e}")

    print("\n" + "=" * 60)
    print(f"완료. history: {HISTORY_PATH.relative_to(BASE_DIR)}")


if __name__ == "__main__":
    main()
