#!/usr/bin/env python3
"""
situation_index.py — daily_signal 페이지 → 일자별 '국면 스냅샷' 벡터 (패턴 엔진 Phase 2)
==========================================================================================
유사국면 검색(pattern_match.py)의 입력. daily_signal 의 '강세 테마 랭킹'·'종목 랭킹'
표를 파싱해 하루를 테마 가중 벡터로 만든다.

입력 : wiki/daily_signals/*.md
출력 : _cache/situations.json
        "YYYY-MM-DD" -> {theme_vector{테마:가중치}, leaders[{name,ctrt,value}], breadth{picks,reappear,avg_ctrt}}

가중치 = (강세 종목수) x log10(거래대금합)  ... 수급(거래대금)과 폭(종목수) 동시 반영.

사용: python situation_index.py
"""

import sys, re, json, math
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

BASE_DIR = Path(__file__).parent
SIGNALS  = BASE_DIR / "wiki" / "daily_signals"
OUT      = BASE_DIR / "_cache" / "situations.json"


def unlink(s):
    """[[themes/슬러그|라벨]] -> 라벨, [[stocks/이름]] -> 이름, 일반텍스트 -> 그대로."""
    def rep(m):
        inner = m.group(0)[2:-2]
        if "|" in inner:
            return inner.split("|", 1)[1]
        return inner.split("/", 1)[-1]
    return re.sub(r"\[\[[^\]]+\]\]", rep, s).strip()


def num(s):
    s = (s or "").replace(",", "").replace("%", "").replace("+", "").strip()
    try:
        return float(s)
    except ValueError:
        return None


def int_or_none(m):
    return int(m.group(1)) if m else None


def parse_table(lines, header_key):
    """header_key 를 포함한 표 헤더를 찾아 데이터 행들을 셀 리스트로 반환."""
    start = None
    for i, l in enumerate(lines):
        if header_key in l and l.strip().startswith("|"):
            start = i
            break
    if start is None:
        return []
    rows = []
    j = start + 2  # 헤더 + |---| 구분선 건너뜀
    while j < len(lines) and lines[j].strip().startswith("|"):
        cells = [c.strip() for c in lines[j].strip().strip("|").split("|")]
        rows.append(cells)
        j += 1
    return rows


def parse_signal(path):
    text  = path.read_text(encoding="utf-8")
    lines = text.splitlines()

    m = re.search(r"date:\s*(\d{4}-\d{2}-\d{2})", text)
    date  = m.group(1) if m else path.stem
    picks = int_or_none(re.search(r"picks_count:\s*(\d+)", text))
    reapp = int_or_none(re.search(r"reappear_count:\s*(\d+)", text))

    # 강세 테마 랭킹: | # | 테마 | 강세 종목수 | 평균 등락률 | 거래대금 합(백만) | 대장 | 재등장 |
    theme_vec = {}
    for cells in parse_table(lines, "강세 종목수"):
        if len(cells) < 5:
            continue
        theme = unlink(cells[1])
        cnt   = num(cells[2])
        val   = num(cells[4])
        if not theme or cnt is None:
            continue
        theme_vec[theme] = round(cnt * math.log10(max(val or 0, 10)), 3)

    # 종목 랭킹: | # | 종목 | 등락률 | 거래대금(백만) | 메모 |
    leaders, ctrts = [], []
    for cells in parse_table(lines, "거래대금(백만)"):
        if len(cells) < 4:
            continue
        name = unlink(cells[1])
        ctrt = num(cells[2])
        val  = num(cells[3])
        if not name:
            continue
        if ctrt is not None:
            ctrts.append(ctrt)
        leaders.append({"name": name, "ctrt": ctrt, "value": int(val) if val else None})

    leaders  = sorted([l for l in leaders if l["value"]], key=lambda x: -x["value"])[:5]
    avg_ctrt = round(sum(ctrts) / len(ctrts), 2) if ctrts else None

    return date, {
        "theme_vector": theme_vec,
        "leaders": leaders,
        "breadth": {"picks": picks, "reappear": reapp, "avg_ctrt": avg_ctrt},
    }


def main():
    if not SIGNALS.exists():
        print(f"[경고] {SIGNALS} 없음"); return
    sits = {}
    for p in sorted(SIGNALS.glob("*.md")):
        date, snap = parse_signal(p)
        sits[date] = snap
        tv  = snap["theme_vector"]
        top = ", ".join(t for t, _ in sorted(tv.items(), key=lambda x: -x[1])[:3])
        lead = snap["leaders"][0]["name"] if snap["leaders"] else "-"
        print(f"  {date} | 테마 {len(tv)}개 | top: {top} | 대장(수급): {lead}")
    OUT.parent.mkdir(exist_ok=True)
    OUT.write_text(json.dumps(sits, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[저장] {OUT} ({len(sits)}일)")


if __name__ == "__main__":
    main()
