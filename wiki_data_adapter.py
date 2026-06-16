#!/usr/bin/env python3
"""
wiki_data_adapter.py — wiki/stocks/[종목명].md 파서

목적: 노션 푸시 시 위키 마크다운에 이미 있는 데이터를
notion_body_builder.reconstruct_page() 의 kwargs 로 변환.

DART API 재호출 없이 정적 파일 파싱만으로 다음 4종 데이터 추출:
- homepage         : `## 기업 기본 정보` 의 **홈페이지**: 항목
- business_summary : 같은 섹션의 **사업 구조**: 항목
- share_ratio      : 같은 섹션의 **유통가능주식 비율**: 항목
- one_line_summary : `## 한줄 요약` 본문 (placeholder 면 None)
- finance.outlook  : `## 현재 투자 포인트` 의 강세/리스크 불릿
- finance.quarterly_trend : `## 재무 현황` 의 `### 분기 실적` 표

placeholder (`[⏳ ...]`, `미작성`) 는 자동 제외.
"""

import os
import re
from pathlib import Path

# wiki/stocks 디렉토리 자동 탐색 (이 파일 옆 wiki/ 폴더 기준)
_BASE = Path(__file__).resolve().parent
WIKI_STOCKS_DIR = _BASE / "wiki" / "stocks"

_PLACEHOLDER_PATTERNS = [
    re.compile(r"\[⏳"),
    re.compile(r"미작성"),
    re.compile(r"정보\s*없음"),
    re.compile(r"^\s*-\s*$"),
]


def _is_placeholder(text):
    if not text:
        return True
    t = text.strip()
    if not t:
        return True
    return any(p.search(t) for p in _PLACEHOLDER_PATTERNS)


def _read_wiki(name):
    """종목명 → wiki 파일 경로. 없으면 None."""
    candidates = [
        WIKI_STOCKS_DIR / f"{name}.md",
        WIKI_STOCKS_DIR / f"{name.replace(' ', '')}.md",
    ]
    for p in candidates:
        if p.exists():
            try:
                return p.read_text(encoding="utf-8")
            except Exception:
                return None
    return None


def _section_text(md, heading):
    """`## heading` 부터 다음 `## ` 또는 `---` 전까지 본문 추출."""
    pattern = rf"^{re.escape(heading)}\s*$"
    lines = md.split("\n")
    start = None
    for i, line in enumerate(lines):
        if re.match(pattern, line):
            start = i + 1
            break
    if start is None:
        return ""
    out = []
    for line in lines[start:]:
        if re.match(r"^##\s", line) or line.strip() == "---":
            break
        out.append(line)
    return "\n".join(out).strip()


# ── 섹션별 추출 함수 ────────────────────────────────────────────────────────
def _extract_company_info(md):
    """## 기업 기본 정보 → {homepage, business_summary, share_ratio}"""
    body = _section_text(md, "## 기업 기본 정보")
    out = {"homepage": None, "business_summary": None, "share_ratio": None}
    if not body:
        return out

    # 홈페이지: `- **홈페이지**: [URL](URL)` 또는 `: URL`
    m = re.search(r"\*\*홈페이지\*\*\s*:\s*(.+)", body)
    if m:
        raw = m.group(1).strip()
        url_match = re.search(r"\((https?://[^\s\)]+)\)", raw) or re.search(r"(https?://\S+)", raw)
        if url_match:
            out["homepage"] = url_match.group(1)

    # 사업 구조: `- **사업 구조**: 내용` (여러 줄 가능, 다음 `- **` 전까지)
    m = re.search(r"\*\*사업\s*구조\*\*\s*:\s*(.+?)(?=\n-\s*\*\*|\Z)", body, re.DOTALL)
    if m:
        text = m.group(1).strip()
        if not _is_placeholder(text):
            # 줄바꿈 정리 — 너무 길면 자름 (Notion quote 가독성)
            text = re.sub(r"\s+", " ", text)
            out["business_summary"] = text[:500]

    # 유통가능주식 비율: `- **유통가능주식 비율**: 75.99%`
    m = re.search(r"\*\*유통가능주식\s*비율\*\*\s*:\s*([^\n]+)", body)
    if m:
        text = m.group(1).strip()
        if not _is_placeholder(text):
            out["share_ratio"] = text

    return out


def _extract_one_line_summary(md):
    """## 한줄 요약 → str | None"""
    body = _section_text(md, "## 한줄 요약")
    if not body or _is_placeholder(body):
        return None
    # 첫 의미 있는 줄만
    for line in body.split("\n"):
        line = line.strip()
        if line and not _is_placeholder(line):
            return line[:300]
    return None


def _extract_outlook(md):
    """## 현재 투자 포인트 → list[str] (강세/리스크 불릿 텍스트)"""
    body = _section_text(md, "## 현재 투자 포인트")
    if not body:
        return []
    out = []
    for line in body.split("\n"):
        m = re.match(r"^\s*-\s*(.+)", line)
        if not m:
            continue
        text = m.group(1).strip()
        if _is_placeholder(text):
            continue
        out.append(text[:200])
    return out


# 분기 행: `| 2025.Q1 | 2,611억 | 159억 | 52억 | 303.1% |`
_QUARTER_ROW = re.compile(
    r"^\|\s*([\d.QqHh분기]+)\s*\|"          # 분기
    r"\s*([^|]+?)\s*\|"                      # 매출
    r"\s*([^|]+?)\s*\|"                      # 영업이익
    r"\s*([^|]+?)\s*\|"                      # 순이익
)


def _extract_quarterly_trend(md):
    """## 재무 현황 > ### 분기 실적 (DART 연결) 표 → quarterly_trend str.

    반환 포맷 (notion_body_builder 가 기대):
      `[2025.Q1] Sales 2,611억 | OP 159억 | Net 52억`
    """
    body = _section_text(md, "## 재무 현황")
    if not body:
        return None
    # placeholder 체크는 행 단위로만 적용 — 섹션 내 일부 셀에 `[⏳]` 가 있어도
    # 다른 표(밸류에이션 등)는 정상일 수 있다.

    # ### 분기 실적 ... 표만 추림
    lines = body.split("\n")
    in_table = False
    rows = []
    for line in lines:
        if re.match(r"^###\s*분기\s*실적", line):
            in_table = True
            continue
        if in_table and re.match(r"^###\s", line):
            break  # 다음 ### 섹션
        if not in_table:
            continue
        m = _QUARTER_ROW.match(line)
        if not m:
            continue
        q, sales, op, net = (s.strip() for s in m.groups())
        # 헤더 행과 구분선 행 스킵
        if q in ("분기", "------") or "---" in q:
            continue
        # placeholder/빈 값 행 스킵
        if _is_placeholder(sales) and _is_placeholder(op) and _is_placeholder(net):
            continue
        rows.append(f"[{q}] Sales {sales} | OP {op} | Net {net}")

    return "\n".join(rows) if rows else None


# ── public ──────────────────────────────────────────────────────────────────
def extract_stock_data(name):
    """종목명 → reconstruct_page() 의 kwargs dict.

    반환 키: homepage, business_summary, share_ratio,
            one_line_summary, finance (or 누락 시 빈 dict)
    파일이 없으면 빈 dict 반환 (호출자 측에서 graceful skip).
    """
    md = _read_wiki(name)
    if not md:
        return {}

    company = _extract_company_info(md)
    outlook = _extract_outlook(md)
    qt      = _extract_quarterly_trend(md)

    out = {}
    if company.get("homepage"):
        out["homepage"] = company["homepage"]
    if company.get("business_summary"):
        out["business_summary"] = company["business_summary"]
    if company.get("share_ratio"):
        out["share_ratio"] = company["share_ratio"]

    one_line = _extract_one_line_summary(md)
    if one_line:
        out["one_line_summary"] = one_line

    finance = {}
    if outlook:
        finance["outlook"] = outlook
    if qt:
        finance["quarterly_trend"] = qt
    if finance:
        out["finance"] = finance

    return out


# ── manual smoke test ─────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys, json
    name = sys.argv[1] if len(sys.argv) > 1 else "AJ네트웍스"
    data = extract_stock_data(name)
    print(f"=== {name} ===")
    print(json.dumps(data, ensure_ascii=False, indent=2))
