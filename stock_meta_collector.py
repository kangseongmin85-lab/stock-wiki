#!/usr/bin/env python3
"""
stock_meta_collector.py — 종목 기본 메타 데이터 외부 수집

진짜 신규 종목(노션/위키 모두 비어 있는 경우) 푸시 시:
  1) DART 기업개황 API → 홈페이지, 산업분류, 본사 주소, 설립일, 상장일
  2) FnGuide 기업개요 크롤링 → 사업구조 상세, 유통가능주식 비율

반환 형식: notion_body_builder.reconstruct_page() 의 kwargs 호환.
어느 소스가 실패해도 graceful — 가능한 것만 채워서 반환.
"""

import os
import re
import json
import urllib.request
import urllib.error
from typing import Optional

# DART
DART_API_KEY = os.environ.get("DART_API_KEY", "")
DART_COMPANY_URL = "https://opendart.fss.or.kr/api/company.json"

# FnGuide
FNGUIDE_OVERVIEW_URL = "https://comp.fnguide.com/SVO2/asp/SVD_main.asp?pGB=1&gicode=A{code}"
FNGUIDE_FINANCE_URL  = "https://comp.fnguide.com/SVO2/asp/SVD_Finance.asp?pGB=1&gicode=A{code}&stkGb=701"
_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"

_TIMEOUT = 8


# ── DART ───────────────────────────────────────────────────────────────────
def _dart_resolve_corp_code(name_or_code: str) -> Optional[str]:
    """종목명 또는 6자리 코드 → DART corp_code (8자리 문자열). 실패 시 None."""
    try:
        import dart_fss as dart
        if not DART_API_KEY:
            return None
        dart.set_api_key(DART_API_KEY)
        corps = dart.get_corp_list()
        if re.match(r"^\d{6}$", name_or_code):
            corp = corps.find_by_stock_code(name_or_code)
        else:
            res = corps.find_by_corp_name(name_or_code, exactly=True) \
                  or corps.find_by_corp_name(name_or_code, exactly=False)
            corp = res[0] if isinstance(res, list) and res else res
        if not corp:
            return None
        return getattr(corp, "corp_code", None) or getattr(corp, "_corp_code", None)
    except Exception:
        return None


def dart_company_info(name_or_code: str) -> dict:
    """DART 기업개황 → {homepage, induty, address, est_dt, list_dt, ceo}.
    실패 시 빈 dict.
    """
    corp_code = _dart_resolve_corp_code(name_or_code)
    if not corp_code:
        return {}
    try:
        url = f"{DART_COMPANY_URL}?crtfc_key={DART_API_KEY}&corp_code={corp_code}"
        with urllib.request.urlopen(url, timeout=_TIMEOUT) as r:
            data = json.loads(r.read())
    except Exception:
        return {}
    if data.get("status") != "000":
        return {}

    out = {}
    hm = (data.get("hm_url") or "").strip()
    if hm:
        # http:// 안 붙어있으면 보정
        if not hm.startswith(("http://", "https://")):
            hm = "http://" + hm
        out["homepage"] = hm
    for src, dst in [
        ("induty_code", "induty_code"),
        ("adres",       "address"),
        ("est_dt",      "est_dt"),
        ("list_dt",     "list_dt"),
        ("ceo_nm",      "ceo"),
        ("corp_name",   "corp_name"),
    ]:
        v = (data.get(src) or "").strip()
        if v:
            out[dst] = v
    return out


# ── FnGuide ────────────────────────────────────────────────────────────────
def fnguide_overview(stock_code: str) -> dict:
    """FnGuide 기업개요 페이지 → {business_summary, share_ratio}.
    셀렉터가 자주 바뀌므로 실패 시 빈 dict.
    """
    if not re.match(r"^\d{6}$", stock_code or ""):
        return {}
    out = {}
    try:
        url = FNGUIDE_OVERVIEW_URL.format(code=stock_code)
        req = urllib.request.Request(url, headers={"User-Agent": _UA})
        with urllib.request.urlopen(req, timeout=_TIMEOUT) as r:
            raw = r.read()
        html = raw.decode("utf-8", errors="replace")
    except Exception:
        return {}

    try:
        from bs4 import BeautifulSoup
    except ImportError:
        # fallback: 정규식만으로 핵심 부분만 추출 (덜 정확)
        m = re.search(r"기업개요[^<]*</[^>]+>\s*<[^>]+>([^<]{30,400})", html)
        if m:
            out["business_summary"] = re.sub(r"\s+", " ", m.group(1)).strip()[:500]
        return out

    soup = BeautifulSoup(html, "html.parser")

    # 사업구조 — FnGuide 기업개요 박스 (id="bizSummary", "compBody", 등 시도)
    for sel in ["#bizSummary", "#compBody p", "div.um_table p",
                "div[class*='cmp_comm']", "div.cmp_oview p"]:
        node = soup.select_one(sel)
        if node:
            text = node.get_text(separator=" ", strip=True)
            if text and len(text) > 30:
                out["business_summary"] = re.sub(r"\s+", " ", text)[:500]
                break

    # 유통비율 — 페이지 어딘가에 "유동주식비율" 또는 "유통주식비율" 표기
    m = re.search(r"유[동통]주식\s*비?율[^0-9]{0,20}([\d.]+)\s*%", html)
    if m:
        out["share_ratio"] = f"{m.group(1)}%"

    return out


# ── FnGuide 분기 실적 ───────────────────────────────────────────────────────
def fnguide_finance(stock_code: str) -> dict:
    """FnGuide Snapshot 페이지의 분기 실적 표 → {quarterly_trend: str}.

    출력 형식 (notion_body_builder 가 기대):
      `[2025.Q1] Sales 2,611억 | OP 159억 | Net 52억`
    셀렉터 실패 시 빈 dict.
    """
    if not re.match(r"^\d{6}$", stock_code or ""):
        return {}
    # 분기 실적은 Snapshot(SVD_main) 의 #highlight_D_Q 테이블에 있음
    url = FNGUIDE_OVERVIEW_URL.format(code=stock_code)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": _UA})
        with urllib.request.urlopen(req, timeout=_TIMEOUT) as r:
            html = r.read().decode("utf-8", errors="replace")
    except Exception:
        return {}

    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return {}

    soup = BeautifulSoup(html, "html.parser")

    # Financial Highlight 분기 테이블 — 셀렉터 후보 다중 시도
    table = None
    for sel in ["#highlight_D_Q table", "#highlight_D_Q",
                "div[id*='highlight'][id*='Q'] table",
                "table.us_table_ty1"]:
        node = soup.select_one(sel)
        if node and node.name == "table":
            table = node
            break
        elif node:
            t = node.find("table")
            if t:
                table = t
                break
    if not table:
        return {}

    # 헤더 (분기 이름) 와 데이터 행 추출
    headers_row = table.find("thead") or table.find("tr")
    if not headers_row:
        return {}
    header_cells = headers_row.find_all(["th", "td"])
    # 첫 셀은 보통 "주요재무정보" 라벨, 그 다음부터 분기명 (예: 2024/09, 2024/12, 2025/03...)
    quarters = [c.get_text(strip=True) for c in header_cells[1:]]
    quarters = [q for q in quarters if q and re.search(r"\d", q)]
    if not quarters:
        return {}

    # 매출액 / 영업이익 / 당기순이익 행 찾기
    rows = {"매출": None, "영업": None, "순익": None}
    for tr in table.find_all("tr"):
        th = tr.find("th")
        if not th:
            continue
        label = th.get_text(strip=True)
        if "매출액" in label and rows["매출"] is None:
            rows["매출"] = [c.get_text(strip=True) for c in tr.find_all("td")]
        elif "영업이익" in label and rows["영업"] is None and "률" not in label:
            rows["영업"] = [c.get_text(strip=True) for c in tr.find_all("td")]
        elif ("당기순이익" in label or "순이익" in label) and rows["순익"] is None and "률" not in label:
            rows["순익"] = [c.get_text(strip=True) for c in tr.find_all("td")]

    if not (rows["매출"] and rows["영업"] and rows["순익"]):
        return {}

    # 분기 수 맞춤
    n = min(len(quarters), len(rows["매출"]), len(rows["영업"]), len(rows["순익"]))
    lines = []
    for i in range(n):
        q = quarters[i].replace("/", ".")  # 2024/09 → 2024.09
        s = rows["매출"][i] or "-"
        o = rows["영업"][i] or "-"
        nt = rows["순익"][i] or "-"
        # FnGuide 는 단위 "억" 자동 표기 — 숫자가 있을 때만 행 포함
        if not re.search(r"\d", s + o + nt):
            continue
        lines.append(f"[{q}] Sales {s}억 | OP {o}억 | Net {nt}억")

    if not lines:
        return {}
    return {"quarterly_trend": "\n".join(lines)}


# ── public ──────────────────────────────────────────────────────────────────
def collect_meta(name_or_code: str, stock_code: str = None) -> dict:
    """진짜 신규 종목 메타 데이터 수집. 두 소스 병합.

    반환 키 (있는 것만):
      homepage, business_summary, share_ratio,
      induty_code, address, est_dt, list_dt, ceo, corp_name
    """
    dart_data = dart_company_info(name_or_code)

    # stock_code 미지정 시 DART 응답 또는 입력값에서 추정
    code = stock_code
    if not code:
        if re.match(r"^\d{6}$", name_or_code):
            code = name_or_code
        else:
            # DART corp 객체에서 stock_code 얻기 시도
            try:
                import dart_fss as dart
                if DART_API_KEY:
                    dart.set_api_key(DART_API_KEY)
                    corps = dart.get_corp_list()
                    res = corps.find_by_corp_name(name_or_code, exactly=True) \
                          or corps.find_by_corp_name(name_or_code, exactly=False)
                    corp = res[0] if isinstance(res, list) and res else res
                    if corp:
                        sc = getattr(corp, "stock_code", None)
                        if sc:
                            code = sc.strip()
            except Exception:
                pass

    fn_data = fnguide_overview(code) if code else {}
    fn_finance = fnguide_finance(code) if code else {}

    # 병합: DART 가 기본, FnGuide 가 사업구조/유통비율 보완
    out = dict(dart_data)
    for k, v in fn_data.items():
        if v and not out.get(k):
            out[k] = v
    # finance 는 별도 키에 모음 — notion_body_builder.finance 와 같은 구조
    if fn_finance.get("quarterly_trend"):
        out["finance"] = {"quarterly_trend": fn_finance["quarterly_trend"]}
    return out


if __name__ == "__main__":
    import sys
    target = sys.argv[1] if len(sys.argv) > 1 else "삼성전자"
    data = collect_meta(target)
    print(json.dumps(data, ensure_ascii=False, indent=2))
