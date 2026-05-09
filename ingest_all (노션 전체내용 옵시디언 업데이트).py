#!/usr/bin/env python3
"""
ingest_all.py — Notion 종목재료정리 DB → wiki/stocks/[종목명].md 자동 생성

사용법:
  python ingest_all.py                  # 신규 종목만 (기존 파일 스킵)
  python ingest_all.py 삼성전자         # 특정 종목만
  python ingest_all.py --all            # 전체 (기존 파일 덮어쓰기)
  python ingest_all.py --no-finance     # 재무 수집 건너뜀 (속도 우선)
  python ingest_all.py --dry-run        # 파일 쓰기 없이 미리보기
"""

import os, re, sys, time, argparse, subprocess
from datetime import datetime, timedelta
from pathlib import Path

# Windows 터미널 cp949 인코딩 오류 방지 — UTF-8 강제
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass

try:
    import requests
except ImportError:
    print("[ERROR] requests 없음. pip install requests 실행하세요.")
    sys.exit(1)

# ── 설정 ────────────────────────────────────────────────────────────────
NOTION_TOKEN = os.environ.get("NOTION_TOKEN", "")
DB_ID        = "2cbffbf4-6173-80e3-8d07-e8b5e59e36c4"
WIKI_ROOT    = Path(__file__).parent / "wiki" / "stocks"
TODAY        = datetime.now().strftime("%Y-%m-%d")

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}


# ══════════════════════════════════════════════════════════════════════
#  1. Notion API 호출
# ══════════════════════════════════════════════════════════════════════

def notion_get(url: str) -> dict:
    for attempt in range(3):
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code == 429:
            time.sleep(2 ** attempt)
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError(f"GET {url} 실패")


def notion_post(url: str, body: dict) -> dict:
    for attempt in range(3):
        r = requests.post(url, headers=HEADERS, json=body, timeout=30)
        if r.status_code == 429:
            time.sleep(2 ** attempt)
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError(f"POST {url} 실패")


def query_all_pages() -> list:
    """
    DB 전체 페이지 조회 후 종목명별 최신 1개만 반환.
    같은 종목이 날짜별로 여러 행 존재하므로 최근업데이트 기준으로 deduplicate.
    """
    all_pages, cursor = [], None
    while True:
        body = {"page_size": 100}
        if cursor:
            body["start_cursor"] = cursor
        data = notion_post(f"https://api.notion.com/v1/databases/{DB_ID}/query", body)
        all_pages.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")

    # 종목별 최신 페이지만 남기기 (최근업데이트 내림차순)
    latest: dict[str, dict] = {}
    for page in all_pages:
        name = prop_text(page["properties"].get("종목명", {})).strip()
        if not name:
            continue
        date = prop_date(page["properties"].get("최근업데이트", {})) or "0000-00-00"
        if name not in latest or date > prop_date(latest[name]["properties"].get("최근업데이트", {})):
            latest[name] = page

    return list(latest.values())


def get_blocks(block_id: str) -> list:
    """블록 자식 전체 조회 (페이지네이션 처리)"""
    blocks, cursor = [], None
    while True:
        url = f"https://api.notion.com/v1/blocks/{block_id}/children"
        if cursor:
            url += f"?start_cursor={cursor}"
        data = notion_get(url)
        blocks.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
    return blocks


# ══════════════════════════════════════════════════════════════════════
#  2. 텍스트 변환
# ══════════════════════════════════════════════════════════════════════

def rt_to_md(rich_texts: list) -> str:
    """Notion rich_text 배열 → Markdown 문자열 변환 (링크 포함)"""
    result = ""
    for rt in (rich_texts or []):
        text = rt.get("plain_text", "")
        href = rt.get("href")
        if href:
            result += f"[{text}]({href})"
        else:
            result += text
    return result.strip()


def clean_tag(notion_theme: str) -> str:
    """Notion 테마명 → Obsidian 태그 (공백·슬래시→하이픈, 괄호 제거)"""
    tag = notion_theme.lstrip("#").strip()
    tag = re.sub(r"\([^)]*\)", "", tag).strip()  # (고대역폭메모리) 제거
    tag = tag.replace(" ", "-")
    tag = re.sub(r'[/\\:*?"<>|]', "-", tag)   # 슬래시 등 파일명 불가 문자 → 하이픈
    return tag


def clean_theme(notion_theme: str) -> str:
    """Notion 테마명 → theme 필드용 (# 유지, 공백→하이픈)"""
    t = notion_theme.strip()
    if t.startswith("#"):
        t = "#" + t[1:].lstrip()  # "# 텍스트" → "#텍스트"
    else:
        t = "#" + t
    return t.replace(" ", "-")


def prop_text(prop) -> str:
    """Notion rich_text/title 프로퍼티 → 문자열"""
    if not prop:
        return ""
    items = prop.get("rich_text") or prop.get("title") or []
    return rt_to_md(items)


def prop_date(prop) -> str:
    if not prop or not prop.get("date"):
        return ""
    return prop["date"].get("start", "")


def prop_multiselect(prop) -> list:
    if not prop:
        return []
    return [ms["name"] for ms in prop.get("multi_select", [])]


# ══════════════════════════════════════════════════════════════════════
#  3. Notion 페이지 본문 파싱
# ══════════════════════════════════════════════════════════════════════

def parse_page_content(page_id: str) -> dict:
    """
    페이지 블록을 읽어 섹션별로 분리 반환.
    반환 형식:
      {
        '홈페이지': str,
        '사업내용': str,
        '타임라인': [str, ...],   # 추가 중요내용 bulleted items
        '유통주식비율': str,
        '아카이브': [str, ...],   # 지난 기사 Archive
      }
    """
    result = {
        "홈페이지": "",
        "사업내용": "",
        "타임라인": [],
        "유통주식비율": "",
        "아카이브": [],
    }

    try:
        blocks = get_blocks(page_id)
    except Exception as e:
        print(f"  [WARN] 블록 조회 실패: {e}")
        return result

    current_section = None

    for block in blocks:
        btype = block.get("type", "")
        bdata = block.get(btype, {})
        rts   = bdata.get("rich_text", [])
        text  = rt_to_md(rts)
        has_children = block.get("has_children", False)

        # ── 섹션 헤딩 판별 ──
        if btype == "heading_2":
            t = text.lower()
            if "홈페이지" in t or "callout" in t:
                current_section = None
            elif "사업" in t:
                current_section = "사업내용"
            elif "추가 중요" in t or "추가 중요내용" in t:
                current_section = "타임라인"
            elif "재무" in t:
                current_section = "재무"
            else:
                current_section = None
            continue

        if btype == "heading_3":
            current_section = None
            continue

        # ── 홈페이지 callout (첫 번째) ──
        if btype == "callout" and not result["홈페이지"]:
            # rich_text 안에 링크가 있으면 추출
            url = ""
            for rt in rts:
                href = rt.get("href", "")
                if href and href.startswith("http"):
                    url = href
                    break
            if url:
                result["홈페이지"] = url
            continue

        # ── 사업 내용 (quote/paragraph) ──
        if current_section == "사업내용" and btype in ("quote", "paragraph") and text:
            result["사업내용"] += text + " "
            continue

        # ── 추가 중요내용 (타임라인) ──
        if current_section == "타임라인" and btype == "bulleted_list_item" and text:
            result["타임라인"].append(text)
            continue

        # ── 재무: 유통주식비율 callout ──
        if current_section == "재무" and btype == "callout":
            if "유통" in text:
                # "유통가능주식 비율: 75.11%" 형태에서 숫자 추출
                m = re.search(r"[\d.]+%", text)
                result["유통주식비율"] = m.group() if m else text
            continue

        # ── 지난 기사 Archive (toggle) ──
        if btype == "toggle" and has_children:
            toggle_text = text
            if "archive" in toggle_text.lower() or "지난" in toggle_text:
                try:
                    child_blocks = get_blocks(block["id"])
                    for cb in child_blocks:
                        cbtype = cb.get("type", "")
                        cb_rts = cb.get(cbtype, {}).get("rich_text", [])
                        cb_text = rt_to_md(cb_rts)
                        if not cb_text:
                            continue
                        # 순수 URL 단독 항목 skip (홈페이지 링크 등 오진입 방지)
                        plain = re.sub(r"\[([^\]]*)\]\([^\)]*\)", r"\1", cb_text).strip()
                        plain_stripped = plain.strip("()")
                        if re.match(r"^https?://", plain_stripped):
                            continue
                        result["아카이브"].append(cb_text)
                except Exception as e:
                    print(f"  [WARN] 아카이브 조회 실패: {e}")

    result["사업내용"] = result["사업내용"].strip()
    return result


# ══════════════════════════════════════════════════════════════════════
#  4. Wiki .md 파일 생성
# ══════════════════════════════════════════════════════════════════════

def build_wiki_content(props: dict, content: dict, stock_name: str, preserved_fields: dict = None) -> str:
    """props (DB 프로퍼티) + content (페이지 본문) → 마크다운 문자열"""

    themes     = props.get("관련테마", [])   # ['#HBM(고대역폭메모리)', ...]
    categories = props.get("카테고리", [])   # ['반도체', '빅테크']
    rate       = props.get("등락률", "")
    last_upd   = props.get("최근업데이트", TODAY)
    # 오늘자 링크는 아카이브에 포함되므로 별도 사용 안 함
    summary    = props.get("종목요약", "")

    # ── 태그 / 테마 ──
    cat_tags   = [c for c in categories if c]
    theme_tags = [clean_tag(t) for t in themes if t]
    all_tags   = cat_tags + theme_tags
    all_tags   = list(dict.fromkeys(all_tags))  # 중복 제거

    theme_field = [clean_theme(t) for t in themes if t]

    # 섹터 (첫 번째 카테고리)
    sector = categories[0] if categories else "기타"

    # ── 연관 테마 링크 ──
    theme_links = " | ".join([f"[[themes/{clean_tag(t)}]]" for t in themes if t]) if themes else ""

    # ── 타임라인 ──
    timeline_lines = []
    for item in content.get("타임라인", []):
        # 날짜 추출 시도 (item이 이미 날짜로 시작하면 prefix 생략)
        date_m = re.match(r"\[?(20\d\d-\d\d-\d\d)\]?", item)
        date_str = date_m.group(1) if date_m else last_upd or TODAY
        # 등락률 표시 (같은 날짜면 오늘 DB 등락률 사용)
        rate_str = f"등락률: {rate}%" if rate and date_str == last_upd else "등락률 미기재"
        if date_m:
            # item 이미 날짜 포함 → prefix 없이
            timeline_lines.append(f"- {item} / {rate_str} / [등급 미분류]")
        else:
            timeline_lines.append(f"- [{date_str}] {item} / {rate_str} / [등급 미분류]")

    # ── 기사 아카이브 ──
    archive_lines = [f"- {a}" for a in content.get("아카이브", []) if a.strip()]

    # ── 홈페이지 ──
    homepage = content.get("홈페이지", "")
    homepage_str = f"[{homepage}]({homepage})" if homepage else "-"

    # ── 사업 내용 ──
    biz = content.get("사업내용", "") or "-"

    # ── 유통주식비율 ──
    share_ratio = content.get("유통주식비율", "") or "-"

    # ── 조합 ──
    notion_edited = props.get("last_edited_time", "")

    # ── 보존 필드 읽기 (수동 입력값 유지) ──
    pf             = preserved_fields or {}
    # stock_code: 보존값 우선 → 없으면 dart-fss 자동 조회 (캐시 활용으로 1회만 API 호출)
    stock_code     = pf.get("stock_code", "") or props.get("종목코드", "") or ""
    if not stock_code:
        stock_code = get_stock_code(stock_name) or ""
    is_leader      = pf.get("is_leader",       "")
    recent_breakout = pf.get("recent_breakout", "")

    lines = [
        f"---",
        f"tags: [{', '.join(all_tags)}]",
        f"sector: {sector}",
        f"last_updated: {TODAY}",
        f"notion_last_edited: {notion_edited}",
        f"theme: [{', '.join([chr(34)+t+chr(34) for t in theme_field])}]",
        f"stock_code: {stock_code}",
        f"is_leader: {is_leader}",
        f"recent_breakout: {recent_breakout}",
        f"---",
        f"",
        f"# {stock_name}",
        f"",
        f"## 한줄 요약",
        f"",
        summary if summary else "[⏳ 요약 미작성]",
        f"",
        f"---",
        f"",
        f"## 현재 투자 포인트",
        f"",
        f"- 🟢 [강세] [⏳ Notion 재료 분석 필요]",
        f"- 🔴 [리스크] [⏳ 리스크 분석 필요]",
        f"",
        f"---",
        f"",
        f"## 대장주 판단",
        f"",
        f"[⏳ 대장주 판단 미작성]",
        f"",
        f"---",
        f"",
        f"## 최근 재료 타임라인",
        f"",
        f"> 출처: Notion `추가 중요내용` 섹션 | 등락률은 Notion DB `등락률(%)` 컬럼 기준",
        f"",
    ]

    if timeline_lines:
        lines.extend(timeline_lines)
    else:
        lines.append("_[⏳ 추가 중요내용 없음]_")

    lines += [
        f"",
        f"---",
        f"",
        f"## 기사 아카이브",
        f"",
        f"> 출처: Notion `지난기사 아카이브 + 링크` 100% 전수 복사 (요약·누락 금지)",
        f"",
    ]

    if archive_lines:
        lines.extend(archive_lines)
    else:
        lines.append("_[⏳ 아카이브 없음]_")

    lines += [
        f"",
        f"---",
        f"",
        f"## 기업 기본 정보",
        f"",
        f"- **홈페이지**: {homepage_str}",
        f"- **사업 구조**: {biz[:200] + '...' if len(biz) > 200 else biz}",
        f"- **유통가능주식 비율**: {share_ratio}",
        f"",
        f"---",
        f"",
        f"## 재무 현황",
        f"",
        f"[⏳ fetch_finance.py 실행 필요]",
        f"",
        f"---",
        f"",
        f"## 차트·거래량 메모",
        f"",
        f"_직접 추가 또는 LLM이 분석 시 기록_",
        f"",
        f"---",
        f"",
        f"## 연관 테마",
        f"",
        theme_links if theme_links else "[⏳ 테마 없음]",
        f"",
        f"---",
        f"",
        f"## 마지막 업데이트",
        f"",
        f"{TODAY} | Notion ingest (ingest_all.py 자동 생성)",
    ]

    return "\n".join(lines)



# ══════════════════════════════════════════════════════════════════════
#  4-b. 기사 아카이브 등락률 매핑 (enrich_archive 통합)
# ══════════════════════════════════════════════════════════════════════

_price_cache: dict = {}   # {code: {date: change_pct}}
_code_cache:  dict = {}   # {name: stock_code}

def extract_date(title: str, url: str):
    """기사 제목/URL에서 날짜 추출 → 'YYYY-MM-DD'. 실패 시 None."""
    m = re.search(r'\((\d{2})\.(\d{2})\.(\d{2})\)', title)
    if m:
        yy, mm, dd = m.groups()
        return f"{2000 + int(yy)}-{mm}-{dd}"
    m = re.search(r'(?<!\d)(202[0-9])([01]\d)([0-3]\d)(?!\d)', url)
    if m:
        yyyy, mm, dd = m.groups()
        try:
            datetime.strptime(f"{yyyy}-{mm}-{dd}", "%Y-%m-%d")
            return f"{yyyy}-{mm}-{dd}"
        except ValueError:
            pass
    m = re.search(r'/(202[0-9])/([01]\d)/([0-3]\d)/', url)
    if m:
        yyyy, mm, dd = m.groups()
        return f"{yyyy}-{mm}-{dd}"
    m = re.search(r'ud=(202[0-9])([01]\d)([0-3]\d)', url)
    if m:
        yyyy, mm, dd = m.groups()
        return f"{yyyy}-{mm}-{dd}"
    return None


def get_change_pct(code: str, date_str: str):
    """종목코드 + 날짜 → 당일 등락률(%). 없으면 None."""
    import FinanceDataReader as fdr
    if code not in _price_cache:
        _price_cache[code] = {}
    if date_str in _price_cache[code]:
        return _price_cache[code][date_str]
    try:
        dt    = datetime.strptime(date_str, "%Y-%m-%d")
        start = (dt - timedelta(days=3)).strftime("%Y-%m-%d")
        end   = (dt + timedelta(days=1)).strftime("%Y-%m-%d")
        df    = fdr.DataReader(code, start, end)
        if df.empty:
            _price_cache[code][date_str] = None
            return None
        df.index = df.index.normalize()
        row = df.loc[dt] if dt in df.index else (df[df.index <= dt].iloc[-1] if len(df[df.index <= dt]) else None)
        if row is None:
            _price_cache[code][date_str] = None
            return None
        if 'Change' in df.columns and row['Change'] is not None:
            val = float(row['Change']) * 100
        elif 'Close' in df.columns and 'Open' in df.columns and float(row['Open']) > 0:
            val = (float(row['Close']) - float(row['Open'])) / float(row['Open']) * 100
        else:
            val = None
        _price_cache[code][date_str] = val
        return val
    except Exception as e:
        print(f"    [WARN] FDR 실패 ({code}, {date_str}): {e}")
        _price_cache[code][date_str] = None
        return None


def get_stock_code(name: str):
    """종목명 → 6자리 종목코드. 없으면 None."""
    if name in _code_cache:
        return _code_cache[name]
    try:
        import dart_fss as dart
        corps = dart.get_corp_list()
        for exactly in (True, False):
            result = corps.find_by_corp_name(name, exactly=exactly)
            if result:
                for corp in result[:5]:
                    if getattr(corp, 'stock_code', None):
                        _code_cache[name] = corp.stock_code.strip()
                        return _code_cache[name]
    except Exception as e:
        print(f"  [WARN] 코드 조회 실패 ({name}): {e}")
    _code_cache[name] = None
    return None


_ARTICLE_RE = re.compile(r'^(-\s+)\[([^\]]*)]\(([^)]+)\)(.*)')

def enrich_file(wiki_file: Path, name: str, dry_run: bool = False) -> int:
    """wiki 파일의 기사 아카이브에 등락률 추가. 추가된 건수 반환."""
    code = get_stock_code(name)
    if not code:
        print(f"  [ENRICH] {name} — 종목코드 없음, 스킵")
        return 0
    content = wiki_file.read_text(encoding="utf-8")
    if "## 기사 아카이브" not in content:
        return 0
    lines = content.split('\n')
    in_archive, new_lines, enriched = False, [], 0
    for line in lines:
        if line.strip() == "## 기사 아카이브":
            in_archive = True
            new_lines.append(line)
            continue
        if in_archive and re.match(r'^## ', line):
            in_archive = False
        if not in_archive:
            new_lines.append(line)
            continue
        m = _ARTICLE_RE.match(line)
        if not m:
            new_lines.append(line)
            continue
        prefix, title, url, suffix = m.group(1), m.group(2), m.group(3), m.group(4)
        if '등락률' in suffix or '날짜미확인' in suffix:
            new_lines.append(line)
            continue
        date_str = extract_date(title, url)
        if not date_str:
            new_lines.append(line)
            continue
        pct = get_change_pct(code, date_str)
        note = (f" / {date_str} / 등락률: +{pct:.1f}%" if pct is not None and pct >= 0
                else f" / {date_str} / 등락률: {pct:.1f}%" if pct is not None
                else f" / {date_str} / 날짜미확인")
        new_lines.append(f"{prefix}[{title}]({url}){suffix}{note}")
        enriched += 1
    if enriched > 0 and not dry_run:
        wiki_file.write_text('\n'.join(new_lines), encoding="utf-8")
    return enriched


# ══════════════════════════════════════════════════════════════════════
#  5. 단일 종목 처리
# ══════════════════════════════════════════════════════════════════════

def extract_props(page: dict) -> dict:
    """Notion 페이지 properties → 깔끔한 dict"""
    p = page.get("properties", {})
    return {
        "종목명":           prop_text(p.get("종목명")),
        "종목코드":         prop_text(p.get("종목코드")),
        "관련테마":         prop_multiselect(p.get("관련테마")),
        "카테고리":         prop_multiselect(p.get("카테고리")),
        "키워드":           prop_multiselect(p.get("키워드")),
        "등락률":           prop_text(p.get("등락률 (%)")),
        "최근업데이트":     prop_date(p.get("최근업데이트")),
        "링크":             prop_text(p.get("링크")),
        "종목요약":         prop_text(p.get("종목 재료 요약")),
        "page_id":          page["id"],
        "last_edited_time": page.get("last_edited_time", ""),  # Notion 자동 갱신 타임스탬프
    }


def read_wiki_last_updated(wiki_file: Path) -> str:
    """기존 wiki 파일의 frontmatter last_updated 값 반환. 없으면 빈 문자열."""
    try:
        for line in wiki_file.read_text(encoding="utf-8").splitlines():
            if line.startswith("last_updated:"):
                return line.split(":", 1)[1].strip()
    except Exception:
        pass
    return ""


def read_wiki_notion_last_edited(wiki_file: Path) -> str:
    """기존 wiki 파일 frontmatter에서 notion_last_edited 값 반환. 없으면 빈 문자열."""
    try:
        for line in wiki_file.read_text(encoding="utf-8").splitlines():
            if line.startswith("notion_last_edited:"):
                return line.split(":", 1)[1].strip()
    except Exception:
        pass
    return ""

def read_wiki_preserved_fields(wiki_file: Path) -> dict:
    """기존 wiki 파일 frontmatter에서 수동 입력 필드값 읽기.
    ingest 시 자동 덮어쓰지 않아야 하는 필드 (대장주 판단 등) 보존용."""
    result = {
        "stock_code":      "",
        "is_leader":       "",
        "recent_breakout": "",
    }
    if not wiki_file.exists():
        return result
    try:
        in_fm = False
        for line in wiki_file.read_text(encoding="utf-8").splitlines():
            if line.strip() == "---":
                if not in_fm:
                    in_fm = True
                    continue
                else:
                    break  # frontmatter 끝
            if in_fm:
                for field in ("stock_code", "is_leader", "recent_breakout"):
                    if line.startswith(f"{field}:"):
                        val = line.split(":", 1)[1].strip()
                        if val:
                            result[field] = val
    except Exception:
        pass
    return result


def process_stock(page: dict, overwrite: bool, no_finance: bool, dry_run: bool):
    """단일 종목 ingest. 성공 시 종목명 반환, 스킵 시 None."""
    props = extract_props(page)
    name  = props["종목명"].strip()

    if not name:
        return None

    wiki_file = WIKI_ROOT / f"{name}.md"

    if wiki_file.exists() and not overwrite:
        # 스마트 동기화: Notion last_edited_time > wiki notion_last_edited 인 경우만 업데이트
        # last_edited_time은 본문·속성 어떤 변경이든 Notion이 자동 갱신 (ISO 8601 타임스탬프)
        notion_edited = props.get("last_edited_time", "") or ""
        wiki_edited   = read_wiki_notion_last_edited(wiki_file)
        if notion_edited and wiki_edited and notion_edited <= wiki_edited:
            print(f"  [SKIP] {name} — 변경 없음 (Notion: {notion_edited[:16]}, wiki: {wiki_edited[:16]})")
            return None
        if notion_edited:
            print(f"  [UPDATE] {name} — Notion 변경 감지 ({notion_edited[:16]} > {wiki_edited[:16]})")

    print(f"\n  {'─'*40}")
    print(f"  종목: {name}")

    # 페이지 본문 파싱
    print(f"  [1] Notion 페이지 본문 조회...")
    content = parse_page_content(props["page_id"])
    print(f"      타임라인 {len(content['타임라인'])}건 / 아카이브 {len(content['아카이브'])}건")

    # 마크다운 생성 (기존 파일의 수동 입력값 보존)
    preserved = read_wiki_preserved_fields(wiki_file)
    md = build_wiki_content(props, content, name, preserved_fields=preserved)

    if dry_run:
        print(f"  [DRY-RUN] {wiki_file.name} 생성 예정 ({len(md)}자)")
        return name

    # 파일 저장
    WIKI_ROOT.mkdir(parents=True, exist_ok=True)
    wiki_file.write_text(md, encoding="utf-8")
    print(f"  [OK] {wiki_file.name} 저장 완료")

    # 등락률 누적 로그 추가
    rate    = props.get("등락률", "") or ""
    upd_dt  = props.get("최근업데이트", TODAY) or TODAY
    summary = props.get("종목요약", "") or ""
    if not summary:
        # 요약 없으면 아카이브 첫 번째 기사 제목 사용
        archive = content.get("아카이브", [])
        summary = archive[0][:80] if archive else ""
    _append_rate_log(wiki_file, upd_dt, rate, summary)
    if rate:
        print(f"  [LOG] 등락률 누적 기록: {upd_dt} / {rate}%")

    # 재무 데이터
    if not no_finance:
        print(f"  [2] fetch_finance.py 실행...")
        try:
            script = Path(__file__).parent / "fetch_finance.py"
            result = subprocess.run(
                [sys.executable, str(script), name, "--year", str(datetime.now().year - 1)],
                capture_output=True, text=True, encoding="utf-8", errors="replace",
                cwd=str(Path(__file__).parent)
            )
            if "완료" in result.stdout or "OK" in result.stdout or result.returncode == 0:
                print(f"  [OK] 재무 데이터 완료")
            else:
                print(f"  [WARN] 재무 수집 결과 불명확 (DART에 없는 종목일 수 있음)")
        except Exception as e:
            print(f"  [WARN] fetch_finance.py 실패: {e}")

    return name



# ══════════════════════════════════════════════════════════════════════
#  5-b. 등락률 누적 로그 (ingest 시 자동 누적)
# ══════════════════════════════════════════════════════════════════════

_LOG_HEADER  = "## 등락률 누적 로그"
_LOG_MARKER  = "<!-- ingest 자동 기록 -->"
_LOG_TBL_HDR = "| 날짜 | 등락률 | 재료 요약 |\n|------|--------|-----------|"


def _read_existing_log_rows(wiki_file: Path) -> list:
    """기존 wiki 파일에서 등락률 누적 로그 데이터 행 목록 반환. 없으면 []."""
    if not wiki_file.exists():
        return []
    rows, in_log = [], False
    for line in wiki_file.read_text(encoding="utf-8").splitlines():
        if line.strip() == _LOG_HEADER:
            in_log = True
            continue
        if in_log:
            if line.startswith("## "):
                break
            if line.startswith("| ") and not line.startswith("| 날짜") and not line.startswith("|---"):
                rows.append(line.strip())
    return rows


def _append_rate_log(wiki_file: Path, date: str, rate: str, summary: str):
    """
    wiki 파일의 '## 등락률 누적 로그' 섹션에 오늘 등락률 추가.
    - 섹션 없으면 '## 마지막 업데이트' 앞에 새로 삽입
    - 같은 날짜 행이 이미 있으면 스킵 (idempotent)
    - 날짜 내림차순 정렬 유지
    """
    if not rate:
        return

    try:
        r = float(str(rate).replace("%", "").strip())
        rate_fmt = f"+{r:.1f}%" if r >= 0 else f"{r:.1f}%"
    except ValueError:
        rate_fmt = f"{rate}%"

    summary_clean = (summary or "재료 미기재").replace("|", "／").replace("\n", " ").strip()[:100]
    new_row = f"| {date} | {rate_fmt} | {summary_clean} |"

    text = wiki_file.read_text(encoding="utf-8")

    # 같은 날짜 이미 있으면 스킵
    if f"| {date} |" in text:
        return

    existing = _read_existing_log_rows(wiki_file)
    all_rows = sorted(set(existing + [new_row]), reverse=True)

    # 섹션 내용 (--- 구분선 제외)
    section_body = "\n".join([
        _LOG_HEADER,
        "",
        _LOG_MARKER,
        "",
        _LOG_TBL_HDR,
    ] + all_rows + [""])

    if _LOG_HEADER in text:
        # 기존 섹션 교체: 헤더부터 다음 ## 직전까지 section_body 로 대체
        lines = text.splitlines()
        out, in_log, inserted = [], False, False
        for line in lines:
            if line.strip() == _LOG_HEADER:
                in_log = True
                if not inserted:
                    out.append(section_body)
                    inserted = True
                continue
            if in_log:
                if line.startswith("## "):
                    in_log = False
                    out.append(line)
                continue
            out.append(line)
        text = "\n".join(out)
    else:
        # 섹션 없음 → '## 마지막 업데이트' 앞에 삽입 (--- 포함)
        fresh = "\n---\n\n" + section_body + "\n"
        text = text.replace("\n## 마지막 업데이트", fresh + "## 마지막 업데이트", 1)

    wiki_file.write_text(text, encoding="utf-8")

# ══════════════════════════════════════════════════════════════════════
#  6. index.md / log.md 업데이트
# ══════════════════════════════════════════════════════════════════════

def update_log(msg: str):
    log_file = Path(__file__).parent / "wiki" / "log.md"
    if not log_file.exists():
        return
    content = log_file.read_text(encoding="utf-8")
    marker = "<!-- 아래에 최신 항목을 맨 위에 추가 -->"
    entry = f"\n## [{TODAY}] ingest | {msg}"
    new_content = content.replace(marker, marker + entry)
    log_file.write_text(new_content, encoding="utf-8")


def update_index(processed: list):
    """index.md 종목 테이블 업데이트 (신규 종목만 추가)"""
    index_file = Path(__file__).parent / "wiki" / "index.md"
    if not index_file.exists():
        return
    content = index_file.read_text(encoding="utf-8")

    for name in processed:
        wiki_file = WIKI_ROOT / f"{name}.md"
        if not wiki_file.exists():
            continue
        # 이미 있으면 날짜만 업데이트
        if f"[[stocks/{name}]]" in content:
            content = re.sub(
                rf"\[\[stocks/{re.escape(name)}\]\].*",
                f"[[stocks/{name}]] | - | - | - | {TODAY}",
                content
            )
        else:
            # 종목 테이블 마지막 행 뒤에 추가
            new_row = f"| [[stocks/{name}]] | - | - | - | {TODAY} |"
            content = content.replace(
                "---\n\n## 최근 분석 결과",
                f"{new_row}\n\n---\n\n## 최근 분석 결과"
            )

    index_file.write_text(content, encoding="utf-8")


# ══════════════════════════════════════════════════════════════════════
#  main
# ══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Notion DB → wiki/stocks/ 자동 ingest")
    parser.add_argument("stock", nargs="?", help="특정 종목명 (생략 시 전체)")
    parser.add_argument("--all",            action="store_true", help="기존 파일도 덮어쓰기")
    parser.add_argument("--no-finance",     action="store_true", help="재무 수집 건너뜀")
    parser.add_argument("--dry-run",        action="store_true", help="파일 쓰기 없이 미리보기")
    parser.add_argument("--delete-orphans", action="store_true", help="Notion에 없는 wiki 파일 삭제")
    parser.add_argument("--enrich",         action="store_true", help="ingest 후 기사 아카이브 등락률 매핑")
    args = parser.parse_args()

    if not NOTION_TOKEN:
        print("[ERROR] NOTION_TOKEN이 없습니다. .env 파일을 확인하세요.")
        sys.exit(1)

    print("=" * 50)
    print(f"  Notion → Wiki Ingest")
    print(f"  날짜: {TODAY}")
    print(f"  모드: {'DRY-RUN' if args.dry_run else '실제 저장'} / "
          f"{'전체 덮어쓰기' if args.all else '신규만'} / "
          f"{'재무 생략' if args.no_finance else '재무 포함'}")
    print("=" * 50)

    # 전체 페이지 조회
    print("\n[DB 조회] Notion 종목재료정리 전체 로드 중...")
    all_pages = query_all_pages()
    print(f"  → 총 {len(all_pages)}개 종목 확인")

    # 특정 종목 필터
    if args.stock:
        all_pages = [p for p in all_pages if prop_text(p["properties"].get("종목명")) == args.stock]
        if not all_pages:
            print(f"[ERROR] '{args.stock}' 종목을 DB에서 찾을 수 없습니다.")
            sys.exit(1)

    # 처리
    processed, skipped, failed = [], 0, 0
    for i, page in enumerate(all_pages, 1):
        name = prop_text(page["properties"].get("종목명", {}))
        print(f"\n[{i}/{len(all_pages)}] {name or '(이름 없음)'}")
        try:
            result = process_stock(page, args.all, args.no_finance, args.dry_run)
            if result:
                processed.append(result)
            else:
                skipped += 1
        except Exception as e:
            print(f"  [ERROR] {e}")
            failed += 1

    # index / log 업데이트
    if processed and not args.dry_run:
        update_index(processed)
        update_log(f"전체 ingest — {len(processed)}개 완료 / {skipped}개 스킵 / {failed}개 실패")

    # ── 기사 아카이브 등락률 매핑 (--enrich) ──
    if args.enrich and processed and not args.dry_run:
        print(f"\n[Enrich] {len(processed)}개 종목 등락률 매핑 시작...")
        try:
            import dart_fss as dart
            dart.set_api_key(os.environ.get("DART_API_KEY", ""))
            dart.get_corp_list()
        except Exception as e:
            print(f"  [WARN] DART 초기화 실패: {e}")
        total_enriched = 0
        for name in processed:
            wiki_file = WIKI_ROOT / f"{name}.md"
            if not wiki_file.exists():
                continue
            n = enrich_file(wiki_file, name, dry_run=False)
            if n:
                print(f"  [ENRICH] {name} — +{n}건 등락률 추가")
            total_enriched += n
        print(f"[Enrich] 완료: 총 {total_enriched}건 추가")
        update_log(f"enrich 완료 — {len(processed)}개 종목, {total_enriched}건 추가")

    # ── 고아 파일 삭제 (--delete-orphans) ──
    deleted = []
    if args.delete_orphans:
        notion_names = set(
            prop_text(p["properties"].get("종목명", {})).strip()
            for p in all_pages
        ) - {""}
        wiki_files = [f for f in WIKI_ROOT.glob("*.md") if not f.name.startswith("_")]
        orphans = [f for f in wiki_files if f.stem not in notion_names]

        if not orphans:
            print("\n[고아 파일] 삭제 대상 없음 — Notion과 wiki가 일치합니다.")
        else:
            print(f"\n[고아 파일] Notion에 없는 wiki 파일 {len(orphans)}개 감지:")
            for f in orphans:
                print(f"  - {f.name}")
            if args.dry_run:
                print("  [DRY-RUN] 실제 삭제는 수행하지 않음")
            else:
                for f in orphans:
                    f.unlink()
                    deleted.append(f.stem)
                    print(f"  [삭제] {f.name}")
                update_log(f"고아 파일 삭제 — {len(deleted)}개: {', '.join(deleted)}")

    # 요약
    print(f"\n{'=' * 50}")
    print(f"  완료: {len(processed)}개 | 스킵: {skipped}개 | 실패: {failed}개", end="")
    if deleted:
        print(f" | 삭제: {len(deleted)}개", end="")
    print(f"\n{'=' * 50}")


if __name__ == "__main__":
    main()
