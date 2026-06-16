#!/usr/bin/env python3
"""
notion_body_builder.py — 종목 페이지 본문(children blocks) 재구성

설계서: notion_page_layout_spec.md (이브닝 자동 업데이트 양식 기반)
호출처: notion_pusher.push_reviewed() 가 upsert 후 page_id 와 함께 호출.

레이아웃 (위→아래):
  1. 🏠 홈페이지 callout              [homepage 있을 때만]
  2. ## 1. 종목 요약 / 종목 재료 요약 + paragraph
  3. ## 2. 사업 내용 (상세)           [business_summary 있을 때만]
  4. ## 3. 추가 중요 내용             + bulleted list ([YYYY-MM-DD] 제목+링크)
  5. ## 4. 재무 상태 및 전망          [share_ratio/finance 있을 때만]
  6. divider
  7. 🗄️ 지난 기사 (Archive) toggle    + 기존 페이지 기사 보존 (URL dedup)

데이터 없는 섹션은 생략. 본문 재구성 실패해도 호출자(upsert) 는 영향 X.
"""

import os
import re
import json
import urllib.request
import urllib.error

NOTION_VERSION = "2022-06-28"
NOTION_API     = "https://api.notion.com/v1"
CHUNK_SIZE     = 100  # Notion children append 단일 요청 한도


# ── HTTP helper ─────────────────────────────────────────────────────────────
def _req(method, url, token, body=None, timeout=15):
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Authorization":  f"Bearer {token}",
            "Notion-Version": NOTION_VERSION,
            "Content-Type":   "application/json",
        },
        method=method,
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def _req_delete(url, token, timeout=15):
    req = urllib.request.Request(
        url,
        headers={
            "Authorization":  f"Bearer {token}",
            "Notion-Version": NOTION_VERSION,
        },
        method="DELETE",
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status


# ── 기존 블록 조회 / 추출 ───────────────────────────────────────────────────
def _get_all_blocks(page_id, token):
    """페이지 children 전체 조회 (pagination)."""
    out = []
    cursor = None
    while True:
        url = f"{NOTION_API}/blocks/{page_id}/children?page_size=100"
        if cursor:
            url += f"&start_cursor={cursor}"
        try:
            res = _req("GET", url, token)
        except Exception:
            break
        out.extend(res.get("results", []))
        if not res.get("has_more"):
            break
        cursor = res.get("next_cursor")
    return out


def _get_block_children(block_id, token):
    out = []
    cursor = None
    while True:
        url = f"{NOTION_API}/blocks/{block_id}/children?page_size=100"
        if cursor:
            url += f"&start_cursor={cursor}"
        try:
            res = _req("GET", url, token)
        except Exception:
            break
        out.extend(res.get("results", []))
        if not res.get("has_more"):
            break
        cursor = res.get("next_cursor")
    return out


def _extract_news_links_from_blocks(blocks, token):
    """기존 블록 전체에서 (text, url) 쌍 추출 — toggle 내부 BFS 포함."""
    extracted = []
    seen = set()
    target_types = {
        "bulleted_list_item", "numbered_list_item",
        "paragraph", "callout", "quote", "toggle",
    }
    queue = list(blocks)
    while queue:
        block = queue.pop(0)
        b_type = block.get("type")
        if b_type in target_types:
            rich = block.get(b_type, {}).get("rich_text", [])
            for rt in rich:
                text_obj = rt.get("text") or {}
                text = (text_obj.get("content") or "").strip()
                link_obj = text_obj.get("link")
                url = link_obj.get("url") if link_obj else None
                if text and url and url.startswith("http") and len(text) > 5:
                    if url not in seen:
                        extracted.append((text, url))
                        seen.add(url)
        if b_type == "toggle" and block.get("has_children"):
            children = _get_block_children(block["id"], token)
            if children:
                queue.extend(children)
    return extracted


def _delete_blocks(block_ids, token):
    for bid in block_ids:
        try:
            _req_delete(f"{NOTION_API}/blocks/{bid}", token)
        except Exception:
            pass


# ── 블록 생성 helper ────────────────────────────────────────────────────────
def _h2(text):
    return {"object": "block", "type": "heading_2",
            "heading_2": {"rich_text": [{"type": "text", "text": {"content": text}}]}}


def _h3(text):
    return {"object": "block", "type": "heading_3",
            "heading_3": {"rich_text": [{"type": "text", "text": {"content": text}}]}}


def _para(text):
    return {"object": "block", "type": "paragraph",
            "paragraph": {"rich_text": [{"type": "text", "text": {"content": text}}]}}


def _quote(text):
    return {"object": "block", "type": "quote",
            "quote": {"rich_text": [{"type": "text", "text": {"content": text}}]}}


def _bullet(text):
    return {"object": "block", "type": "bulleted_list_item",
            "bulleted_list_item": {"rich_text": [{"type": "text", "text": {"content": text}}]}}


def _divider():
    return {"object": "block", "type": "divider", "divider": {}}


def _callout(emoji, rich_text, color=None, children=None):
    blk = {
        "object": "block", "type": "callout",
        "callout": {
            "icon": {"emoji": emoji},
            "rich_text": rich_text,
        },
    }
    if color:
        blk["callout"]["color"] = color
    if children:
        blk["callout"]["children"] = children
    return blk


def _article_blocks(article):
    """article = {title, url, date, desc|summary} → [bullet, (optional desc paragraph)].
    한 기사당 1~2 블록 반환. 빈 데이터면 빈 리스트.
    """
    title = (article.get("title") or "").strip()
    url   = (article.get("url")   or "").strip()
    date_ = (article.get("date")  or "").strip()
    # 본문 요약: desc 또는 summary 둘 다 폴백
    desc  = (article.get("desc") or article.get("summary") or "").strip()

    rich = []
    if date_:
        rich.append({
            "type": "text",
            "text": {"content": f"[{date_}] ", "link": None},
            "annotations": {"color": "gray"},
        })
    if url and title:
        rich.append({"type": "text", "text": {"content": title, "link": {"url": url}}})
    elif title:
        rich.append({"type": "text", "text": {"content": title}})
    elif url:
        rich.append({"type": "text", "text": {"content": url, "link": {"url": url}}})
    if not rich:
        return []

    blocks = [{"object": "block", "type": "bulleted_list_item",
               "bulleted_list_item": {"rich_text": rich}}]

    # 본문 요약을 들여쓴 작은 회색 paragraph 로 표시 (한눈에 보이게)
    if desc:
        # Notion text chunk 한도 2000자 — 안전하게 1500자
        desc_text = desc[:1500] + ("..." if len(desc) > 1500 else "")
        blocks.append({
            "object": "block", "type": "paragraph",
            "paragraph": {"rich_text": [{
                "type": "text",
                "text": {"content": f"    └ {desc_text}"},
                "annotations": {"color": "gray", "italic": True},
            }]},
        })

    return blocks


def _article_bullet(article):
    """legacy 호환 — 첫 블록(불릿)만 반환. 신규 코드는 _article_blocks 사용."""
    blocks = _article_blocks(article)
    return blocks[0] if blocks else None


# ── 재무 8분기 표 파서 (drive_to_notion.py 와 동일 포맷) ──────────────────────
_PATTERN_EN = re.compile(
    r"\[(.*?)\]\s*Sales\s*(.*?)\s*\|\s*OP\s*(.*?)\s*\|\s*Net\s*(.*)",
    re.IGNORECASE,
)
_PATTERN_KR = re.compile(
    r"\[(.*?)\]\s*매출\s*(.*?)\s*\|\s*영업익\s*(.*?)\s*\|\s*순익\s*(.*)"
)


def _build_finance_table(quarterly_trend_str):
    """quarterly_trend_str → table block (헤더 포함). 파싱 실패 시 None."""
    if not quarterly_trend_str:
        return None
    rows = [{
        "type": "table_row",
        "table_row": {"cells": [
            [{"type": "text", "text": {"content": "분기"}}],
            [{"type": "text", "text": {"content": "매출"}}],
            [{"type": "text", "text": {"content": "영업이익"}}],
            [{"type": "text", "text": {"content": "순이익"}}],
        ]},
    }]
    valid = 0
    for line in quarterly_trend_str.strip().split("\n"):
        line = line.strip()
        if not line:
            continue
        m = _PATTERN_EN.search(line) or _PATTERN_KR.search(line)
        if not m:
            continue
        q_part = f"[{m.group(1).strip()}]"
        sales  = m.group(2).strip() or "-"
        op     = m.group(3).strip() or "-"
        net    = m.group(4).strip() or "-"
        rows.append({
            "type": "table_row",
            "table_row": {"cells": [
                [{"type": "text", "text": {"content": q_part}}],
                [{"type": "text", "text": {"content": sales}}],
                [{"type": "text", "text": {"content": op}}],
                [{"type": "text", "text": {"content": net}}],
            ]},
        })
        valid += 1
    if valid == 0:
        return None
    return {"object": "block", "type": "table",
            "table": {"table_width": 4, "has_column_header": True, "children": rows}}


# ── children payload 빌더 (네트워크 호출 X — 단위 테스트 용이) ────────────────
def build_children_payload(
    articles=None,
    homepage=None,
    one_line_summary=None,
    business_summary=None,
    share_ratio=None,
    finance=None,
):
    """
    데이터 → 본문 children blocks 리스트 변환. 네트워크 호출 없음.

    articles          : list[dict{title, url, date, summary?}]
    homepage          : str | None
    one_line_summary  : str | None
    business_summary  : str | None
    share_ratio       : str | None  (예: "45.2%")
    finance           : dict | None — {outlook: list[str] | str, quarterly_trend: str}

    반환: list[block dict]
    """
    children = []
    articles = articles or []
    finance  = finance or {}

    # [1] 홈페이지
    if homepage:
        children.append(_callout("🏠", [
            {"type": "text", "text": {"content": "홈페이지 바로가기"}},
            {"type": "text", "text": {"content": f" ({homepage})", "link": {"url": homepage}}},
        ]))

    # [2] 종목 요약 — 우선순위: one_line_summary > business_summary 첫 문장 > 헤드라인
    if one_line_summary:
        children.append(_h2("1. 종목 요약"))
        children.append(_para(one_line_summary))
    elif business_summary:
        # FnGuide 사업구조 첫 문장을 종목 요약으로 사용 (보통 "동사는 YYYY년 설립되어 ZZZ 영위하는 기업임" 형식)
        first_sentence = re.split(r"(?<=[.。])\s+", business_summary.strip(), maxsplit=1)[0]
        if len(first_sentence) > 200:
            first_sentence = first_sentence[:200] + "..."
        children.append(_h2("1. 종목 요약"))
        children.append(_para(first_sentence))
    else:
        children.append(_h2("1. 종목 재료 요약"))
        heads = [(a.get("title") or "").strip() for a in articles[:3] if a.get("title")]
        children.append(_para(" / ".join(heads) if heads else "특이사항 없음"))

    # [3] 사업 내용 (데이터 있을 때만)
    if business_summary:
        children.append(_h2("2. 사업 내용 (상세)"))
        children.append(_quote(business_summary))

    # [4] 추가 중요 내용 — 항상 헤딩은 두고, 데이터 없으면 안내 문구
    children.append(_h2("3. 추가 중요 내용"))
    if articles:
        for art in articles:
            children.extend(_article_blocks(art))  # bullet + (있으면) desc paragraph
    else:
        children.append(_para("업데이트된 중요 이슈가 없습니다."))

    # [5] 재무 상태 및 전망
    has_finance_data = bool(share_ratio) or bool(finance.get("outlook")) or bool(finance.get("quarterly_trend"))
    if has_finance_data:
        children.append(_h2("4. 재무 상태 및 전망"))

        if share_ratio:
            children.append(_callout("📊", [
                {"type": "text", "text": {"content": "유통가능주식 비율: "}, "annotations": {"bold": True}},
                {"type": "text", "text": {"content": share_ratio}},
            ]))

        outlook = finance.get("outlook")
        if outlook:
            bullets = outlook if isinstance(outlook, list) else [s.strip() for s in str(outlook).split(".") if s.strip()]
            callout_children = [_bullet(b) for b in bullets if b]
            children.append(_callout("📈",
                [{"type": "text", "text": {"content": "실적 전망 & 리스크 포인트"}}],
                children=callout_children,
            ))

        table = _build_finance_table(finance.get("quarterly_trend"))
        if table:
            children.append(_h3("📊 최근 실적 추이 (8분기)"))
            children.append(table)
        else:
            # 분기 데이터 미수집 안내 — 사용자가 "표 안 보임" 으로 혼동하지 않게 명시
            children.append(_callout("⚠️", [
                {"type": "text", "text": {"content": "분기 실적 데이터 미수집"},
                 "annotations": {"bold": True}},
                {"type": "text", "text": {"content": " — DART에 데이터가 없거나 신규 상장 종목입니다. "
                                                     "fetch_finance.py 재실행 또는 FnGuide 직접 확인 권장."}},
            ], color="gray_background"))

    # [6] divider
    children.append(_divider())

    return children


# ── 페이지 재구성 (네트워크 호출 포함) ─────────────────────────────────────
def reconstruct_page(
    page_id,
    token=None,
    articles=None,
    homepage=None,
    one_line_summary=None,
    business_summary=None,
    share_ratio=None,
    finance=None,
):
    """
    페이지 본문을 재구성. 호출 흐름:
      1. 기존 블록 조회 + 기존 기사 URL 추출 (Archive 보존용)
      2. 기존 블록 전부 삭제
      3. 새 본문 children append (chunk_size=100)
      4. Archive 토글 별도 append → toggle 내부에 옛 기사 + 새 기사 dedup 으로 채움

    반환: {"ok": bool, "deleted": N, "appended": N, "archived": N, "error": str|None}
    """
    token = token or os.environ.get("NOTION_TOKEN") or ""
    if not token:
        return {"ok": False, "deleted": 0, "appended": 0, "archived": 0,
                "error": "NOTION_TOKEN 없음"}

    articles = articles or []
    new_urls = {(a.get("url") or "").strip() for a in articles if a.get("url")}
    new_urls.discard("")

    # 1) 기존 블록 조회 + 기사 추출
    try:
        old_blocks = _get_all_blocks(page_id, token)
        old_news = _extract_news_links_from_blocks(old_blocks, token)
    except Exception as e:
        return {"ok": False, "deleted": 0, "appended": 0, "archived": 0,
                "error": f"기존 블록 조회 실패: {e}"}

    archived_old = [(t, u) for (t, u) in old_news if u not in new_urls]

    # 2) 기존 블록 삭제
    if old_blocks:
        _delete_blocks([b["id"] for b in old_blocks], token)
    deleted_n = len(old_blocks)

    # 3) 새 본문 append
    new_children = build_children_payload(
        articles=articles,
        homepage=homepage,
        one_line_summary=one_line_summary,
        business_summary=business_summary,
        share_ratio=share_ratio,
        finance=finance,
    )

    append_url = f"{NOTION_API}/blocks/{page_id}/children"
    appended_n = 0
    try:
        for i in range(0, len(new_children), CHUNK_SIZE):
            chunk = new_children[i:i + CHUNK_SIZE]
            _req("PATCH", append_url, token, {"children": chunk})
            appended_n += len(chunk)
    except Exception as e:
        return {"ok": False, "deleted": deleted_n, "appended": appended_n,
                "archived": 0, "error": f"본문 append 실패: {e}"}

    # 4) Archive 토글 (별도 append → toggle id 받고 자식 채움)
    archived_n = 0
    try:
        toggle_payload = {"children": [{
            "object": "block", "type": "toggle",
            "toggle": {"rich_text": [{"type": "text",
                                       "text": {"content": "🗄️ 지난 기사 (Archive)"}}]},
        }]}
        toggle_res = _req("PATCH", append_url, token, toggle_payload)
        toggle_id = (toggle_res.get("results") or [{}])[0].get("id")

        # 옛 기사 + 새 기사 dedup (URL 기준)
        all_items = archived_old + [
            ((a.get("title") or "").strip(), (a.get("url") or "").strip())
            for a in articles if a.get("url")
        ]
        seen = set()
        archive_blocks = []
        for title, url in all_items:
            if not url or url in seen:
                continue
            seen.add(url)
            if homepage and url in homepage:
                continue
            archive_blocks.append({
                "object": "block", "type": "bulleted_list_item",
                "bulleted_list_item": {"rich_text": [
                    {"type": "text", "text": {"content": title or url, "link": {"url": url}}}
                ]},
            })

        if toggle_id and archive_blocks:
            nested_url = f"{NOTION_API}/blocks/{toggle_id}/children"
            for i in range(0, len(archive_blocks), CHUNK_SIZE):
                chunk = archive_blocks[i:i + CHUNK_SIZE]
                _req("PATCH", nested_url, token, {"children": chunk})
                archived_n += len(chunk)
    except Exception as e:
        return {"ok": True, "deleted": deleted_n, "appended": appended_n,
                "archived": archived_n, "error": f"Archive 부분 실패: {e}"}

    return {"ok": True, "deleted": deleted_n, "appended": appended_n,
            "archived": archived_n, "error": None}


# ── manual smoke test ──────────────────────────────────────────────────────
if __name__ == "__main__":
    # 가짜 데이터로 payload 만 출력 (네트워크 호출 X)
    sample = build_children_payload(
        articles=[
            {"title": "삼성전자, HBM4 양산 본격화", "url": "https://example.com/1", "date": "2026-05-23"},
            {"title": "엔비디아 공급 확대 기대",   "url": "https://example.com/2", "date": "2026-05-22"},
        ],
        homepage="https://www.samsung.com",
        business_summary="반도체 / 디스플레이 / 가전 등을 영위하는 종합 IT 기업.",
        share_ratio="45.2%",
        finance={
            "outlook": ["메모리 업사이클 진입", "AI 수요 견조"],
            "quarterly_trend": "[2024.09] Sales 79 | OP 9.2 | Net 7.4\n[2024.12] Sales 75 | OP 6.5 | Net 5.0",
        },
    )
    print(json.dumps(sample, ensure_ascii=False, indent=2))
