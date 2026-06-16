#!/usr/bin/env python3
"""
notion_pusher.py — 종목재료정리 DB (2cbffbf4617380e38d07e8b5e59e36c4) upsert

signal_report.py 의 로컬 helper 서버에서 호출.
종목명으로 query → 있으면 update, 없으면 create.
컬럼: 종목명(title) / 등락률 (%)(rich_text) / 오늘 기사 내용(rich_text) /
      링크(rich_text, 하이퍼링크) / 최근업데이트(date)

drive_to_notion.py 의 update_notion_smart 패턴과 동일 키 사용.
"""

import os
import json
import urllib.request
import urllib.error
from datetime import datetime, date

NOTION_VERSION = "2022-06-28"
JAEROO_DB_ID = "2cbffbf4617380e38d07e8b5e59e36c4"  # 종목재료정리

# ── config 로드 (signal_report.py 와 동일 패턴) ─────────────────────────────
_BASE = os.path.dirname(os.path.abspath(__file__))


def _load_config():
    p = os.path.join(_BASE, "config.json")
    if os.path.exists(p):
        try:
            return json.load(open(p, encoding="utf-8"))
        except Exception:
            pass
    return {}


_CFG = _load_config()
NOTION_TOKEN = os.environ.get("NOTION_TOKEN") or _CFG.get("NOTION_TOKEN", "")

# 신규 종목 자동 wiki 생성 스크립트 (subprocess 호출)
# ingest_all 가 fetch_finance 까지 자동으로 부른다.
_INGEST_SCRIPT = os.path.join(_BASE, "ingest_all (노션 전체내용 옵시디언 업데이트).py")
_INGEST_TIMEOUT_SEC = 180  # 신규 종목당 wiki 생성 + DART 재무 수집 한도


def _auto_generate_wiki(stock_name: str) -> dict:
    """신규 종목 → ingest_all 동기 호출로 wiki + 재무 생성.
    반환: {ok: bool, error: str|None, stdout_tail: str}
    """
    import subprocess, sys as _sys
    if not os.path.exists(_INGEST_SCRIPT):
        return {"ok": False, "error": "ingest_all 스크립트 없음", "stdout_tail": ""}
    try:
        result = subprocess.run(
            [_sys.executable, _INGEST_SCRIPT, stock_name],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
            cwd=_BASE,
            timeout=_INGEST_TIMEOUT_SEC,
        )
        ok = (result.returncode == 0)
        return {
            "ok": ok,
            "error": None if ok else f"returncode={result.returncode}",
            "stdout_tail": (result.stdout or "")[-400:],
        }
    except subprocess.TimeoutExpired:
        return {"ok": False, "error": f"timeout ({_INGEST_TIMEOUT_SEC}s)", "stdout_tail": ""}
    except Exception as e:
        return {"ok": False, "error": str(e), "stdout_tail": ""}


# ── HTTP helper ────────────────────────────────────────────────────────────
def _req(method, url, body=None, timeout=10):
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Authorization":  f"Bearer {NOTION_TOKEN}",
            "Notion-Version": NOTION_VERSION,
            "Content-Type":   "application/json",
        },
        method=method,
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


# ── DB 조회 ────────────────────────────────────────────────────────────────
def _find_page_by_name(stock_name):
    """종목명(title equals) 으로 페이지 1건 찾기. 없으면 None."""
    body = {
        "filter": {"property": "종목명", "title": {"equals": stock_name}},
        "page_size": 1,
    }
    res = _req("POST", f"https://api.notion.com/v1/databases/{JAEROO_DB_ID}/query", body)
    items = res.get("results", [])
    return items[0]["id"] if items else None


# ── properties 빌더 ─────────────────────────────────────────────────────────
def _build_properties(stock_name, ctrt, article, today_iso, recent_articles=None):
    """노션 properties payload 생성.
    stock_name      : str
    ctrt            : float (%) 또는 None
    article         : dict {title, url, summary|desc, date} 또는 None (대표 1건)
    today_iso       : 'YYYY-MM-DD'
    recent_articles : list[dict] (선택) — `링크` 컬럼에 5건까지 줄바꿈으로 나열
    """
    props = {
        "종목명": {"title": [{"text": {"content": stock_name}}]},
        "최근업데이트": {"date": {"start": today_iso}},
    }

    if ctrt is not None:
        sign = "+" if ctrt >= 0 else ""
        props["등락률 (%)"] = {
            "rich_text": [{"type": "text", "text": {"content": f"{sign}{ctrt:.2f}%"}}]
        }

    if article:
        title = (article.get("title") or "").strip()
        url   = (article.get("url")   or "").strip()
        # signal_report.py 의 review payload 는 `desc` 키로 보냄 — 둘 다 폴백.
        summ  = (article.get("summary") or article.get("desc") or "").strip()
        date_ = (article.get("date") or "").strip()

        # 오늘 기사 내용 = [날짜] 제목(하이퍼링크) + 본문 요약(plain)
        if date_ and title:
            head = f"[{date_}] {title}"
        else:
            head = title or date_ or ""
        head = head[:200]  # 제목 청크 한도

        chunks = []
        if head:
            head_text = {"content": head}
            if url:
                head_text["link"] = {"url": url}
            chunks.append({"type": "text", "text": head_text})
        if summ:
            # Notion rich_text 청크 한도 2000자, head 와 합쳐 1900자 이내로 자름
            remaining = max(0, 1900 - len(head) - 2)  # 2 = \n\n
            body_txt = ("\n\n" + summ)[:remaining + 2]
            if body_txt.strip():
                chunks.append({"type": "text", "text": {"content": body_txt}})

        if chunks:
            props["오늘 기사 내용"] = {"rich_text": chunks}

        # 링크 컬럼: 최근 N건 (대표 + recent_articles) URL dedup 후 줄바꿈으로 나열.
        # 각 줄: "[YYYY.MM.DD] 제목" 형태, 제목에 hyperlink. 최대 5건.
        link_items = []
        seen_urls = set()
        for a in [article] + list(recent_articles or []):
            if not isinstance(a, dict):
                continue
            u = (a.get("url") or "").strip()
            t = (a.get("title") or "").strip()
            d = (a.get("date") or "").strip()
            if not u or not t or u in seen_urls:
                continue
            seen_urls.add(u)
            link_items.append((d, t, u))
            if len(link_items) >= 5:
                break

        if link_items:
            link_rich = []
            for i, (d, t, u) in enumerate(link_items):
                if i > 0:
                    link_rich.append({"type": "text", "text": {"content": "\n"}})
                if d:
                    link_rich.append({
                        "type": "text",
                        "text": {"content": f"[{d}] ", "link": None},
                        "annotations": {"color": "gray"},
                    })
                link_rich.append({
                    "type": "text",
                    "text": {"content": t[:120], "link": {"url": u}},
                })
            props["링크"] = {"rich_text": link_rich}

    return props


# ── public: upsert 한 건 ─────────────────────────────────────────────────────
def upsert_stock(stock_name, ctrt=None, article=None, today_iso=None, recent_articles=None):
    """종목재료정리 DB에 종목 한 건 upsert.
    반환: {"name": ..., "status": "created"|"updated"|"error", "error": str|None, "page_id": str|None}
    recent_articles 가 있으면 `링크` 컬럼에 5건 줄바꿈으로 나열.
    """
    if not NOTION_TOKEN:
        return {"name": stock_name, "status": "error", "error": "NOTION_TOKEN 없음", "page_id": None}

    today_iso = today_iso or date.today().isoformat()
    props = _build_properties(stock_name, ctrt, article, today_iso, recent_articles=recent_articles)

    try:
        existing = _find_page_by_name(stock_name)
    except Exception as e:
        return {"name": stock_name, "status": "error", "error": f"query 실패: {e}", "page_id": None}

    try:
        if existing:
            _req("PATCH", f"https://api.notion.com/v1/pages/{existing}", {"properties": props})
            return {"name": stock_name, "status": "updated", "error": None, "page_id": existing}
        else:
            body = {"parent": {"database_id": JAEROO_DB_ID}, "properties": props}
            res = _req("POST", "https://api.notion.com/v1/pages", body)
            return {"name": stock_name, "status": "created", "error": None, "page_id": res.get("id")}
    except urllib.error.HTTPError as e:
        try:
            err_body = e.read().decode("utf-8", errors="replace")[:300]
        except Exception:
            err_body = str(e)
        return {"name": stock_name, "status": "error", "error": f"HTTP {e.code}: {err_body}", "page_id": None}
    except Exception as e:
        return {"name": stock_name, "status": "error", "error": str(e), "page_id": None}


# ── public: 배치 push ───────────────────────────────────────────────────────
def push_reviewed(reviewed_json):
    """review HTML 이 보낸 JSON 페이로드를 받아 승인된 종목만 푸시.
    reviewed_json: {stocks: [{name, ctrt, decision, chosen_article: {...}|None}, ...]}
    반환: {"total": N, "ok": N, "created": N, "updated": N, "errors": [{name, error}, ...]}
    """
    today_iso = date.today().isoformat()
    # JS export 는 "results" 키, 다른 호출자는 "stocks" 키 — 둘 다 허용
    stocks = reviewed_json.get("results") or reviewed_json.get("stocks") or []
    approved = [s for s in stocks if s.get("decision") == "approved"]

    summary = {"total": len(approved), "ok": 0, "created": 0, "updated": 0, "errors": []}

    for s in approved:
        name = (s.get("name") or "").strip()
        if not name:
            continue
        ctrt_raw = s.get("ctrt")
        try:
            ctrt = float(ctrt_raw) if ctrt_raw is not None and ctrt_raw != "" else None
        except (TypeError, ValueError):
            ctrt = None

        article = s.get("chosen_article")
        # review HTML 이 보내준 상위 N개 후보. 없으면 빈 리스트.
        recent_articles = s.get("recent_articles") or []
        res = upsert_stock(name, ctrt=ctrt, article=article,
                           today_iso=today_iso, recent_articles=recent_articles)
        if res["status"] in ("created", "updated"):
            summary["ok"] += 1
            summary[res["status"]] += 1

            # 진짜 신규 종목 → 외부 메타 데이터 수집 (DART + FnGuide)
            # 노션/위키 둘 다 비어 있으므로 외부에서 홈페이지/사업구조/유통비율 끌어옴.
            external_meta = {}
            if res["status"] == "created":
                try:
                    from stock_meta_collector import collect_meta
                    print(f"   [신규] {name} — 외부 메타 데이터 수집 (DART+FnGuide)...")
                    external_meta = collect_meta(name) or {}
                    if external_meta:
                        print(f"   [신규] {name} — 메타 수집: {list(external_meta.keys())}")
                except Exception as e:
                    summary.setdefault("meta_errors", []).append({"name": name, "error": str(e)})

            # wiki 동기화 — 신규+갱신 모두. ingest_all 의 스마트 동기화 로직이
            # 변경 없는 종목은 자동 SKIP, 변경 있으면 wiki/노션 본문 누적.
            print(f"   [{res['status']}] {name} — wiki 동기화 시작 (최대 {_INGEST_TIMEOUT_SEC}s)...")
            gen = _auto_generate_wiki(name)
            if gen["ok"]:
                print(f"   [{res['status']}] {name} — wiki 동기화 완료")
            else:
                print(f"   [{res['status']}] {name} — wiki 동기화 실패: {gen['error']}")
            summary.setdefault("wiki_synced", []).append({
                "name": name, "status": res["status"], "ok": gen["ok"], "error": gen.get("error"),
            })

            # 본문 재구성 (이브닝 양식). 실패해도 upsert 성공 카운트는 유지.
            page_id = res.get("page_id")
            if page_id and article:
                try:
                    from notion_body_builder import reconstruct_page
                    # wiki 마크다운에서 자동 추출한 값을 review payload 보다 우선.
                    # payload 가 명시적으로 제공한 값은 덮어쓰기 가능.
                    from wiki_data_adapter import extract_stock_data
                    wiki_data = extract_stock_data(name)

                    # 재무 폴백 체인: payload > wiki > external_meta > FnGuide (모든 종목 대상)
                    # wiki/external_meta 둘 다 finance 없으면 FnGuide 분기 표 폴백 시도
                    finance = s.get("finance") or wiki_data.get("finance") or external_meta.get("finance")
                    if not finance:
                        try:
                            from stock_meta_collector import fnguide_finance
                            # stock_code 얻기: payload > wiki frontmatter > DART 검색
                            sc = s.get("code") or wiki_data.get("stock_code") or external_meta.get("stock_code")
                            if not sc:
                                try:
                                    from stock_meta_collector import _dart_resolve_corp_code  # noqa
                                    import dart_fss as dart
                                    import os as _os
                                    key = _os.environ.get("DART_API_KEY", "")
                                    if key:
                                        dart.set_api_key(key)
                                        corps = dart.get_corp_list()
                                        res = corps.find_by_corp_name(name, exactly=True) \
                                              or corps.find_by_corp_name(name, exactly=False)
                                        corp = res[0] if isinstance(res, list) and res else res
                                        if corp:
                                            scc = getattr(corp, "stock_code", "") or ""
                                            sc = scc.strip() or None
                                except Exception:
                                    pass
                            if sc:
                                fn_fin = fnguide_finance(sc)
                                if fn_fin.get("quarterly_trend"):
                                    finance = fn_fin
                                    print(f"   [{res['status']}] {name} — FnGuide 분기 실적 폴백 적용")
                        except Exception as e:
                            summary.setdefault("finance_fallback_errors", []).append({"name": name, "error": str(e)})

                    kwargs = dict(
                        homepage         = s.get("homepage")         or wiki_data.get("homepage")         or external_meta.get("homepage"),
                        one_line_summary = s.get("one_line_summary") or wiki_data.get("one_line_summary"),
                        business_summary = s.get("business_summary") or wiki_data.get("business_summary") or external_meta.get("business_summary"),
                        share_ratio      = s.get("share_ratio")      or wiki_data.get("share_ratio")      or external_meta.get("share_ratio"),
                        finance          = finance,
                    )

                    # 본문 기사: chosen_article + recent_articles (review HTML 이 보내준 상위 5건)
                    # 모두 합쳐 dedup 후 본문 `## 3. 추가 중요 내용` 에 표시.
                    body_articles = []
                    seen_urls = set()
                    for a in [article] + list(s.get("recent_articles") or []):
                        if not isinstance(a, dict):
                            continue
                        u = (a.get("url") or "").strip()
                        if not u or u in seen_urls:
                            continue
                        seen_urls.add(u)
                        body_articles.append(a)
                        if len(body_articles) >= 5:
                            break
                    if not body_articles:
                        body_articles = [article]

                    reconstruct_page(
                        page_id,
                        token=NOTION_TOKEN,
                        articles=body_articles,
                        **kwargs,
                    )
                except Exception as e:
                    summary.setdefault("body_errors", []).append({"name": name, "error": str(e)})
        else:
            summary["errors"].append({"name": name, "error": res["error"]})

    return summary


if __name__ == "__main__":
    # 수동 테스트: python notion_pusher.py <reviewed_json_path>
    import sys
    if len(sys.argv) < 2:
        print(f"NOTION_TOKEN set: {bool(NOTION_TOKEN)}")
        print(f"DB ID: {JAEROO_DB_ID}")
        print("사용법: python notion_pusher.py <reviewed_YYYYMMDD.json>")
        sys.exit(0)
    payload = json.load(open(sys.argv[1], encoding="utf-8"))
    result = push_reviewed(payload)
    print(json.dumps(result, ensure_ascii=False, indent=2))
