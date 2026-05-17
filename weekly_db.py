#!/usr/bin/env python3
"""
weekly_db.py — 주간 Notion DB 로테이션 매니저

- ISO 주차(월~일) 기준으로 매주 새 DB 자동 생성
- 부모 페이지 ID 는 현재 NOTION_DB_ID 의 parent 에서 자동 조회 (추가 env 불필요)
- DB 제목 패턴: '📡 시황 아카이브 YYYY-Www (MM/DD~MM/DD)'

외부 API (fetch_news.py / analyze_after_hours.py 에서 import):
  resolve_active_db_id(token, current_db_id) -> str
      → 현재 주차 DB ID. 없으면 생성. 부모 페이지는 current_db_id 에서 자동 조회.
  get_db_ids_for_window(token, parent_page_id, start_dt, end_dt) -> list[str]
      → 윈도우에 걸린 모든 주차 DB ID (조회 전용, 생성 안 함)
  get_parent_page_id(token, db_id) -> str | None
      → DB ID 로부터 parent page_id 조회
"""

import json
import logging
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta

log = logging.getLogger(__name__)

KST = timezone(timedelta(hours=9))
_NOTION_VERSION = "2022-06-28"

# Notion DB 스키마 — fetch_news.py / 기존 setup_notion_db.py 와 동일
DB_SCHEMA = {
    "제목":     {"title": {}},
    "채널":     {"select": {"options": []}},          # 동적 옵션 (제한 없이 누적)
    "카테고리": {
        "select": {
            "options": [
                {"name": "한국증시",    "color": "green"},
                {"name": "미국증시",    "color": "blue"},
                {"name": "바이오/제약", "color": "pink"},
                {"name": "리포트/뉴스", "color": "purple"},
                {"name": "투자정보",    "color": "orange"},
                {"name": "공시",        "color": "red"},
                {"name": "RSS",         "color": "default"},
                {"name": "네이버",      "color": "green"},
                {"name": "시황분석",    "color": "yellow"},
                {"name": "기타",        "color": "gray"},
            ]
        }
    },
    "테마태그": {"multi_select": {"options": []}},     # 동적 옵션
    "중요도": {
        "select": {
            "options": [
                {"name": "높음", "color": "red"},
                {"name": "보통", "color": "yellow"},
                {"name": "낮음", "color": "default"},
            ]
        }
    },
    "날짜":     {"date": {}},
    "원문링크": {"url": {}},
    "원문내용": {"rich_text": {}},
}


# ── 공통 HTTP ────────────────────────────────────────────────────────────────
def _request(method, url, token, body=None, timeout=15):
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Authorization":  f"Bearer {token}",
            "Notion-Version": _NOTION_VERSION,
            "Content-Type":   "application/json",
        },
        method=method,
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


# ── 주차 라벨 & 윈도우 ───────────────────────────────────────────────────────
def week_label_for(dt=None):
    """KST 기준 ISO 주차 라벨, 주 시작/끝 date 반환.
    예: ('2026-W20', date(2026,5,11), date(2026,5,17))
    """
    if dt is None:
        dt = datetime.now(KST)
    elif dt.tzinfo is None:
        dt = dt.replace(tzinfo=KST)
    else:
        dt = dt.astimezone(KST)
    iso_year, iso_week, _ = dt.isocalendar()
    monday = datetime.fromisocalendar(iso_year, iso_week, 1).date()
    sunday = monday + timedelta(days=6)
    return f"{iso_year}-W{iso_week:02d}", monday, sunday


def db_title_for(dt=None):
    label, mon, sun = week_label_for(dt)
    return f"📡 시황 아카이브 {label} ({mon.month:02d}/{mon.day:02d}~{sun.month:02d}/{sun.day:02d})"


# ── DB 조회 / 생성 ──────────────────────────────────────────────────────────
def get_parent_page_id(token, db_id):
    """DB ID 로부터 parent page_id 조회. 부모가 page 가 아니면 None."""
    if not token or not db_id:
        return None
    try:
        data = _request("GET", f"https://api.notion.com/v1/databases/{db_id}", token)
    except Exception as e:
        log.warning(f"[weekly_db] DB 조회 실패({db_id}): {e}")
        return None
    parent = data.get("parent", {})
    if parent.get("type") == "page_id":
        return parent.get("page_id")
    log.warning(f"[weekly_db] DB 부모가 page 가 아님: {parent.get('type')}")
    return None


def _find_db_by_title(token, parent_page_id, target_title):
    """Notion search 로 제목 매칭 DB 검색. 부모 일치 검증 후 ID 반환."""
    if not token:
        return None
    try:
        data = _request("POST", "https://api.notion.com/v1/search", token, body={
            "query":     target_title,
            "filter":    {"property": "object", "value": "database"},
            "page_size": 50,
        })
    except Exception as e:
        log.warning(f"[weekly_db] search 실패: {e}")
        return None
    target_parent = (parent_page_id or "").replace("-", "")
    for r in data.get("results", []):
        title = "".join(t.get("plain_text", "") for t in r.get("title", []))
        if title != target_title:
            continue
        parent = r.get("parent", {})
        if parent.get("type") == "page_id" and parent.get("page_id", "").replace("-", "") == target_parent:
            return r["id"]
    return None


def _create_db(token, parent_page_id, target_title):
    log.info(f"[weekly_db] 신규 주차 DB 생성: {target_title}")
    data = _request("POST", "https://api.notion.com/v1/databases", token, body={
        "parent":     {"type": "page_id", "page_id": parent_page_id},
        "title":      [{"type": "text", "text": {"content": target_title}}],
        "properties": DB_SCHEMA,
    })
    log.info(f"[weekly_db] 생성 완료: id={data['id']}")
    return data["id"]


# ── 외부 API ────────────────────────────────────────────────────────────────
def resolve_active_db_id(token, current_db_id, dt=None):
    """현재 주차 DB ID 를 반환.
    - parent page id 는 current_db_id 에서 자동 조회 (env 추가 불필요)
    - 현재 주차 DB 가 없으면 생성
    - 실패 시 current_db_id 그대로 반환 (안전 폴백)
    """
    if not (token and current_db_id):
        return current_db_id

    parent_id = get_parent_page_id(token, current_db_id)
    if not parent_id:
        log.warning(f"[weekly_db] 부모 페이지 조회 실패 — current_db_id 폴백 사용")
        return current_db_id

    target_title = db_title_for(dt)
    existing = _find_db_by_title(token, parent_id, target_title)
    if existing:
        return existing
    try:
        return _create_db(token, parent_id, target_title)
    except Exception as e:
        log.error(f"[weekly_db] 신규 DB 생성 실패 — current_db_id 폴백 사용: {e}")
        return current_db_id


def get_db_ids_for_window(token, parent_page_id, start_dt, end_dt):
    """윈도우 [start_dt, end_dt] 를 포함하는 모든 주차 DB ID 목록 반환.
    조회 전용 — 없는 주차는 건너뜀.
    """
    if start_dt.tzinfo is None:
        start_dt = start_dt.replace(tzinfo=KST)
    if end_dt.tzinfo is None:
        end_dt = end_dt.replace(tzinfo=KST)

    db_ids = []
    seen_labels = set()
    cur = start_dt
    while cur <= end_dt:
        label, _, _ = week_label_for(cur)
        if label not in seen_labels:
            seen_labels.add(label)
            title = db_title_for(cur)
            db_id = _find_db_by_title(token, parent_page_id, title)
            if db_id:
                db_ids.append(db_id)
            else:
                log.info(f"[weekly_db] 윈도우 내 DB 없음(스킵): {title}")
        cur += timedelta(days=1)
    return db_ids


# ── CLI: 수동 점검 / 트리거 ─────────────────────────────────────────────────
if __name__ == "__main__":
    import os, sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    token = os.getenv("NOTION_TOKEN", "")
    cur   = os.getenv("NOTION_DB_ID", "")
    if not (token and cur):
        print("NOTION_TOKEN, NOTION_DB_ID 환경변수 설정 필요")
        sys.exit(1)
    active = resolve_active_db_id(token, cur)
    print(f"현재 주차 DB ID: {active}")
    print(f"제목: {db_title_for()}")
