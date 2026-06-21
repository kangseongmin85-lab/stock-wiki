#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
schedule_scan.py — 주식 매매에 영향 주는 "일정" 후보 수집기 (읽기 전용)

리포트_실행.bat [일정] 단계에서 호출. taxonomy_backfill --list 와 동일한
"후보 덤프 → Claude 적용" 패턴. 실제 노션/위키 입력은 하지 않는다.

수집 소스 (둘 다 = 사용자 선택):
  1) 노션 🗓️ 주식 일정 DB 기존 항목      → 중복제거(dedup) 기준
  2) 오늘의 시황/시황_*.docx 헤드라인     → 뉴스에서 날짜 있는 미래 이벤트
  3) 네이버 뉴스 API (일정 키워드)        → 외부 캘린더성 헤드라인 보충

산출:
  reports/schedule_todo.json
    {
      generated, today,
      tag_vocab: [...],                  # Claude가 고를 태그 어휘 (4범위)
      existing_events: [{name,date,tags,page_id}],
      candidates: [{headline, snippet, date_hints:[...], source}]
    }

Claude 루틴(요청: "일정 업데이트해줘"):
  1. 이 파일 읽기
  2. candidates 중 '날짜 확정된 매매영향 이벤트'만 선별 → 태그 부여 → existing 과 중복제거
  3. picks.json 작성 → python schedule_push.py --apply picks.json (먼저 --dry-run)
"""

import os
import re
import sys
import json
import glob
import argparse
import urllib.parse
import urllib.request
from datetime import datetime, date

# 콘솔(cp949)에서 한글 print 안전
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

BASE = os.path.dirname(os.path.abspath(__file__))
SCHEDULE_DB_ID = "20dffbf4617380a99df6c90c5de6fdc9"  # 🗓️ 주식 일정
NOTION_VERSION = "2022-06-28"

# Claude가 고를 태그 어휘 — 사용자 선택 4범위
TAG_VOCAB = ["실적발표", "IPO/상장", "락업해제", "거시/정책", "지정학", "종목공시"]

# 날짜 있는 미래 이벤트일 가능성 신호 (헤드라인 필터; recall 우선, 판단은 Claude)
SIGNAL_WORDS = [
    "예정", "예정일", "상장", "재상장", "변경상장", "공모", "청약", "수요예측",
    "락업", "보호예수", "해제", "만기", "실적발표", "잠정실적", "발표", "콘퍼런스콜",
    "컨퍼런스콜", "IR", "FOMC", "금리", "기준금리", "CPI", "PCE", "고용지표",
    "회의", "개최", "시행", "착수", "양산", "출시", "승인", "심사", "마일스톤",
    "학회", "데이터", "임상", "정상회담", "회담", "OPEC", "선거", "총회", "주총",
    "배당", "분기", "마감", "디데이", "D-",
]

# 날짜 힌트 추출 정규식
_DATE_PATS = [
    (re.compile(r"(20\d{2})[.\-/](\d{1,2})[.\-/](\d{1,2})"), "ymd"),
    (re.compile(r"(\d{1,2})\s*월\s*(\d{1,2})\s*일"), "md"),
    (re.compile(r"\b(\d{1,2})/(\d{1,2})\b"), "slash_md"),
    (re.compile(r"D[-−](\d{1,3})"), "dminus"),
]


def _load_config():
    p = os.path.join(BASE, "config.json")
    cfg = {}
    if os.path.exists(p):
        try:
            cfg = json.load(open(p, encoding="utf-8"))
        except Exception:
            pass
    return cfg


_CFG = _load_config()
NOTION_TOKEN = os.environ.get("NOTION_TOKEN") or _CFG.get("NOTION_TOKEN", "")
NAVER_ID = os.environ.get("NAVER_CLIENT_ID") or _CFG.get("NAVER_CLIENT_ID", "")
NAVER_SECRET = os.environ.get("NAVER_CLIENT_SECRET") or _CFG.get("NAVER_CLIENT_SECRET", "")


def _today():
    return date.today().isoformat()


def extract_date_hints(text, today_iso):
    """텍스트에서 날짜 후보를 ISO 문자열 리스트로. 연도 없으면 올해 가정(이미 지났으면 내년)."""
    hints = []
    ty, tm, td = (int(x) for x in today_iso.split("-"))
    for pat, kind in _DATE_PATS:
        for m in pat.finditer(text):
            try:
                if kind == "ymd":
                    y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
                elif kind in ("md", "slash_md"):
                    mo, d = int(m.group(1)), int(m.group(2))
                    if not (1 <= mo <= 12 and 1 <= d <= 31):
                        continue
                    y = ty
                    # 이미 한참 지난 달이면 내년 이벤트로 가정
                    if (mo, d) < (tm, td) and (tm - mo) > 6:
                        y = ty + 1
                elif kind == "dminus":
                    # D-N 은 절대일자 계산 불가 → 상대만 표기
                    hints.append("D-" + m.group(1))
                    continue
                iso = f"{y:04d}-{mo:02d}-{d:02d}"
                datetime.strptime(iso, "%Y-%m-%d")  # 유효성
                if iso not in hints:
                    hints.append(iso)
            except Exception:
                continue
    return hints


# ── 1) 노션 기존 일정 (dedup 기준) ──────────────────────────────────────────
def fetch_existing_events():
    if not NOTION_TOKEN:
        return [], "NOTION_TOKEN 없음"
    out = []
    cursor = None
    try:
        while True:
            body = {"page_size": 100}
            if cursor:
                body["start_cursor"] = cursor
            req = urllib.request.Request(
                f"https://api.notion.com/v1/databases/{SCHEDULE_DB_ID}/query",
                data=json.dumps(body).encode("utf-8"),
                headers={
                    "Authorization": "Bearer " + NOTION_TOKEN,
                    "Notion-Version": NOTION_VERSION,
                    "Content-Type": "application/json",
                },
                method="POST",
            )
            r = json.load(urllib.request.urlopen(req, timeout=20))
            for p in r.get("results", []):
                pr = p.get("properties", {})
                name = "".join(t.get("plain_text", "") for t in pr.get("이름", {}).get("title", []))
                dd = pr.get("날짜", {}).get("date") or {}
                tags = [o["name"] for o in pr.get("태그", {}).get("multi_select", [])]
                out.append({
                    "name": name.strip(),
                    "date": dd.get("start", ""),
                    "end": dd.get("end"),
                    "tags": tags,
                    "page_id": p.get("id", ""),
                })
            if r.get("has_more"):
                cursor = r.get("next_cursor")
            else:
                break
        return out, None
    except Exception as e:
        return out, f"노션 query 실패: {e}"


# ── 2) 오늘의 시황 docx 헤드라인 ────────────────────────────────────────────
def headlines_from_docx(today_iso):
    cands = []
    docs = sorted(glob.glob(os.path.join(BASE, "오늘의 시황", "시황_*.docx")),
                  key=os.path.getmtime, reverse=True)
    if not docs:
        return cands, "docx 없음"
    latest = docs[0]
    try:
        import docx  # python-docx
    except Exception:
        return cands, "python-docx 미설치"
    try:
        d = docx.Document(latest)
    except Exception as e:
        return cands, f"docx 열기 실패: {e}"
    seen = set()
    for p in d.paragraphs:
        line = p.text.strip()
        if len(line) < 6:
            continue
        if not any(w in line for w in SIGNAL_WORDS):
            continue
        # 기사 헤드라인성 라인만 (불릿/번호 제거)
        clean = re.sub(r"^[\d\.••\-\s]+", "", line).strip()
        key = clean[:40]
        if key in seen:
            continue
        seen.add(key)
        hints = extract_date_hints(line, today_iso)
        cands.append({
            "headline": clean[:160],
            "snippet": "",
            "date_hints": hints,
            "source": "docx:" + os.path.basename(latest),
        })
    return cands, None


# ── 3) 네이버 뉴스 API (일정 키워드) ────────────────────────────────────────
_NAVER_QUERIES = [
    "실적발표 일정", "잠정실적 발표", "신규상장 일정", "공모주 청약",
    "보호예수 해제", "FOMC 일정", "기준금리 결정", "주주총회 일정",
]


def headlines_from_naver(today_iso, per_query=10):
    cands = []
    if not (NAVER_ID and NAVER_SECRET):
        return cands, "네이버 API 키 없음"
    err = None
    for q in _NAVER_QUERIES:
        try:
            url = ("https://openapi.naver.com/v1/search/news.json?query="
                   + urllib.parse.quote(q) + f"&display={per_query}&sort=date")
            req = urllib.request.Request(url, headers={
                "X-Naver-Client-Id": NAVER_ID,
                "X-Naver-Client-Secret": NAVER_SECRET,
            })
            r = json.load(urllib.request.urlopen(req, timeout=10))
            for it in r.get("items", []):
                title = re.sub(r"<[^>]+>", "", it.get("title", "")).replace("&quot;", '"').replace("&amp;", "&")
                desc = re.sub(r"<[^>]+>", "", it.get("description", "")).replace("&quot;", '"').replace("&amp;", "&")
                blob = title + " " + desc
                if not any(w in blob for w in SIGNAL_WORDS):
                    continue
                hints = extract_date_hints(blob, today_iso)
                cands.append({
                    "headline": title[:160],
                    "snippet": desc[:200],
                    "date_hints": hints,
                    "source": "naver:" + q,
                })
        except Exception as e:
            err = f"네이버 검색 일부 실패: {e}"
            continue
    # 제목 기준 dedup
    seen, uniq = set(), []
    for c in cands:
        k = c["headline"][:40]
        if k in seen:
            continue
        seen.add(k)
        uniq.append(c)
    return uniq, err


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join("reports", "schedule_todo.json"))
    ap.add_argument("--no-naver", action="store_true", help="네이버 API 호출 생략")
    args = ap.parse_args()

    today = _today()
    notes = []

    existing, e1 = fetch_existing_events()
    if e1:
        notes.append(e1)

    doc_c, e2 = headlines_from_docx(today)
    if e2:
        notes.append(e2)

    nav_c, e3 = ([], None) if args.no_naver else headlines_from_naver(today)
    if e3:
        notes.append(e3)

    candidates = doc_c + nav_c

    bundle = {
        "generated": datetime.now().isoformat(timespec="seconds"),
        "today": today,
        "tag_vocab": TAG_VOCAB,
        "existing_count": len(existing),
        "existing_events": existing,
        "candidates": candidates,
        "notes": notes,
    }

    out_path = os.path.join(BASE, args.out) if not os.path.isabs(args.out) else args.out
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(bundle, f, ensure_ascii=False, indent=2)

    print("[schedule_scan] today=%s | existing=%d | candidates=%d (docx %d + naver %d)"
          % (today, len(existing), len(candidates), len(doc_c), len(nav_c)))
    if notes:
        print("  notes: " + " / ".join(notes))
    print("  -> " + out_path)
    print("  다음: Claude에게 \"일정 업데이트해줘\" (후보 판단·태그·중복제거 후 노션+위키 입력)")


if __name__ == "__main__":
    main()
