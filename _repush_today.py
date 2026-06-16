"""오늘 푸쉬된 종목을 새 로직(본문 500자 + 하이퍼링크)으로 재푸쉬. 일회성."""
import json
import re
import sys
import time
import requests
from bs4 import BeautifulSoup

sys.path.insert(0, ".")
from notion_pusher import upsert_stock

NAV = ("로그인", "회원가입", "구독", "전체메뉴", "검색 기사검색",
       "정치 사회 경제", "공유 페이스북", "링크가 복사되었습니다",
       "PREMIUM", "글자크기")


def fetch_body(url, max_chars=500):
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=6, allow_redirects=True)
        r.encoding = r.apparent_encoding or "utf-8"
        soup = BeautifulSoup(r.text, "html.parser")
    except Exception:
        return ""

    for sel in ("#dic_area", "#articeBody", "#newsct_article", ".go_trans"):
        el = soup.select_one(sel)
        if el:
            t = re.sub(r"\s+", " ", el.get_text(" ", strip=True))
            if len(t) > 50:
                return t[:max_chars] + ("..." if len(t) > max_chars else "")

    meta = soup.find("meta", {"property": "og:description"}) or soup.find("meta", {"name": "description"})
    og = (meta.get("content", "") if meta else "").strip()
    if len(og) >= 80:
        return og[:max_chars] + ("..." if len(og) > max_chars else "")

    el = soup.select_one("article")
    if el:
        t = re.sub(r"\s+", " ", el.get_text(" ", strip=True))
        if len(t) > 50 and not any(kw in t[:120] for kw in NAV):
            return t[:max_chars] + ("..." if len(t) > max_chars else "")

    return og[:max_chars] if og else ""


def parse_cell(text):
    m = re.match(r"^\[(\d{4}\.\d{2}\.\d{2})\]\s*(.+?)(?:\.\.\.)?\s*$", text or "")
    if m:
        return m.group(1), m.group(2)
    return "", (text or "").strip()


def run():
    rows = json.load(open("_today_pages.json", encoding="utf-8"))
    today = "2026-05-19"
    ok = body_ok = body_empty = 0
    errors = []

    for i, r in enumerate(rows, 1):
        name = r["name"]
        url = r["url"]
        cs = (r.get("ctrt") or "").replace("%", "").replace("+", "").strip()
        try:
            ctrt = float(cs) if cs else None
        except ValueError:
            ctrt = None

        date_str, title = parse_cell(r.get("today_text_head", ""))
        body = fetch_body(url, 500) if url else ""
        time.sleep(0.2)

        if body:
            body_ok += 1
        else:
            body_empty += 1

        res = upsert_stock(name, ctrt=ctrt,
                           article={"title": title, "url": url, "summary": body, "date": date_str},
                           today_iso=today)
        mark = "OK" if res["status"] in ("created", "updated") else "ERR"
        print(f"[{i:>2}/{len(rows)}] {mark} | {name:<12} | {res['status']:<8} | body={len(body)}자")
        if res["status"] in ("created", "updated"):
            ok += 1
        else:
            errors.append({"name": name, "error": res.get("error")})

    print()
    print(json.dumps({"total": len(rows), "ok": ok, "body_ok": body_ok, "body_empty": body_empty, "errors": errors}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    run()
