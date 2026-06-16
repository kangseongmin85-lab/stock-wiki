#!/usr/bin/env python3
"""
sync_watchlist_github.py — 관심종목 감시목록 → GitHub Secret 동기화
====================================================================
공개 저장소에 스크리너 CSV(메모·연구내용 포함)를 올리지 않기 위해,
종목명·종목코드·등록일만 추린 감시목록을 GitHub Secret(ALERT_WATCHLIST)으로
전달한다. 클라우드 가격알림(price_alert.yml)이 이 Secret을 읽는다.

리포트_실행.bat 마지막 단계에서 자동 호출됨 (저녁 CSV 저장 → 자동 동기화).
인증: gh CLI 로그인 재사용.

사용: python sync_watchlist_github.py          # 최근 14일 CSV → Secret
      python sync_watchlist_github.py --days 30
"""

import sys
import argparse
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import daily_picks_tracker as dpt

BASE_DIR  = Path(__file__).parent
PICKS_DIR = BASE_DIR / "오늘의 관심종목"
REPO      = "kangseongmin85-lab/stock-wiki"
MAX_WATCH = 120


def build_watchlist(days: int) -> str:
    """최근 N일 CSV 누적 → '종목명,종목코드,등록일' 컴팩트 CSV (메모 제외)."""
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    watch = {}
    for p in sorted(PICKS_DIR.glob("*.csv")):
        d = dpt.parse_csv_date(p.name)
        if not d or d < cutoff:
            continue
        try:
            for row in dpt.parse_picks_csv(p):
                watch[row["code"].zfill(6)] = {"name": row["name"], "last_seen": d}
        except Exception as e:
            print(f"[경고] CSV 파싱 실패 {p.name}: {e}")
    items = sorted(watch.items(), key=lambda kv: kv[1]["last_seen"], reverse=True)[:MAX_WATCH]
    lines = ["종목명,종목코드,등록일"]
    for code, v in items:
        lines.append(f"{v['name']},{code},{v['last_seen']}")
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=14)
    args = ap.parse_args()

    text = build_watchlist(args.days)
    n = len(text.splitlines()) - 1
    if n <= 0:
        print("[안내] 감시목록 없음 — Secret 갱신 생략")
        return

    r = subprocess.run(
        ["gh", "secret", "set", "ALERT_WATCHLIST", "--repo", REPO],
        input=text, text=True, encoding="utf-8",
        capture_output=True, timeout=30,
    )
    if r.returncode != 0:
        print(f"[오류] Secret 설정 실패: {r.stderr.strip()}")
        sys.exit(1)
    print(f"[완료] ALERT_WATCHLIST 갱신 — {n}종목 (최근 {args.days}일)")


if __name__ == "__main__":
    main()
