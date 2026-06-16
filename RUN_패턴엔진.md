# 패턴 엔진 실행 가이드 (Phase 1~2)

> 스펙: `pattern_engine_spec.md` | 모듈: `fetch_outcomes.py`, `situation_index.py`, `pattern_match.py`

---

## 1. 매일 (공부일) — 자동

영웅문 조건검색 결과를 `오늘의 관심종목/` 폴더에 CSV로 저장 → **`리포트_실행.bat` 더블클릭.**

실행 순서 (bat에 이미 연결됨):
1. `signal_report.py` → daily_signal 생성 + 노션 검수 푸시
2. `fetch_outcomes.py` → 사후 성과 라벨(T+1~10) + 매크로(지수·수급) 갱신
3. `inspector.py` → 품질검사 (파일 있을 때만)

그날 유사국면을 보고 싶으면 이어서:
```
python situation_index.py
python pattern_match.py
```

---

## 2. 최초 1회 — PC 전체 백필

```
cd "C:\Users\vamos\Desktop\주식\코딩\주식_안드레카파시 wiki 만들기"
pip install pykrx finance-datareader python-dotenv      (최초만)

python fetch_outcomes.py        # 전 종목 성과 라벨 + 매크로(수급 포함)
python situation_index.py       # 일자별 국면 벡터
python pattern_match.py         # 최신일 기준 유사 과거국면 Top3
```

> `fetch_outcomes.py` 는 재실행 안전 — 완료된 라벨은 건너뛰고, 시간이 지나며 채워질
> T+N 만 보충한다. 매일 돌려도 부담 없음.

---

## 3. 수급 컬럼 확인 (PC 최초 1회 — 중요)

개발 샌드박스에선 KRX 수급 엔드포인트가 막혀 `supply: null` 로 저장됐다.
네 PC(열린 네트워크)에선 채워지는데, KRX 컬럼명이 버전마다 달라 한 번만 확인하면 된다:

```
python -c "from pykrx import stock; df=stock.get_market_trading_value_by_date('20260529','20260529','KOSPI'); print(df.columns.tolist()); print(df)"
```

- 출력 컬럼에 외국인/기관/개인 관련 이름이 보이면, `fetch_outcomes.py` 의 `fetch_supply()`
  안 `g("외국인합계","외국인")` 후보에 그 정확한 이름을 넣으면 끝.
- 막혀도 지수만으로 국면 판단엔 충분 (수급은 보조 신호).

---

## 산출물 (_cache/)

| 파일 | 내용 |
|------|------|
| `outcomes.json` | "코드\|flag일" → fwd{t1..t10} + mfe/mae + status |
| `macro.json` | "YYYY-MM-DD" → 지수(KOSPI/KOSDAQ) + 수급 |
| `situations.json` | "YYYY-MM-DD" → 테마 벡터 + 대장 + breadth |

---

## 검증된 결과 (샌드박스, 2026-06-02)

- **5/26(반도체장비) ↔ 5/22(반도체장비) 유사도 0.47** — 같은 국면 정확히 매칭.
- 5/22 픽 이후: T+1 +4.3% → T+5 −14.7%, T+3 승률 19% (초반 반짝 후 밀리는 셋업).
- 5/29(로봇) ↔ 6/1(로봇·ESS) 0.40 — 로봇 국면끼리 묶임.

> ⚠️ 공부일 7일 표본 — 통계는 아직 참고용. 누적될수록 신뢰도 상승.
> 다음(Phase 3): 반복 셋업을 `wiki/patterns/[패턴].md` 카드로 굳히고 승률 집계.
