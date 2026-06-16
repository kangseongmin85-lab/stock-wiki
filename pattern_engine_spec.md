# 패턴 엔진 스펙 — 유사국면 검색 + 패턴 카드

> 작성: 2026-06-02 | 상태: 설계 (구현 전) | 목적: "동일한 상황이 되면 매매"를 코드로 구현
> 관련 시스템: `daily_picks_tracker.py`, `theme_rollup.py`, `wiki/daily_signals/`, `wiki/overview.md`, `_cache/`

---

## 1. 한 줄 정의

매일의 시장을 **국면 스냅샷**으로 기록해두고, 오늘과 **닮은 과거 날**을 찾아 *"그때 강세였던 종목들이 이후 어떻게 됐는지"* 를 보여주는 엔진. 반복적으로 먹히는 셋업은 **패턴 카드**로 굳혀 승률과 함께 축적한다.

이것은 성민님 매매방식(시황·재료로 종목 반응을 학습 → 동일 상황 재현 시 매매)의 직접적 자동화다.

---

## 2. 매매방식 ↔ 엔진 매핑

| 성민님 단계 | 엔진 구성요소 |
|---|---|
| 시황·기사 분석 | SituationSnapshot (국면 + 강세 테마 + 재료 유형) |
| "이 상황에서 어떤 종목이 움직였나" 학습 | StockOutcome (종목 flag 이후 T+1/3/5/10 수익률 라벨) |
| "동일한 상황이 됐나" 판단 | 유사국면 검색 (오늘 벡터 vs 과거 벡터 코사인 유사도) |
| 반복되는 셋업을 규칙화 | PatternCard (조건 → 사례 → 승률 집계) |

---

## 3. 현재 가용 데이터 (조사 결과 2026-06-02)

**있는 것 (재사용 가능):**
- `wiki/daily_signals/*.md` — 일자별 종목 랭킹(등락률·거래대금·재료 메모) + 강세 테마 랭킹(종목수·평균등락·거래대금합·대장). 구조화돼 있어 그대로 벡터화 가능.
- `_cache/daily_picks_history.json` — 종목코드 keyed, `{change_rate, date, memo_snippet, name}`. 백필 시드.
- `_cache/stock_taxonomy.json` — 종목명 → `{category, themes[]}` (2,341종목). 종목→테마 매핑.
- `wiki/overview.md` 🤖 섹션 — theme_rollup 누적 강세 테마 집계.
- `requirements.txt` 에 `finance-datareader`, `pykrx` **이미 선언** (현재 미사용) → 주가/거래량 수집 즉시 가능.

**없는 것 (이번에 보강):**
- ❌ **사후 수익률 라벨** — 종목이 잡힌 뒤 주가가 어떻게 됐는지 기록 0건. → `fetch_outcomes.py` 신규.
- ❌ **매크로 국면 데이터** — 지수 등락·외국인/기관 수급·변동성. overview.md 🧠 국면 필드는 비어 있음. → Phase 4 보강.
- ⚠️ **표본 부족** — 공부일 7일치(5/17·19·22·26·29, 6/1·2). 유사도 엔진은 cold-start. 누적될수록 정확해지는 구조로 설계.

---

## 4. 데이터 모델

### 4.1 SituationSnapshot (하루 = 국면 한 장)
`_cache/situations.json` — 일자 keyed.
```json
{
  "2026-05-29": {
    "theme_vector": { "로봇": 5.0, "데이터센터/클라우드": 4.6, "스마트팩토리": 4.2, ... },
    "leaders": [ {"name":"LG전자","code":"066570","theme":"로봇","ctrt":29.93,"value":6222061}, ... ],
    "breadth": { "picks": 20, "avg_ctrt": 21.4, "reappear": 2 },
    "macro": { "kospi_chg": null, "kosdaq_chg": null, "foreign_net": null }   // Phase 4
  }
}
```
- `theme_vector`: 테마별 가중치 = `강세종목수 × log10(거래대금합)` (수급·폭 동시 반영). daily_signal 강세 테마 표에서 추출.
- `leaders`: 거래대금 상위 종목 (대장 후보).
- `macro`: Phase 4까지 null 허용.

### 4.2 StockOutcome (종목 flag 이후 성과 라벨)
`_cache/outcomes.json` — `"{code}|{flag_date}"` keyed.
```json
{
  "066570|2026-05-29": {
    "name":"LG전자", "flag_ctrt":29.93,
    "fwd": { "t1":2.1, "t3":-3.4, "t5":1.2, "t10":8.9 },   // flag일 종가 대비 종가 수익률(%)
    "mfe_t5": 6.0, "mae_t5": -5.1,                            // 윈도 내 최대상승/최대하락 (손절·익절 분석용)
    "source":"pykrx", "computed_at":"..." 
  }
}
```
- 진입 가정: **flag일 종가**. exit: T+n **거래일** 종가. (가정의 한계는 §8.)
- `mfe/mae`: 차트 손절·전고점 돌파 분석에 사용.

### 4.3 PatternCard (반복 셋업)
`wiki/patterns/[패턴슬러그].md` (Obsidian 카드).
```markdown
---
type: pattern
status: 후보 | 검증 | 폐기
tags: [국면, 재료유형, 테마]
n_cases: 4
hit_rate_t3: 0.75
avg_fwd_t3: 6.2
last_updated: YYYY-MM-DD
---
# [패턴명] 예: 외국인 순매수 전환 + 로봇 재료 → 대장주 익일 갭

## 셋업 조건
- 국면: [공격 매수 / 선별 매수 / ...]
- 트리거 재료: [정책/수주/지정학 ...]
- 테마: [[themes/로봇]]

## 집계 (자동)
| 지표 | T+1 | T+3 | T+5 |
|------|-----|-----|-----|
| 평균 수익률 | | | |
| 승률 | | | |
| n | | | |

## 사례 (자동 링크)
- [2026-05-29] [[stocks/LG전자]] +29.93% → T3 ...  [[daily_signals/2026-05-29]]

## 🧠 본인 메모 (매매 규칙)
- 진입 / 손절 / 비중:
```

---

## 5. 유사도 방식

- **v1 (Phase 2, 현재 데이터로 가능):** 테마 벡터 코사인 유사도. 오늘 `theme_vector` vs 모든 과거일 → Top-K 근접일. 설명가능하고 7일치로도 동작.
- **v2 (Phase 4):** 테마 + 매크로(지수·수급·변동성) 결합 벡터. 가중치 튜닝.
- 출력: 유사일 Top3 + 각 유사일의 대장주 `fwd` 분포 → "닮은 날엔 대장주가 T+3 평균 +X% (n/N 양봉)" 형태.

---

## 6. 신규 / 확장 모듈

| 모듈 | 신규/확장 | 역할 |
|------|-----------|------|
| `fetch_outcomes.py` | 신규 | daily_signal 전 종목의 fwd 수익률(pykrx/FDR) 백필 + 신규 자동. `_cache/outcomes.json` |
| `situation_index.py` | 신규 | daily_signal → SituationSnapshot 벡터화. `_cache/situations.json` |
| `pattern_match.py` | 신규 | 유사국면 검색 + 반복 셋업 후보 surfacing |
| `daily_picks_tracker.py` | 확장 | daily_signal 렌더에 "🤖 유사 과거 국면 Top3" 섹션 추가 (🧠 본인 분석 불침범) |
| `wiki/patterns/` + `_TEMPLATE.md` | 신규 | 패턴 카드 저장소 + index |

> 모든 자동 출력은 기존 규칙(🤖 자동 / 🧠 본인 분석 분리) 준수. 패턴 카드의 매매 규칙은 본인 소유 영역.

---

## 7. 워크플로

**일일 (공부일):** 영웅문 CSV 푸시 → daily_signal 생성(기존) → `fetch_outcomes` 가 과거 분 라벨 갱신 → `situation_index` 가 오늘 스냅샷 추가 → `pattern_match` 가 유사 과거국면 Top3 + 매칭 패턴카드를 daily_signal 에 주석.

**주간:** `pattern_match --surface` 가 반복 셋업 후보 출력 → Claude 가 패턴 카드 초안 작성 → 본인이 매매 규칙 확정(검증/폐기).

---

## 8. 한계·주의 (반드시 인지)

- **Cold-start:** 7일치로는 유사도가 빈약. 표본이 쌓일수록 가치 상승. 가능하면 Notion DB created 날짜로 과거 백필 검토.
- **Look-ahead 금지:** fwd 수익률은 flag일 *이후* 데이터만. 스냅샷/유사도 계산에 미래 종가 유입 절대 차단 (코드 가드 + 테스트).
- **생존편향:** 상폐·거래정지 종목 누락 시 승률 과대. pykrx 결측은 `null` 로 남기고 집계서 제외(낙관 금지).
- **체결 가정:** 종가진입은 실제와 다름(갭·슬리피지·상한가 미체결). 승률은 참고치, 단정 금지.
- **과최적화:** 패턴 조건을 사례에 맞춰 좁히면 미래 적중 하락. n 작은 카드는 `후보` 로만.

---

## 9. 단계별 구현 계획

| Phase | 산출물 | 검증 기준 |
|-------|--------|-----------|
| **1. 성과 라벨 토대** | `fetch_outcomes.py` | 7일치 daily_signal 전 종목 `outcomes.json` 채워짐 / look-ahead 테스트 통과 / 결측 null 처리 |
| **2. 유사국면 검색** | `situation_index.py` + `pattern_match.py` + daily_signal 섹션 | 임의 날짜 입력 시 Top3 유사일 + 대장주 fwd 분포 출력. 5/29↔5/26(로봇·클라우드) 유사 감지 |
| **3. 패턴 카드** | `wiki/patterns/` + 집계 | 반복 셋업 1건 end-to-end 카드화(조건→사례→hit-rate) |
| **4. 매크로·알림 보강** | regime 수집 + 실시간 매칭 알림 | 지수·수급 벡터 결합, 텔레그램 "학습된 패턴 매칭" 신호 |

> Phase 1 이 나머지 전부의 토대 (유사국면이 "이후 어떻게 됐나"를 답하려면 fwd 라벨 필수). 그래서 #3을 골랐어도 Phase 1은 lean하게 먼저 깔고 간다.

---

## 10. 미해결 결정사항 (착수 전 확정)

1. **fwd 기준일**: 거래일 기준 T+1/3/5/10 (권장) vs 달력일.
2. **진입 가정**: flag일 종가(권장, 단순) vs 익일 시가(현실적이나 데이터 1틱 더 필요).
3. **국면에 매크로 포함 시점**: Phase 2는 테마만, 매크로는 Phase 4 (권장) — 아니면 처음부터 결합?
4. **백필 범위**: 현재 7일만 vs Notion created 날짜로 과거 확장.
