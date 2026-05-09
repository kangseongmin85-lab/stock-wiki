# 📊 투자 위키 대시보드

> Dataview 쿼리 모음. Obsidian에서 Dataview 플러그인 설치 필요.
> 설치: Settings → Community plugins → Dataview 검색 후 설치·활성화

---

## ✅ 지금 바로 작동하는 쿼리

---

### 📈 등락률 상위 20종목

> `change_rate` 필드 기준. ingest 시 자동 갱신.

```dataview
TABLE stock_code AS "코드", sector AS "섹터", change_rate AS "등락률", close_price AS "종가"
FROM "wiki/stocks"
WHERE change_rate != null AND change_rate != "" AND file.name != "_TEMPLATE"
SORT change_rate DESC
LIMIT 20
```

---

### 🏭 섹터별 종목 수

```dataview
TABLE length(rows) AS "종목 수"
FROM "wiki/stocks"
WHERE file.name != "_TEMPLATE" AND sector != null AND sector != "" AND sector != "기타"
GROUP BY sector
SORT length(rows) DESC
```

---

### 🏷️ 테마별 종목 수 (상위 50개)

> `tags` 필드 기반. 수급 집중 테마 파악용.

```dataview
TABLE length(rows) AS "종목 수", rows.file.link AS "종목"
FROM "wiki/stocks"
WHERE file.name != "_TEMPLATE"
FLATTEN tags AS tag
WHERE tag != null
GROUP BY tag
SORT length(rows) DESC
LIMIT 50
```

---

### 🔍 특정 테마 종목 검색

> `HBM` 자리에 원하는 테마명 입력. (예: 방산, 로봇, 이차전지)

```dataview
TABLE stock_code AS "코드", sector AS "섹터", change_rate AS "등락률"
FROM "wiki/stocks"
WHERE contains(tags, "HBM") AND file.name != "_TEMPLATE"
SORT change_rate DESC
```

---

### 📅 최근 업데이트 종목 (최근 30개)

```dataview
TABLE stock_code AS "코드", sector AS "섹터", change_rate AS "등락률", last_updated AS "업데이트"
FROM "wiki/stocks"
WHERE file.name != "_TEMPLATE" AND last_updated != null
SORT last_updated DESC
LIMIT 30
```

---

## 🔥 스크리닝 연동 쿼리 (fetch_screener.py 실행 후 자동 갱신)

> `python fetch_screener.py` 실행 시 자동 기록.
> **leader_score** = 등락률(%) × (오늘 거래대금 / 직전 20일 평균 거래대금)
> 값이 클수록 수급 폭발 + 주가 주도력이 강한 종목 → 테마 내 1위 = 대장주.

---

### 💥 최근 거래대금 돌파 종목

> `recent_breakout` 날짜 기준 최신순. 매매 후보 1순위 목록.

```dataview
TABLE stock_code AS "코드", sector AS "섹터", recent_breakout AS "돌파일", leader_score AS "점수", tags AS "테마"
FROM "wiki/stocks"
WHERE recent_breakout != null AND recent_breakout != "" AND file.name != "_TEMPLATE"
SORT recent_breakout DESC, leader_score DESC
LIMIT 30
```

---

### 🥇 leader_score 전체 순위

> 스크리닝 통과 종목 전체를 점수 순으로. 숫자가 클수록 수급·주도력 강함.

```dataview
TABLE stock_code AS "코드", sector AS "섹터", leader_score AS "점수", recent_breakout AS "돌파일", tags AS "테마"
FROM "wiki/stocks"
WHERE leader_score != null AND leader_score > 0 AND file.name != "_TEMPLATE"
SORT leader_score DESC
LIMIT 50
```

---

### 🎯 특정 테마 대장주 순위

> `HBM` 자리에 원하는 테마명 입력 → 점수 1위 = 대장주, 2~5위 = 2군.

```dataview
TABLE stock_code AS "코드", leader_score AS "점수", recent_breakout AS "돌파일"
FROM "wiki/stocks"
WHERE contains(tags, "HBM") AND leader_score > 0 AND file.name != "_TEMPLATE"
SORT leader_score DESC
```

---

### 📆 오늘 스크리닝 통과 종목

> `recent_breakout` 이 오늘 날짜인 종목만. 당일 매매 후보.

```dataview
TABLE stock_code AS "코드", sector AS "섹터", leader_score AS "점수", tags AS "테마"
FROM "wiki/stocks"
WHERE recent_breakout = date(today) AND file.name != "_TEMPLATE"
SORT leader_score DESC
```

---

## 🛠️ 관리용 쿼리

---

### ⚠️ 스크리닝 미통과 종목

> 아직 한 번도 스크리닝을 통과하지 못한 종목.

```dataview
TABLE stock_code AS "코드", sector AS "섹터", change_rate AS "등락률"
FROM "wiki/stocks"
WHERE (leader_score = null OR leader_score = 0) AND change_rate != null AND change_rate != "" AND file.name != "_TEMPLATE"
SORT change_rate DESC
LIMIT 50
```

---

### 📋 종목코드 없는 파일

```dataview
TABLE sector AS "섹터", last_updated AS "업데이트"
FROM "wiki/stocks"
WHERE (stock_code = null OR stock_code = "") AND file.name != "_TEMPLATE"
SORT file.name ASC
```
