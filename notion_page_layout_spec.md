# 노션 페이지 본문 레이아웃 설계서

> 기준: `이브닝 자동 업데이트 제미나이\drive_to_notion.py` 의 `reconstruct_page()` 출력 양식
> 목적: 위키 프로젝트의 `notion_pusher.py` 가 향후 페이지 본문까지 채울 때 사용할 표준 양식
> 대상 DB: `2cbffbf4617380e38d07e8b5e59e36c4` (종목재료정리)

---

## 1. 페이지 본문 블록 순서

페이지 본문(children blocks)을 위에서 아래로 다음 순서로 구성한다.

| # | 블록 타입 | 내용 | 비고 |
|---|---|---|---|
| 1 | callout | 🏠 "홈페이지 바로가기 (URL)" | URL 텍스트 자체에 hyperlink |
| 2 | heading_2 | `## 1. 종목 요약` (또는 fallback: `## 1. 종목 재료 요약`) | |
| 3 | paragraph | 한 줄 요약 문장 | 데이터 없으면 최근 뉴스 헤드라인 3건을 ` / ` 로 연결 |
| 4 | heading_2 | `## 2. 사업 내용 (상세)` | |
| 5 | quote | 사업 내용 본문 | 한 개 quote 블록 |
| 6 | heading_2 | `## 3. 추가 중요 내용` | |
| 7 | bulleted_list_item × N | 기사 리스트 | 각 항목: `[YYYY-MM-DD] 제목` (날짜=회색, 제목=링크) |
| 8 | heading_2 | `## 4. 재무 상태 및 전망` | |
| 9 | callout | 📊 "유통가능주식 비율: XX%" | bold 라벨 |
| 10 | callout | 📈 "실적 전망 & 리스크 포인트" + 내부 불릿 | callout.children 에 bulleted_list_item 다수 |
| 11 | heading_3 | `### 📊 최근 실적 추이 (8분기)` | |
| 12 | table | 4열 표 | 분기 / 매출 / 영업이익 / 순이익 |
| 13 | divider | 구분선 | |
| 14 | toggle | 🗄️ "지난 기사 (Archive)" | 내부에 과거 기사 bulleted list |

---

## 2. 섹션별 데이터 스펙

### 2.1 홈페이지 (callout, emoji 🏠)
- **필요 데이터:** `homepage_url: str`
- **노션 표현:**
  ```
  icon: 🏠
  rich_text:
    - "홈페이지 바로가기"
    - " (URL)" with link → URL
  ```
- **없을 때:** 블록 자체를 생략

### 2.2 종목 요약 (heading_2 + paragraph)
- **필요 데이터:** `one_line_summary: str` (선택)
- **있을 때:** 헤딩 `1. 종목 요약` + paragraph
- **없을 때:** fallback 으로 헤딩 `1. 종목 재료 요약` + 최근 뉴스 3건 헤드라인을 ` / ` 로 join

### 2.3 사업 내용 (heading_2 + quote)
- **필요 데이터:** `business_summary: str`
- **노션 표현:** quote 블록 한 개
- **없을 때:** "정보 없음" 텍스트로 표시

### 2.4 추가 중요 내용 (heading_2 + bulleted_list_item × N)
- **필요 데이터:** `issues: list[dict | str]`
- **dict 형식:** `{title, link, desc, press, date}` (`date` = `YYYY-MM-DD`)
- **노션 표현 (dict일 때):**
  ```
  bulleted_list_item rich_text:
    - "[YYYY-MM-DD] " (annotations.color = gray)
    - "제목" with link → URL
  ```
- **str 형식일 때:** 그냥 plain text bulleted list
- **데이터 없을 때:** 신규 뉴스 리스트를 fallback 으로 사용. 그것마저 없으면 "업데이트된 중요 이슈가 없습니다." paragraph

### 2.5 재무 상태 및 전망 (heading_2 + callout + callout + heading_3 + table)
- **필요 데이터:**
  - `share_ratio: str` — 유통가능주식 비율 (예: `"45.2%"`)
  - `finance.outlook: list[str] | str` — 전망 불릿 또는 문자열
  - `finance.quarterly_trend: str` — 8분기 실적 (포맷은 아래)
- **유통비율 callout (emoji 📊):**
  ```
  rich_text:
    - "유통가능주식 비율: " (bold)
    - "45.2%"
  ```
- **전망 callout (emoji 📈):**
  ```
  rich_text: "실적 전망 & 리스크 포인트"
  children: bulleted_list_item × N (불릿 항목)
  ```
  - `outlook` 이 list 면 그대로 불릿화
  - `outlook` 이 str 이면 `.` 단위로 split 해서 불릿화
- **8분기 테이블:**
  - 헤더: `분기 | 매출 | 영업이익 | 순이익`
  - 데이터 행 포맷 (parser 가 받는 입력 문자열):
    - 영문: `[2024.09] Sales 100 | OP 50 | Net 30`
    - 한글: `[2024.09] 매출 100 | 영업익 50 | 순익 30`
  - 양쪽 모두 regex 로 파싱 (drive_to_notion.py 의 `reconstruct_page()` 참고)
- **모든 재무 데이터가 없을 때:** ⚠️ callout 로 "재무 데이터 수집에 실패했거나 정보가 없습니다." 표시 (회색 배경)

### 2.6 지난 기사 Archive (toggle)
- **필요 데이터:** 기존 페이지에서 추출한 옛 기사 + 이번에 들어온 새 기사
- **노션 표현:** toggle 블록 1개, 텍스트 = "🗄️ 지난 기사 (Archive)"
- **toggle 내부:** bulleted_list_item × N, 각 항목 = `제목` with link
- **dedup 규칙:** URL 기준으로 중복 제거 (홈페이지 URL 과 겹치는 것도 제외)

---

## 3. 페이지 재구성 알고리즘

기존 페이지가 있을 때 다음 순서로 진행한다.

1. `GET /v1/blocks/{page_id}/children` — 기존 블록 전부 조회 (pagination 포함)
2. 기존 블록에서 토글 내부까지 BFS 로 모든 `(text, url)` 쌍을 추출 → `old_news`
3. 새 기사 URL 집합 `new_urls` 와 비교 → `archived = old_news \ new_urls` 로 보존 대상 추림
4. 기존 블록 전부 `DELETE /v1/blocks/{block_id}` (Batch API 없음 → 순차 삭제)
5. 새 children 구성 (위 §1 의 1~13번)
6. `PATCH /v1/blocks/{page_id}/children` — chunk_size=100 으로 1차 append (본문)
7. Archive 토글 블록만 별도로 append → 응답에서 toggle ID 받기
8. `PATCH /v1/blocks/{toggle_id}/children` 으로 archive 내용 append (chunk_size=100)

> Notion API 의 단일 요청당 children 100개 제한과 toggle 내부 자식 직접 삽입 제약을 모두 우회하는 패턴.

---

## 4. 현재 `notion_pusher.py` 와의 갭

| 항목 | 현재 보유 | 추가 필요 |
|---|---|---|
| 종목명 | ✅ | — |
| 등락률 | ✅ (properties) | — |
| 기사 1건 (title/url/summary/date) | ✅ | — |
| **홈페이지 URL** | ❌ | 종목별 마스터 데이터 또는 크롤링 |
| **사업 내용 (1~3문장)** | ❌ | 종목별 마스터 데이터 (FnGuide 기업개요 등) |
| **한 줄 요약** | ❌ | LLM 생성 (선택) |
| **유통비율** | ❌ | FnGuide / KIND |
| **재무 전망 (outlook)** | ❌ | LLM (Gemini/Opus) 생성 |
| **8분기 실적** | ❌ | FnGuide API (메모리 룰: Notion 표 금지, FnGuide 사용) |
| **뉴스 N건 리스트** | 1건만 | N건으로 확장, dict 형식 통일 |
| **기존 페이지 본문 재구성 로직** | ❌ (properties 만 patch) | `reconstruct_page()` 이식 |
| **Archive 토글 보존** | ❌ | URL dedup 로직 포함 이식 |

---

## 5. 구현 시 권장 분리

1. **`notion_pusher.py`** — properties 만 담당 (현재 그대로 유지)
2. **신규 `notion_body_builder.py`** — 본문 children 블록 생성 + 재구성
3. **`signal_report.py`** 에서 종목 승인 시:
   - `notion_pusher.upsert_stock()` → properties patch
   - `notion_body_builder.reconstruct_page(page_id, data)` → 본문 재구성
4. 데이터 수집 책임 분리:
   - 마스터 데이터(홈페이지, 사업내용): 별도 캐시 파일 또는 종목 메타 DB
   - 재무: `fetch_finance.py` 이미 있음 → 출력 포맷만 §2.5 의 quarterly_trend 문자열로 맞추기
   - 전망(outlook): LLM 호출 (선택)

---

## 6. 참고

- 원본 코드: `이브닝 자동 업데이트 제미나이\drive_to_notion.py`
  - `reconstruct_page()` (line 436~823) — 본문 재구성 본체
  - `extract_news_links_from_blocks()` (line 251~286) — 기존 기사 추출 BFS
  - `extract_finance_data()` (line 288~357) — 재무 섹션 추출 (참고용)
- 메모리 룰 참고:
  - 재무 데이터는 Notion 표 금지, FnGuide API 사용
  - 태그(관련테마)는 100% 원본 복사
