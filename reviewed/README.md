# reviewed/

검수가 끝난 결과 JSON을 보관하는 폴더.

워크플로:
1. `python signal_report.py` 실행 → `reports/review_YYYYMMDD.html` 생성
2. HTML 더블클릭으로 열어서 종목별 기사 검수 (승인/거부/스킵)
3. 진행 상황은 브라우저 localStorage에 자동 저장 (창 닫아도 보존)
4. 다 검토하면 상단 "결과 내보내기" 클릭 → `reviewed_YYYY-MM-DD.json` 다운로드
5. 다운로드된 파일을 이 폴더로 옮기기

JSON 구조:
- date, exported_at, summary{total, approved, rejected, skipped, pending}
- results: [{name, code, ctrt, vol_eok, theme, decision, chosen_article, candidates_seen, total_candidates}, ...]

Phase 2 (예정): 이 JSON을 읽어 Notion에 업데이트하는 스크립트 추가.
