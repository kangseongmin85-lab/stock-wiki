@echo off
chcp 65001 > nul
cd /d "%~dp0"
set PYTHON="C:\Users\vamos\AppData\Local\Programs\Python\Python311\python.exe"

echo ============================================================
echo  위키 수동 업데이트 루틴 (장 마감 후 직접 실행)
echo  %date% %time%
echo ============================================================
echo.

echo [1/5] Lint 검사 (오류 사전 탐지)...
%PYTHON% run_lint.py --wiki-dir wiki
if errorlevel 1 echo [경고] Lint 오류 발생.

echo.
echo [2/5] Notion ingest...
%PYTHON% "ingest_all (노션 전체내용 옵시디언 업데이트).py" --no-finance --delete-orphans --enrich
if errorlevel 1 echo [경고] ingest 오류.

echo.
echo [3/5] 전체 종목 등락률 업데이트...
%PYTHON% fetch_change_rate.py --wiki-dir wiki
if errorlevel 1 echo [경고] 등락률 업데이트 오류.

echo.
echo [4/5] 업종별 수급 분석...
%PYTHON% fetch_sector.py
if errorlevel 1 echo [경고] 수급 분석 오류.

echo.
echo [5/5] 스크리닝 (F/C/D/E 조건)...
%PYTHON% fetch_screener.py --wiki-dir wiki
if errorlevel 1 echo [경고] 스크리닝 오류.

echo.
echo ============================================================
echo  완료: %date% %time%
echo  Lint 결과: wiki\analysis\lint_최신.md 확인
echo ============================================================
pause
