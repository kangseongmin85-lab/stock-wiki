@echo off
chcp 65001 >nul
pushd "%~dp0"
set PYTHON="C:\Users\vamos\AppData\Local\Programs\Python\Python311\python.exe"

echo ============================================================
echo  Signal Evening Report + Pattern Engine
echo  %date% %time%
echo ============================================================
echo  [Prep] Save the screener CSV into the watchlist folder first.
echo ============================================================
echo.
echo [수집] 네이버 테마 시세 수집을 백그라운드로 동시 시작...
echo        (data\themes\ CSV 적재 + wiki\analysis\시황_날짜.md 생성)
start "fetch_themes" /min %PYTHON% fetch_themes.py
echo.
echo [1/4] signal_report (CSV - daily_signal - review HTML - Notion push)...
%PYTHON% signal_report.py
if errorlevel 1 (
    echo [ERROR] signal_report failed. See messages above.
    pause
    exit /b 1
)
echo.
echo [분류] 오늘 푸시분 중 빈 카테고리/관련테마/키워드 탐지...
%PYTHON% taxonomy_backfill.py --list --today --out reports\taxonomy_todo.json
if errorlevel 1 (echo   [WARN] taxonomy 탐지 실패 - 건너뜀.) else (echo   - 빈 종목은 reports\taxonomy_todo.json 에 적재. Claude에게 "빈 분류 채워줘" 하면 저장된 어휘로만 채움.)
echo.
echo [2/4] fetch_outcomes (forward returns + macro)...
%PYTHON% fetch_outcomes.py
if errorlevel 1 echo [WARN] outcomes update failed (report still ok).
echo.
echo [3/4] situation vectors + annotate daily_signal...
%PYTHON% situation_index.py
%PYTHON% pattern_match.py --annotate-all
if errorlevel 1 echo [WARN] pattern annotate failed.
echo.
echo [4/4] inspector (only if present)...
if exist inspector.py (
    %PYTHON% inspector.py
    if errorlevel 1 echo [WARN] quality issues - check inspections folder.
) else (
    echo   inspector.py not found - skip.
)

:end
echo.
echo [AIDC] AI데이터센터 밸류체인 교차검증 + 순환 트래킹...
echo        (백그라운드 네이버 테마수집 완료 대기 후 실행)
for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyy-MM-dd"') do set TODAY=%%i
set /a _tries=0
:aidc_wait
if exist "data\themes\%TODAY%_stocks.csv" goto aidc_settle
set /a _tries+=1
if %_tries% GEQ 30 (echo   [WARN] 오늘 CSV 미생성 - 최신 가용분으로 실행. & goto aidc_run)
timeout /t 10 /nobreak >nul
goto aidc_wait
:aidc_settle
echo   오늘 테마 CSV 확인 - 적재 안정화 대기...
timeout /t 5 /nobreak >nul
:aidc_run
%PYTHON% aidc_crosscheck.py
if errorlevel 1 echo   [WARN] aidc_crosscheck failed.
%PYTHON% aidc_rotation.py
if errorlevel 1 echo   [WARN] aidc_rotation failed.
echo   결과: wiki\analysis\AIDC_매매교차검증_(날짜).md + AIDC_순환트래킹.md
echo.
echo [일정] 주식 일정 후보 스캔 (노션 기존 + 오늘 뉴스 + 네이버 캘린더)...
%PYTHON% schedule_scan.py --out reports\schedule_todo.json
if errorlevel 1 (echo   [WARN] 일정 스캔 실패 - 건너뜀.) else (echo   - 후보는 reports\schedule_todo.json. Claude에게 "일정 업데이트해줘" 하면 판단·태그·중복제거 후 노션+wiki\일정.md 입력.)
echo.
echo [클라우드 동기화] 관심종목 CSV + 보유종목 - GitHub (가격알림용)...
%PYTHON% sync_watchlist_github.py
if errorlevel 1 echo   [WARN] watchlist sync failed - cloud alert uses stale list.
if exist "보유종목.csv" (
    gh secret set ALERT_HOLDINGS --repo kangseongmin85-lab/stock-wiki < "보유종목.csv" >nul 2>&1
    if errorlevel 1 (echo   [WARN] holdings secret sync failed.) else (echo   보유종목 시크릿 동기화 완료.)
)
echo.
echo ============================================================
echo  Done: %date% %time%
echo ============================================================
echo  창은 10초 후 자동으로 닫힙니다. (아무 키나 누르면 즉시 닫힘)
timeout /t 10 >nul
exit
