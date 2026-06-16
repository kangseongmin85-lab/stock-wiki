@echo off
chcp 65001 > nul
cd /d "%~dp0"
set PYTHON="C:\Users\vamos\AppData\Local\Programs\Python\Python311\python.exe"

echo ============================================================
echo  Market Window Briefing
echo  Window: last weekday 15:30 KST  -^> now
echo  %date% %time%
echo ============================================================
echo.

%PYTHON% fetch_market_window.py
if errorlevel 1 (echo [ERROR] Collection failed. See messages above. & pause & exit /b 1)

echo.
echo [순환테마] 누적된 테마 시세 CSV로 로테이션 분석 생성...
%PYTHON% track_rotation.py
if errorlevel 1 echo [WARN] rotation tracking failed (briefing still ok).

echo.
echo ============================================================
echo  Done. Output saved to "오늘의 시황" folder.
echo.
echo  Next step:
echo    1. Open the .docx file
echo    2. Or attach it to Claude chat and ask for market analysis
echo.
echo  순환테마 트래킹 (옵시디언에서 열기):
echo    wiki\analysis\순환테마_트래킹.md      (항상 최신)
echo    wiki\analysis\시황_(오늘날짜).md      (당일 강세/거래대금 순위)
echo ============================================================
pause
