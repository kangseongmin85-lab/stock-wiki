@echo off
cd /d "%~dp0"
:loop
echo [%time%] 뉴스 크롤링 시작...
python fetch_news.py
echo.
echo 10분 대기 중...
timeout /t 600
goto loop
