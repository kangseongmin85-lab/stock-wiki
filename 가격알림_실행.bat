@echo off
chcp 65001 >nul
cd /d "%~dp0"
set PYTHON="C:\Users\vamos\AppData\Local\Programs\Python\Python311\python.exe"

echo ============================================================
echo  Price Alert Loop (5min) - watchlist + holdings
echo  Telegram: ALERT_BOT_TOKEN (news bot and separate)
echo  Stop: Ctrl+C or close this window
echo ============================================================
echo.

:loop
%PYTHON% price_alert.py
echo.
echo [%time%] next scan in 5 min...
timeout /t 300 /nobreak
goto loop
