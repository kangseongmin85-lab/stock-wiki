@echo off
chcp 65001 >nul
pushd "%~dp0"
set PYTHON="C:\Users\vamos\AppData\Local\Programs\Python\Python311\python.exe"

echo ============================================================
echo  Pattern Engine - First-time Backfill (run ONCE)
echo  %date% %time%
echo ============================================================
echo  Takes 1-2 minutes. Just wait until it says Done.
echo ============================================================
echo.
echo [1/4] Installing packages (skips if already installed)...
%PYTHON% -m pip install pykrx finance-datareader python-dotenv
echo.
echo [2/4] Backfill forward returns + macro (index, supply)...
%PYTHON% fetch_outcomes.py
echo.
echo [3/4] Build situation vectors...
%PYTHON% situation_index.py
echo.
echo [4/4] Annotate daily_signal pages...
%PYTHON% pattern_match.py --annotate-all
echo.
echo ============================================================
echo  Done. Cache built in _cache (outcomes / macro / situations).
echo  From now on just run the evening report .bat.
echo  %date% %time%
echo ============================================================
pause
