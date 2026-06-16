@echo off
chcp 65001 > nul
cd /d "%~dp0"
set PYTHON="C:\Users\vamos\AppData\Local\Programs\Python\Python311\python.exe"

echo ============================================================
echo  Notion -^> Obsidian ingest
echo  %date% %time%
echo ============================================================
echo.

%PYTHON% "ingest_all (노션 전체내용 옵시디언 업데이트).py" --no-finance --delete-orphans --since-days 2
if errorlevel 1 (echo [ERROR] ingest failed. See messages above. & pause & exit /b 1)

echo.
echo ============================================================
echo  Done: %date% %time%
echo ============================================================
pause
