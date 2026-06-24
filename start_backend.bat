@echo off
echo ========================================
echo   X Scraper - Starting Backend Server
echo ========================================
echo.
echo Backend API: http://localhost:8765
echo API Docs:    http://localhost:8765/docs
echo Frontend:    Open frontend/index.html in browser
echo.
cd /d "%~dp0backend"
..\.venv\Scripts\python.exe -m uvicorn main:app --host 0.0.0.0 --port 8765 --reload
pause
