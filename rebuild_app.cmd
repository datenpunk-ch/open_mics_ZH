@echo off
REM Rebuild data + static site, optionally start the app.
REM Usage:
REM   rebuild_app.cmd          (rebuild only)
REM   rebuild_app.cmd --app    (rebuild then start Streamlit)

where pixi >nul 2>nul
if errorlevel 1 (
  echo pixi not found. Install Pixi first, then re-run.
  exit /b 1
)

pixi run rebuild-site
if errorlevel 1 exit /b %errorlevel%

if "%~1"=="--app" (
  pixi run app
)

