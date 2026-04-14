@echo off
REM One-command launcher for Windows CMD.
REM Usage: start_app.cmd

where pixi >nul 2>nul
if errorlevel 1 (
  echo pixi not found. Install Pixi first, then re-run.
  exit /b 1
)

pixi run app

