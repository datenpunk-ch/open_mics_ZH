@echo off
REM Root launcher stub (keeps code in src\).
REM Usage: start_app.cmd

call "%~dp0src\start_app.cmd"

@echo off
REM One-command launcher for Windows CMD.
REM Usage: start_app.cmd

where pixi >nul 2>nul
if errorlevel 1 (
  echo pixi not found. Install Pixi first, then re-run.
  exit /b 1
)

pixi run app

