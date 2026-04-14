# Root launcher stub (keeps code in src/).
# Usage: .\start_app.ps1

$ErrorActionPreference = "Stop"

& "$PSScriptRoot\src\start_app.ps1"

# One-command launcher for Windows PowerShell.
# Usage: .\start_app.ps1

$ErrorActionPreference = "Stop"

if (-not (Get-Command pixi -ErrorAction SilentlyContinue)) {
  Write-Host "pixi not found. Install Pixi first, then re-run." -ForegroundColor Red
  exit 1
}

pixi run app

