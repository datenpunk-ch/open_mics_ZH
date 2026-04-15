# Rebuild data + static site, optionally start the app.
# Usage:
#   .\rebuild_app.ps1          (rebuild only)
#   .\rebuild_app.ps1 -App     (rebuild then start Streamlit)

param(
  [switch]$App
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command pixi -ErrorAction SilentlyContinue)) {
  Write-Host "pixi not found. Install Pixi first, then re-run." -ForegroundColor Red
  exit 1
}

pixi run rebuild-site

if ($App) {
  pixi run app
}

