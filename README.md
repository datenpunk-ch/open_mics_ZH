# Open Mics Zurich

Ein Projekt zur Frage:
Wo kann man wann in Zürich lachen (oder selbst auf der Bühne stehen)?

## Ziel
- Open Mic Events sammeln
- Daten aus verschiedenen Quellen integrieren
- Regelmässige Events auflisten und visuell darstellen
	- Weekday, Location, Time, Cost, Comedy_language

## Struktur
- scrapers/: Daten sammeln
- data/: Rohdaten und bereinigte Daten
- src/: Analyse
- docs/: Notizen und Methodik
- `src/collect_data.py`: **ein Skript** — nutzt **Pixi** (`.pixi/envs/default`), falls `[tool.pixi.workspace]` im `pyproject.toml` und die Umgebung existiert bzw. `pixi install` möglich ist; sonst `.venv` + `requirements.txt`
- **Ohne Argumente** (oder `pixi run collect`): voller Lauf — Listing → angereicherte JSON → **`data/processed/events_flat.csv`** (u. a. Weekday, Location, Time, Cost, **Comedy_language**, Regularity, **Event_title**, URL; gleiche Serie an mehreren Terminen → Regularity „recurring“). **Comedy_language** = on-stage language (not inferred from ``/en/`` etc. in the URL).
- Nur Listing-JSON ohne viele Detailseiten: `python src/collect_data.py listing` bzw. `pixi run listing`
- Einzelschritte weiterhin: `python -m scrapers enrich` / `flatten` (ohne `--from` bzw. `-i` = jeweils neueste Datei unter `data/raw` / `data/processed`)
- IDE-Interpreter (Cursor/VS Code): unter Windows z. B. `.pixi/envs/default/python.exe`, unter Linux/macOS `.pixi/envs/default/bin/python` (nach `pixi install`; siehe `.vscode/settings.json` ggf. anpassen)

## Quellcode & Contributor

- **Repository:** [github.com/datenpunk-ch/open_mics_ZH](https://github.com/datenpunk-ch/open_mics_ZH)
- **Organisation / Contributor:** [datenpunk-ch](https://github.com/datenpunk-ch)

## Visualisierung (lokale Web-App)

Interaktive Ansicht mit Filtern (Wochentag/Sprache/Regularity), Event-Liste und Karte.

- **Mit Pixi**:
  - `pixi run app`
  - oder: `pixi run start-app`
- **Windows “ein Script”**:
  - PowerShell: `.\start_app.ps1` (oder `.\src\start_app.ps1`)
  - CMD: `start_app.cmd` (oder `src\start_app.cmd`)
- **Ohne Pixi**:
  - `python -m pip install -r requirements.txt`
  - `streamlit run src/open_mics_app.py`

## Website (GitHub Pages, statisch)

Für das Einbetten/Hosting auf einer Website (z. B. GitHub Pages) gibt es eine **statische** Version unter `docs/`.
Sie lädt `docs/data/events.json` und zeigt Karte + Filter + Event-Liste (ohne Server, ohne Runtime-Geocoding).

- **Build-time Geocoding (einmalig / wenn neue Locations kommen)**:
  - `pixi run geocode`
- **Statische Site exportieren**:
  - `pixi run export-site`

Dann in GitHub unter **Settings → Pages** als Source **“Deploy from a branch”** wählen und als Folder **`/docs`**.
