# Open Mics Zurich

Ein Projekt zur Frage:
Wo kann man wann in Zürich lachen (oder selbst auf der Bühne stehen)?

## Ziel
- Open Mic Events sammeln
- Daten aus verschiedenen Quellen integrieren
- Regelmässige Events auflisten und visuell darstellen
	- Wochentag, Location, Uhrzeit, Kosten, Sprache

## Struktur
- scrapers/: Daten sammeln
- data/: Rohdaten und bereinigte Daten
- src/: Analyse
- docs/: Notizen und Methodik
- collect_data.py: **ein Skript** — nutzt **Pixi** (`.pixi/envs/default`), falls `[tool.pixi.workspace]` im `pyproject.toml` und die Umgebung existiert bzw. `pixi install` möglich ist; sonst `.venv` + `requirements.txt`
- **Ohne Argumente** (oder `pixi run collect`): voller Lauf — Listing → angereicherte JSON → **`data/processed/events_flat.csv`** (u. a. Wochentag, Location, Uhrzeit, Kosten, Sprache, Regelmäßigkeit, **Titel_Event**, URL; gleiche Serie an mehreren Terminen → Regelmäßigkeit „regelmäßig“)
- Nur Listing-JSON ohne viele Detailseiten: `python collect_data.py listing` bzw. `pixi run listing`
- Einzelschritte weiterhin: `python -m scrapers enrich` / `flatten` (ohne `--from` bzw. `-i` = jeweils neueste Datei unter `data/raw` / `data/processed`)
- IDE-Interpreter (Cursor/VS Code): unter Windows z. B. `.pixi/envs/default/python.exe`, unter Linux/macOS `.pixi/envs/default/bin/python` (nach `pixi install`; siehe `.vscode/settings.json` ggf. anpassen)

## Quellcode & Contributor

- **Repository:** [github.com/datenpunk-ch/open_mics_ZH](https://github.com/datenpunk-ch/open_mics_ZH)
- **Organisation / Contributor:** [datenpunk-ch](https://github.com/datenpunk-ch)

### Git (Commit-Autor: datenpunk-ch)

Im Projektordner (lokal, nur dieses Repo):

```text
git config user.name "datenpunk-ch"
git config user.email "datenpunk.ch@gmail.com"
```

Remote **`origin`:** [https://github.com/datenpunk-ch/open_mics_ZH.git](https://github.com/datenpunk-ch/open_mics_ZH.git) — falls noch nicht gesetzt: `git remote add origin https://github.com/datenpunk-ch/open_mics_ZH.git`
