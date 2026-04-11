#!/usr/bin/env python3
"""
Ein Skript: Umgebung prüfen/anlegen, dann scrapen.

- Wenn **Pixi** konfiguriert ist (``pyproject.toml`` + ``.pixi``): dieses Python wird genutzt
  (optional: fehlende Umgebung → ``pixi install``, fehlender Browser → ``playwright install``).
- Sonst: klassische ``.venv`` anlegen und mit ``pip`` + ``requirements.txt`` befüllen.
- Scraping nutzt dasselbe Python wie die gewählte Umgebung: **im Prozess**, wenn du den
  Pixi-/.venv-Interpreter schon gewählt hast; sonst **Subprozess** mit dem ermittelten Python.

**Ohne Argumente:** voller Ablauf wie ``run`` — Listing scrapen → Detailseiten
anreichern → ``data/processed/events_flat.csv``.

Nur das Listing (schnell, ohne jede Event-URL):

  python collect_data.py listing

Weitere Optionen wie bei ``python -m scrapers …`` (z. B. ``--headed``, ``--limit``).

Beispiele:
  python collect_data.py
  python collect_data.py --headed
  python collect_data.py --limit 5
  python collect_data.py run
  python collect_data.py listing --source eventfrog eventfrog_de
  python collect_data.py enrich
  python collect_data.py enrich --from data/raw/eventfrog_listing_....json --limit 3
  python collect_data.py flatten
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

_ROOT_CMDS = frozenset({"listing", "event-page", "list-sources", "enrich", "flatten", "run"})


def _project_root() -> Path:
    return Path(__file__).resolve().parent


def _venv_python(project: Path) -> Path:
    if os.name == "nt":
        return project / ".venv" / "Scripts" / "python.exe"
    return project / ".venv" / "bin" / "python"


def _pyproject_has_pixi_workspace(project: Path) -> bool:
    p = project / "pyproject.toml"
    if not p.is_file():
        return False
    try:
        return "[tool.pixi.workspace]" in p.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return False


def _pixi_default_python(project: Path) -> Path | None:
    if os.name == "nt":
        cand = project / ".pixi" / "envs" / "default" / "python.exe"
    else:
        cand = project / ".pixi" / "envs" / "default" / "bin" / "python"
    return cand if cand.is_file() else None


def _find_bootstrap(project: Path) -> tuple[list[str] | None, str | None]:
    """Python nur für das Anlegen von ``.venv``: aktueller Prozess, dann ``py``, dann ``PATH``."""
    target = _venv_python(project)
    try:
        cur = Path(sys.executable).resolve()
        if cur.is_file() and cur != target.resolve():
            return [str(cur)], None
    except OSError:
        pass

    py_launch = shutil.which("py")
    if py_launch:
        return None, py_launch

    py = shutil.which("python")
    if py:
        return [py], None

    return None, None


def _run(cmd: list[str], cwd: Path, *, step: str) -> None:
    print("+", " ".join(cmd))
    try:
        subprocess.run(cmd, cwd=cwd, check=True)
    except subprocess.CalledProcessError as e:
        print(
            f"\n[collect_data] Fehler in Schritt „{step}“ (Exitcode {e.returncode}).",
            file=sys.stderr,
        )
        print(
            "[collect_data] Tipp: Umgebung prüfen (``pixi install`` / IDE-Interpreter), "
            "Terminal neu starten, dann erneut versuchen.",
            file=sys.stderr,
        )
        raise


def _deps_check(venv_py: Path, project: Path) -> tuple[bool, str]:
    r = subprocess.run(
        [str(venv_py), "-c", "import playwright, bs4"],
        cwd=project,
        capture_output=True,
        text=True,
        timeout=120,
        encoding="utf-8",
        errors="replace",
    )
    msg = ((r.stderr or "").strip() + "\n" + (r.stdout or "").strip()).strip()
    return r.returncode == 0, msg


def _install_into_venv(project: Path, venv_py: Path) -> None:
    _run([str(venv_py), "-m", "pip", "install", "--upgrade", "pip"], project, step="pip upgrade")
    req = project / "requirements.txt"
    if req.is_file():
        _run(
            [str(venv_py), "-m", "pip", "install", "-r", str(req)],
            project,
            step="pip install -r requirements.txt",
        )
    _run(
        [str(venv_py), "-m", "playwright", "install", "chromium"],
        project,
        step="playwright install chromium",
    )


def _playwright_install_browsers(project: Path, py: Path) -> None:
    _run(
        [str(py), "-m", "playwright", "install", "chromium"],
        project,
        step="playwright install chromium",
    )


def _ensure_pixi_environment(project: Path) -> Path | None:
    """Wenn Pixi-Manifest + Umgebung da ist, Pfad zum default-Python; sonst None."""
    if not _pyproject_has_pixi_workspace(project):
        return None

    pixi_exe = shutil.which("pixi")
    py = _pixi_default_python(project)
    if py is None:
        if not pixi_exe:
            return None
        print("[collect_data] pixi: Umgebung fehlt — „pixi install“ …\n")
        _run([pixi_exe, "install"], project, step="pixi install")
        py = _pixi_default_python(project)
    if py is None:
        print("[collect_data] pixi: Nach „pixi install“ fehlt noch .pixi/envs/default.")
        return None

    print("[collect_data] Nutze Pixi-Python:", py, "\n")
    ok, hint = _deps_check(py, project)
    if not ok and pixi_exe:
        print("[collect_data] pixi: Abgleich („pixi install“) …\n")
        if hint:
            print("[collect_data] Import-Test:\n", hint[:3000], "\n", sep="")
        _run([pixi_exe, "install"], project, step="pixi install (sync)")
        ok, hint = _deps_check(py, project)
    if not ok:
        print("[collect_data] pixi: Playwright-Browser installieren …\n")
        _playwright_install_browsers(project, py)
        ok, hint = _deps_check(py, project)
    if not ok:
        print("[collect_data] Fehler: Pixi-Umgebung unvollständig.")
        if hint:
            print(hint[:3000], file=sys.stderr)
        raise SystemExit(1)
    return py


def ensure_environment(project: Path) -> Path:
    """Pixi bevorzugen, sonst .venv. Gibt Pfad zum Python für den Scraper-Subprozess zurück."""
    os.chdir(project)

    pixi_py = _ensure_pixi_environment(project)
    if pixi_py is not None:
        return pixi_py

    vp = _venv_python(project)

    if not vp.is_file():
        print("[collect_data] Setup: virtuelle Umgebung .venv anlegen …\n")
        boot, pylaunch = _find_bootstrap(project)
        if not boot and not pylaunch:
            print(
                "[collect_data] Kein Python zum Anlegen von .venv gefunden.\n"
                "Bitte in der IDE einen Interpreter wählen (z. B. Pixi unter ``.pixi/...``) "
                "oder ``py`` / ``python`` im PATH setzen.\n"
            )
            raise SystemExit(1)
        if boot:
            _run(boot + ["-m", "venv", ".venv"], project, step="python -m venv .venv")
        else:
            _run([pylaunch, "-3", "-m", "venv", ".venv"], project, step="py -3 -m venv .venv")
        if not vp.is_file():
            print("[collect_data] Fehler: .venv/python fehlt nach Erzeugung.")
            raise SystemExit(1)
        print("[collect_data] Setup: Pakete und Playwright …\n")
        _install_into_venv(project, vp)
        return vp

    ok, hint = _deps_check(vp, project)
    if not ok:
        print("[collect_data] Setup: Pakete / Playwright nachinstallieren …\n")
        if hint:
            print("[collect_data] Vorheriger Import-Test:\n", hint[:3000], "\n", sep="")
        _install_into_venv(project, vp)
        ok2, hint2 = _deps_check(vp, project)
        if not ok2:
            print("[collect_data] Fehler: Abhängigkeiten konnten nicht geladen werden.")
            if hint2:
                print(hint2[:3000], file=sys.stderr)
            raise SystemExit(1)

    return vp


def _scraper_argv() -> list[str]:
    rest = sys.argv[1:]
    if not rest or rest[0] not in _ROOT_CMDS:
        rest = ["run", *rest]
    return rest


def _same_executable(a: Path, b: Path) -> bool:
    try:
        return a.resolve() == b.resolve()
    except OSError:
        return False


def main() -> int:
    project = _project_root()
    try:
        runner_py = ensure_environment(project)
    except subprocess.CalledProcessError as e:
        print("\n[collect_data] Setup fehlgeschlagen, Exitcode:", e.returncode, file=sys.stderr)
        return e.returncode

    argv = _scraper_argv()
    cur = Path(sys.executable)

    if _same_executable(cur, runner_py):
        print("[collect_data] Scraping im aktuellen Interpreter:", runner_py, "\n")
        from scrapers.cli import main as cli_main

        return int(cli_main(argv))

    cmd = [str(runner_py), "-m", "scrapers", *argv]
    print("[collect_data] Scraping (Subprozess) mit:", runner_py)
    print("[collect_data] Befehl:", " ".join(cmd), "\n")
    r = subprocess.run(cmd, cwd=project)
    if r.returncode == 0:
        if argv and argv[0] == "listing":
            print("\n[collect_data] Fertig. Listing: data/raw/")
        elif argv and argv[0] == "run":
            print(
                "\n[collect_data] Fertig. Listing: data/raw/ — "
                "Tabelle: data/processed/events_flat.csv"
            )
        else:
            print("\n[collect_data] Fertig.")
    else:
        print(
            f"\n[collect_data] Scraping beendet mit Exitcode {r.returncode}. "
            "Meldungen oben prüfen (Netzwerk, Playwright, Zielseite).",
            file=sys.stderr,
        )
    return r.returncode


if __name__ == "__main__":
    raise SystemExit(main())
