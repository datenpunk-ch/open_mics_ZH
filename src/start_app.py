#!/usr/bin/env python3
from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    app = root / "src" / "open_mics_app.py"
    if not app.is_file():
        print(f"[start_app] Missing app file: {app}", file=sys.stderr)
        return 2

    cmd = [sys.executable, "-m", "streamlit", "run", str(app), *sys.argv[1:]]
    print("+", " ".join(cmd))
    return subprocess.call(cmd, cwd=str(root))


if __name__ == "__main__":
    raise SystemExit(main())

