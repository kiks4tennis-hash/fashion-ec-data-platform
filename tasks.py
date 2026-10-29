"""Cross-platform developer commands (Windows, macOS and Linux).

Usage:
    python tasks.py install   # editable install with dev dependencies
    python tasks.py lint      # ruff check + format check
    python tasks.py format    # auto-fix lint issues and format code
    python tasks.py test      # run pytest
    python tasks.py check     # lint + test (same checks as CI)
    python tasks.py clean     # remove caches and build artifacts
"""

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def run(*module_args: str) -> None:
    """Run `python -m <module_args>` in the repo root; exit on failure."""
    print("+ python -m", " ".join(module_args), flush=True)
    result = subprocess.run([sys.executable, "-m", *module_args], cwd=ROOT)
    if result.returncode != 0:
        sys.exit(result.returncode)


def install() -> None:
    run("pip", "install", "-e", ".[dev]")


def lint() -> None:
    run("ruff", "check", ".")
    run("ruff", "format", "--check", ".")


def format_code() -> None:
    run("ruff", "check", "--fix", ".")
    run("ruff", "format", ".")


def test() -> None:
    run("pytest")


def check() -> None:
    lint()
    test()


def clean() -> None:
    names = {".pytest_cache", ".ruff_cache", "build", "dist", "__pycache__"}
    for path in ROOT.rglob("*"):
        if ".venv" in path.parts or ".git" in path.parts:
            continue
        if path.is_dir() and (path.name in names or path.name.endswith(".egg-info")):
            shutil.rmtree(path, ignore_errors=True)
            print(f"removed {path.relative_to(ROOT)}")


COMMANDS = {
    "install": install,
    "lint": lint,
    "format": format_code,
    "test": test,
    "check": check,
    "clean": clean,
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Developer commands for this repository.")
    parser.add_argument("command", choices=COMMANDS)
    args = parser.parse_args()
    COMMANDS[args.command]()


if __name__ == "__main__":
    main()
