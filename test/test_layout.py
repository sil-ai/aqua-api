"""Guard against the #837 regression: test files that never run in CI.

`make test` — and all three CI workflows (pr/main/release) — run `pytest test`,
so any file matching pytest's default globs (`test_*.py` / `*_test.py`) that
lives *outside* the `test/` directory is silently skipped. Four such files sat
at the repo root for months, passing locally but never running in CI. This test
fails loudly if one reappears, so the orphaned-test problem can't recur unnoticed.
"""

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
TEST_DIR = REPO_ROOT / "test"

# Directories that legitimately contain test-named files we never collect
# (third-party packages, caches, VCS) — pruned from the walk.
_IGNORE_DIRS = {
    "venv",
    ".venv",
    "env",
    ".git",
    ".hg",
    "node_modules",
    "__pycache__",
    ".mypy_cache",
    ".pytest_cache",
    ".tox",
    "site-packages",
    "build",
    "dist",
    ".eggs",
}


def _looks_like_test_file(name: str) -> bool:
    return name.endswith(".py") and (
        name.startswith("test_") or name.endswith("_test.py")
    )


def test_no_test_files_outside_test_dir():
    stray = []
    for dirpath, dirnames, filenames in os.walk(REPO_ROOT):
        dirnames[:] = [d for d in dirnames if d not in _IGNORE_DIRS]
        here = Path(dirpath)
        if here == TEST_DIR or TEST_DIR in here.parents:
            continue  # inside test/ — these are collected by `pytest test`
        for name in filenames:
            if _looks_like_test_file(name):
                stray.append(str((here / name).relative_to(REPO_ROOT)))

    assert not stray, (
        "Found test files outside test/ that CI never runs "
        "(`make test` == `pytest test`): "
        f"{sorted(stray)}. Move them into test/ (see issue #837)."
    )
