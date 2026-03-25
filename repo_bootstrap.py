"""Checkout layout: put ``src`` on ``sys.path`` and load the repo ``.env`` (for ``pip install``-less runs)."""

from __future__ import annotations

import sys
from pathlib import Path


def find_repo_root(start_file: Path) -> Path:
    cur = start_file.resolve().parent
    for _ in range(64):
        if (cur / "pyproject.toml").is_file() and (cur / "src" / "qbo_pipeline").is_dir():
            return cur
        if cur.parent == cur:
            break
        cur = cur.parent
    raise RuntimeError(
        "Could not find project root (pyproject.toml + src/qbo_pipeline). "
        f"Started from {start_file}"
    )


def ensure_src_on_path(*, start_file: Path) -> Path:
    root = find_repo_root(start_file)
    src = root / "src"
    if not src.is_dir():
        raise RuntimeError(f"Expected {src}")
    s = str(src)
    if s not in sys.path:
        sys.path.insert(0, s)
    return root


def configure_for_checkout(start_file: Path) -> Path:
    """Make ``qbo_pipeline`` importable and load ``<repo>/.env`` (does not override existing env)."""
    from dotenv import load_dotenv

    root = ensure_src_on_path(start_file=start_file)
    load_dotenv(root / ".env", override=False)
    return root
