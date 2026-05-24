"""Environment loading helpers.

Wraps ``python-dotenv`` so callers do not import it directly. This lets us
swap implementations later (e.g. ``os.environ.update(...)`` from a YAML file)
without touching every CLI.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable, Optional

from dotenv import load_dotenv as _load_dotenv


def load_env(
    path: Optional[Path] = None,
    *,
    candidates: Optional[Iterable[Path]] = None,
    override: bool = False,
) -> Optional[Path]:
    """Load environment variables from a ``.env`` file.

    If ``path`` is given, load that file. Otherwise try ``candidates`` in order
    and load the first one that exists; when no candidates are provided, fall
    back to ``python-dotenv`` defaults (CWD and parents).

    Returns the path actually loaded, or ``None`` when nothing was loaded.
    """
    if path is not None:
        if path.exists():
            _load_dotenv(path, override=override)
            return path
        return None

    for cand in candidates or ():
        if cand.exists():
            _load_dotenv(cand, override=override)
            return cand

    _load_dotenv(override=override)
    return None


def get_env(name: str, default: Optional[str] = None) -> Optional[str]:
    """Convenience wrapper for ``os.environ.get`` with consistent typing."""
    return os.environ.get(name, default)


def get_envs(names: list, default: Optional[str] = None) -> list[Optional[str]]:
    """Convenience wrapper for ``os.environ.get`` with consistent typing."""
    return [os.environ.get(name, default) for name in names]