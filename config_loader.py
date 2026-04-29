"""
Config loader — finds and loads the shared .env file, normalizing env var names.

Looks in these locations in order:
    1. OPTIONS_EDGE_ENV_FILE environment variable (explicit override)
    2. ./.env (current project root)
    3. C:\\Users\\dange\\OneDrive\\Documents\\Claude Projects\\options-edge\\.env

Maps legacy variable names to the names broker/alpaca.py expects:
    ALPACA_PAPER_KEY    -> ALPACA_API_KEY   (when ALPACA_PAPER=true/unset)
    ALPACA_PAPER_SECRET -> ALPACA_API_SECRET
    ALPACA_LIVE_KEY     -> ALPACA_API_KEY   (when ALPACA_PAPER=false)
    ALPACA_LIVE_SECRET  -> ALPACA_API_SECRET

Import this module once at the top of any entry-point script before
importing broker code. It's idempotent.
"""

from __future__ import annotations

import os
from pathlib import Path

_CANDIDATE_PATHS = [
    Path(__file__).resolve().parent / ".env",
]

_LOADED = False


def _env_bool(name: str, default: bool) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y"}


def load_env(verbose: bool = False) -> str | None:
    """
    Load the first .env found, map Alpaca paper/live -> API key names.

    Returns the path loaded, or None if none found.
    Idempotent — only loads once per process.
    """
    global _LOADED
    if _LOADED:
        return None

    try:
        from dotenv import load_dotenv
    except ImportError:
        if verbose:
            print("[config_loader] python-dotenv not installed; cannot load .env")
        return None

    # 1. Explicit override
    override = os.environ.get("OPTIONS_EDGE_ENV_FILE")
    paths = [Path(override)] if override else []
    paths.extend(_CANDIDATE_PATHS)

    loaded_path: Path | None = None
    for p in paths:
        if p.exists() and p.is_file():
            load_dotenv(p, override=False)  # don't clobber real env vars
            loaded_path = p
            break

    if loaded_path is None:
        if verbose:
            print("[config_loader] no .env found in known locations")
        _LOADED = True
        return None

    # Map legacy names -> canonical names (only if canonical not already set)
    use_paper = _env_bool("ALPACA_PAPER", True)
    if use_paper:
        _set_if_missing("ALPACA_API_KEY", os.environ.get("ALPACA_PAPER_KEY"))
        _set_if_missing("ALPACA_API_SECRET", os.environ.get("ALPACA_PAPER_SECRET"))
    else:
        _set_if_missing("ALPACA_API_KEY", os.environ.get("ALPACA_LIVE_KEY"))
        _set_if_missing("ALPACA_API_SECRET", os.environ.get("ALPACA_LIVE_SECRET"))

    # Ensure ALPACA_PAPER is set explicitly so downstream code can trust it
    if os.environ.get("ALPACA_PAPER") is None:
        os.environ["ALPACA_PAPER"] = "true"

    if verbose:
        have_key = bool(os.environ.get("ALPACA_API_KEY"))
        have_sec = bool(os.environ.get("ALPACA_API_SECRET"))
        print(f"[config_loader] loaded {loaded_path}")
        print(f"[config_loader] ALPACA_API_KEY: {'set' if have_key else 'MISSING'}")
        print(f"[config_loader] ALPACA_API_SECRET: {'set' if have_sec else 'MISSING'}")
        print(f"[config_loader] ALPACA_PAPER: {os.environ.get('ALPACA_PAPER')}")

    _LOADED = True
    return str(loaded_path)


def _set_if_missing(name: str, value: str | None) -> None:
    if value and not os.environ.get(name):
        os.environ[name] = value


# Auto-load on import for convenience
load_env(verbose=False)


if __name__ == "__main__":
    # Re-run explicitly for verbose diagnostic
    _LOADED = False
    load_env(verbose=True)
