"""
cache.py — Transparent disk + remote cache for Bronzeville pipeline data fetches.

Load order for every key:
  1. Local disk  (_cache/<key>.json) — fastest, always tried first
  2. Netlify CDN (tworivers.us/bronzeville/cache/<key>.json) — seeded after every
     successful pipeline run; lets any machine run the pipeline without hitting
     the original flaky APIs
  3. Original API — last resort, result is written to local disk for next time

Usage:
    from cache import load_cache, save_cache

    rows = load_cache("osm_landmarks")
    if rows is None:
        rows = <fetch from API>
        save_cache("osm_landmarks", rows)

Delete _cache/ locally to force a full refresh from Netlify or the APIs.
"""
from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_CACHE_DIR   = Path(__file__).parent / "_cache"
_DEFAULT_TTL_DAYS = 7

# Remote cache served from the deployed Netlify site.
# After each pipeline run, cache files are copied to the public/ dir and
# committed to git, making them available here automatically.
_REMOTE_BASE = "https://www.tworivers.us/bronzeville/cache"


def _cache_path(key: str) -> Path:
    _CACHE_DIR.mkdir(exist_ok=True)
    return _CACHE_DIR / f"{key}.json"


def _load_local(key: str, max_age_days: float) -> Any | None:
    path = _cache_path(key)
    if not path.exists():
        return None
    if max_age_days > 0:
        age = time.time() - path.stat().st_mtime
        if age > max_age_days * 86_400:
            logger.debug(f"Local cache expired for '{key}' ({age/86400:.1f} d old)")
            return None
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        # Reject empty lists/dicts that were saved during a failed fetch
        if isinstance(data, (list, dict)) and len(data) == 0:
            return None
        logger.info(f"Local cache hit: '{key}' ({path.stat().st_size // 1024} KB)")
        return data
    except Exception as exc:
        logger.warning(f"Local cache read error for '{key}': {exc}")
        return None


def _load_remote(key: str) -> Any | None:
    """Try to fetch the cache file from the Netlify-hosted remote cache."""
    try:
        import requests
        url = f"{_REMOTE_BASE}/{key}.json"
        resp = requests.get(url, timeout=15)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, (list, dict)) and len(data) == 0:
            return None
        logger.info(f"Remote cache hit: '{key}' from {url} ({len(resp.content)//1024} KB)")
        # Persist locally so subsequent calls are instant
        _save_local(key, data)
        return data
    except Exception as exc:
        logger.debug(f"Remote cache miss for '{key}': {exc}")
        return None


def _save_local(key: str, data: Any) -> None:
    path = _cache_path(key)
    try:
        with path.open("w", encoding="utf-8") as f:
            json.dump(data, f, separators=(",", ":"))
        logger.debug(f"Cache saved locally: '{key}' ({path.stat().st_size // 1024} KB)")
    except Exception as exc:
        logger.warning(f"Cache write error for '{key}': {exc}")


def load_cache(key: str, max_age_days: float = _DEFAULT_TTL_DAYS) -> Any | None:
    """
    Load cached data for *key* — tries local disk first, then Netlify CDN.
    Returns None if both miss (caller should fetch from the real API).
    """
    data = _load_local(key, max_age_days)
    if data is not None:
        return data
    return _load_remote(key)


def save_cache(key: str, data: Any) -> None:
    """
    Persist *data* to local disk.  The CI/deploy step copies _cache/ to the
    public/ directory so it gets served from Netlify on the next git push.
    """
    if isinstance(data, (list, dict)) and len(data) == 0:
        logger.debug(f"Skipping empty cache write for '{key}'")
        return
    _save_local(key, data)
