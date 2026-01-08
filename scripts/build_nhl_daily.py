# scripts/build_nhl_daily.py

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any

# ... your existing imports ...

from scripts.fetch_starters_dailyfaceoff import fetch_dailyfaceoff_starters


def _safe_set_source_status(slim: dict[str, Any], key: str, ok: bool, meta: dict[str, Any] | None = None) -> None:
    if "source_status" not in slim or not isinstance(slim["source_status"], dict):
        slim["source_status"] = {}
    slim["source_status"][key] = {"ok": bool(ok)}
    if meta:
        slim["source_status"][key]["meta"] = meta


def _validate_starters(starters: Any) -> tuple[bool, str | None]:
    if starters is None:
        return True, None  # allow missing if scrape failed, source_status will show it
    if not isinstance(starters, list):
        return False, "slim.starters must be a list"

    required_top = {"game_key", "date_et", "away", "home", "source"}
    required_side = {"team", "goalie", "status"}
    required_source = {"site", "url", "last_updated_utc"}

    for idx, g in enumerate(starters):
        if not isinstance(g, dict):
            return False, f"slim.starters[{idx}] must be an object"
        if not required_top.issubset(g.keys()):
            return False, f"slim.starters[{idx}] missing keys: {sorted(required_top - set(g.keys()))}"

        for side_key in ("away", "home"):
            side = g.get(side_key)
            if not isinstance(side, dict):
                return False, f"slim.starters[{idx}].{side_key} must be an object"
            if not required_side.issubset(side.keys()):
                return False, f"slim.starters[{idx}].{side_key} missing keys: {sorted(required_side - set(side.keys()))}"
            if side["status"] not in ("confirmed", "projected"):
                return False, f"slim.starters[{idx}].{side_key}.status must be confirmed|projected"

        src = g.get("source")
        if not isinstance(src, dict):
            return False, f"slim.starters[{idx}].source must be an object"
        if not required_source.issubset(src.keys()):
            return False, f"slim.starters[{idx}].source missing keys: {sorted(required_source - set(src.keys()))}"
        if src.get("site") != "dailyfaceoff":
            return False, f"slim.starters[{idx}].source.site must be dailyfaceoff"

    return True, None


def build_daily_slim(...) -> dict[str, Any]:
    slim: dict[str, Any] = {}

    # ... your existing pipeline:
    # - determine data_date_et
    # - fetch odds
    # - fetch MoneyPuck teams/goalies
    # - compute projections
    # etc.

    # Example: choose the starters slate date
    # If you want "today+1", do it here. Otherwise use slim["data_date_et"].
    date_et = slim.get("data_date_et")
    if not date_et:
        # fallback to tomorrow in ET logic if your script needs it
        # (replace with your existing date logic)
        date_et = (datetime.utcnow() - timedelta(hours=5) + timedelta(days=1)).date().isoformat()
        slim["data_date_et"] = date_et

    # Fetch starters
    try:
        starters = fetch_dailyfaceoff_starters(date_et=date_et)
        slim["starters"] = starters
        _safe_set_source_status(
            slim,
            "starters_dailyfaceoff",
            ok=True,
            meta={"url": f"https://www.dailyfaceoff.com/starting-goalies/{date_et}", "count": len(starters)},
        )
    except Exception as e:
        # Keep build running, but record failure
        slim["starters"] = []
        _safe_set_source_status(
            slim,
            "starters_dailyfaceoff",
            ok=False,
            meta={"url": f"https://www.dailyfaceoff.com/starting-goalies/{date_et}", "error": str(e)},
        )

    # Validations
    slim["validations"] = slim.get("validations", {})
    if not isinstance(slim["validations"], dict):
        slim["validations"] = {}

    slim["validations"]["starters_count"] = len(slim.get("starters", []) or [])

    ok, err = _validate_starters(slim.get("starters"))
    slim["validations"]["starters_schema_ok"] = bool(ok)
    if not ok:
        slim["validations"]["starters_schema_error"] = err

    # ... write JSON output as you already do ...
    return slim
