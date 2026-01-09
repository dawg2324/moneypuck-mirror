#!/usr/bin/env python3
from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import sys
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd

from scripts.fetch_starters_dailyfaceoff import fetch_dailyfaceoff_starters
from scripts.compute_rest import build_slim_rest

SCHEMA_VERSION = "1.0.5"
SPORT_KEY = "icehockey_nhl"
DAILYFACEOFF_BASE = "https://www.dailyfaceoff.com"


# --------------------------- time helpers ------------------------------------

def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def et_today_date_str() -> str:
    try:
        from zoneinfo import ZoneInfo
        et = dt.datetime.now(ZoneInfo("America/New_York"))
        return et.date().isoformat()
    except Exception:
        return dt.datetime.now(dt.timezone.utc).date().isoformat()


# --------------------------- network helpers ---------------------------------

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def http_get_json(url: str, headers: Optional[Dict[str, str]] = None, timeout: int = 30) -> Tuple[Any, bytes]:
    req = Request(url, headers=headers or {})
    with urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
    return json.loads(raw.decode("utf-8")), raw


def http_get_bytes(url: str, headers: Optional[Dict[str, str]] = None, timeout: int = 30) -> bytes:
    req = Request(url, headers=headers or {})
    with urlopen(req, timeout=timeout) as resp:
        return resp.read()


def fetch_csv(url: str) -> Tuple[pd.DataFrame, str]:
    raw = http_get_bytes(url)
    df = pd.read_csv(pd.io.common.BytesIO(raw))
    return df, sha256_bytes(raw)


# --------------------------- odds: current -----------------------------------

def fetch_odds_current() -> Tuple[Optional[List[Dict[str, Any]]], Dict[str, Any], Optional[str]]:
    api_key = os.environ.get("ODDS_API_KEY", "").strip()
    if not api_key:
        return None, {"ok": False, "reason": "Missing ODDS_API_KEY env var"}, None

    regions = os.environ.get("ODDS_API_REGIONS", "us").strip()
    markets = os.environ.get("ODDS_API_MARKETS", "h2h,totals").strip()

    base_url = "https://api.the-odds-api.com/v4/sports/icehockey_nhl/odds"
    qs = urlencode(
        {
            "apiKey": api_key,
            "regions": regions,
            "markets": markets,
            "oddsFormat": "american",
            "dateFormat": "iso",
        }
    )
    url = f"{base_url}?{qs}"

    try:
        payload, raw = http_get_json(url)
        return payload, {"ok": True}, sha256_bytes(raw)
    except Exception as e:
        return None, {"ok": False, "error": str(e)}, None


# --------------------------- main --------------------------------------------

def main() -> int:
    season = os.environ.get("MP_SEASON", "2025").strip()

    out_json = os.environ.get("OUT_JSON", "data/nhl_daily_slim.json").strip()
    out_yml = os.environ.get("OUT_YML", "data/nhl_daily_slim.yml").strip()

    generated_at = utc_now_iso()
    data_date = et_today_date_str()

    source_status: Dict[str, Any] = {}
    validations: Dict[str, Any] = {}
    inputs_hash: Dict[str, Any] = {}
    slim: Dict[str, Any] = {}

    # ---------------- Odds ----------------
    odds_payload, odds_status, odds_sha = fetch_odds_current()
    source_status["odds_current"] = odds_status
    if odds_sha:
        inputs_hash["odds_current_sha256"] = odds_sha

    odds_payload = odds_payload or []
    slim["odds_current"] = odds_payload
    validations["odds_games_count"] = len(odds_payload)

    # ---------------- REST (FIXED, FULLY WIRED) ----------------
    slate_for_rest: List[Dict[str, Any]] = []

    for g in odds_payload:
        away = g.get("away_team")
        home = g.get("home_team")
        commence = g.get("commence_time")

        if not away or not home or not commence:
            continue

        game_key = f"{away}_vs_{home}_{commence[:10]}"

        slate_for_rest.append(
            {
                "game_key": game_key,
                "away_team": away,
                "home_team": home,
                "commence_time_utc": commence,
            }
        )

    slim["rest"] = build_slim_rest(slate_for_rest)
    validations["rest_games_count"] = len(slim["rest"])

    # ---------------- OUTPUT ----------------
    out_obj: Dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": generated_at,
        "data_date_et": data_date,
        "source_status": source_status,
        "validations": validations,
        "inputs_hash": inputs_hash,
        "slim": slim,
    }

    os.makedirs(os.path.dirname(out_json), exist_ok=True)
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(out_obj, f, ensure_ascii=False, separators=(",", ":"), sort_keys=False)

    try:
        import yaml
        with open(out_yml, "w", encoding="utf-8") as f:
            yaml.safe_dump(out_obj, f, sort_keys=False, allow_unicode=True)
    except Exception:
        pass

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
