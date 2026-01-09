#!/usr/bin/env python3
from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd

from scripts.fetch_starters_dailyfaceoff import fetch_dailyfaceoff_starters
from scripts.compute_rest import build_slim_rest

SCHEMA_VERSION = "1.0.5"
SPORT_KEY = "icehockey_nhl"
DAILYFACEOFF_BASE = "https://www.dailyfaceoff.com"


# --------------------------- TEAM NAME → ABBREV -------------------------------

TEAM_NAME_TO_ABBREV = {
    "ANAHEIM DUCKS": "ANA",
    "ARIZONA COYOTES": "ARI",
    "BOSTON BRUINS": "BOS",
    "BUFFALO SABRES": "BUF",
    "CALGARY FLAMES": "CGY",
    "CAROLINA HURRICANES": "CAR",
    "CHICAGO BLACKHAWKS": "CHI",
    "COLORADO AVALANCHE": "COL",
    "COLUMBUS BLUE JACKETS": "CBJ",
    "DALLAS STARS": "DAL",
    "DETROIT RED WINGS": "DET",
    "EDMONTON OILERS": "EDM",
    "FLORIDA PANTHERS": "FLA",
    "LOS ANGELES KINGS": "LAK",
    "MINNESOTA WILD": "MIN",
    "MONTREAL CANADIENS": "MTL",
    "NASHVILLE PREDATORS": "NSH",
    "NEW JERSEY DEVILS": "NJD",
    "NEW YORK ISLANDERS": "NYI",
    "NEW YORK RANGERS": "NYR",
    "OTTAWA SENATORS": "OTT",
    "PHILADELPHIA FLYERS": "PHI",
    "PITTSBURGH PENGUINS": "PIT",
    "SAN JOSE SHARKS": "SJS",
    "SEATTLE KRAKEN": "SEA",
    "ST. LOUIS BLUES": "STL",
    "TAMPA BAY LIGHTNING": "TBL",
    "TORONTO MAPLE LEAFS": "TOR",
    "VANCOUVER CANUCKS": "VAN",
    "VEGAS GOLDEN KNIGHTS": "VGK",
    "WASHINGTON CAPITALS": "WSH",
    "WINNIPEG JETS": "WPG",
    "UTAH MAMMOTH": "UTA",
    "ST LOUIS BLUES": "STL",
}


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
    generated_at = utc_now_iso()
    data_date = et_today_date_str()

    source_status: Dict[str, Any] = {}
    validations: Dict[str, Any] = {}
    inputs_hash: Dict[str, Any] = {}
    slim: Dict[str, Any] = {}

    # ---------------- Odds ----------------
    odds_payload, odds_status, odds_sha = fetch_odds_current()
    source_status["odds_current"] = odds_status

    # Store input hashes when available (helps debugging + reproducibility)
    if odds_sha:
        inputs_hash["odds_current_sha256"] = odds_sha

    odds_payload = odds_payload or []
    slim["odds_current"] = odds_payload
    validations["odds_games_count"] = len(odds_payload)

    # ---------------- REST (FIXED) ----------------
    slate_for_rest: List[Dict[str, Any]] = []

    for g in odds_payload:
        away_name = g.get("away_team", "").upper()
        home_name = g.get("home_team", "").upper()
        commence = g.get("commence_time")

        away = TEAM_NAME_TO_ABBREV.get(away_name)
        home = TEAM_NAME_TO_ABBREV.get(home_name)

        if not away or not home or not commence:
            continue  # skip safely instead of crashing

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
    out_obj = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": generated_at,
        "data_date_et": data_date,
        "source_status": source_status,
        "validations": validations,
        "inputs_hash": inputs_hash,
        "slim": slim,
    }

    # Write pretty (multi-line) JSON so RAW view + web dumps can parse reliably
    out_path = Path("data/nhl_daily_slim.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="\n") as f:
        json.dump(
            out_obj,
            f,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        f.write("\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
