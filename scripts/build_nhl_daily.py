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

from scripts.fetch_starters_dailyfaceoff import fetch_dailyfaceoff_starters  # noqa: F401
from scripts.compute_rest import build_slim_rest

SCHEMA_VERSION = "1.0.6"
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
    "ST LOUIS BLUES": "STL",
    "TAMPA BAY LIGHTNING": "TBL",
    "TORONTO MAPLE LEAFS": "TOR",
    "VANCOUVER CANUCKS": "VAN",
    "VEGAS GOLDEN KNIGHTS": "VGK",
    "WASHINGTON CAPITALS": "WSH",
    "WINNIPEG JETS": "WPG",
    "UTAH MAMMOTH": "UTA",
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


# --------------------------- odds helpers ------------------------------------

def median_int(values: List[int]) -> Optional[float]:
    if not values:
        return None
    s = sorted(values)
    n = len(s)
    mid = n // 2
    if n % 2 == 1:
        return float(s[mid])
    return (s[mid - 1] + s[mid]) / 2.0


def pick_most_common_float(values: List[float]) -> Optional[float]:
    """
    Returns the most common value in the list. If tie, returns the smallest of the tied values.
    """
    if not values:
        return None
    counts: Dict[float, int] = {}
    for v in values:
        counts[v] = counts.get(v, 0) + 1
    best_count = max(counts.values())
    tied = [k for k, c in counts.items() if c == best_count]
    return min(tied)


def extract_market(bookmaker: Dict[str, Any], market_key: str) -> Optional[Dict[str, Any]]:
    markets = bookmaker.get("markets") or []
    for m in markets:
        if m.get("key") == market_key:
            return m
    return None


def slim_odds_current(odds_payload: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Produces a small odds structure per game:
      - moneyline: best and consensus (median) by team
      - totals: consensus line (most common point) plus best over/under prices at that line

    Returns: (slim_list, meta_validations)
    """
    slimmed: List[Dict[str, Any]] = []
    meta: Dict[str, Any] = {
        "games_with_h2h": 0,
        "games_with_totals": 0,
    }

    for g in odds_payload or []:
        game_id = g.get("id")
        commence = g.get("commence_time")
        home_team = g.get("home_team")
        away_team = g.get("away_team")
        if not (commence and home_team and away_team):
            continue

        bookmakers = g.get("bookmakers") or []

        # ---------------- Moneyline (h2h) ----------------
        best_home: Optional[Tuple[int, str]] = None  # (price, book_key)
        best_away: Optional[Tuple[int, str]] = None
        home_prices: List[int] = []
        away_prices: List[int] = []

        # ---------------- Totals ----------------
        # Collect available total points (lines), then pick consensus line (mode).
        total_points: List[float] = []
        # For each point line: track best over/under (highest price is best for bettor)
        best_over_by_point: Dict[float, Tuple[int, str]] = {}
        best_under_by_point: Dict[float, Tuple[int, str]] = {}

        for bm in bookmakers:
            bm_key = bm.get("key") or ""
            # h2h
            m_h2h = extract_market(bm, "h2h")
            if m_h2h:
                outcomes = m_h2h.get("outcomes") or []
                for o in outcomes:
                    name = o.get("name")
                    price = o.get("price")
                    if name == home_team and isinstance(price, int):
                        home_prices.append(pri_
