#!/usr/bin/env python3
"""
fetch_starters_dailyfaceoff.py

Fetch starting goalies from DailyFaceoff date page:
  https://www.dailyfaceoff.com/starting-goalies/YYYY-MM-DD

Output schema:
[
  {
    "game_key": "CAR_vs_ANA_2026-01-08",
    "date_et": "2026-01-08",
    "away": {"team":"ANA","goalie":"John Gibson","status":"projected"},
    "home": {"team":"CAR","goalie":"Pyotr Kochetkov","status":"confirmed"},
    "source": {"site":"dailyfaceoff","url":"...", "last_updated_utc":"..."}
  }
]

Notes:
- DailyFaceoff markup can change. We attempt:
  (A) Static HTML parse (best effort)
  (B) Next.js __NEXT_DATA__ JSON parse fallback
- If we still get 0 records, we save the HTML to data/debug/... for inspection.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup  # ensure beautifulsoup4 is in requirements

from scripts.team_map import team_abbr_from_any_label


BASE = "https://www.dailyfaceoff.com"


# --------------------------- helpers -----------------------------------------

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def http_get_html(url: str, timeout: int = 30) -> Tuple[str, bytes, Dict[str, str]]:
    """
    Returns (html_text, raw_bytes, response_headers_subset)
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": BASE + "/",
    }

    req = Request(url, headers=headers)
    with urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
        # Keep a small subset of headers for debugging
        resp_headers = {}
        for k in ["content-type", "cache-control", "server", "cf-ray", "x-cache"]:
            v = resp.headers.get(k)
            if v:
                resp_headers[k] = v

    html = raw.decode("utf-8", errors="replace")
    return html, raw, resp_headers


def normalize_status(raw: str) -> str:
    s = (raw or "").strip().lower()
    return "confirmed" if "confirm" in s else "projected"


def extract_last_updated_utc(text: str) -> Optional[str]:
    """
    Best effort. If the page contains something like "Last updated: Jan 8, 2026 4:21 PM ET"
    we convert ET to UTC ISO.
    If not found, return None.
    """
    if not text:
        return None

    m = re.search(r"last updated\s*:\s*([A-Za-z]{3,9})\s+(\d{1,2}),\s*(\d{4})\s+(\d{1,2}):(\d{2})\s*(AM|PM)\s*ET", text, re.I)
    if not m:
        return None

    mon_str, day, year, hh, mm, ap = m.groups()

    # month mapping
    months = {
        "jan": 1, "january": 1,
        "feb": 2, "february": 2,
        "mar": 3, "march": 3,
        "apr": 4, "april": 4,
        "may": 5,
        "jun": 6, "june": 6,
        "jul": 7, "july": 7,
        "aug": 8, "august": 8,
        "sep": 9, "sept": 9, "september": 9,
        "oct": 10, "october": 10,
        "nov": 11, "november": 11,
        "dec": 12, "december": 12,
    }

    mon = months.get(mon_str.strip().lower())
    if not mon:
        return None

    h = int(hh)
    if ap.upper() == "PM" and h != 12:
        h += 12
    if ap.upper() == "AM" and h == 12:
        h = 0

    # ET -> UTC: use zoneinfo if available
    try:
        from zoneinfo import ZoneInfo
        et = ZoneInfo("America/New_York")
        dt_et = dt.datetime(int(year), mon, int(day), h, int(mm), tzinfo=et)
        dt_utc = dt_et.astimezone(dt.timezone.utc)
        return dt_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    except Exception:
        return None


def ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(path)
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)


def write_debug_html(date_et: str, html: str) -> str:
    path = f"data/debug/dailyfaceoff_starting_goalies_{date_et}.html"
    ensure_parent_dir(path)
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    return path


# --------------------------- Next.js JSON utilities ---------------------------

def extract_next_data_json(html: str) -> Optional[Dict[str, Any]]:
    """
    DailyFaceoff is often a Next.js app. If so, page contains:
      <script id="__NEXT_DATA__" type="application/json">...</script>
    """
    m = re.search(r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.S)
    if not m:
        return None
    blob = m.group(1).strip()
    try:
        return json.loads(blob)
    except Exception:
        return None


def walk_find_candidate_games(obj: Any) -> List[Dict[str, Any]]:
    """
    Generic recursive search for dicts that look like a "game" record.
    Since we cannot rely on fixed keys, we search for dicts containing both teams and both goalies.
    """
    out: List[Dict[str, Any]] = []

    def rec(x: Any) -> None:
        if isinstance(x, dict):
            keys = set(x.keys())

            # These are common shapes across site variants
            # We accept a dict if it contains at least:
            # - away/home team labels
            # - away/home goalie names or nested goalie dicts
            possible_team_keys = [
                ("awayTeam", "homeTeam"),
                ("away_team", "home_team"),
                ("away", "home"),
            ]
            possible_goalie_keys = [
                ("awayGoalie", "homeGoalie"),
                ("away_goalie", "home_goalie"),
                ("awayStarter", "homeStarter"),
            ]

            has_teams = any(a in keys and b in keys for a, b in possible_team_keys)
            has_goalies = any(a in keys and b in keys for a, b in possible_goalie_keys)

            if has_teams and has_goalies:
                out.append(x)

            for v in x.values():
                rec(v)

        elif isinstance(x, list):
            for v in x:
                rec(v)

    rec(obj)
    return out


def coerce_goalie_name_and_status(v: Any) -> Tuple[Optional[str], str]:
    """
    v can be:
    - string goalie name
    - dict like {"name": "...", "status": "..."} or similar
    """
    if v is None:
        return None, "projected"
    if isinstance(v, str):
        name = v.strip()
        return (name or None), "projected"
    if isinstance(v, dict):
        # common fields
        name = (v.get("name") or v.get("fullName") or v.get("goalie") or v.get("playerName") or "").strip()
        status_raw = (v.get("status") or v.get("starterStatus") or v.get("label") or "").strip()
        return (name or None), normalize_status(status_raw)
    return None, "projected"


# --------------------------- main fetch --------------------------------------

@dataclass
class StartersFetchResult:
    starters: List[Dict[str, Any]]
    status: Dict[str, Any]


def fetch_dailyfaceoff_starters(date_et: str) -> StartersFetchResult:
    url = f"{BASE}/starting-goalies/{date_et}"

    html, raw, resp_headers = http_get_html(url)
    html_sha = sha256_bytes(raw)

    starters: List[Dict[str, Any]] = []
    last_updated_utc = extract_last_updated_utc(html)

    # ---- Attempt A: Static HTML parse (best effort)
    try:
        soup = BeautifulSoup(html, "html.parser")

        # This selector is intentionally broad; DailyFaceoff changes classes often.
        # We find text blocks that include "Confirmed" / "Expected" and a goalie-like name.
        page_text = soup.get_text(" ", strip=True)
        if not last_updated_utc:
            last_updated_utc = extract_last_updated_utc(page_text)

        # If you previously had a working selector, keep it here.
        # For now, we only treat static parse as optional; fallback to Next.js JSON is more reliable.
        # starters remains empty unless you implement a site-specific block parse.

    except Exception:
        pass

    # ---- Attempt B: Next.js __NEXT_DATA__ JSON parse
    if not starters:
        next_data = extract_next_data_json(html)
        if next_data:
            candidates = walk_find_candidate_games(next_data)

            for g in candidates:
                away_label = g.get("awayTeam") or g.get("away_team") or g.get("away")
                home_label = g.get("homeTeam") or g.get("home_team") or g.get("home")

                away_goalie_raw = g.get("awayGoalie") or g.get("away_goalie") or g.get("awayStarter")
                home_goalie_raw = g.get("homeGoalie") or g.get("home_goalie") or g.get("homeStarter")

                away_goalie_name, away_status = coerce_goalie_name_and_status(away_goalie_raw)
                home_goalie_name, home_status = coerce_goalie_name_and_status(home_goalie_raw)

                if not away_label or not home_label:
                    continue
                if not away_goalie_name and not home_goalie_name:
                    continue

                away_team = team_abbr_from_any_label(str(away_label))
                home_team = team_abbr_from_any_label(str(home_label))
                if not away_team or not home_team:
                    continue

                game_key = f"{away_team}_vs_{home_team}_{date_et}"

                starters.append(
                    {
                        "game_key": game_key,
                        "date_et": date_et,
                        "away": {"team": away_team, "goalie": away_goalie_name, "status": away_status},
                        "home": {"team": home_team, "goalie": home_goalie_name, "status": home_status},
                        "source": {
                            "site": "dailyfaceoff",
                            "url": url,
                            "last_updated_utc": last_updated_utc,
                        },
                    }
                )

            # Deduplicate by game_key in case multiple candidate matches
            dedup: Dict[str, Dict[str, Any]] = {}
            for r in starters:
                dedup[r["game_key"]] = r
            starters = list(dedup.values())
            starters.sort(key=lambda x: x["game_key"])

    # ---- If still empty: write debug HTML and mark failure-ish
    debug_path = None
    ok = True
    reason = None

    if len(starters) == 0:
        debug_path = write_debug_html(date_et, html)
        ok = False
        reason = "Parsed 0 starters (likely JS-rendered/blocked/selector drift). Debug HTML saved."

    status = {
        "ok": ok,
        "url": url,
        "count": len(starters),
        "html_sha256": html_sha,
        "resp_headers": resp_headers,
    }
    if reason:
        status["reason"] = reason
    if debug_path:
        status["debug_html_path"] = debug_path

    return StartersFetchResult(starters=starters, status=status)
