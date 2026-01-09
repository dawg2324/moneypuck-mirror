#!/usr/bin/env python3
"""
fetch_starters_dailyfaceoff.py

Fetch starting goalies from DailyFaceoff date page:
  https://www.dailyfaceoff.com/starting-goalies/YYYY-MM-DD

If parsing returns 0 records, saves HTML to:
  data/debug/dailyfaceoff_starting_goalies_YYYY-MM-DD.html
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

from scripts.team_map import team_abbr_from_any_label

BASE = "https://www.dailyfaceoff.com"


# --------------------------- helpers -----------------------------------------

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def http_get_html(url: str, timeout: int = 30) -> Tuple[str, bytes, Dict[str, str]]:
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
        resp_headers: Dict[str, str] = {}
        for k in ["content-type", "cache-control", "server", "cf-ray", "x-cache"]:
            v = resp.headers.get(k)
            if v:
                resp_headers[k] = v

    html = raw.decode("utf-8", errors="replace")
    return html, raw, resp_headers


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


def normalize_status(raw: str) -> str:
    s = (raw or "").strip().lower()
    # DailyFaceoff uses strings like "Confirmed", "Expected", etc.
    if "confirm" in s:
        return "confirmed"
    return "projected"


def extract_next_data_json(html: str) -> Optional[Dict[str, Any]]:
    m = re.search(r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.S)
    if not m:
        return None
    blob = m.group(1).strip()
    try:
        return json.loads(blob)
    except Exception:
        return None


def best_last_updated_utc_from_game_rows(rows: List[Dict[str, Any]]) -> Optional[str]:
    # Rows often include timestamps like:
    # homeNewsCreatedAt: '2026-01-08T23:34:39.410Z'
    # awayNewsCreatedAt: '2026-01-08T23:34:39.410Z'
    stamps: List[str] = []
    for r in rows:
        for k in ["homeNewsCreatedAt", "awayNewsCreatedAt", "newsCreatedAt", "updatedAt", "lastUpdatedAt"]:
            v = r.get(k)
            if isinstance(v, str) and "T" in v and v.endswith("Z"):
                stamps.append(v)
    if not stamps:
        return None
    return max(stamps)


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
    debug_path: Optional[str] = None

    next_data = extract_next_data_json(html)
    rows: List[Dict[str, Any]] = []

    if next_data:
        try:
            rows_raw = next_data.get("props", {}).get("pageProps", {}).get("data", [])
            if isinstance(rows_raw, list):
                rows = [r for r in rows_raw if isinstance(r, dict)]
        except Exception:
            rows = []

    last_updated_utc = best_last_updated_utc_from_game_rows(rows) or utc_now_iso()

    # Parse each game row into your normalized schema
    for r in rows:
        away_team_label = r.get("awayTeamName") or r.get("away_team") or r.get("awayTeam")
        home_team_label = r.get("homeTeamName") or r.get("home_team") or r.get("homeTeam")

        away_goalie_name = (r.get("awayGoalieName") or "").strip() or None
        home_goalie_name = (r.get("homeGoalieName") or "").strip() or None

        away_status = normalize_status(r.get("awayNewsStrengthName") or r.get("awayGoalieStatus") or "")
        home_status = normalize_status(r.get("homeNewsStrengthName") or r.get("homeGoalieStatus") or "")

        if not away_team_label or not home_team_label:
            continue
        if not away_goalie_name and not home_goalie_name:
            continue

        away_team = team_abbr_from_any_label(str(away_team_label))
        home_team = team_abbr_from_any_label(str(home_team_label))
        if not away_team or not home_team:
            continue

        game_key = f"{away_team}_vs_{home_team}_{date_et}"

        starters.append(
            {
                "game_key": game_key,
                "date_et": date_et,
                "away": {"team": away_team, "goalie": away_goalie_name, "status": away_status},
                "home": {"team": home_team, "goalie": home_goalie_name, "status": home_status},
                "source": {"site": "dailyfaceoff", "url": url, "last_updated_utc": last_updated_utc},
            }
        )

    # Dedup and sort
    dedup: Dict[str, Dict[str, Any]] = {}
    for s in starters:
        dedup[s["game_key"]] = s
    starters = list(dedup.values())
    starters.sort(key=lambda x: x["game_key"])

    ok = True
    reason = None

    if len(starters) == 0:
        debug_path = write_debug_html(date_et, html)
        ok = False
        reason = "Parsed 0 starters (blocked/selector drift). Debug HTML saved."

    status: Dict[str, Any] = {
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
