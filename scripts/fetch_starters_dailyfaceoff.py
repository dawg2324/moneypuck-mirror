# scripts/fetch_starters_dailyfaceoff.py
from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Optional, Tuple

import requests
from bs4 import BeautifulSoup

from scripts.team_map import team_abbr_from_any_label, normalize_team_name


DFO_BASE = "https://www.dailyfaceoff.com"
DFO_PATH_PATTERN = "/starting-goalies/{date_yyyy_mm_dd}"
DEFAULT_TIMEOUT = 25


@dataclass(frozen=True)
class StarterSide:
    team: str  # abbreviation
    goalie: str
    status: str  # confirmed | projected


def _utc_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_iso_z(s: str) -> Optional[datetime]:
    s = (s or "").strip()
    if not s:
        return None
    # Handles: 2025-10-22T15:59:03.447Z and 2025-10-22T23:00:00.000Z
    try:
        if s.endswith("Z"):
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def _normalize_status(raw_status: str) -> str:
    rs = (raw_status or "").strip().lower()
    if "confirmed" in rs:
        return "confirmed"
    return "projected"


def _clean_lines(lines: Iterable[str]) -> list[str]:
    out: list[str] = []
    for x in lines:
        t = (x or "").strip()
        if not t:
            continue
        out.append(t)
    return out


_GAME_RE = re.compile(r"^(?P<away>.+?)\s+at\s+(?P<home>.+?)$")


def _extract_text_lines(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")

    # Remove scripts/styles so text extraction is stable
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    text = soup.get_text("\n")
    lines = text.split("\n")
    return _clean_lines(lines)


def _parse_games_from_lines(lines: list[str], date_et: str, source_url: str) -> list[dict[str, Any]]:
    """
    DailyFaceoff page (server-rendered) commonly contains blocks like:
      "Minnesota Wild at New Jersey Devils"
      "2025-10-22T23:00:00.000Z"
      "Filip Gustavsson"
      "Confirmed"
      "2025-10-22T15:59:03.447Z"
      ...
      "Nico Daws"
      "Confirmed"
      "2025-10-20T16:32:31.647Z"
    (then next matchup) :contentReference[oaicite:0]{index=0}
    """
    starters: list[dict[str, Any]] = []

    i = 0
    n = len(lines)

    while i < n:
        m = _GAME_RE.match(lines[i])
        if not m:
            i += 1
            continue

        away_full = normalize_team_name(m.group("away"))
        home_full = normalize_team_name(m.group("home"))

        # Next non-empty line should be game time in ISO (Z)
        if i + 1 >= n:
            break
        game_time_utc = _parse_iso_z(lines[i + 1])

        # Heuristic: after game time, we expect away goalie name, status, updated time,
        # then home goalie name, status, updated time.
        # Because page includes lots of other text, we scan forward carefully.

        def scan_goalie_block(start_idx: int) -> Tuple[Optional[str], Optional[str], Optional[datetime], int]:
            j = start_idx
            # find a plausible goalie name line (letters/spaces, at least 2 words)
            name = None
            while j < n:
                s = lines[j]
                if re.match(r"^[A-Za-z .'-]+$", s) and len(s.split()) >= 2:
                    name = s
                    j += 1
                    break
                j += 1
            if name is None or j >= n:
                return None, None, None, j

            status = None
            while j < n:
                s = lines[j].strip()
                if s.lower() in {"confirmed", "expected", "likely", "unconfirmed", "projected"}:
                    status = s
                    j += 1
                    break
                # Sometimes "Confirmed" is present, sometimes other label.
                # If we hit an ISO timestamp before status, treat as missing status.
                if _parse_iso_z(s) is not None:
                    status = ""
                    break
                j += 1

            updated = None
            while j < n:
                dt = _parse_iso_z(lines[j])
                if dt is not None:
                    updated = dt
                    j += 1
                    break
                # stop if another matchup starts
                if _GAME_RE.match(lines[j]):
                    break
                j += 1

            return name, status, updated, j

        # Start scanning after the game time line
        j0 = i + 2
        away_goalie_name, away_status_raw, away_updated, j1 = scan_goalie_block(j0)
        home_goalie_name, home_status_raw, home_updated, j2 = scan_goalie_block(j1)

        # If we failed, skip this matchup and keep searching
        if not away_goalie_name or not home_goalie_name:
            i += 1
            continue

        away_abbr = team_abbr_from_any_label(away_full)
        home_abbr = team_abbr_from_any_label(home_full)

        last_updated = None
        if away_updated and home_updated:
            last_updated = max(away_updated, home_updated)
        else:
            last_updated = away_updated or home_updated or game_time_utc

        game_key = f"{away_abbr}_vs_{home_abbr}_{date_et}"

        starters.append(
            {
                "game_key": game_key,
                "date_et": date_et,
                "away": {
                    "team": away_abbr,
                    "goalie": away_goalie_name,
                    "status": _normalize_status(away_status_raw or ""),
                },
                "home": {
                    "team": home_abbr,
                    "goalie": home_goalie_name,
                    "status": _normalize_status(home_status_raw or ""),
                },
                "source": {
                    "site": "dailyfaceoff",
                    "url": source_url,
                    "last_updated_utc": _utc_iso(last_updated) if last_updated else None,
                },
            }
        )

        # Continue scanning from where we ended to avoid O(n^2)
        i = max(i + 1, j2)

    return starters


def fetch_dailyfaceoff_starters(date_et: str, session: Optional[requests.Session] = None) -> list[dict[str, Any]]:
    """
    Fetch starters for date_et (YYYY-MM-DD) from DailyFaceoff and return slim.starters list.
    """
    url = DFO_BASE + DFO_PATH_PATTERN.format(date_yyyy_mm_dd=date_et)

    sess = session or requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; nhl_daily_slim/1.0; +https://github.com/dawg2324/moneypuck-mirror)"
    }

    resp = sess.get(url, headers=headers, timeout=DEFAULT_TIMEOUT)
    resp.raise_for_status()

    lines = _extract_text_lines(resp.text)
    starters = _parse_games_from_lines(lines, date_et=date_et, source_url=url)

    # Stable sort for diffs
    starters.sort(key=lambda x: x.get("game_key", ""))

    return starters


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date-et", required=True, help="Slate date in ET, YYYY-MM-DD")
    args = ap.parse_args()

    starters = fetch_dailyfaceoff_starters(args.date_et)
    print(json.dumps({"date_et": args.date_et, "starters": starters}, indent=2))


if __name__ == "__main__":
    main()
