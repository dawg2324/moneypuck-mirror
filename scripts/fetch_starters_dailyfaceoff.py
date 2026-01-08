# scripts/fetch_starters_dailyfaceoff.py
from __future__ import annotations

import datetime as dt
import json
import re
from html.parser import HTMLParser
from typing import Any, Dict, List, Optional, Tuple
from urllib.request import Request, urlopen

from scripts.team_map import normalize_team_name, team_abbr_from_any_label


DFO_BASE = "https://www.dailyfaceoff.com"
DFO_PATH_PATTERN = "/starting-goalies/{date_yyyy_mm_dd}"


class _TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._chunks: List[str] = []

    def handle_data(self, data: str) -> None:
        if data:
            self._chunks.append(data)

    def text(self) -> str:
        return "\n".join(self._chunks)


def http_get_text(url: str, headers: Optional[Dict[str, str]] = None, timeout: int = 30) -> str:
    req = Request(url, headers=headers or {})
    with urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
    return raw.decode("utf-8", errors="replace")


def _utc_iso(dt_obj: dt.datetime) -> str:
    if dt_obj.tzinfo is None:
        dt_obj = dt_obj.replace(tzinfo=dt.timezone.utc)
    return dt_obj.astimezone(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_iso_z(s: str) -> Optional[dt.datetime]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.datetime.fromisoformat(s)
    except Exception:
        return None


def _normalize_status(raw: str) -> str:
    t = (raw or "").strip().lower()
    if "confirmed" in t:
        return "confirmed"
    return "projected"


def _extract_lines_from_html(html: str) -> List[str]:
    parser = _TextExtractor()
    parser.feed(html)
    text = parser.text()

    # Normalize whitespace and split
    lines = [ln.strip() for ln in text.split("\n")]
    lines = [ln for ln in lines if ln]

    return lines


_GAME_RE = re.compile(r"^(?P<away>.+?)\s+at\s+(?P<home>.+?)$")
_NAME_RE = re.compile(r"^[A-Za-z .'\-]+$")


def _scan_goalie_block(lines: List[str], start_idx: int) -> Tuple[Optional[str], Optional[str], Optional[dt.datetime], int]:
    """
    Scans forward for:
      goalie name (2+ words)
      status (Confirmed/Expected/Likely/Unconfirmed/Projected)
      last updated iso timestamp (optional)
    Returns (name, status_raw, updated_dt, next_index).
    """
    n = len(lines)
    j = start_idx

    name: Optional[str] = None
    while j < n:
        s = lines[j]
        if _NAME_RE.match(s) and len(s.split()) >= 2:
            name = s
            j += 1
            break
        # stop early if a new matchup begins
        if _GAME_RE.match(s):
            return None, None, None, j
        j += 1

    if name is None:
        return None, None, None, j

    status_raw: Optional[str] = None
    while j < n:
        s = lines[j].strip()
        sl = s.lower()
        if sl in {"confirmed", "expected", "likely", "unconfirmed", "projected"}:
            status_raw = s
            j += 1
            break
        # If we hit an ISO timestamp immediately, status is missing
        if _parse_iso_z(s) is not None:
            status_raw = ""
            break
        if _GAME_RE.match(s):
            break
        j += 1

    updated: Optional[dt.datetime] = None
    while j < n:
        maybe = _parse_iso_z(lines[j])
        if maybe is not None:
            updated = maybe
            j += 1
            break
        if _GAME_RE.match(lines[j]):
            break
        j += 1

    return name, status_raw, updated, j


def fetch_dailyfaceoff_starters(date_et: str) -> List[Dict[str, Any]]:
    """
    date_et: YYYY-MM-DD
    returns slim["starters"] list with strict schema.
    """
    url = DFO_BASE + DFO_PATH_PATTERN.format(date_yyyy_mm_dd=date_et)
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; moneypuck-mirror/nhl_daily_slim; +https://github.com/dawg2324/moneypuck-mirror)"
    }

    html = http_get_text(url, headers=headers, timeout=30)
    lines = _extract_lines_from_html(html)

    starters: List[Dict[str, Any]] = []

    i = 0
    n = len(lines)
    while i < n:
        m = _GAME_RE.match(lines[i])
        if not m:
            i += 1
            continue

        away_full = normalize_team_name(m.group("away"))
        home_full = normalize_team_name(m.group("home"))

        # The next line is often a game time ISO Z, but treat it as optional
        game_time = _parse_iso_z(lines[i + 1]) if (i + 1) < n else None

        # Scan for away and home goalie blocks after matchup header
        j0 = i + 1
        away_goalie, away_status_raw, away_updated, j1 = _scan_goalie_block(lines, j0)
        home_goalie, home_status_raw, home_updated, j2 = _scan_goalie_block(lines, j1)

        # If either goalie missing, skip this matchup
        if not away_goalie or not home_goalie:
            i += 1
            continue

        try:
            away_abbr = team_abbr_from_any_label(away_full)
            home_abbr = team_abbr_from_any_label(home_full)
        except KeyError:
            # Team label unknown, skip
            i = max(i + 1, j2)
            continue

        last_updated = away_updated or home_updated or game_time or dt.datetime.now(dt.timezone.utc)

        starters.append(
            {
                "game_key": f"{away_abbr}_vs_{home_abbr}_{date_et}",
                "date_et": date_et,
                "away": {"team": away_abbr, "goalie": away_goalie, "status": _normalize_status(away_status_raw or "")},
                "home": {"team": home_abbr, "goalie": home_goalie, "status": _normalize_status(home_status_raw or "")},
                "source": {"site": "dailyfaceoff", "url": url, "last_updated_utc": _utc_iso(last_updated)},
            }
        )

        i = max(i + 1, j2)

    starters.sort(key=lambda x: x["game_key"])
    return starters


def main() -> int:
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--date-et", required=True)
    args = ap.parse_args()

    starters = fetch_dailyfaceoff_starters(args.date_et)
    print(json.dumps({"date_et": args.date_et, "starters": starters}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
