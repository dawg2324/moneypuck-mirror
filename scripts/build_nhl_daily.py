import base64
import csv
import hashlib
import io
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from dateutil import tz

NY_TZ = tz.gettz("America/New_York")

TEAMS_RAW = "https://raw.githubusercontent.com/dawg2324/moneypuck-mirror/main/data/teams.csv"
GOALIES_RAW = "https://raw.githubusercontent.com/dawg2324/moneypuck-mirror/main/data/goalies.csv"

TEAMS_API = "https://api.github.com/repos/dawg2324/moneypuck-mirror/contents/data/teams.csv?ref=main"
GOALIES_API = "https://api.github.com/repos/dawg2324/moneypuck-mirror/contents/data/goalies.csv?ref=main"

SLIM_PATH = "data/nhl_daily_slim.json"


@dataclass
class SourceStatus:
    ok: bool
    url: Optional[str] = None
    error: Optional[str] = None
    reason: Optional[str] = None
    snapshot_requested: Optional[str] = None


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def now_et() -> datetime:
    return datetime.now(tz=NY_TZ)


def should_run_now_et(target_hour: int = 10, window_minutes: int = 25) -> bool:
    """
    DST-proof gating:
    Workflow runs twice a day (14:05 and 15:05 UTC).
    This function allows execution only near 10:00 AM ET.
    """
    t = now_et()
    if t.hour != target_hour:
        return False
    return t.minute <= window_minutes


def http_get_bytes(url: str, timeout: int = 30, headers: Optional[Dict[str, str]] = None) -> bytes:
    r = requests.get(url, timeout=timeout, headers=headers or {})
    r.raise_for_status()
    return r.content


def fetch_github_contents_api_text(url: str) -> str:
    # Unauthenticated is OK for public repos, but rate-limited. Works fine for your use.
    r = requests.get(url, timeout=30, headers={"Accept": "application/vnd.github+json"})
    r.raise_for_status()
    j = r.json()
    if "content" not in j:
        raise RuntimeError("Contents API response missing 'content'")
    content_b64 = j["content"]
    content_bytes = base64.b64decode(content_b64)
    return content_bytes.decode("utf-8", errors="replace")


def fetch_csv_with_fallback(raw_url: str, api_url: str) -> Tuple[str, str, SourceStatus]:
    """
    Returns: (source_used, text, status)
    source_used is "RAW" or "API"
    """
    try:
        b = http_get_bytes(raw_url)
        text = b.decode("utf-8", errors="replace")
        # Sanity: if we got something too short, fallback
        if len(text.strip()) < 50:
            raise RuntimeError("RAW returned too little content")
        return "RAW", text, SourceStatus(ok=True, url=raw_url)
    except Exception as e:
        try:
            text = fetch_github_contents_api_text(api_url)
            return "API", text, SourceStatus(ok=True, url=api_url)
        except Exception as e2:
            return "NONE", "", SourceStatus(ok=False, url=raw_url, error=f"RAW failed: {e}; API failed: {e2}")


def safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        s = str(x).strip()
        if s == "":
            return None
        return float(s)
    except Exception:
        return None


def parse_csv(text: str) -> List[Dict[str, str]]:
    f = io.StringIO(text)
    reader = csv.DictReader(f)
    rows: List[Dict[str, str]] = []
    for row in reader:
        rows.append(row)
    return rows


def pick_col(row: Dict[str, str], candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in row and str(row[c]).strip() != "":
            return c
    return None


def normalize_team_abbr(team: str) -> str:
    t = (team or "").strip()
    # MoneyPuck commonly uses 3-letter abbreviations already.
    return t


def build_slim_teams(team_rows: List[Dict[str, str]]) -> Tuple[List[Dict[str, Any]], float, Optional[str]]:
    """
    Filters to situation == all and position == Team Level if columns exist.
    Computes xGF_pg and xGA_pg from venue-adjusted xGoals and games played.
    """
    filtered = team_rows

    if team_rows and "situation" in team_rows[0]:
        filtered = [r for r in filtered if str(r.get("situation", "")).strip().lower() == "all"]

    if filtered and "position" in filtered[0]:
        filtered = [r for r in filtered if str(r.get("position", "")).strip().lower() == "team level"]

    slim: List[Dict[str, Any]] = []
    xgf_vals: List[float] = []

    for r in filtered:
        team_col = pick_col(r, ["team", "Team", "teamAbbrev", "team_abbrev"])
        gp_col = pick_col(r, ["games_played", "gamesPlayed", "gp", "GP"])
        xgf_col = pick_col(r, ["scoreVenueAdjustedxGoalsFor", "xGoalsFor", "xGF", "xGoalsForVenueAdjusted"])
        xga_col = pick_col(r, ["scoreVenueAdjustedxGoalsAgainst", "xGoalsAgainst", "xGA", "xGoalsAgainstVenueAdjusted"])

        if not team_col or not gp_col or not xgf_col or not xga_col:
            continue

        team = normalize_team_abbr(r.get(team_col, ""))
        gp = safe_float(r.get(gp_col))
        xgf = safe_float(r.get(xgf_col))
        xga = safe_float(r.get(xga_col))

        if not team or not gp or gp <= 0 or xgf is None or xga is None:
            continue

        xgf_pg = xgf / gp
        xga_pg = xga / gp

        slim.append(
            {
                "team": team,
                "games_played": int(gp),
                "xGF_pg": xgf_pg,
                "xGA_pg": xga_pg,
            }
        )
        xgf_vals.append(xgf_pg)

    league_avg_lambda = float(sum(xgf_vals) / len(xgf_vals)) if xgf_vals else 0.0
    return slim, league_avg_lambda, None if slim else "No usable team rows after filtering / column detection."


def parse_icetime_to_minutes(v: Any) -> Optional[float]:
    """
    Supports:
      - seconds (int/float)
      - minutes (int/float)
      - "MM:SS"
    Heuristic:
      - if value is large (> 5000), assume seconds
      - if it looks like "MM:SS", parse that
    """
    if v is None:
        return None
    s = str(v).strip()
    if s == "":
        return None

    if ":" in s:
        parts = s.split(":")
        if len(parts) == 2:
            mm = safe_float(parts[0])
            ss = safe_float(parts[1])
            if mm is None or ss is None:
                return None
            return mm + (ss / 60.0)
        return None

    num = safe_float(s)
    if num is None:
        return None

    # Heuristic seconds vs minutes
    if num > 5000:
        return num / 60.0
    return num


def build_slim_goalies(goalie_rows: List[Dict[str, str]]) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    Goalies output is many rows. We compute gsa_x60 if:
      - gsa_x60 exists
      - or GSAx/60 exists
      - or derive from (xGoals - goals) / (icetime/60)
      - or derive from goalsSavedAboveExpected / (icetime/60)
    If we cannot compute, we skip that row.
    """
    slim: List[Dict[str, Any]] = []

    for r in goalie_rows:
        name_col = pick_col(r, ["name", "player", "goalie", "Goalie", "playerName"])
        team_col = pick_col(r, ["team", "Team", "teamAbbrev", "team_abbrev"])

        if not name_col or not team_col:
            continue

        name = str(r.get(name_col, "")).strip()
        team = normalize_team_abbr(r.get(team_col, ""))
        if not name or not team:
            continue

        # Direct columns
        gsa60_col = pick_col(r, ["gsa_x60", "GSAx/60", "GSAx_per60", "gsax_per60", "gsaX60"])
        gsa_x60 = safe_float(r.get(gsa60_col)) if gsa60_col else None

        if gsa_x60 is None:
            # Try derive from goalsSavedAboveExpected and icetime
            gsa_col = pick_col(r, ["goalsSavedAboveExpected", "goals_saved_above_expected", "GSAx", "gsax"])
            it_col = pick_col(r, ["iceTime", "icetime", "timeOnIce", "toi", "TOI"])
            gsa = safe_float(r.get(gsa_col)) if gsa_col else None
            it_min = parse_icetime_to_minutes(r.get(it_col)) if it_col else None
            if gsa is not None and it_min is not None and it_min > 0:
                gsa_x60 = gsa / (it_min / 60.0)

        if gsa_x60 is None:
            # Try derive from (xGoals - goals) and icetime
            xg_col = pick_col(r, ["xGoals", "xGoalsAgainst", "xGA", "xG"])
            g_col = pick_col(r, ["goals", "goalsAgainst", "GA"])
            it_col = pick_col(r, ["iceTime", "icetime", "timeOnIce", "toi", "TOI"])
            xg = safe_float(r.get(xg_col)) if xg_col else None
            ga = safe_float(r.get(g_col)) if g_col else None
            it_min = parse_icetime_to_minutes(r.get(it_col)) if it_col else None
            if xg is not None and ga is not None and it_min is not None and it_min > 0:
                # goalsSavedAboveExpected ~= xGoalsAgainst - goalsAgainst
                gsa = xg - ga
                gsa_x60 = gsa / (it_min / 60.0)

        if gsa_x60 is None:
            continue

        slim.append({"name": name, "team": team, "gsa_x60": float(gsa_x60)})

    if not slim:
        return [], "Goalies CSV missing gsa_x60 and cannot derive from available columns."
    return slim, None


def odds_api_current(odds_api_key: str) -> Tuple[Optional[List[Dict[str, Any]]], SourceStatus, Optional[str]]:
    if not odds_api_key:
        return None, SourceStatus(ok=False, error="Missing ODDS_API_KEY secret."), "Missing ODDS_API_KEY"

    url = (
        "https://api.the-odds-api.com/v4/sports/icehockey_nhl/odds"
        f"?regions=us&markets=h2h,totals&oddsFormat=american&dateFormat=iso&apiKey={odds_api_key}"
    )

    try:
        b = http_get_bytes(url, timeout=30)
        # Odds API returns JSON array
        data = json.loads(b.decode("utf-8", errors="replace"))
        if not isinstance(data, list):
            raise RuntimeError("Unexpected Odds API payload shape")
        return data, SourceStatus(ok=True), None
    except Exception as e:
        return None, SourceStatus(ok=False, error=str(e)), str(e)


def ensure_data_dir() -> None:
    os.makedirs("data", exist_ok=True)


def write_json_multiline(path: str, obj: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
        f.write("\n")


def main() -> int:
    # DST-proof gating
    if not should_run_now_et(target_hour=10, window_minutes=25):
        print("Not 10:00 AM ET window. Exiting without changes.")
        return 0

    ensure_data_dir()

    et = now_et()
    data_date_et = et.date().isoformat()
    generated_at_utc = datetime.now(timezone.utc).isoformat(timespec="seconds")

    # Fetch teams + goalies with fallback
    teams_source, teams_text, teams_status = fetch_csv_with_fallback(TEAMS_RAW, TEAMS_API)
    goalies_source, goalies_text, goalies_status = fetch_csv_with_fallback(GOALIES_RAW, GOALIES_API)

    teams_sha = sha256_bytes(teams_text.encode("utf-8", errors="replace")) if teams_text else ""
    goalies_sha = sha256_bytes(goalies_text.encode("utf-8", errors="replace")) if goalies_text else ""

    # Parse teams
    slim_teams: List[Dict[str, Any]] = []
    league_avg_lambda = 0.0
    teams_err: Optional[str] = None

    if teams_status.ok and teams_text:
        try:
            team_rows = parse_csv(teams_text)
            slim_teams, league_avg_lambda, teams_err = build_slim_teams(team_rows)
            if not slim_teams:
                teams_status = SourceStatus(ok=False, url=teams_status.url, error=teams_err or "No teams parsed.")
        except Exception as e:
            teams_status = SourceStatus(ok=False, url=teams_status.url, error=str(e))

    # Parse goalies
    slim_goalies: List[Dict[str, Any]] = []
    goalies_err: Optional[str] = None

    if goalies_status.ok and goalies_text:
        try:
            goalie_rows = parse_csv(goalies_text)
            slim_goalies, goalies_err = build_slim_goalies(goalie_rows)
            if not slim_goalies:
                goalies_status = SourceStatus(ok=False, url=goalies_status.url, error=goalies_err or "No goalies parsed.")
        except Exception as e:
            goalies_status = SourceStatus(ok=False, url=goalies_status.url, error=str(e))

    # Odds (current only)
    odds_key = os.environ.get("ODDS_API_KEY", "").strip()
    odds_current, odds_status, odds_err = odds_api_current(odds_key)
    odds_sha = sha256_bytes(json.dumps(odds_current, sort_keys=True).encode("utf-8")) if odds_current else ""

    # Build artifact
    artifact: Dict[str, Any] = {
        "schema_version": "1.0.1",
        "generated_at_utc": generated_at_utc,
        "data_date_et": data_date_et,
        "source_status": {
            "odds_current": {"ok": bool(odds_status.ok), **({"error": odds_status.error} if odds_status.error else {})},
            "odds_open": {"ok": False, "reason": "Historical odds not available on current Odds API plan"},
            "teams": {
                "ok": bool(teams_status.ok),
                "url": teams_status.url,
                **({"error": teams_status.error} if teams_status.error else {}),
            },
            "goalies": {
                "ok": bool(goalies_status.ok),
                "url": goalies_status.url,
                **({"error": goalies_status.error} if goalies_status.error else {}),
            },
        },
        "validations": {
            "teams_count": len(slim_teams),
            "goalies_count": len(slim_goalies),
            "odds_games_count": len(odds_current) if isinstance(odds_current, list) else 0,
        },
        "inputs_hash": {
            "odds_current_sha256": odds_sha,
            "teams_sha256": teams_sha,
            "goalies_sha256": goalies_sha,
        },
        "slim": {
            "league_avg_lambda": league_avg_lambda,
            "teams": slim_teams,
            "goalies": slim_goalies,
            "odds_current": odds_current if isinstance(odds_current, list) else [],
        },
    }

    # Write pretty JSON so any downstream fetcher can parse it
    write_json_multiline(SLIM_PATH, artifact)

    dated_path = f"data/nhl_daily_slim_{data_date_et}.json"
    write_json_multiline(dated_path, artifact)

    # Print a tiny summary for Actions logs
    print(f"Wrote: {SLIM_PATH}")
    print(f"Wrote: {dated_path}")
    print(f"Teams: {len(slim_teams)}; Goalies: {len(slim_goalies)}; Odds games: {artifact['validations']['odds_games_count']}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
