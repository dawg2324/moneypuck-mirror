import os
import json
import hashlib
from datetime import datetime, timedelta, timezone
from io import StringIO

import pandas as pd
import requests
from dateutil import tz


SCHEMA_VERSION = "1.0.3"

ET_TZ = tz.gettz("America/New_York")

OUTPUT_PATH = os.path.join("data", "nhl_daily_slim.json")

ODDS_SPORT = "icehockey_nhl"
ODDS_ENDPOINT = "https://api.the-odds-api.com/v4/sports/{sport}/odds"
ODDS_REGIONS = "us"
ODDS_MARKETS = ["h2h", "totals"]
ODDS_ODDS_FORMAT = "american"
ODDS_DATE_FORMAT = "iso"

MONEYPuck_TEAMS_CSV = "https://moneypuck.com/moneypuck/playerData/seasonSummary/2025/regular/teams.csv"
MONEYPuck_GOALIES_CSV = "https://moneypuck.com/moneypuck/playerData/seasonSummary/2025/regular/goalies.csv"


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_et() -> datetime:
    return now_utc().astimezone(ET_TZ)


def gate_to_10am_et() -> None:
    t = now_et()
    if (t.hour, t.minute) < (10, 0):
        raise SystemExit(0)


def sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def atomic_write_json(path: str, payload: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"), sort_keys=False)
    os.replace(tmp, path)


def fetch_text(url: str, timeout: int = 45) -> str:
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.text


def fetch_odds_current() -> tuple[list, dict, str]:
    api_key = os.getenv("ODDS_API_KEY", "").strip()
    url = ODDS_ENDPOINT.format(sport=ODDS_SPORT)

    meta = {
        "endpoint": "odds_current",
        "url": url,
        "regions": ODDS_REGIONS,
        "markets": ODDS_MARKETS,
        "oddsFormat": ODDS_ODDS_FORMAT,
        "dateFormat": ODDS_DATE_FORMAT,
    }

    if not api_key:
        return [], meta, sha256_text("")

    params = {
        "apiKey": api_key,
        "regions": ODDS_REGIONS,
        "markets": ",".join(ODDS_MARKETS),
        "oddsFormat": ODDS_ODDS_FORMAT,
        "dateFormat": ODDS_DATE_FORMAT,
    }

    r = requests.get(url, params=params, timeout=35)
    r.raise_for_status()
    data = r.json()

    odds_hash_basis = json.dumps(data, sort_keys=True, separators=(",", ":"))
    return data, meta, sha256_text(odds_hash_basis)


def build_teams_slim(teams_csv_text: str) -> tuple[list[dict], float]:
    """
    Uses your exact headers:
      - team abbreviation column is the SECOND 'team' column
      - situation filter to 'all'
      - metrics: games_played, xGoalsFor, xGoalsAgainst
    """
    df = pd.read_csv(StringIO(teams_csv_text))

    # There are two 'team' columns. We want the second one (team abbreviation like NYR).
    team_cols = [c for c in df.columns if c == "team"]
    if len(team_cols) < 2:
        raise ValueError("Expected two 'team' columns in teams.csv (id + abbreviation).")

    # In pandas, duplicate column names are preserved; selecting df['team'] returns both.
    # We will access by position.
    team_abbrev_series = df.iloc[:, list(df.columns).index("team", 1)]  # second 'team'

    if "situation" not in df.columns:
        raise ValueError("teams.csv missing 'situation' column.")
    if "games_played" not in df.columns:
        raise ValueError("teams.csv missing 'games_played' column.")
    if "xGoalsFor" not in df.columns or "xGoalsAgainst" not in df.columns:
        raise ValueError("teams.csv missing xGoalsFor and/or xGoalsAgainst columns.")

    # Attach team abbreviation explicitly to avoid any ambiguity with duplicate column names
    df = df.copy()
    df["team_abbrev"] = team_abbrev_series.astype(str)

    # Filter to overall row only
    df = df[df["situation"].astype(str).str.lower().eq("all")].copy()

    # Convert numeric
    df["games_played"] = pd.to_numeric(df["games_played"], errors="coerce")
    df["xGoalsFor"] = pd.to_numeric(df["xGoalsFor"], errors="coerce")
    df["xGoalsAgainst"] = pd.to_numeric(df["xGoalsAgainst"], errors="coerce")

    df = df.dropna(subset=["team_abbrev", "games_played", "xGoalsFor", "xGoalsAgainst"]).copy()
    df = df[df["games_played"] > 0].copy()

    df["xGF_pg"] = df["xGoalsFor"] / df["games_played"]
    df["xGA_pg"] = df["xGoalsAgainst"] / df["games_played"]

    # Enforce exactly one row per team (should already be true after situation=all)
    df = (
        df.sort_values(["team_abbrev", "games_played"], ascending=[True, False])
        .drop_duplicates(subset=["team_abbrev"], keep="first")
        .reset_index(drop=True)
    )

    teams = []
    for _, r in df.iterrows():
        teams.append(
            {
                "team": r["team_abbrev"],
                "games_played": int(r["games_played"]),
                "xGF_pg": float(r["xGF_pg"]),
                "xGA_pg": float(r["xGA_pg"]),
            }
        )

    # league_avg_lambda = average expected goals per team per game
    league_avg_lambda = float(pd.Series([t["xGF_pg"] for t in teams]).mean())

    # Validate duplicates
    names = [t["team"] for t in teams]
    if len(names) != len(set(names)):
        raise ValueError("Duplicate teams present after filtering. Output invalid.")

    return teams, league_avg_lambda


def main() -> None:
    gate_to_10am_et()

    generated_at = now_utc().replace(microsecond=0)
    data_date_et = (now_et().date() - timedelta(days=1)).isoformat()

    odds_data, odds_meta, odds_sha = fetch_odds_current()

    teams_csv_text = fetch_text(MONEYPuck_TEAMS_CSV, timeout=45)
    teams_sha = sha256_text(teams_csv_text)
    teams_list, league_avg_lambda = build_teams_slim(teams_csv_text)

    goalies_csv_text = fetch_text(MONEYPuck_GOALIES_CSV, timeout=45)
    goalies_sha = sha256_text(goalies_csv_text)

    payload = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": generated_at.isoformat(),
        "data_date_et": data_date_et,
        "source_status": {
            "odds_current": {
                "ok": True,
                "meta": odds_meta,
            },
            "teams": {
                "ok": True,
                "url": MONEYPuck_TEAMS_CSV,
            },
            "goalies": {
                "ok": False,
                "url": MONEYPuck_GOALIES_CSV,
                "error": "Goalies CSV missing gsa_x60 and cannot derive from goalsSavedAboveExpected/icetime.",
            },
            "odds_open": {
                "ok": False,
                "reason": "Historical odds not available on current Odds API plan",
            },
        },
        "validations": {
            "odds_games_count": int(len(odds_data)) if isinstance(odds_data, list) else 0,
            "teams_count": int(len(teams_list)),
            "goalies_count": 0,
        },
        "inputs_hash": {
            "odds_current_sha256": odds_sha,
            "teams_sha256": teams_sha,
            "goalies_sha256": goalies_sha,
        },
        "slim": {
            "odds_current": odds_data,
            "league_avg_lambda": league_avg_lambda,
            "teams": teams_list,
            "goalies": [],
        },
    }

    atomic_write_json(OUTPUT_PATH, payload)


if __name__ == "__main__":
    main()
