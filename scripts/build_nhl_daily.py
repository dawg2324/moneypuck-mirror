import os
import json
import hashlib
from datetime import datetime, timezone
import pytz
import requests
import pandas as pd


TEAMS_CSV_URL = "https://raw.githubusercontent.com/dawg2324/moneypuck-mirror/main/data/teams.csv"
GOALIES_CSV_URL = "https://raw.githubusercontent.com/dawg2324/moneypuck-mirror/main/data/goalies.csv"

OUTPUT_PATH = "data/nhl_daily_slim.json"
SCHEMA_VERSION = "1.0.2"

NY_TZ = pytz.timezone("America/New_York")


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def now_ny() -> datetime:
    return datetime.now(tz=NY_TZ)


def should_write_today() -> bool:
    """
    We run twice daily for DST coverage.
    Only write at 10:00 AM America/New_York (minute must be 00..10 window for safety).
    """
    t = now_ny()
    return (t.hour == 10 and 0 <= t.minute <= 10)


def fetch_bytes(url: str, timeout: int = 45) -> tuple[bytes, str]:
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.content, url


def fetch_odds_current(api_key: str) -> tuple[list, dict]:
    """
    Pull current h2h + totals for NHL from The Odds API.
    We store the raw per-game bookmaker blocks (already in your slim example).
    """
    url = (
        "https://api.the-odds-api.com/v4/sports/icehockey_nhl/odds"
        f"?regions=us&markets=h2h,totals&oddsFormat=american&dateFormat=iso&apiKey={api_key}"
    )
    r = requests.get(url, timeout=45)
    r.raise_for_status()
    data = r.json()
    meta = {
        "endpoint": "odds_current",
        "url": "https://api.the-odds-api.com/v4/sports/icehockey_nhl/odds",
        "regions": "us",
        "markets": ["h2h", "totals"],
        "oddsFormat": "american",
        "dateFormat": "iso",
    }
    return data, meta


def parse_teams_csv(csv_bytes: bytes) -> tuple[list[dict], float]:
    """
    Expected columns used:
      team (or Team)
      games_played (or GP)
      scoreVenueAdjustedxGoalsFor (or scoreVenueAdjustedxGoalsFor)
      scoreVenueAdjustedxGoalsAgainst (or scoreVenueAdjustedxGoalsAgainst)

    We output:
      team, games_played, xGF_pg, xGA_pg
    and league_avg_lambda = mean(xGF_pg)
    """
    df = pd.read_csv(pd.io.common.BytesIO(csv_bytes))

    # Flexible column resolution
    col_team = "team" if "team" in df.columns else ("Team" if "Team" in df.columns else None)
    col_gp = "games_played" if "games_played" in df.columns else ("GP" if "GP" in df.columns else None)
    col_xgf = "scoreVenueAdjustedxGoalsFor" if "scoreVenueAdjustedxGoalsFor" in df.columns else None
    col_xga = "scoreVenueAdjustedxGoalsAgainst" if "scoreVenueAdjustedxGoalsAgainst" in df.columns else None

    if not all([col_team, col_gp, col_xgf, col_xga]):
        raise ValueError(f"Teams CSV missing required columns. Found: {list(df.columns)}")

    df = df[[col_team, col_gp, col_xgf, col_xga]].copy()
    df.columns = ["team", "games_played", "xGF", "xGA"]
    df["xGF_pg"] = df["xGF"] / df["games_played"]
    df["xGA_pg"] = df["xGA"] / df["games_played"]

    teams = []
    for _, row in df.iterrows():
        teams.append({
            "team": str(row["team"]).strip(),
            "games_played": int(row["games_played"]),
            "xGF_pg": float(row["xGF_pg"]),
            "xGA_pg": float(row["xGA_pg"]),
        })

    league_avg_lambda = float(df["xGF_pg"].mean())
    return teams, league_avg_lambda


def parse_goalies_csv(csv_bytes: bytes) -> tuple[list[dict], str | None]:
    """
    We try to produce slim.goalies with fields:
      name, team, gsa_x60

    Supported inputs (any one path is enough):
    1) gsa_x60 exists directly (case-insensitive match)
    2) derive from goalsSavedAboveExpected and icetime
       gsa_x60 = goalsSavedAboveExpected / (icetime_minutes / 60)

    If we cannot derive, return [] with a reason string.
    """
    df = pd.read_csv(pd.io.common.BytesIO(csv_bytes))

    cols_lower = {c.lower(): c for c in df.columns}

    # Required identity fields
    name_col = cols_lower.get("name") or cols_lower.get("goalie") or cols_lower.get("player")
    team_col = cols_lower.get("team") or cols_lower.get("teamabbrev") or cols_lower.get("team_abbrev")

    if not name_col or not team_col:
        return [], f"Goalies CSV missing name/team columns. Found: {list(df.columns)}"

    # Direct gsa_x60
    gsa_x60_col = cols_lower.get("gsa_x60") or cols_lower.get("gsax/60") or cols_lower.get("gsax_per60")

    if gsa_x60_col:
        out = []
        for _, r in df.iterrows():
            try:
                out.append({
                    "name": str(r[name_col]).strip(),
                    "team": str(r[team_col]).strip(),
                    "gsa_x60": float(r[gsa_x60_col]),
                })
            except Exception:
                continue
        return out, None

    # Derive gsa_x60 from goalsSavedAboveExpected and icetime
    gsa_col = cols_lower.get("goalssavedaboveexpected") or cols_lower.get("goalsSavedAboveExpected".lower())
    icetime_col = cols_lower.get("icetime") or cols_lower.get("timeonice") or cols_lower.get("toi")

    if not gsa_col or not icetime_col:
        return [], "Goalies CSV missing gsa_x60 and cannot derive from goalsSavedAboveExpected/icetime."

    out = []
    for _, r in df.iterrows():
        try:
            gsax = float(r[gsa_col])
            toi = float(r[icetime_col])
            # If TOI looks like seconds, convert to minutes
            if toi > 10000:
                toi_minutes = toi / 60.0
            else:
                toi_minutes = toi
            if toi_minutes <= 0:
                continue
            gsa_x60 = gsax / (toi_minutes / 60.0)
            out.append({
                "name": str(r[name_col]).strip(),
                "team": str(r[team_col]).strip(),
                "gsa_x60": float(gsa_x60),
            })
        except Exception:
            continue

    if len(out) == 0:
        return [], "Goalies CSV derivation produced zero usable rows."
    return out, None


def main():
    # If running on schedule, only write at 10AM NY
    if os.getenv("GITHUB_EVENT_NAME") == "schedule":
        if not should_write_today():
            print("Not 10:00 AM America/New_York. Exiting without changes.")
            return

    data_date_et = now_ny().date().isoformat()
    generated_at_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    artifact = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": generated_at_utc,
        "data_date_et": data_date_et,
        "source_status": {},
        "validations": {},
        "inputs_hash": {},
        "slim": {},
    }

    # Odds current
    odds_ok = False
    odds = []
    odds_hash = None
    odds_error = None
    odds_meta = None

    api_key = os.getenv("ODDS_API_KEY", "").strip()
    if not api_key:
        odds_error = "Missing ODDS_API_KEY secret."
    else:
        try:
            odds, odds_meta = fetch_odds_current(api_key)
            odds_ok = True
            odds_bytes = json.dumps(odds, sort_keys=True).encode("utf-8")
            odds_hash = sha256_bytes(odds_bytes)
        except Exception as e:
            odds_error = str(e)

    artifact["source_status"]["odds_current"] = {"ok": odds_ok}
    if odds_meta:
        artifact["source_status"]["odds_current"]["meta"] = odds_meta
    if odds_error:
        artifact["source_status"]["odds_current"]["error"] = odds_error

    # Teams
    teams_ok = False
    teams = []
    teams_hash = None
    teams_error = None
    league_avg_lambda = None

    try:
        teams_bytes, teams_url = fetch_bytes(TEAMS_CSV_URL)
        teams_hash = sha256_bytes(teams_bytes)
        teams, league_avg_lambda = parse_teams_csv(teams_bytes)
        teams_ok = True
        artifact["source_status"]["teams"] = {"ok": True, "url": teams_url}
    except Exception as e:
        teams_error = str(e)
        artifact["source_status"]["teams"] = {"ok": False, "url": TEAMS_CSV_URL, "error": teams_error}

    # Goalies
    goalies_ok = False
    goalies = []
    goalies_hash = None
    goalies_error = None

    try:
        goalies_bytes, goalies_url = fetch_bytes(GOALIES_CSV_URL)
        goalies_hash = sha256_bytes(goalies_bytes)
        goalies, reason = parse_goalies_csv(goalies_bytes)
        if reason:
            goalies_ok = False
            goalies_error = reason
            artifact["source_status"]["goalies"] = {"ok": False, "url": goalies_url, "error": reason}
        else:
            goalies_ok = True
            artifact["source_status"]["goalies"] = {"ok": True, "url": goalies_url}
    except Exception as e:
        goalies_error = str(e)
        artifact["source_status"]["goalies"] = {"ok": False, "url": GOALIES_CSV_URL, "error": goalies_error}

    # No historical odds, explicitly
    artifact["source_status"]["odds_open"] = {
        "ok": False,
        "reason": "Historical odds not available on current Odds API plan"
    }

    artifact["validations"]["teams_count"] = int(len(teams))
    artifact["validations"]["goalies_count"] = int(len(goalies))
    artifact["validations"]["odds_games_count"] = int(len(odds)) if isinstance(odds, list) else 0

    if odds_hash:
        artifact["inputs_hash"]["odds_current_sha256"] = odds_hash
    if teams_hash:
        artifact["inputs_hash"]["teams_sha256"] = teams_hash
    if goalies_hash:
        artifact["inputs_hash"]["goalies_sha256"] = goalies_hash

    # Hard stops
    if artifact["validations"]["odds_games_count"] == 0:
        raise SystemExit("Skipped: missing odds (odds_games_count == 0).")
    if artifact["validations"]["teams_count"] < 20:
        raise SystemExit("Skipped: missing teams (teams_count < 20).")

    artifact["slim"]["league_avg_lambda"] = float(league_avg_lambda) if league_avg_lambda is not None else None
    artifact["slim"]["teams"] = teams
    artifact["slim"]["goalies"] = goalies  # may be empty, that is allowed
    artifact["slim"]["odds_current"] = odds

    os.makedirs("data", exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(artifact, f, ensure_ascii=False, separators=(",", ":"))

    print(f"Wrote {OUTPUT_PATH}")
    print(f"teams_count={artifact['validations']['teams_count']} goalies_count={artifact['validations']['goalies_count']} odds_games_count={artifact['validations']['odds_games_count']}")


if __name__ == "__main__":
    main()
