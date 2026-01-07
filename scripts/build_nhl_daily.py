import os
import json
import hashlib
from datetime import datetime, timezone
import requests
import pandas as pd
from dateutil import tz


REPO_RAW_BASE = "https://raw.githubusercontent.com/dawg2324/moneypuck-mirror/main"
TEAMS_URL = f"{REPO_RAW_BASE}/data/teams.csv"
GOALIES_URL = f"{REPO_RAW_BASE}/data/goalies.csv"

ODDS_CURRENT_URL = "https://api.the-odds-api.com/v4/sports/icehockey_nhl/odds"
ODDS_HIST_URL = "https://api.the-odds-api.com/v4/historical/sports/icehockey_nhl/odds"

DATA_DIR = "data"
SCHEMA_VERSION = "1.0.0"
OPENING_SNAPSHOT_TIME_Z = "T12:00:00Z"


def sha256_bytes(b):
    return hashlib.sha256(b).hexdigest()


def safe_get(url, params=None, timeout=30):
    r = requests.get(
        url,
        params=params,
        timeout=timeout,
        headers={"User-Agent": "nhl-daily-artifact/1.0"},
    )
    r.raise_for_status()
    return r


def utc_now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def et_today_date_str():
    et = tz.gettz("America/New_York")
    return datetime.now(timezone.utc).astimezone(et).date().isoformat()


def american_to_prob(odds):
    if odds < 0:
        return (-odds) / ((-odds) + 100.0)
    return 100.0 / (odds + 100.0)


def novig_from_two_sides(away_odds, home_odds):
    pa = american_to_prob(int(away_odds))
    ph = american_to_prob(int(home_odds))
    s = pa + ph
    if s <= 0:
        return None
    return {"away": pa / s, "home": ph / s}


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def main():
    odds_key = os.environ.get("ODDS_API_KEY", "").strip()
    if not odds_key:
        raise RuntimeError("Missing ODDS_API_KEY secret.")

    ensure_dir(DATA_DIR)

    generated_at_utc = utc_now_iso()
    data_date_et = et_today_date_str()
    snapshot_iso_z = data_date_et + OPENING_SNAPSHOT_TIME_Z

    source_status = {}
    validations = {}
    inputs_hash = {}

    # Odds current
    odds_current = []
    try:
        params = {
            "apiKey": odds_key,
            "regions": "us",
            "markets": "h2h,totals",
            "oddsFormat": "american",
            "dateFormat": "iso",
        }
        r = safe_get(ODDS_CURRENT_URL, params)
        inputs_hash["odds_current_sha256"] = sha256_bytes(r.content)
        odds_current = r.json()
        source_status["odds_current"] = {"ok": True}
    except Exception as e:
        source_status["odds_current"] = {"ok": False, "error": str(e)}

    # Odds opening
    try:
        params = {
            "apiKey": odds_key,
            "regions": "us",
            "markets": "h2h,totals",
            "oddsFormat": "american",
            "dateFormat": "iso",
            "date": snapshot_iso_z,
        }
        r = safe_get(ODDS_HIST_URL, params)
        inputs_hash["odds_open_sha256"] = sha256_bytes(r.content)
        source_status["odds_open"] = {"ok": True, "snapshot_requested": snapshot_iso_z}
    except Exception as e:
        source_status["odds_open"] = {"ok": False, "error": str(e)}

    # Teams
    teams = []
    league_avg_lambda = None
    try:
        r = safe_get(TEAMS_URL)
        inputs_hash["teams_sha256"] = sha256_bytes(r.content)
        df = pd.read_csv(pd.io.common.BytesIO(r.content))

        if "situation" in df.columns:
            df = df[df["situation"].str.lower() == "all"]
        if "position" in df.columns:
            df = df[df["position"].str.lower() == "team level"]

        df["xGF_pg"] = df["scoreVenueAdjustedxGoalsFor"] / df["games_played"]
        df["xGA_pg"] = df["scoreVenueAdjustedxGoalsAgainst"] / df["games_played"]

        league_avg_lambda = float(df["xGF_pg"].mean())
        teams = df[["team", "games_played", "xGF_pg", "xGA_pg"]].to_dict("records")
        validations["teams_count"] = len(teams)
        source_status["teams"] = {"ok": True}
    except Exception as e:
        source_status["teams"] = {"ok": False, "error": str(e)}

    # Goalies
    goalies = []
    try:
        r = safe_get(GOALIES_URL)
        inputs_hash["goalies_sha256"] = sha256_bytes(r.content)
        df = pd.read_csv(pd.io.common.BytesIO(r.content))

        if "goalsSavedAboveExpected" in df.columns and "icetime" in df.columns:
            df["gsa_x60"] = df["goalsSavedAboveExpected"] * 3600 / df["icetime"]

        df = df.dropna(subset=["gsa_x60", "name", "team"])
        df = df.drop_duplicates(subset=["name", "team"])

        goalies = df[["name", "team", "gsa_x60"]].to_dict("records")
        validations["goalies_count"] = len(goalies)
        source_status["goalies"] = {"ok": True}
    except Exception as e:
        source_status["goalies"] = {"ok": False, "error": str(e)}

    validations["odds_games_count"] = len(odds_current)

    output = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": generated_at_utc,
        "data_date_et": data_date_et,
        "source_status": source_status,
        "validations": validations,
        "inputs_hash": inputs_hash,
        "slim": {
            "league_avg_lambda": league_avg_lambda,
            "teams": teams,
            "goalies": goalies,
            "odds_current": odds_current,
        },
    }

    with open(f"{DATA_DIR}/nhl_daily_slim.json", "w") as f:
        json.dump(output, f, indent=2)


if __name__ == "__main__":
    main()
