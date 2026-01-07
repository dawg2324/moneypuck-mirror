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

DATA_DIR = "data"
SCHEMA_VERSION = "1.0.1"  # bumped because we removed odds_open


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def et_now():
    et = tz.gettz("America/New_York")
    return datetime.now(timezone.utc).astimezone(et)


def et_today_date_str() -> str:
    return et_now().date().isoformat()


def safe_get(url: str, params=None, timeout: int = 30) -> requests.Response:
    r = requests.get(
        url,
        params=params,
        timeout=timeout,
        headers={"User-Agent": "nhl-daily-artifact/1.0"},
    )
    r.raise_for_status()
    return r


def main():
    odds_key = os.environ.get("ODDS_API_KEY", "").strip()
    if not odds_key:
        raise RuntimeError("Missing ODDS_API_KEY secret.")

    ensure_dir(DATA_DIR)

    generated_at_utc = utc_now_iso()
    data_date_et = et_today_date_str()

    source_status = {
        "odds_current": {"ok": False},
        "odds_open": {
            "ok": False,
            "reason": "Historical odds not available on current Odds API plan",
        },
        "teams": {"ok": False, "url": TEAMS_URL},
        "goalies": {"ok": False, "url": GOALIES_URL},
    }
    validations = {
        "teams_count": 0,
        "goalies_count": 0,
        "odds_games_count": 0,
    }
    inputs_hash = {}

    slim = {
        "league_avg_lambda": None,
        "teams": [],
        "goalies": [],
        "odds_current": [],
    }

    # -------------------------
    # ODDS CURRENT (required)
    # -------------------------
    try:
        params = {
            "apiKey": odds_key,
            "regions": "us",
            "markets": "h2h,totals",
            "oddsFormat": "american",
            "dateFormat": "iso",
        }
        r = safe_get(ODDS_CURRENT_URL, params=params)
        inputs_hash["odds_current_sha256"] = sha256_bytes(r.content)
        odds_current = r.json() if r.content else []
        slim["odds_current"] = odds_current
        validations["odds_games_count"] = len(odds_current)
        source_status["odds_current"] = {"ok": True}
    except Exception as e:
        source_status["odds_current"] = {"ok": False, "error": str(e)}

    # -------------------------
    # TEAMS (required)
    # -------------------------
    try:
        r = safe_get(TEAMS_URL)
        inputs_hash["teams_sha256"] = sha256_bytes(r.content)
        df = pd.read_csv(pd.io.common.BytesIO(r.content))

        if "situation" in df.columns:
            df = df[df["situation"].astype(str).str.lower() == "all"]
        if "position" in df.columns:
            df = df[df["position"].astype(str).str.lower() == "team level"]

        req = {"team", "games_played", "scoreVenueAdjustedxGoalsFor", "scoreVenueAdjustedxGoalsAgainst"}
        missing = [c for c in req if c not in df.columns]
        if missing:
            raise RuntimeError(f"Teams CSV missing columns: {missing}")

        df = df[df["games_played"] > 0].copy()
        df["xGF_pg"] = df["scoreVenueAdjustedxGoalsFor"] / df["games_played"]
        df["xGA_pg"] = df["scoreVenueAdjustedxGoalsAgainst"] / df["games_played"]

        league_avg_lambda = float(df["xGF_pg"].mean())
        slim["league_avg_lambda"] = league_avg_lambda

        teams = df[["team", "games_played", "xGF_pg", "xGA_pg"]].copy()
        teams = teams.sort_values("team")
        slim["teams"] = teams.to_dict("records")

        validations["teams_count"] = len(slim["teams"])
        source_status["teams"] = {"ok": True, "url": TEAMS_URL}
    except Exception as e:
        source_status["teams"] = {"ok": False, "url": TEAMS_URL, "error": str(e)}

    # -------------------------
    # GOALIES (optional but preferred)
    # Dedup to one row per (name, team)
    # -------------------------
    try:
        r = safe_get(GOALIES_URL)
        inputs_hash["goalies_sha256"] = sha256_bytes(r.content)
        df = pd.read_csv(pd.io.common.BytesIO(r.content))

        if "gsa_x60" not in df.columns:
            if "goalsSavedAboveExpected" in df.columns and "icetime" in df.columns:
                df = df[df["icetime"] > 0].copy()
                df["gsa_x60"] = df["goalsSavedAboveExpected"] * 3600.0 / df["icetime"]
            else:
                raise RuntimeError("Goalies CSV missing gsa_x60 and cannot derive from goalsSavedAboveExpected/icetime.")

        for c in ["name", "team"]:
            if c not in df.columns:
                raise RuntimeError(f"Goalies CSV missing column: {c}")

        df = df.dropna(subset=["name", "team", "gsa_x60"]).copy()

        sort_cols = []
        for c in ["icetime", "games_played", "games"]:
            if c in df.columns:
                sort_cols.append(c)
        if sort_cols:
            df = df.sort_values(sort_cols, ascending=False)

        df = df.drop_duplicates(subset=["name", "team"], keep="first").copy()

        goalies = df[["name", "team", "gsa_x60"]].copy()
        goalies = goalies.sort_values(["team", "name"])
        slim["goalies"] = goalies.to_dict("records")

        validations["goalies_count"] = len(slim["goalies"])
        source_status["goalies"] = {"ok": True, "url": GOALIES_URL}
    except Exception as e:
        source_status["goalies"] = {"ok": False, "url": GOALIES_URL, "error": str(e)}

    output = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": generated_at_utc,
        "data_date_et": data_date_et,
        "source_status": source_status,
        "validations": validations,
        "inputs_hash": inputs_hash,
        "slim": slim,
    }

    dated_path = f"{DATA_DIR}/nhl_daily_slim_{data_date_et}.json"
    latest_path = f"{DATA_DIR}/nhl_daily_slim.json"

    with open(dated_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, sort_keys=False)

    with open(latest_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, sort_keys=False)


if __name__ == "__main__":
    main()
