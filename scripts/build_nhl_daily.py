import os
import json
import hashlib
from datetime import datetime, timezone

import pytz
import requests
import pandas as pd


# Pull directly from MoneyPuck public CSV endpoints (no repo CSV dependency)
# If these ever change, you update them here, once.
MONEYPUCK_TEAMS_CSV_URL = (
    "https://moneypuck.com/moneypuck/playerData/seasonSummary/2025/regular/teams.csv"
)
MONEYPUCK_GOALIES_CSV_URL = (
    "https://moneypuck.com/moneypuck/playerData/seasonSummary/2025/regular/goalies.csv"
)

OUTPUT_PATH = "data/nhl_daily_slim.json"
SCHEMA_VERSION = "1.0.3"

NY_TZ = pytz.timezone("America/New_York")


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def now_ny() -> datetime:
    return datetime.now(tz=NY_TZ)


def should_write_today() -> bool:
    """
    Runs twice daily for DST coverage.
    Only write if it's 10:00 AM America/New_York (minute 00-10 buffer).
    """
    t = now_ny()
    return (t.hour == 10 and 0 <= t.minute <= 10)


def fetch_bytes(url: str, timeout: int = 60) -> bytes:
    r = requests.get(
        url,
        timeout=timeout,
        headers={"User-Agent": "nhl-daily-bot/1.0 (+github actions)"},
    )
    r.raise_for_status()
    return r.content


def fetch_odds_current(api_key: str) -> tuple[list, dict, str]:
    url = (
        "https://api.the-odds-api.com/v4/sports/icehockey_nhl/odds"
        f"?regions=us&markets=h2h,totals&oddsFormat=american&dateFormat=iso&apiKey={api_key}"
    )
    r = requests.get(url, timeout=60)
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

    raw_hash = sha256_bytes(json.dumps(data, sort_keys=True).encode("utf-8"))
    return data, meta, raw_hash


def parse_teams_csv(csv_bytes: bytes) -> tuple[list[dict], float]:
    df = pd.read_csv(pd.io.common.BytesIO(csv_bytes))

    # Common MoneyPuck columns
    # We compute:
    # xGF_pg = scoreVenueAdjustedxGoalsFor / games_played
    # xGA_pg = scoreVenueAdjustedxGoalsAgainst / games_played
    # league_avg_lambda = mean(xGF_pg)
    needed = [
        "team",
        "games_played",
        "scoreVenueAdjustedxGoalsFor",
        "scoreVenueAdjustedxGoalsAgainst",
    ]

    # Some MP files use different casing. Normalize.
    cols_lower = {c.lower(): c for c in df.columns}
    resolved = {}
    for k in needed:
        resolved[k] = cols_lower.get(k.lower())

    if any(v is None for v in resolved.values()):
        raise ValueError(f"Teams CSV missing required columns. Found: {list(df.columns)}")

    df = df[[resolved["team"], resolved["games_played"], resolved["scoreVenueAdjustedxGoalsFor"], resolved["scoreVenueAdjustedxGoalsAgainst"]]].copy()
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
    Output rows: name, team, gsa_x60

    Accepts:
    - gsa_x60 directly if present (case-insensitive)
    - else derive using goalsSavedAboveExpected and icetime (minutes or seconds)
    If neither possible: return [] with reason
    """
    df = pd.read_csv(pd.io.common.BytesIO(csv_bytes))
    cols_lower = {c.lower(): c for c in df.columns}

    name_col = cols_lower.get("name") or cols_lower.get("goalie") or cols_lower.get("player")
    team_col = cols_lower.get("team") or cols_lower.get("teamabbrev") or cols_lower.get("team_abbrev")

    if not name_col or not team_col:
        return [], f"Goalies CSV missing name/team columns. Found: {list(df.columns)}"

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
        if len(out) == 0:
            return [], "Goalies CSV has gsa_x60 but produced zero usable rows."
        return out, None

    # Derive path
    gsa_col = cols_lower.get("goalssavedaboveexpected") or cols_lower.get("goalssavedaboveexpected".lower())
    icetime_col = cols_lower.get("icetime") or cols_lower.get("timeonice") or cols_lower.get("toi")

    if not gsa_col or not icetime_col:
        return [], "Goalies CSV missing gsa_x60 and cannot derive from goalsSavedAboveExpected/icetime."

    out = []
    for _, r in df.iterrows():
        try:
            gsax = float(r[gsa_col])
            toi = float(r[icetime_col])
            # If TOI looks like seconds, convert to minutes
            toi_minutes = toi / 60.0 if toi > 10000 else toi
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
    if os.getenv("GITHUB_EVENT_NAME") == "schedule":
        if not should_write_today():
            print("Not 10:00 AM America/New_York. Exiting without changes.")
            return

    generated_at_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    data_date_et = now_ny().date().isoformat()

    artifact = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": generated_at_utc,
        "data_date_et": data_date_et,
        "source_status": {},
        "validations": {},
        "inputs_hash": {},
        "slim": {},
    }

    # Odds
    api_key = os.getenv("ODDS_API_KEY", "").strip()
    if not api_key:
        artifact["source_status"]["odds_current"] = {"ok": False, "error": "Missing ODDS_API_KEY secret."}
        artifact["validations"]["odds_games_count"] = 0
    else:
        try:
            odds, meta, odds_hash = fetch_odds_current(api_key)
            artifact["source_status"]["odds_current"] = {"ok": True, "meta": meta}
            artifact["inputs_hash"]["odds_current_sha256"] = odds_hash
            artifact["slim"]["odds_current"] = odds
            artifact["validations"]["odds_games_count"] = int(len(odds)) if isinstance(odds, list) else 0
        except Exception as e:
            artifact["source_status"]["odds_current"] = {"ok": False, "error": str(e)}
            artifact["validations"]["odds_games_count"] = 0

    # Teams (direct from MoneyPuck)
    teams = []
    league_avg_lambda = None
    try:
        teams_bytes = fetch_bytes(MONEYPUCK_TEAMS_CSV_URL)
        artifact["source_status"]["teams"] = {"ok": True, "url": MONEYPUCK_TEAMS_CSV_URL}
        artifact["inputs_hash"]["teams_sha256"] = sha256_bytes(teams_bytes)
        teams, league_avg_lambda = parse_teams_csv(teams_bytes)
    except Exception as e:
        artifact["source_status"]["teams"] = {"ok": False, "url": MONEYPUCK_TEAMS_CSV_URL, "error": str(e)}

    artifact["validations"]["teams_count"] = int(len(teams))

    # Goalies (direct from MoneyPuck)
    goalies = []
    try:
        goalies_bytes = fetch_bytes(MONEYPUCK_GOALIES_CSV_URL)
        artifact["inputs_hash"]["goalies_sha256"] = sha256_bytes(goalies_bytes)
        parsed_goalies, reason = parse_goalies_csv(goalies_bytes)
        if reason:
            artifact["source_status"]["goalies"] = {"ok": False, "url": MONEYPUCK_GOALIES_CSV_URL, "error": reason}
        else:
            artifact["source_status"]["goalies"] = {"ok": True, "url": MONEYPUCK_GOALIES_CSV_URL}
            goalies = parsed_goalies
    except Exception as e:
        artifact["source_status"]["goalies"] = {"ok": False, "url": MONEYPUCK_GOALIES_CSV_URL, "error": str(e)}

    artifact["validations"]["goalies_count"] = int(len(goalies))

    # Historical odds explicitly disabled (plan)
    artifact["source_status"]["odds_open"] = {
        "ok": False,
        "reason": "Historical odds not available on current Odds API plan"
    }

    # Guardrails
    if artifact["validations"]["odds_games_count"] == 0:
        raise SystemExit("Skipped: missing odds (odds_games_count == 0).")
    if artifact["validations"]["teams_count"] < 20:
        raise SystemExit("Skipped: missing teams (teams_count < 20).")

    artifact["slim"]["league_avg_lambda"] = float(league_avg_lambda) if league_avg_lambda is not None else None
    artifact["slim"]["teams"] = teams
    artifact["slim"]["goalies"] = goalies  # allowed to be []

    os.makedirs("data", exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        # minified, consistent
        json.dump(artifact, f, ensure_ascii=False, separators=(",", ":"))

    print(f"Wrote {OUTPUT_PATH}")
    print(
        f"teams_count={artifact['validations']['teams_count']} "
        f"goalies_count={artifact['validations']['goalies_count']} "
        f"odds_games_count={artifact['validations']['odds_games_count']}"
    )


if __name__ == "__main__":
    main()
