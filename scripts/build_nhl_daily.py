import os
import json
import math
import time
import hashlib
from datetime import datetime, timedelta, timezone, date

import requests
import pandas as pd
from dateutil import tz


SCHEMA_VERSION = "1.0.3"

ODDS_SPORT = "icehockey_nhl"
ODDS_ENDPOINT = "https://api.the-odds-api.com/v4/sports/{sport}/odds"
ODDS_REGIONS = "us"
ODDS_MARKETS = "h2h,totals"
ODDS_ODDS_FORMAT = "american"
ODDS_DATE_FORMAT = "iso"

MONEYPuck_TEAMS_CSV = "https://moneypuck.com/moneypuck/playerData/seasonSummary/2025/regular/teams.csv"
OUTPUT_PATH = os.path.join("data", "nhl_daily_slim.json")

ET_TZ = tz.gettz("America/New_York")


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_et() -> datetime:
    return now_utc().astimezone(ET_TZ)


def gate_to_10am_et() -> None:
    """
    Script exits cleanly unless current ET time is >= 10:00.
    This matches your existing gate behavior so early cron runs do nothing.
    """
    t = now_et()
    if (t.hour, t.minute) < (10, 0):
        print(f"Gate: ET time is {t.isoformat()} which is before 10:00. Exiting.")
        raise SystemExit(0)


def safe_get_env(name: str) -> str:
    v = os.getenv(name, "").strip()
    return v


def http_get_json(url: str, params: dict, timeout: int = 30) -> dict:
    r = requests.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()


def http_get_csv(url: str, timeout: int = 45) -> pd.DataFrame:
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return pd.read_csv(pd.io.common.StringIO(r.text))


def normalize_columns(df: pd.DataFrame) -> dict:
    return {c.lower(): c for c in df.columns}


def filter_teams_to_one_row_per_team(df: pd.DataFrame) -> pd.DataFrame:
    """
    MoneyPuck teams.csv includes multiple split rows per team.
    We want exactly one row per team: the overall row.

    Priority:
    1) situation == 'all' (if column exists)
    2) score == 'all' (if column exists)
    3) if still duplicates, keep the row with max GP (or a deterministic first row)
    """
    cols = normalize_columns(df)

    situation_col = cols.get("situation")
    score_col = cols.get("score")

    filtered = df.copy()

    if situation_col:
        filtered = filtered[filtered[situation_col].astype(str).str.lower().eq("all")]

    if score_col:
        filtered = filtered[filtered[score_col].astype(str).str.lower().eq("all")]

    team_col = cols.get("team") or cols.get("teamname") or cols.get("team_name")
    if not team_col:
        raise ValueError(f"Could not find team column in teams.csv. Columns: {list(df.columns)}")

    gp_col = cols.get("gp") or cols.get("gamesplayed") or cols.get("games_played") or cols.get("games")

    if gp_col and gp_col in filtered.columns:
        filtered[gp_col] = pd.to_numeric(filtered[gp_col], errors="coerce").fillna(0)
        filtered = (
            filtered.sort_values([team_col, gp_col], ascending=[True, False])
            .drop_duplicates(subset=[team_col], keep="first")
        )
    else:
        filtered = filtered.sort_values([team_col]).drop_duplicates(subset=[team_col], keep="first")

    filtered = filtered.reset_index(drop=True)

    # Validate uniqueness
    dupes = filtered[team_col].duplicated().sum()
    if dupes:
        raise ValueError(f"Teams still duplicated after filtering: {dupes}")

    return filtered


def infer_stat_columns(df: pd.DataFrame) -> tuple[str, str, str, str]:
    """
    Find team key, games, xGF, xGA columns using common MoneyPuck names.
    """
    cols = normalize_columns(df)

    team_col = cols.get("team") or cols.get("teamname") or cols.get("team_name")
    if not team_col:
        raise ValueError("Missing team column")

    gp_col = cols.get("gp") or cols.get("gamesplayed") or cols.get("games_played") or cols.get("games")
    if not gp_col:
        raise ValueError("Missing games played column (gp/gamesPlayed/games_played/games)")

    xgf_col = (
        cols.get("xgoalsfor")
        or cols.get("xgoals_for")
        or cols.get("xgf")
        or cols.get("xgfor")
    )
    if not xgf_col:
        raise ValueError("Missing xGoalsFor column (xGoalsFor/xgf/xgFor)")

    xga_col = (
        cols.get("xgoalsagainst")
        or cols.get("xgoals_against")
        or cols.get("xga")
        or cols.get("xgagainst")
    )
    if not xga_col:
        raise ValueError("Missing xGoalsAgainst column (xGoalsAgainst/xga/xgAgainst)")

    return team_col, gp_col, xgf_col, xga_col


def build_teams_payload() -> dict:
    teams_raw = http_get_csv(MONEYPuck_TEAMS_CSV)
    teams = filter_teams_to_one_row_per_team(teams_raw)

    team_col, gp_col, xgf_col, xga_col = infer_stat_columns(teams)

    teams[gp_col] = pd.to_numeric(teams[gp_col], errors="coerce")
    teams[xgf_col] = pd.to_numeric(teams[xgf_col], errors="coerce")
    teams[xga_col] = pd.to_numeric(teams[xga_col], errors="coerce")

    teams = teams.dropna(subset=[team_col, gp_col, xgf_col, xga_col]).copy()
    teams = teams[teams[gp_col] > 0].copy()

    teams["xGF_pg"] = teams[xgf_col] / teams[gp_col]
    teams["xGA_pg"] = teams[xga_col] / teams[gp_col]

    # league_avg_lambda is "per team per game" average expected goals
    league_avg_lambda = float(teams["xGF_pg"].mean())

    out_rows = []
    for _, r in teams.iterrows():
        out_rows.append(
            {
                "team": str(r[team_col]),
                "gp": int(r[gp_col]),
                "xGF_pg": float(r["xGF_pg"]),
                "xGA_pg": float(r["xGA_pg"]),
            }
        )

    return {
        "ok": True,
        "url": MONEYPuck_TEAMS_CSV,
        "teams_count": len(out_rows),
        "league_avg_lambda": league_avg_lambda,
        "teams": out_rows,
    }


def fetch_odds_current() -> dict:
    api_key = safe_get_env("ODDS_API_KEY")
    if not api_key:
        return {
            "ok": False,
            "error": "Missing ODDS_API_KEY secret",
        }

    url = ODDS_ENDPOINT.format(sport=ODDS_SPORT)
    params = {
        "apiKey": api_key,
        "regions": ODDS_REGIONS,
        "markets": ODDS_MARKETS,
        "oddsFormat": ODDS_ODDS_FORMAT,
        "dateFormat": ODDS_DATE_FORMAT,
    }

    try:
        data = http_get_json(url, params=params, timeout=35)
        return {
            "ok": True,
            "meta": {
                "endpoint": "odds_current",
                "url": url,
                "regions": ODDS_REGIONS,
                "markets": ODDS_MARKETS.split(","),
                "oddsFormat": ODDS_ODDS_FORMAT,
                "dateFormat": ODDS_DATE_FORMAT,
            },
            "games_count": len(data) if isinstance(data, list) else 0,
            "data": data,
        }
    except Exception as e:
        return {
            "ok": False,
            "error": str(e),
            "meta": {
                "endpoint": "odds_current",
                "url": url,
            },
        }


def atomic_write_json(path: str, payload: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp_path = path + ".tmp"

    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"), sort_keys=True)

    os.replace(tmp_path, path)


def file_sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> None:
    gate_to_10am_et()

    generated_at = now_utc()
    et_now = now_et()

    # Your artifact shows data_date_et lags by a day due to upstream timing.
    # Keep that behavior: label data_date_et as "yesterday" ET.
    data_date_et = (et_now.date() - timedelta(days=1)).isoformat()

    odds_current = fetch_odds_current()

    teams_block = build_teams_payload()

    payload = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": generated_at.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "data_date_et": data_date_et,
        "source_status": {
            "odds_current": {
                "ok": odds_current.get("ok", False),
                **({"meta": odds_current.get("meta")} if odds_current.get("meta") else {}),
                **({"error": odds_current.get("error")} if odds_current.get("error") else {}),
            },
            "odds_open": {
                "ok": False,
                "error": "not_on_plan",
            },
            "teams": {
                "ok": teams_block.get("ok", False),
                "url": teams_block.get("url"),
            },
            "goalies": {
                "ok": False,
                "error": "not_used",
            },
        },
        "validations": {
            "odds_games_count": odds_current.get("games_count", 0),
            "teams_count": teams_block.get("teams_count", 0),
            "goalies_count": 0,
        },
        "league_avg_lambda": teams_block["league_avg_lambda"],
        "teams": teams_block["teams"],
        "odds_current": odds_current.get("data", []),
    }

    # Only overwrite if content changed (helps avoid meaningless commits)
    if os.path.exists(OUTPUT_PATH):
        before = file_sha256(OUTPUT_PATH)
        atomic_write_json(OUTPUT_PATH, payload)
        after = file_sha256(OUTPUT_PATH)
        if before == after:
            print("No content change in output JSON.")
        else:
            print("Output JSON updated.")
    else:
        atomic_write_json(OUTPUT_PATH, payload)
        print("Output JSON created.")

    # Hard validation: one row per team
    team_names = [t["team"] for t in payload["teams"]]
    if len(team_names) != len(set(team_names)):
        raise ValueError("Duplicate teams present in output payload. Filtering failed.")


if __name__ == "__main__":
    main()
