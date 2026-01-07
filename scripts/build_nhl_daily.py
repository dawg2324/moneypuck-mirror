import os
import json
import hashlib
from datetime import datetime, timezone
from dateutil import tz
import requests
import pandas as pd


REPO_RAW_BASE = "https://raw.githubusercontent.com/dawg2324/moneypuck-mirror/main"
TEAMS_URL = f"{REPO_RAW_BASE}/data/teams.csv"
GOALIES_URL = f"{REPO_RAW_BASE}/data/goalies.csv"

ODDS_CURRENT_URL = "https://api.the-odds-api.com/v4/sports/icehockey_nhl/odds"
ODDS_HIST_URL = "https://api.the-odds-api.com/v4/historical/sports/icehockey_nhl/odds"

DATA_DIR = "data"
SCHEMA_VERSION = "1.0.0"
OPENING_SNAPSHOT_TIME_Z = "T12:00:00Z"


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def safe_get(url: str, params: dict | None = None, timeout: int = 30) -> requests.Response:
    r = requests.get(
        url,
        params=params,
        timeout=timeout,
        headers={"User-Agent": "nhl-daily-artifact/1.0"},
    )
    r.raise_for_status()
    return r


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def et_today_date_str() -> str:
    et = tz.gettz("America/New_York")
    return datetime.now(timezone.utc).astimezone(et).date().isoformat()


def american_to_prob(odds: int) -> float:
    if odds < 0:
        return (-odds) / ((-odds) + 100.0)
    return 100.0 / (odds + 100.0)


def novig_from_two_sides(away_odds: int, home_odds: int) -> dict | None:
    pa = american_to_prob(int(away_odds))
    ph = american_to_prob(int(home_odds))
    s = pa + ph
    if s <= 0:
        return None
    return {"away": pa / s, "home": ph / s}


def round_to_half(x: float) -> float:
    return round(x * 2) / 2


def pick_best_h2h(bookmakers: list, home_team: str, away_team: str) -> dict:
    best_home = None
    best_away = None
    best_home_book = None
    best_away_book = None

    for bk in bookmakers or []:
        bkey = bk.get("key")
        btitle = bk.get("title")

        for market in bk.get("markets", []):
            if market.get("key") != "h2h":
                continue

            for outc in market.get("outcomes", []):
                name = outc.get("name")
                price = outc.get("price")
                if not isinstance(price, (int, float)):
                    continue
                price = int(price)

                if name == home_team:
                    if best_home is None or price > best_home:
                        best_home = price
                        best_home_book = {"key": bkey, "title": btitle}

                if name == away_team:
                    if best_away is None or price > best_away:
                        best_away = price
                        best_away_book = {"key": bkey, "title": btitle}

    return {
        "home_price": best_home,
        "away_price": best_away,
        "home_book": best_home_book,
        "away_book": best_away_book,
    }


def pick_best_totals(bookmakers: list) -> dict | None:
    pts = []
    for bk in bookmakers or []:
        for market in bk.get("markets", []):
            if market.get("key") != "totals":
                continue
            for outc in market.get("outcomes", []):
                p = outc.get("point")
                if isinstance(p, (int, float)):
                    pts.append(float(p))

    if not pts:
        return None

    rounded = [round_to_half(p) for p in pts]
    mode_point = max(set(rounded), key=rounded.count)

    best_over = None
    best_under = None
    best_over_book = None
    best_under_book = None

    for bk in bookmakers or []:
        bkey = bk.get("key")
        btitle = bk.get("title")

        for market in bk.get("markets", []):
            if market.get("key") != "totals":
                continue

            for outc in market.get("outcomes", []):
                p = outc.get("point")
                if p is None:
                    continue
                p = round_to_half(float(p))
                if p != mode_point:
                    continue

                name = outc.get("name")
                price = outc.get("price")
                if not isinstance(price, (int, float)):
                    continue
                price = int(price)

                if name == "Over":
                    if best_over is None or price > best_over:
                        best_over = price
                        best_over_book = {"key": bkey, "title": btitle}
                elif name == "Under":
                    if best_under is None or price > best_under:
                        best_under = price
                        best_under_book = {"key": bkey, "title": btitle}

    return {
        "total": mode_point,
        "over_price": best_over,
        "under_price": best_under,
        "over_book": best_over_book,
        "under_book": best_under_book,
    }


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def main() -> None:
    odds_key = os.environ.get("ODDS_API_KEY", "").strip()
    if not odds_key:
        raise RuntimeError("Missing ODDS_API_KEY secret in GitHub Actions.")

    ensure_dir(DATA_DIR)

    generated_at_utc = utc_now_iso()
    data_date_et = et_today_date_str()
    snapshot_iso_z = f"{data_date_et}{OPENING_SNAPSHOT_TIME_Z}"

    source_status = {}
    validations = {}
    inputs_hash = {}

    # 1) Odds current (ALWAYS uses secret ODDS_API_KEY)
    odds_current = None
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
        odds_current = r.json()
        source_status["odds_current"] = {"ok": True}
    except Exception as e:
        source_status["odds_current"] = {"ok": False, "error": str(e)}

    # 2) Odds opening snapshot (ALWAYS uses secret ODDS_API_KEY)
    odds_open = None
    try:
        params = {
            "apiKey": odds_key,
            "regions": "us",
            "markets": "h2h,totals",
            "oddsFormat": "american",
            "dateFormat": "iso",
            "date": snapshot_iso_z,
        }
        r = safe_get(ODDS_HIST_URL, params=params)
        inputs_hash["odds_open_sha256"] = sha256_bytes(r.content)
        odds_open = r.json()
        source_status["odds_open"] = {
            "ok": True,
            "snapshot_requested": snapshot_iso_z,
            "snapshot_used": odds_open.get("timestamp"),
        }
    except Exception as e:
        source_status["odds_open"] = {"ok": False, "error": str(e), "snapshot_requested": snapshot_iso_z}

    # 3) Teams
    teams_slim = []
    league_avg_lambda = None
    try:
        r = safe_get(TEAMS_URL)
        inputs_hash["teams_sha256"] = sha256_bytes(r.content)
        df = pd.read_csv(pd.io.common.BytesIO(r.content))

        if "situation" in df.columns:
            df = df[df["situation"].astype(str).str.lower() == "all"]
        if "position" in df.columns:
            df = df[df["position"].astype(str).str.lower() == "team level"]

        needed = ["team", "games_played", "scoreVenueAdjustedxGoalsFor", "scoreVenueAdjustedxGoalsAgainst"]
        for c in needed:
            if c not in df.columns:
                raise RuntimeError(f"teams.csv missing column: {c}")

        df = df[needed].copy()
        df["xGF_pg"] = df["scoreVenueAdjustedxGoalsFor"] / df["games_played"]
        df["xGA_pg"] = df["scoreVenueAdjustedxGoalsAgainst"] / df["games_played"]

        league_avg_lambda = float(df["xGF_pg"].mean())
        teams_slim = df[["team", "games_played", "xGF_pg", "xGA_pg"]].to_dict(orient="records")

        source_status["teams"] = {"ok": True, "url": TEAMS_URL}
        validations["teams_count"] = int(df["team"].nunique())
    except Exception as e:
        source_status["teams"] = {"ok": False, "error": str(e), "url": TEAMS_URL}

    # 4) Goalies (filter + dedupe to one row per (name, team))
    goalies_slim = []
    try:
        r = safe_get(GOALIES_URL)
        inputs_hash["goalies_sha256"] = sha256_bytes(r.content)
        df = pd.read_csv(pd.io.common.BytesIO(r.content))

        # Normalize possible split columns and filter to overall rows if they exist
        for col in ["situation", "split", "strength", "gameState"]:
            if col in df.columns:
                df[col] = df[col].astype(str).str.lower()

        if "situation" in df.columns:
            df = df[df["situation"] == "all"]
        if "split" in df.columns:
            df = df[df["split"].isin(["all", "overall"])]

        cols = set(df.columns)
        if {"xGoals", "goals", "icetime"}.issubset(cols):
            df["gsa_x60"] = (df["xGoals"] - df["goals"]) * 3600.0 / df["icetime"].replace(0, pd.NA)
        elif {"goalsSavedAboveExpected", "icetime"}.issubset(cols):
            df["gsa_x60"] = df["goalsSavedAboveExpected"] * 3600.0 / df["icetime"].replace(0, pd.NA)
        else:
            raise RuntimeError("goalies.csv missing fields to derive gsa_x60")

        if "name" not in df.columns or "team" not in df.columns:
            raise RuntimeError("goalies.csv missing name/team")

        slim = df[["name", "team", "gsa_x60"]].dropna(subset=["gsa_x60"]).copy()

        # Dedupe to one row per (name, team)
        slim = slim.sort_values(by=["name", "team"]).drop_duplicates(subset=["name", "team"], keep="first")

        goalies_slim = slim.to_dict(orient="records")

        source_status["goalies"] = {"ok": True, "url": GOALIES_URL}
        validations["goalies_count"] = int(slim.shape[0])
    except Exception as e:
        source_status["goalies"] = {"ok": False, "error": str(e), "url": GOALIES_URL}

    # 5) Slim odds transform (current)
    slim_events = []
    if isinstance(odds_current, list):
        for g in odds_current:
            home = g.get("home_team")
            away = g.get("away_team")
            commence = g.get("commence_time")
            bks = g.get("bookmakers", [])

            best_h2h = pick_best_h2h(bks, home, away)
            best_tot = pick_best_totals(bks)

            novig = None
            if best_h2h["home_price"] is not None and best_h2h["away_price"] is not None:
                novig = novig_from_two_sides(best_h2h["away_price"], best_h2h["home_price"])

            slim_events.append({
                "id": g.get("id"),
                "commence_time": commence,
                "home_team": home,
                "away_team": away,
                "h2h": {
                    "home": best_h2h["home_price"],
                    "away": best_h2h["away_price"],
                    "home_book": best_h2h["home_book"],
                    "away_book": best_h2h["away_book"],
                    "novig": novig,
                },
                "totals": best_tot,
            })

    validations["odds_games_count"] = int(len(slim_events))

    slim_out = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": generated_at_utc,
        "data_date_et": data_date_et,
        "source_status": source_status,
        "validations": validations,
        "inputs_hash": inputs_hash,
        "slim": {
            "league_avg_lambda": league_avg_lambda,
            "teams": teams_slim,
            "goalies": goalies_slim,
            "odds_current": slim_events,
        },
    }

    health = {
        "generated_at_utc": generated_at_utc,
        "data_date_et": data_date_et,
        "ok": bool(source_status.get("odds_current", {}).get("ok", False) and source_status.get("teams", {}).get("ok", False)),
        "source_status": source_status,
        "validations": validations,
    }

    latest_path = f"{DATA_DIR}/nhl_daily_slim.json"
    archive_path = f"{DATA_DIR}/nhl_daily_slim_{data_date_et}.json"
    health_path = f"{DATA_DIR}/health.json"

    with open(latest_path, "w", encoding="utf-8") as f:
        json.dump(slim_out, f, ensure_ascii=False, indent=2)
    with open(archive_path, "w", encoding="utf-8") as f:
        json.dump(slim_out, f, ensure_ascii=False, indent=2)
    with open(health_path, "w", encoding="utf-8") as f:
        json.dump(health, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
```0
