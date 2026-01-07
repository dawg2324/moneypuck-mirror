import os
import json
import hashlib
from datetime import datetime, timezone, timedelta
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

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def safe_get(url: str, params: dict | None = None, timeout: int = 25):
    r = requests.get(url, params=params, timeout=timeout, headers={"User-Agent": "nhl-daily-artifact/1.0"})
    r.raise_for_status()
    return r

def now_utc_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def et_date_str(dt_utc: datetime):
    et = tz.gettz("America/New_York")
    return dt_utc.astimezone(et).date().isoformat()

def pick_best_h2h(bookmakers, home_team, away_team):
    """
    Returns best (max) american odds for each side across all books.
    """
    best = {
        "home": None, "away": None,
        "home_book": None, "away_book": None
    }
    for bk in bookmakers or []:
        bkey = bk.get("key")
        btitle = bk.get("title")
        for market in bk.get("markets", []):
            if market.get("key") != "h2h":
                continue
            for outcome in market.get("outcomes", []):
                name = outcome.get("name")
                price = outcome.get("price")
                if name == home_team and isinstance(price, (int, float)):
                    if best["home"] is None or price > best["home"]:
                        best["home"] = int(price)
                        best["home_book"] = {"key": bkey, "title": btitle}
                if name == away_team and isinstance(price, (int, float)):
                    if best["away"] is None or price > best["away"]:
                        best["away"] = int(price)
                        best["away_book"] = {"key": bkey, "title": btitle}
    return best

def pick_best_totals(bookmakers):
    """
    Returns a single totals line and best prices for Over and Under at that line.
    Strategy:
      - choose the most common totals point among books (mode)
      - within that point, choose max (best) American price for Over and Under
    """
    points = []
    for bk in bookmakers or []:
        for market in bk.get("markets", []):
            if market.get("key") != "totals":
                continue
            for outcome in market.get("outcomes", []):
                pt = outcome.get("point")
                if isinstance(pt, (int, float)):
                    points.append(float(pt))
    if not points:
        return None

    # mode with rounding to 0.5
    rounded = [round(p * 2) / 2 for p in points]
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
            for outcome in market.get("outcomes", []):
                pt = outcome.get("point")
                if pt is None:
                    continue
                pt = round(float(pt) * 2) / 2
                if pt != mode_point:
                    continue
                price = outcome.get("price")
                name = outcome.get("name")  # "Over" / "Under"
                if not isinstance(price, (int, float)):
                    continue
                if name == "Over":
                    if best_over is None or price > best_over:
                        best_over = int(price)
                        best_over_book = {"key": bkey, "title": btitle}
                if name == "Under":
                    if best_under is None or price > best_under:
                        best_under = int(price)
                        best_under_book = {"key": bkey, "title": btitle}

    return {
        "total": mode_point,
        "over_price": best_over,
        "under_price": best_under,
        "over_book": best_over_book,
        "under_book": best_under_book
    }

def american_to_prob(odds: int) -> float:
    # implied probability including vig
    if odds < 0:
        return (-odds) / ((-odds) + 100.0)
    return 100.0 / (odds + 100.0)

def novig_from_two_sides(away_odds: int, home_odds: int):
    pa = american_to_prob(away_odds)
    ph = american_to_prob(home_odds)
    s = pa + ph
    if s <= 0:
        return None
    return {"away": pa / s, "home": ph / s}

def poisson_over_prob(lam: float, total: float) -> float:
    # P(goals > total) for half-goal totals
    import math
    k_max = int(total)  # for 6.5, k_max=6
    cdf = 0.0
    for k in range(0, k_max + 1):
        cdf += math.exp(-lam) * (lam ** k) / math.factorial(k)
    return 1.0 - cdf

def logistic_win_prob(goal_diff: float, scale: float = 1.05) -> float:
    import math
    return 1.0 / (1.0 + math.exp(-goal_diff / scale))

def main():
    odds_key = os.environ.get("ODDS_API_KEY", "").strip()
    if not odds_key:
        raise RuntimeError("Missing ODDS_API_KEY secret")

    os.makedirs(DATA_DIR, exist_ok=True)

    run_utc = datetime.now(timezone.utc).replace(microsecond=0)
    data_date_et = et_date_str(run_utc)

    out = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": run_utc.isoformat(),
        "data_date_et": data_date_et,
        "source_status": {},
        "inputs_hash": {},
        "validations": {},
        "raw": {},
        "slim": {}
    }

    # 1) Pull Odds API current
    try:
        params = {
            "apiKey": odds_key,
            "regions": "us",
            "markets": "h2h,totals",
            "oddsFormat": "american",
            "dateFormat": "iso"
        }
        r = safe_get(ODDS_CURRENT_URL, params=params)
        b = r.content
        out["inputs_hash"]["odds_current_sha256"] = sha256_bytes(b)
        odds_current = r.json()
        out["raw"]["odds_current"] = odds_current
        out["source_status"]["odds_current"] = {"ok": True}
    except Exception as e:
        out["source_status"]["odds_current"] = {"ok": False, "error": str(e)}
        odds_current = None

    # 2) Pull Odds API historical snapshot (opening proxy)
    # Define opening snapshot as 12:00:00Z on the ET date
    try:
        et = tz.gettz("America/New_York")
        et_midnight = datetime.fromisoformat(data_date_et).replace(tzinfo=et)
        snap_utc = (et_midnight + timedelta(hours=7)).astimezone(timezone.utc)  # approx aligns with 12:00Z winter? keep stable
        snap_iso = snap_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")

        params = {
            "apiKey": odds_key,
            "regions": "us",
            "markets": "h2h,totals",
            "oddsFormat": "american",
            "dateFormat": "iso",
            "date": snap_iso
        }
        r = safe_get(ODDS_HIST_URL, params=params)
        b = r.content
        out["inputs_hash"]["odds_open_sha256"] = sha256_bytes(b)
        odds_open = r.json()
        out["raw"]["odds_open"] = odds_open
        out["source_status"]["odds_open"] = {"ok": True, "snapshot_requested": snap_iso, "snapshot_used": odds_open.get("timestamp")}
    except Exception as e:
        out["source_status"]["odds_open"] = {"ok": False, "error": str(e)}
        odds_open = None

    # 3) Pull teams.csv
    try:
        r = safe_get(TEAMS_URL)
        b = r.content
        out["inputs_hash"]["teams_sha256"] = sha256_bytes(b)
        df = pd.read_csv(pd.io.common.BytesIO(b))
        out["source_status"]["teams"] = {"ok": True, "url": TEAMS_URL}

        # filter if columns present
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
        out["slim"]["teams"] = df[["team", "games_played", "xGF_pg", "xGA_pg"]].to_dict(orient="records")
        out["slim"]["league_avg_lambda"] = league_avg_lambda
        out["validations"]["teams_count"] = int(df["team"].nunique())
    except Exception as e:
        out["source_status"]["teams"] = {"ok": False, "error": str(e)}

    # 4) Pull goalies.csv
    try:
        r = safe_get(GOALIES_URL)
        b = r.content
        out["inputs_hash"]["goalies_sha256"] = sha256_bytes(b)
        df = pd.read_csv(pd.io.common.BytesIO(b))
        out["source_status"]["goalies"] = {"ok": True, "url": GOALIES_URL}

        # common filters if present
        if "situation" in df.columns:
            df = df[df["situation"].astype(str).str.lower() == "all"]
        if "position" in df.columns:
            df = df[df["position"].astype(str).str.lower() == "goalie"]

        # derive GSAx/60 when possible
        cols = set(df.columns)
        if {"xGoals", "goals", "icetime"}.issubset(cols):
            df["gsa_x60"] = (df["xGoals"] - df["goals"]) * 3600.0 / df["icetime"].replace(0, pd.NA)
        elif {"goalsSavedAboveExpected", "icetime"}.issubset(cols):
            df["gsa_x60"] = df["goalsSavedAboveExpected"] * 3600.0 / df["icetime"].replace(0, pd.NA)
        else:
            raise RuntimeError("goalies.csv missing fields to derive gsa_x60")

        # require name, team
        if "name" not in df.columns or "team" not in df.columns:
            raise RuntimeError("goalies.csv missing name/team")

        slim = df[["name", "team", "gsa_x60"]].dropna(subset=["gsa_x60"]).copy()
        out["slim"]["goalies"] = slim.to_dict(orient="records")
        out["validations"]["goalies_count"] = int(slim.shape[0])
    except Exception as e:
        out["source_status"]["goalies"] = {"ok": False, "error": str(e)}

    # 5) Build slim odds view for scheduled task
    slim_games = []
    if isinstance(odds_current, list):
        for g in odds_current:
            home = g.get("home_team")
            away = g.get("away_team")
            commence = g.get("commence_time")
            bks = g.get("bookmakers", [])
            best_h2h = pick_best_h2h(bks, home, away)
            best_tot = pick_best_totals(bks)
            slim_games.append({
                "id": g.get("id"),
                "commence_time": commence,
                "home_team": home,
                "away_team": away,
                "h2h": {
                    "home": best_h2h["home"],
                    "away": best_h2h["away"],
                    "home_book": best_h2h["home_book"],
                    "away_book": best_h2h["away_book"]
                },
                "totals": best_tot
            })

    out["slim"]["odds_current"] = slim_games
    out["validations"]["odds_games_count"] = len(slim_games)

    # Health file for monitoring
    health = {
        "generated_at_utc": out["generated_at_utc"],
        "data_date_et": out["data_date_et"],
        "ok": all(v.get("ok") for v in out["source_status"].values()) if out["source_status"] else False,
        "source_status": out["source_status"],
        "validations": out["validations"]
    }

    # Write outputs
    daily_path = f"{DATA_DIR}/nhl_daily_{data_date_et}.json"
    latest_path = f"{DATA_DIR}/nhl_daily.json"
    health_path = f"{DATA_DIR}/health.json"

    with open(daily_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    with open(latest_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    with open(health_path, "w", encoding="utf-8") as f:
        json.dump(health, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
