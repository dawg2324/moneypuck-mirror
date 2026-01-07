import os
import json
import hashlib
from datetime import datetime, timezone, timedelta
from dateutil import tz
import requests
import pandas as pd


# -----------------------------
# CONFIG
# -----------------------------
REPO_RAW_BASE = "https://raw.githubusercontent.com/dawg2324/moneypuck-mirror/main"
TEAMS_URL = f"{REPO_RAW_BASE}/data/teams.csv"
GOALIES_URL = f"{REPO_RAW_BASE}/data/goalies.csv"

ODDS_CURRENT_URL = "https://api.the-odds-api.com/v4/sports/icehockey_nhl/odds"
ODDS_HIST_URL = "https://api.the-odds-api.com/v4/historical/sports/icehockey_nhl/odds"

DATA_DIR = "data"
SCHEMA_VERSION = "1.0.0"

# Historical "opening" snapshot time (UTC). You requested 12:00:00Z.
OPENING_SNAPSHOT_HOUR_UTC = 12


# -----------------------------
# HELPERS
# -----------------------------
def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def safe_get(url: str, params: dict | None = None, timeout: int = 25) -> requests.Response:
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


def et_today_date() -> str:
    et = tz.gettz("America/New_York")
    return datetime.now(timezone.utc).astimezone(et).date().isoformat()


def to_snapshot_iso_z(date_et: str, hour_utc: int) -> str:
    # date_et is YYYY-MM-DD. We request YYYY-MM-DDThh:00:00Z (UTC).
    # This is intentionally fixed in UTC, per your spec.
    dt = datetime.fromisoformat(date_et).replace(tzinfo=timezone.utc)
    snap = dt.replace(hour=hour_utc, minute=0, second=0, microsecond=0)
    return snap.isoformat().replace("+00:00", "Z")


def american_to_prob(odds: int) -> float:
    if odds < 0:
        return (-odds) / ((-odds) + 100.0)
    return 100.0 / (odds + 100.0)


def novig_from_two_sides(away_odds: int, home_odds: int) -> dict | None:
    pa = american_to_prob(away_odds)
    ph = american_to_prob(home_odds)
    s = pa + ph
    if s <= 0:
        return None
    return {"away": pa / s, "home": ph / s}


def pick_best_h2h(bookmakers: list, home_team: str, away_team: str) -> dict:
    """
    Best = highest (most favorable to bettor) American odds across books.
    Returns best odds for home and away plus the book that provided it.
    """
    best = {
        "home_price": None,
        "away_price": None,
        "home_book": None,
        "away_book": None,
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
                if not isinstance(price, (int, float)):
                    continue
                price = int(price)

                if name == home_team:
                    if best["home_price"] is None or price > best["home_price"]:
                        best["home_price"] = price
                        best["home_book"] = {"key": bkey, "title": btitle}

                if name == away_team:
                    if best["away_price"] is None or price > best["away_price"]:
                        best["away_price"] = price
                        best["away_book"] = {"key": bkey, "title": btitle}

    return best


def pick_best_totals(bookmakers: list) -> dict | None:
    """
    Picks the most common total point among books (rounded to 0.5),
    then chooses the best (highest) American price for Over and Under at that point.
    """
    pts = []
    for bk in bookmakers or []:
        for market in bk.get("markets", []):
            if market.get("key") != "totals":
                continue
            for outcome in market.get("outcomes", []):
                p = outcome.get("point")
                if isinstance(p, (int, float)):
                    pts.append(float(p))

    if not pts:
        return None

    rounded = [round(p * 2) / 2 for p in pts]
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
                p = outcome.get("point")
                if p is None:
                    continue
                p = round(float(p) * 2) / 2
                if p != mode_point:
                    continue

                name = outcome.get("name")  # "Over" / "Under"
                price = outcome.get("price")
                if not isinstance(price, (int, float)):
                    continue
                price = int(price)

                if name == "Over":
                    if best_over is None or price > best_over:
                        best_over = price
                        best_over_book = {"key": bkey, "title": btitle}

                if name == "Under":
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


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


# -----------------------------
# MAIN
# -----------------------------
def main():
    odds_key = os.environ.get("ODDS_API_KEY", "").strip()
    if not odds_key:
        raise RuntimeError("Missing ODDS_API_KEY secret in GitHub Actions.")

    ensure_dir(DATA_DIR)

    generated_at_utc = utc_now_iso()
    data_date_et = et_today_date()
    snapshot_iso_z = to_snapshot_iso_z(data_date_et, OPENING_SNAPSHOT_HOUR_UTC)

    source_status = {}
    validations = {}
    inputs_hash = {}

    raw_counts = {}

    # -----------------------------
    # 1) Odds current
    # -----------------------------
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
        b = r.content
        inputs_hash["odds_current_sha256"] = sha256_bytes(b)
        odds_current = r.json()
        source_status["odds_current"] = {"ok": True}
        raw_counts["odds_current_events"] = len(odds_current) if isinstance(odds_current, list) else 0
    except Exception as e:
        source_status["odds_current"] = {"ok": False, "error": str(e)}

    # -----------------------------
    # 2) Odds opening snapshot
    # -----------------------------
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
        b = r.content
        inputs_hash["odds_open_sha256"] = sha256_bytes(b)
        odds_open = r.json()
        source_status["odds_open"] = {
            "ok": True,
            "snapshot_requested": snapshot_iso_z,
            "snapshot_used": odds_open.get("timestamp"),
        }
        # For historical endpoint, events are usually in odds_open["data"]
        data = odds_open.get("data")
        raw_counts["odds_open_events"] = len(data) if isinstance(data, list) else 0
    except Exception as e:
        source_status["odds_open"] = {"ok": False, "error": str(e), "snapshot_requested": snapshot_iso_z}

    # -----------------------------
    # 3) Teams CSV (MoneyPuck mirror)
    # -----------------------------
    teams_slim = []
    league_avg_lambda = None
    try:
        r = safe_get(TEAMS_URL)
        b = r.content
        inputs_hash["teams_sha256"] = sha256_bytes(b)

        df = pd.read_csv(pd.io.common.BytesIO(b))

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

    # -----------------------------
    # 4) Goalies CSV (MoneyPuck mirror)
    # -----------------------------
    goalies_slim = []
    try:
        r = safe_get(GOALIES_URL)
        b = r.content
        inputs_hash["goalies_sha256"] = sha256_bytes(b)

        df = pd.read_csv(pd.io.common.BytesIO(b))

        # Derive gsa_x60
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
        goalies_slim = slim.to_dict(orient="records")

        source_status["goalies"] = {"ok": True, "url": GOALIES_URL}
        validations["goalies_count"] = int(slim.shape[0])
    except Exception as e:
        source_status["goalies"] = {"ok": False, "error": str(e), "url": GOALIES_URL}

    # -----------------------------
    # 5) Slim odds transformation
    # -----------------------------
    slim_events = []

    if isinstance(odds_current, list):
        for g in odds_current:
            home = g.get("home_team")
            away = g.get("away_team")
            commence = g.get("commence_time")
            bks = g.get("bookmakers", [])

            best_h2h = pick_best_h2h(bks, home, away)
            best_tot = pick_best_totals(bks)

            # Require both sides for ML
            if best_h2h["home_price"] is None or best_h2h["away_price"] is None:
                novig = None
            else:
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
                    "novig": novig
                },
                "totals": best_tot
            })

    validations["odds_games_count"] = int(len(slim_events))

    # -----------------------------
    # 6) Build final SLIM artifact
    # -----------------------------
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
        }
    }

    # Health file
    health = {
        "generated_at_utc": generated_at_utc,
        "data_date_et": data_date_et,
        "ok": (
            source_status.get("odds_current", {}).get("ok", False)
            and source_status.get("teams", {}).get("ok", False)
        ),
        "source_status": source_status,
        "validations": validations,
        "raw_counts": raw_counts
    }

    # Write outputs (multiline, stable)
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
    main()    """
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
