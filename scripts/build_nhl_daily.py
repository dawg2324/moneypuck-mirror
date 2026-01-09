#!/usr/bin/env python3
from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd

from scripts.fetch_starters_dailyfaceoff import fetch_dailyfaceoff_starters
from scripts.compute_rest import build_slim_rest

SCHEMA_VERSION = "1.0.7"
SPORT_KEY = "icehockey_nhl"
DAILYFACEOFF_BASE = "https://www.dailyfaceoff.com"

# MoneyPuck endpoints (you already used these patterns in prior runs)
MP_TEAMS_URL = "https://moneypuck.com/moneypuck/playerData/seasonSummary/2025/regular/teams.csv"
MP_GOALIES_URL = "https://moneypuck.com/moneypuck/playerData/seasonSummary/2025/regular/goalies.csv"


# --------------------------- TEAM NAME → ABBREV -------------------------------

TEAM_NAME_TO_ABBREV = {
    "ANAHEIM DUCKS": "ANA",
    "ARIZONA COYOTES": "ARI",
    "BOSTON BRUINS": "BOS",
    "BUFFALO SABRES": "BUF",
    "CALGARY FLAMES": "CGY",
    "CAROLINA HURRICANES": "CAR",
    "CHICAGO BLACKHAWKS": "CHI",
    "COLORADO AVALANCHE": "COL",
    "COLUMBUS BLUE JACKETS": "CBJ",
    "DALLAS STARS": "DAL",
    "DETROIT RED WINGS": "DET",
    "EDMONTON OILERS": "EDM",
    "FLORIDA PANTHERS": "FLA",
    "LOS ANGELES KINGS": "LAK",
    "MINNESOTA WILD": "MIN",
    "MONTREAL CANADIENS": "MTL",
    "NASHVILLE PREDATORS": "NSH",
    "NEW JERSEY DEVILS": "NJD",
    "NEW YORK ISLANDERS": "NYI",
    "NEW YORK RANGERS": "NYR",
    "OTTAWA SENATORS": "OTT",
    "PHILADELPHIA FLYERS": "PHI",
    "PITTSBURGH PENGUINS": "PIT",
    "SAN JOSE SHARKS": "SJS",
    "SEATTLE KRAKEN": "SEA",
    "ST. LOUIS BLUES": "STL",
    "ST LOUIS BLUES": "STL",
    "TAMPA BAY LIGHTNING": "TBL",
    "TORONTO MAPLE LEAFS": "TOR",
    "VANCOUVER CANUCKS": "VAN",
    "VEGAS GOLDEN KNIGHTS": "VGK",
    "WASHINGTON CAPITALS": "WSH",
    "WINNIPEG JETS": "WPG",
    "UTAH MAMMOTH": "UTA",
}


# --------------------------- time helpers ------------------------------------

def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def et_today_date_str() -> str:
    try:
        from zoneinfo import ZoneInfo
        et = dt.datetime.now(ZoneInfo("America/New_York"))
        return et.date().isoformat()
    except Exception:
        return dt.datetime.now(dt.timezone.utc).date().isoformat()


# --------------------------- network helpers ---------------------------------

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def http_get_bytes(url: str, headers: Optional[Dict[str, str]] = None, timeout: int = 30) -> bytes:
    req = Request(url, headers=headers or {})
    with urlopen(req, timeout=timeout) as resp:
        return resp.read()


def http_get_json(url: str, headers: Optional[Dict[str, str]] = None, timeout: int = 30) -> Tuple[Any, bytes]:
    raw = http_get_bytes(url, headers=headers, timeout=timeout)
    return json.loads(raw.decode("utf-8")), raw


def read_csv_url(url: str, timeout: int = 30) -> Tuple[pd.DataFrame, str]:
    raw = http_get_bytes(url, headers={"User-Agent": "nhl-daily-slim"}, timeout=timeout)
    sha = sha256_bytes(raw)
    df = pd.read_csv(pd.io.common.BytesIO(raw))
    return df, sha


def norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


def pick_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = set(df.columns)
    for c in candidates:
        if c in cols:
            return c
    # try case-insensitive match
    lower_map = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    return None


# --------------------------- odds helpers ------------------------------------

def median_int(values: List[int]) -> Optional[float]:
    if not values:
        return None
    s = sorted(values)
    n = len(s)
    mid = n // 2
    if n % 2 == 1:
        return float(s[mid])
    return (s[mid - 1] + s[mid]) / 2.0


def pick_most_common_float(values: List[float]) -> Optional[float]:
    if not values:
        return None
    counts: Dict[float, int] = {}
    for v in values:
        counts[v] = counts.get(v, 0) + 1
    best_count = max(counts.values())
    tied = [k for k, c in counts.items() if c == best_count]
    return min(tied)


def extract_market(bookmaker: Dict[str, Any], market_key: str) -> Optional[Dict[str, Any]]:
    markets = bookmaker.get("markets") or []
    for m in markets:
        if m.get("key") == market_key:
            return m
    return None


def slim_odds_current(odds_payload: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    slimmed: List[Dict[str, Any]] = []
    meta: Dict[str, Any] = {
        "games_with_h2h": 0,
        "games_with_totals": 0,
    }

    for g in odds_payload or []:
        game_id = g.get("id")
        commence = g.get("commence_time")
        home_team = g.get("home_team")
        away_team = g.get("away_team")
        if not (commence and home_team and away_team):
            continue

        bookmakers = g.get("bookmakers") or []

        best_home: Optional[Tuple[int, str]] = None
        best_away: Optional[Tuple[int, str]] = None
        home_prices: List[int] = []
        away_prices: List[int] = []

        total_points: List[float] = []
        best_over_by_point: Dict[float, Tuple[int, str]] = {}
        best_under_by_point: Dict[float, Tuple[int, str]] = {}

        for bm in bookmakers:
            bm_key = bm.get("key") or ""

            m_h2h = extract_market(bm, "h2h")
            if m_h2h:
                outcomes = m_h2h.get("outcomes") or []
                for o in outcomes:
                    name = o.get("name")
                    price = o.get("price")
                    if name == home_team and isinstance(price, int):
                        home_prices.append(price)
                        if best_home is None or price > best_home[0]:
                            best_home = (price, bm_key)
                    elif name == away_team and isinstance(price, int):
                        away_prices.append(price)
                        if best_away is None or price > best_away[0]:
                            best_away = (price, bm_key)

            m_totals = extract_market(bm, "totals")
            if m_totals:
                outcomes = m_totals.get("outcomes") or []
                for o in outcomes:
                    name = o.get("name")
                    price = o.get("price")
                    point = o.get("point")
                    if not isinstance(price, int):
                        continue
                    if not isinstance(point, (int, float)):
                        continue
                    pt = float(point)
                    total_points.append(pt)

                    if name == "Over":
                        cur = best_over_by_point.get(pt)
                        if cur is None or price > cur[0]:
                            best_over_by_point[pt] = (price, bm_key)
                    elif name == "Under":
                        cur = best_under_by_point.get(pt)
                        if cur is None or price > cur[0]:
                            best_under_by_point[pt] = (price, bm_key)

        consensus_home = median_int(home_prices)
        consensus_away = median_int(away_prices)

        has_h2h = best_home is not None and best_away is not None
        if has_h2h:
            meta["games_with_h2h"] += 1

        consensus_total_line = pick_most_common_float(total_points)
        has_totals = consensus_total_line is not None
        if has_totals:
            meta["games_with_totals"] += 1

        out_game: Dict[str, Any] = {
            "id": game_id,
            "commence_time": commence,
            "home_team": home_team,
            "away_team": away_team,
        }

        if has_h2h:
            out_game["h2h"] = {
                "best": {
                    "home_price": best_home[0],
                    "home_book": best_home[1],
                    "away_price": best_away[0],
                    "away_book": best_away[1],
                },
                "consensus_median": {
                    "home_price": consensus_home,
                    "away_price": consensus_away,
                    "n_books_home": len(home_prices),
                    "n_books_away": len(away_prices),
                },
            }

        if has_totals:
            best_over = best_over_by_point.get(consensus_total_line)
            best_under = best_under_by_point.get(consensus_total_line)
            out_game["totals"] = {
                "line": consensus_total_line,
                "best": {
                    "over_price": best_over[0] if best_over else None,
                    "over_book": best_over[1] if best_over else None,
                    "under_price": best_under[0] if best_under else None,
                    "under_book": best_under[1] if best_under else None,
                },
            }

        slimmed.append(out_game)

    return slimmed, meta


# --------------------------- odds: current -----------------------------------

def fetch_odds_current() -> Tuple[Optional[List[Dict[str, Any]]], Dict[str, Any], Optional[str]]:
    api_key = os.environ.get("ODDS_API_KEY", "").strip()
    if not api_key:
        return None, {"ok": False, "reason": "Missing ODDS_API_KEY env var"}, None

    regions = os.environ.get("ODDS_API_REGIONS", "us").strip()
    markets = os.environ.get("ODDS_API_MARKETS", "h2h,totals").strip()

    base_url = "https://api.the-odds-api.com/v4/sports/icehockey_nhl/odds"
    qs = urlencode(
        {
            "apiKey": api_key,
            "regions": regions,
            "markets": markets,
            "oddsFormat": "american",
            "dateFormat": "iso",
        }
    )
    url = f"{base_url}?{qs}"

    try:
        payload, raw = http_get_json(url)
        return payload, {"ok": True, "meta": {"endpoint": "odds_current", "url": base_url, "regions": regions, "markets": markets}}, sha256_bytes(raw)
    except Exception as e:
        return None, {"ok": False, "error": str(e)}, None


# --------------------------- teams / goalies (MoneyPuck) ----------------------

def build_slim_teams_and_lambda(teams_df: pd.DataFrame) -> Tuple[List[Dict[str, Any]], Optional[float]]:
    """
    Returns slim.teams as list of dicts and league_avg_lambda.

    Attempts to compute per-game xGF/xGA when possible, otherwise falls back to goals for per game.
    """
    df = norm_cols(teams_df)

    team_col = pick_col(df, ["team", "Team", "teamName", "teamname", "name"])
    gp_col = pick_col(df, ["gamesPlayed", "GP", "gp", "games"])
    xgf_col = pick_col(df, ["xGoalsFor", "xGF", "xGoalsForAll", "xGoalsFor5v5", "xGoalsForTotal"])
    xga_col = pick_col(df, ["xGoalsAgainst", "xGA", "xGoalsAgainstAll", "xGoalsAgainst5v5", "xGoalsAgainstTotal"])
    gf_col = pick_col(df, ["goalsFor", "GF", "goals_for"])
    ga_col = pick_col(df, ["goalsAgainst", "GA", "goals_against"])

    slim_teams: List[Dict[str, Any]] = []

    # compute per-game rates where possible
    league_lambdas: List[float] = []

    for _, r in df.iterrows():
        team_val = r.get(team_col) if team_col else None
        if team_val is None:
            continue

        gp = float(r.get(gp_col)) if gp_col and pd.notna(r.get(gp_col)) else None

        xgf_pg = None
        xga_pg = None
        gf_pg = None
        ga_pg = None

        if gp and gp > 0:
            if xgf_col and pd.notna(r.get(xgf_col)):
                xgf_pg = float(r.get(xgf_col)) / gp
            if xga_col and pd.notna(r.get(xga_col)):
                xga_pg = float(r.get(xga_col)) / gp
            if gf_col and pd.notna(r.get(gf_col)):
                gf_pg = float(r.get(gf_col)) / gp
            if ga_col and pd.notna(r.get(ga_col)):
                ga_pg = float(r.get(ga_col)) / gp

        team_str = str(team_val).strip()
        team_abbrev = TEAM_NAME_TO_ABBREV.get(team_str.upper(), None)

        out = {
            "team": team_str,
            "abbrev": team_abbrev,
            "gp": gp,
            "xGF_pg": xgf_pg,
            "xGA_pg": xga_pg,
            "GF_pg": gf_pg,
            "GA_pg": ga_pg,
        }
        slim_teams.append(out)

        # league lambda: prefer xGF_pg if available, else GF_pg
        if xgf_pg is not None:
            league_lambdas.append(xgf_pg)
        elif gf_pg is not None:
            league_lambdas.append(gf_pg)

    league_avg_lambda = float(sum(league_lambdas) / len(league_lambdas)) if league_lambdas else None
    return slim_teams, league_avg_lambda


def build_slim_goalies(goalies_df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Returns slim.goalies as list of dicts with minimal fields.
    Column names vary, so we store what we can reliably map.
    """
    df = norm_cols(goalies_df)

    name_col = pick_col(df, ["goalie", "Goalie", "playerName", "name"])
    team_col = pick_col(df, ["team", "Team", "teamName", "teamAbbrev", "abbrev"])
    gp_col = pick_col(df, ["gamesPlayed", "GP", "gp", "games"])

    # common adjustment-like columns seen in various exports
    gsae_col = pick_col(df, ["goalsSavedAboveExpected", "GSAx", "gsae", "gsax"])
    gsae60_col = pick_col(df, ["goalsSavedAboveExpectedPer60", "GSAx/60", "gsaxPer60", "gsax_per60"])

    slim_goalies: List[Dict[str, Any]] = []

    for _, r in df.iterrows():
        name = r.get(name_col) if name_col else None
        if name is None:
            continue
        team = r.get(team_col) if team_col else None
        gp = float(r.get(gp_col)) if gp_col and pd.notna(r.get(gp_col)) else None

        gsae = float(r.get(gsae_col)) if gsae_col and pd.notna(r.get(gsae_col)) else None
        gsae60 = float(r.get(gsae60_col)) if gsae60_col and pd.notna(r.get(gsae60_col)) else None

        slim_goalies.append(
            {
                "goalie": str(name).strip(),
                "team": (str(team).strip() if team is not None else None),
                "gp": gp,
                "GSAx": gsae,
                "GSAx_per60": gsae60,
            }
        )

    return slim_goalies


def fetch_moneypuck_teams() -> Tuple[Optional[pd.DataFrame], Dict[str, Any], Optional[str]]:
    try:
        df, sha = read_csv_url(MP_TEAMS_URL)
        return df, {"ok": True, "url": MP_TEAMS_URL}, sha
    except Exception as e:
        return None, {"ok": False, "url": MP_TEAMS_URL, "error": str(e)}, None


def fetch_moneypuck_goalies() -> Tuple[Optional[pd.DataFrame], Dict[str, Any], Optional[str]]:
    try:
        df, sha = read_csv_url(MP_GOALIES_URL)
        return df, {"ok": True, "url": MP_GOALIES_URL}, sha
    except Exception as e:
        return None, {"ok": False, "url": MP_GOALIES_URL, "error": str(e)}, None


# --------------------------- starters (DailyFaceoff) --------------------------

def fetch_starters() -> Tuple[Optional[List[Dict[str, Any]]], Dict[str, Any]]:
    """
    Uses your existing fetch_dailyfaceoff_starters implementation.
    Must return list[dict] or None.
    """
    try:
        starters = fetch_dailyfaceoff_starters()
        return starters, {"ok": True, "source": "dailyfaceoff"}
    except Exception as e:
        return None, {"ok": False, "source": "dailyfaceoff", "error": str(e)}


# --------------------------- game_rest join -----------------------------------

def build_game_rest(slimmed_odds: List[Dict[str, Any]], rest_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Joins rest metrics onto the game slate by game_key (away_abbrev vs home_abbrev and date).
    """
    by_key: Dict[str, Dict[str, Any]] = {}
    for r in rest_rows or []:
        k = r.get("game_key")
        if k:
            by_key[str(k)] = r

    out: List[Dict[str, Any]] = []
    for g in slimmed_odds:
        away_name = (g.get("away_team") or "").upper()
        home_name = (g.get("home_team") or "").upper()
        commence = g.get("commence_time")
        away = TEAM_NAME_TO_ABBREV.get(away_name)
        home = TEAM_NAME_TO_ABBREV.get(home_name)
        if not away or not home or not commence:
            continue
        game_key = f"{away}_vs_{home}_{commence[:10]}"
        rest = by_key.get(game_key)
        if not rest:
            continue

        # Keep it minimal but useful for modeling
        out.append(
            {
                "game_key": game_key,
                "away_team": away,
                "home_team": home,
                "commence_time_utc": commence,
                "away_rest_days": rest.get("away_rest_days"),
                "home_rest_days": rest.get("home_rest_days"),
                "rest_adv_home": rest.get("rest_adv_home"),
            }
        )

    return out


# --------------------------- main --------------------------------------------

def main() -> int:
    generated_at = utc_now_iso()
    data_date = et_today_date_str()

    source_status: Dict[str, Any] = {}
    validations: Dict[str, Any] = {}
    inputs_hash: Dict[str, Any] = {}
    slim: Dict[str, Any] = {}

    store_full_odds = os.environ.get("STORE_FULL_ODDS", "0").strip() == "1"

    # ---------------- Odds ----------------
    odds_payload, odds_status, odds_sha = fetch_odds_current()
    source_status["odds_current"] = odds_status
    if odds_sha:
        inputs_hash["odds_current_sha256"] = odds_sha

    odds_payload = odds_payload or []
    validations["odds_games_raw_count"] = len(odds_payload)

    slimmed_odds, odds_meta = slim_odds_current(odds_payload)
    slim["odds_current"] = slimmed_odds
    validations["odds_games_slim_count"] = len(slimmed_odds)
    validations["odds_games_count"] = len(slimmed_odds)  # backward-compatible key
    validations.update({f"odds_{k}": v for k, v in odds_meta.items()})

    if store_full_odds:
        slim["odds_current_full"] = odds_payload

    # ---------------- Teams ----------------
    teams_df, teams_status, teams_sha = fetch_moneypuck_teams()
    source_status["teams"] = teams_status
    if teams_sha:
        inputs_hash["teams_sha256"] = teams_sha

    if teams_df is not None and not teams_df.empty:
        slim_teams, league_avg_lambda = build_slim_teams_and_lambda(teams_df)
        slim["teams"] = slim_teams
        slim["league_avg_lambda"] = league_avg_lambda
        validations["teams_count"] = len(slim_teams)
    else:
        validations["teams_count"] = 0

    # ---------------- Goalies ----------------
    goalies_df, goalies_status, goalies_sha = fetch_moneypuck_goalies()
    source_status["goalies"] = goalies_status
    if goalies_sha:
        inputs_hash["goalies_sha256"] = goalies_sha

    if goalies_df is not None and not goalies_df.empty:
        slim_goalies = build_slim_goalies(goalies_df)
        slim["goalies"] = slim_goalies
        validations["goalies_count"] = len(slim_goalies)
    else:
        validations["goalies_count"] = 0

    # ---------------- Starters ----------------
    starters_rows, starters_status = fetch_starters()
    source_status["starters"] = starters_status
    if starters_rows:
        slim["starters"] = starters_rows
        validations["starters_count"] = len(starters_rows)
    else:
        validations["starters_count"] = 0

    # ---------------- REST ----------------
    slate_for_rest: List[Dict[str, Any]] = []
    for g in slimmed_odds:
        away_name = (g.get("away_team") or "").upper()
        home_name = (g.get("home_team") or "").upper()
        commence = g.get("commence_time")

        away = TEAM_NAME_TO_ABBREV.get(away_name)
        home = TEAM_NAME_TO_ABBREV.get(home_name)

        if not away or not home or not commence:
            continue

        game_key = f"{away}_vs_{home}_{commence[:10]}"

        slate_for_rest.append(
            {
                "game_key": game_key,
                "away_team": away,
                "home_team": home,
                "commence_time_utc": commence,
            }
        )

    rest_rows = build_slim_rest(slate_for_rest)
    slim["rest"] = rest_rows
    validations["rest_games_count"] = len(rest_rows)

    # ---------------- GAME_REST (joined) ----------------
    game_rest_rows = build_game_rest(slimmed_odds, rest_rows)
    slim["game_rest"] = game_rest_rows
    validations["game_rest_count"] = len(game_rest_rows)

    # ---------------- OUTPUT ----------------
    out_obj = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": generated_at,
        "data_date_et": data_date,
        "source_status": source_status,
        "validations": validations,
        "inputs_hash": inputs_hash,
        "slim": slim,
    }

    out_dir = Path("data")
    out_dir.mkdir(parents=True, exist_ok=True)

    # Pretty multi-line JSON (reliable to parse)
    out_path_pretty = out_dir / "nhl_daily_slim.json"
    with out_path_pretty.open("w", encoding="utf-8", newline="\n") as f:
        json.dump(out_obj, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")

    # Consumer file for automations (small and stable)
    out_path_min = out_dir / "nhl_daily_min.json"
    with out_path_min.open("w", encoding="utf-8", newline="\n") as f:
        json.dump(out_obj, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
