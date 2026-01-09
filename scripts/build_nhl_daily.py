#!/usr/bin/env python3
from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd

from scripts.fetch_starters_dailyfaceoff import fetch_dailyfaceoff_starters
from scripts.compute_rest import build_slim_rest

SCHEMA_VERSION = "1.0.9"
SPORT_KEY = "icehockey_nhl"
DAILYFACEOFF_BASE = "https://www.dailyfaceoff.com"

# MoneyPuck endpoints
MP_TEAMS_URL = "https://moneypuck.com/moneypuck/playerData/seasonSummary/2025/regular/teams.csv"
MP_GOALIES_URL = "https://moneypuck.com/moneypuck/playerData/seasonSummary/2025/regular/goalies.csv"


# --------------------------- TEAM NAME → ABBREV -------------------------------

TEAM_NAME_TO_ABBREV: Dict[str, str] = {
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

ABBREV_SET = set(TEAM_NAME_TO_ABBREV.values())


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

def http_get_bytes(url: str, headers: Optional[Dict[str, str]] = None, timeout: int = 30) -> bytes:
    req = Request(url, headers=headers or {})
    with urlopen(req, timeout=timeout) as resp:
        return resp.read()


def http_get_json(url: str, headers: Optional[Dict[str, str]] = None, timeout: int = 30) -> Tuple[Any, bytes]:
    raw = http_get_bytes(url, headers=headers, timeout=timeout)
    return json.loads(raw.decode("utf-8")), raw


def read_csv_url(url: str, timeout: int = 30) -> pd.DataFrame:
    raw = http_get_bytes(url, headers={"User-Agent": "nhl-daily-slim"}, timeout=timeout)
    return pd.read_csv(pd.io.common.BytesIO(raw))


def norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    return out


def pick_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = set(df.columns)
    for c in candidates:
        if c in cols:
            return c
    lower_map = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    return None


# --------------------------- small math helpers ------------------------------

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


# --------------------------- id helpers --------------------------------------

def game_id_from_names(commence_time_utc: str, away_team_name: str, home_team_name: str) -> Optional[str]:
    """
    Stable id: "{AWAYABBREV}_vs_{HOMEABBREV}_{YYYY-MM-DD}" based on commence_time date.
    """
    away_abbrev = TEAM_NAME_TO_ABBREV.get((away_team_name or "").upper())
    home_abbrev = TEAM_NAME_TO_ABBREV.get((home_team_name or "").upper())
    if not away_abbrev or not home_abbrev or not commence_time_utc:
        return None
    date_str = str(commence_time_utc)[:10]
    return f"{away_abbrev}_vs_{home_abbrev}_{date_str}"


def goalie_id_from_name(name: str) -> str:
    """
    Deterministic goalie id from goalie name. Stable across runs.
    """
    base = re.sub(r"\s+", " ", (name or "").strip().lower())
    h = hashlib.sha1(base.encode("utf-8")).hexdigest()[:8]
    return f"g_{h}"


# --------------------------- odds helpers ------------------------------------

def extract_market(bookmaker: Dict[str, Any], market_key: str) -> Optional[Dict[str, Any]]:
    markets = bookmaker.get("markets") or []
    for m in markets:
        if m.get("key") == market_key:
            return m
    return None


def slim_odds_current(odds_payload: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Returns slimmed odds where "id" is our stable game id.
    """
    slimmed: List[Dict[str, Any]] = []
    meta: Dict[str, Any] = {"games_with_h2h": 0, "games_with_totals": 0}

    for g in odds_payload or []:
        commence = g.get("commence_time")
        home_team = g.get("home_team")
        away_team = g.get("away_team")
        if not (commence and home_team and away_team):
            continue

        gid = game_id_from_names(commence, away_team, home_team)
        if not gid:
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
            "id": gid,
            "commence_time": commence,
            "home_team": home_team,
            "away_team": away_team,
        }

        if has_h2h:
            out_game["h2h"] = {
                "best": {
                    "home_price": best_home[0],
                    "away_price": best_away[0],
                    "home_book": best_home[1],
                    "away_book": best_away[1],
                },
                "consensus_median": {
                    "home_price": consensus_home,
                    "away_price": consensus_away,
                },
            }

        if has_totals:
            best_over = best_over_by_point.get(consensus_total_line)
            best_under = best_under_by_point.get(consensus_total_line)
            out_game["totals"] = {
                "line": consensus_total_line,
                "best": {
                    "over_price": best_over[0] if best_over else None,
                    "under_price": best_under[0] if best_under else None,
                    "over_book": best_over[1] if best_over else None,
                    "under_book": best_under[1] if best_under else None,
                },
            }

        slimmed.append(out_game)

    return slimmed, meta


# --------------------------- odds: current -----------------------------------

def fetch_odds_current() -> Tuple[Optional[List[Dict[str, Any]]], Dict[str, Any]]:
    api_key = os.environ.get("ODDS_API_KEY", "").strip()
    if not api_key:
        return None, {"ok": False, "reason": "Missing ODDS_API_KEY env var"}

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
        payload, _raw = http_get_json(url)
        return payload, {"ok": True}
    except Exception as e:
        return None, {"ok": False, "error": str(e)}


# --------------------------- teams / goalies (MoneyPuck) ----------------------

def build_slim_teams_and_lambda(teams_df: pd.DataFrame) -> Tuple[List[Dict[str, Any]], Optional[float]]:
    """
    Output teams:
      { "team_abbrev": "CHI", "xGF_pg": 2.75, "xGA_pg": 3.25 }
    Also returns league_avg_lambda, computed from xGF_pg when available else GF_pg.
    """
    df = norm_cols(teams_df)

    team_col = pick_col(df, ["team", "Team", "teamName", "teamname", "name"])
    gp_col = pick_col(df, ["gamesPlayed", "GP", "gp", "games"])
    xgf_col = pick_col(df, ["xGoalsFor", "xGF", "xGoalsForAll", "xGoalsFor5v5", "xGoalsForTotal"])
    xga_col = pick_col(df, ["xGoalsAgainst", "xGA", "xGoalsAgainstAll", "xGoalsAgainst5v5", "xGoalsAgainstTotal"])
    gf_col = pick_col(df, ["goalsFor", "GF", "goals_for"])
    ga_col = pick_col(df, ["goalsAgainst", "GA", "goals_against"])

    slim_teams: List[Dict[str, Any]] = []
    league_lambdas: List[float] = []

    for _, r in df.iterrows():
        team_val = r.get(team_col) if team_col else None
        if team_val is None:
            continue

        team_name = str(team_val).strip()
        team_abbrev = TEAM_NAME_TO_ABBREV.get(team_name.upper())
        if not team_abbrev:
            continue

        gp = float(r.get(gp_col)) if gp_col and pd.notna(r.get(gp_col)) else None
        if not gp or gp <= 0:
            continue

        xgf_pg = float(r.get(xgf_col)) / gp if xgf_col and pd.notna(r.get(xgf_col)) else None
        xga_pg = float(r.get(xga_col)) / gp if xga_col and pd.notna(r.get(xga_col)) else None
        gf_pg = float(r.get(gf_col)) / gp if gf_col and pd.notna(r.get(gf_col)) else None
        _ga_pg = float(r.get(ga_col)) / gp if ga_col and pd.notna(r.get(ga_col)) else None

        slim_teams.append(
            {
                "team_abbrev": team_abbrev,
                "xGF_pg": xgf_pg,
                "xGA_pg": xga_pg,
            }
        )

        if xgf_pg is not None:
            league_lambdas.append(xgf_pg)
        elif gf_pg is not None:
            league_lambdas.append(gf_pg)

    league_avg_lambda = float(sum(league_lambdas) / len(league_lambdas)) if league_lambdas else None
    return slim_teams, league_avg_lambda


def build_slim_goalies(goalies_df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Output goalies:
      { "goalie_id": "g_001", "name": "Goalie A", "GSAx_per60": 0.25 }
    goalie_id is deterministic from name, not row order.
    """
    df = norm_cols(goalies_df)

    name_col = pick_col(df, ["goalie", "Goalie", "playerName", "name"])
    gsae60_col = pick_col(df, ["goalsSavedAboveExpectedPer60", "GSAx/60", "gsaxPer60", "gsax_per60", "GSAx_per60"])

    slim_goalies: List[Dict[str, Any]] = []
    for _, r in df.iterrows():
        name_val = r.get(name_col) if name_col else None
        if name_val is None:
            continue
        name = str(name_val).strip()
        if not name:
            continue

        gsae60 = float(r.get(gsae60_col)) if gsae60_col and pd.notna(r.get(gsae60_col)) else None

        slim_goalies.append(
            {
                "goalie_id": goalie_id_from_name(name),
                "name": name,
                "GSAx_per60": gsae60,
            }
        )

    return slim_goalies


def fetch_moneypuck_teams() -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
    try:
        df = read_csv_url(MP_TEAMS_URL)
        return df, {"ok": True}
    except Exception as e:
        return None, {"ok": False, "error": str(e)}


def fetch_moneypuck_goalies() -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
    try:
        df = read_csv_url(MP_GOALIES_URL)
        return df, {"ok": True}
    except Exception as e:
        return None, {"ok": False, "error": str(e)}


# --------------------------- starters (DailyFaceoff) --------------------------

def normalize_status(v: Any) -> str:
    s = str(v or "").strip().lower()
    if "confirm" in s or s == "confirmed":
        return "confirmed"
    if "expect" in s or s == "expected" or "probable" in s:
        return "expected"
    return "unknown"


def normalize_team_abbrev(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip().upper()
    if s in ABBREV_SET:
        return s
    mapped = TEAM_NAME_TO_ABBREV.get(s)
    if mapped:
        return mapped
    mapped = TEAM_NAME_TO_ABBREV.get(s.replace(".", ""))
    if mapped:
        return mapped
    return None


def normalize_goalie_name(v: Any) -> Optional[str]:
    if v is None:
        return None
    name = str(v).strip()
    name = re.sub(r"\s+", " ", name)
    return name or None


def fetch_starters() -> Tuple[Optional[List[Dict[str, Any]]], Dict[str, Any]]:
    try:
        starters = fetch_dailyfaceoff_starters()
        if starters is None:
            return None, {"ok": False, "error": "fetch_dailyfaceoff_starters returned None"}
        if not isinstance(starters, list):
            return None, {"ok": False, "error": "fetch_dailyfaceoff_starters did not return a list"}
        return starters, {"ok": True}
    except Exception as e:
        return None, {"ok": False, "error": str(e)}


def build_starters_for_slate(
    slimmed_odds: List[Dict[str, Any]],
    starters_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Produces starters shaped like:
      { "id": GAME_ID, "team_abbrev": "CHI", "goalie_id": "g_xxx", "goalie_name": "...", "status": "confirmed|expected|unknown" }
    Only includes rows that can be matched to a team in today's odds slate.
    """
    teams_in_slate: set[str] = set()
    game_by_team: Dict[str, str] = {}
    for g in slimmed_odds:
        gid = g.get("id")
        away_name = g.get("away_team") or ""
        home_name = g.get("home_team") or ""
        commence = g.get("commence_time") or ""
        if not gid or not commence:
            continue
        away_abbrev = TEAM_NAME_TO_ABBREV.get(str(away_name).upper())
        home_abbrev = TEAM_NAME_TO_ABBREV.get(str(home_name).upper())
        if not away_abbrev or not home_abbrev:
            continue
        teams_in_slate.add(away_abbrev)
        teams_in_slate.add(home_abbrev)
        game_by_team[away_abbrev] = gid
        game_by_team[home_abbrev] = gid

    out: List[Dict[str, Any]] = []
    for r in starters_rows or []:
        if not isinstance(r, dict):
            continue

        # try common keys without assuming your exact starter schema
        team_abbrev = (
            normalize_team_abbrev(r.get("team_abbrev"))
            or normalize_team_abbrev(r.get("team"))
            or normalize_team_abbrev(r.get("abbrev"))
        )
        if not team_abbrev or team_abbrev not in teams_in_slate:
            continue

        goalie_name = (
            normalize_goalie_name(r.get("goalie_name"))
            or normalize_goalie_name(r.get("goalie"))
            or normalize_goalie_name(r.get("name"))
            or normalize_goalie_name(r.get("starter"))
        )
        if not goalie_name:
            continue

        status = normalize_status(r.get("status") or r.get("starter_status") or r.get("confirmed"))

        gid = game_by_team.get(team_abbrev)
        if not gid:
            continue

        out.append(
            {
                "id": gid,
                "team_abbrev": team_abbrev,
                "goalie_id": goalie_id_from_name(goalie_name),
                "goalie_name": goalie_name,
                "status": status,
            }
        )

    # de-dupe within (id, team_abbrev) preferring confirmed over expected over unknown
    rank = {"confirmed": 2, "expected": 1, "unknown": 0}
    best: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for row in out:
        k = (row["id"], row["team_abbrev"])
        cur = best.get(k)
        if cur is None or rank.get(row["status"], 0) > rank.get(cur["status"], 0):
            best[k] = row

    return list(best.values())


# --------------------------- rest join ---------------------------------------

def build_game_rest(
    slimmed_odds: List[Dict[str, Any]],
    rest_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Output game_rest shaped like:
      {
        "id": GAME_ID,
        "commence_time": "...Z",
        "home_team": "CHI",
        "away_team": "WSH",
        "home_rest_days": 1,
        "away_rest_days": 1,
        "rest_adv_home": 0
      }
    """
    by_key: Dict[str, Dict[str, Any]] = {}
    for r in rest_rows or []:
        k = r.get("game_key")
        if k:
            by_key[str(k)] = r

    out: List[Dict[str, Any]] = []
    for g in slimmed_odds:
        gid = g.get("id")
        commence = g.get("commence_time")
        away_name = g.get("away_team") or ""
        home_name = g.get("home_team") or ""
        away_abbrev = TEAM_NAME_TO_ABBREV.get(str(away_name).upper())
        home_abbrev = TEAM_NAME_TO_ABBREV.get(str(home_name).upper())
        if not gid or not commence or not away_abbrev or not home_abbrev:
            continue

        game_key = f"{away_abbrev}_vs_{home_abbrev}_{str(commence)[:10]}"
        rest = by_key.get(game_key)
        if not rest:
            continue

        out.append(
            {
                "id": gid,
                "commence_time": commence,
                "home_team": home_abbrev,
                "away_team": away_abbrev,
                "home_rest_days": rest.get("home_rest_days"),
                "away_rest_days": rest.get("away_rest_days"),
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
    slim: Dict[str, Any] = {}

    # ---------------- Odds ----------------
    odds_payload, odds_status = fetch_odds_current()
    source_status["odds_current"] = odds_status

    odds_payload = odds_payload or []
    slimmed_odds, _odds_meta = slim_odds_current(odds_payload)
    slim["odds_current"] = slimmed_odds
    validations["odds_games_slim_count"] = len(slimmed_odds)

    # ---------------- Teams ----------------
    teams_df, teams_status = fetch_moneypuck_teams()
    source_status["teams"] = teams_status

    if teams_df is not None and not teams_df.empty:
        slim_teams, league_avg_lambda = build_slim_teams_and_lambda(teams_df)
        slim["teams"] = slim_teams
        slim["league_avg_lambda"] = league_avg_lambda
        validations["teams_count"] = len(slim_teams)
    else:
        slim["teams"] = []
        slim["league_avg_lambda"] = None
        validations["teams_count"] = 0

    # ---------------- Goalies ----------------
    goalies_df, goalies_status = fetch_moneypuck_goalies()
    source_status["goalies"] = goalies_status

    if goalies_df is not None and not goalies_df.empty:
        slim_goalies = build_slim_goalies(goalies_df)
        slim["goalies"] = slim_goalies
        validations["goalies_count"] = len(slim_goalies)
    else:
        slim["goalies"] = []
        validations["goalies_count"] = 0

    # ---------------- Starters ----------------
    starters_rows_raw, starters_status = fetch_starters()
    source_status["starters"] = starters_status

    starters_rows = build_starters_for_slate(slimmed_odds, starters_rows_raw or [])
    slim["starters"] = starters_rows
    validations["starters_count"] = len(starters_rows)

    # ---------------- REST ----------------
    # build slate_for_rest from slimmed_odds so compute_rest stays decoupled
    slate_for_rest: List[Dict[str, Any]] = []
    for g in slimmed_odds:
        gid = g.get("id")
        commence = g.get("commence_time")
        away_name = g.get("away_team") or ""
        home_name = g.get("home_team") or ""

        away_abbrev = TEAM_NAME_TO_ABBREV.get(str(away_name).upper())
        home_abbrev = TEAM_NAME_TO_ABBREV.get(str(home_name).upper())

        if not gid or not commence or not away_abbrev or not home_abbrev:
            continue

        game_key = f"{away_abbrev}_vs_{home_abbrev}_{str(commence)[:10]}"
        slate_for_rest.append(
            {
                "game_key": game_key,
                "away_team": away_abbrev,
                "home_team": home_abbrev,
                "commence_time_utc": commence,
            }
        )

    try:
        rest_rows = build_slim_rest(slate_for_rest)
        source_status["rest"] = {"ok": True}
    except Exception as e:
        rest_rows = []
        source_status["rest"] = {"ok": False, "error": str(e)}

    game_rest_rows = build_game_rest(slimmed_odds, rest_rows)
    slim["game_rest"] = game_rest_rows
    validations["game_rest_count"] = len(game_rest_rows)

    # ---------------- OUTPUT (modeled like your example) ----------------
    out_obj = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": generated_at,
        "data_date_et": data_date,
        "source_status": source_status,
        "validations": validations,
        "slim": slim,
    }

    out_dir = Path("data")
    out_dir.mkdir(parents=True, exist_ok=True)

    # Minimal consumer file (recommended for the scheduled task)
    out_path_min = out_dir / "nhl_daily_min.json"
    with out_path_min.open("w", encoding="utf-8", newline="\n") as f:
        json.dump(out_obj, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")


    # Pretty file for humans
    out_path_pretty = out_dir / "nhl_daily_slim.json"
    with out_path_pretty.open("w", encoding="utf-8", newline="\n") as f:
        json.dump(out_obj, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
