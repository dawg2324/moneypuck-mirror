#!/usr/bin/env python3
"""
build_nhl_daily.py

Creates a daily slim artifact combining:
- Current odds from The Odds API (h2h + totals)
- Team season summary from MoneyPuck (teams.csv)
- Goalie season summary from MoneyPuck (goalies.csv) with robust parsing
- Starting goalies (DailyFaceoff scrape)

Outputs:
- data/nhl_daily_slim.json
- data/nhl_daily_slim.yml   (separate file; will not overwrite json)

Required env:
- ODDS_API_KEY (for odds_current)

Optional env:
- ODDS_API_REGIONS (default: us)
- ODDS_API_MARKETS (default: h2h,totals)
- MP_SEASON (default: 2025)
- OUT_JSON (default: data/nhl_daily_slim.json)
- OUT_YML  (default: data/nhl_daily_slim.yml)
- GOALIES_MIN_ICETIME_MIN (default: 200)

Behavior toggles:
- FAIL_IF_STARTERS_EMPTY_ON_SLATE (default: 0)
  If set to 1, exits non-zero when odds_games_count>0 and starters_count==0.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import sys
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd

from scripts.fetch_starters_dailyfaceoff import fetch_dailyfaceoff_starters


SCHEMA_VERSION = "1.0.4"
SPORT_KEY = "icehockey_nhl"


# --------------------------- time helpers ------------------------------------

def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def et_today_date_str() -> str:
    try:
        from zoneinfo import ZoneInfo  # py3.9+
        et = dt.datetime.now(ZoneInfo("America/New_York"))
        return et.date().isoformat()
    except Exception:
        return dt.datetime.now(dt.timezone.utc).date().isoformat()


# --------------------------- network helpers ---------------------------------

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def http_get_json(url: str, headers: Optional[Dict[str, str]] = None, timeout: int = 30) -> Tuple[Any, bytes]:
    req = Request(url, headers=headers or {})
    with urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
    return json.loads(raw.decode("utf-8")), raw


def http_get_bytes(url: str, headers: Optional[Dict[str, str]] = None, timeout: int = 30) -> bytes:
    req = Request(url, headers=headers or {})
    with urlopen(req, timeout=timeout) as resp:
        return resp.read()


def fetch_csv(url: str) -> Tuple[pd.DataFrame, str]:
    raw = http_get_bytes(url)
    df = pd.read_csv(pd.io.common.BytesIO(raw))
    return df, sha256_bytes(raw)


# --------------------------- parsing: teams ----------------------------------

def parse_moneypuck_teams_csv(teams_df: pd.DataFrame) -> List[Dict[str, Any]]:
    required = ["team", "situation", "position", "games_played", "xGoalsFor", "xGoalsAgainst"]
    missing = [c for c in required if c not in teams_df.columns]
    if missing:
        raise ValueError(f"teams.csv missing required columns: {missing}")

    df = teams_df.copy()
    df["situation"] = df["situation"].astype(str)
    df["position"] = df["position"].astype(str)

    df = df[(df["position"] == "Team Level") & (df["situation"] == "all")].copy()

    for col in ["games_played", "xGoalsFor", "xGoalsAgainst"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["team", "games_played", "xGoalsFor", "xGoalsAgainst"]).copy()
    df = df[df["games_played"] > 0].copy()

    df["xGF_pg"] = df["xGoalsFor"] / df["games_played"]
    df["xGA_pg"] = df["xGoalsAgainst"] / df["games_played"]

    out: List[Dict[str, Any]] = []
    for r in df[["team", "games_played", "xGF_pg", "xGA_pg"]].to_dict(orient="records"):
        out.append(
            {
                "team": str(r["team"]),
                "games_played": int(r["games_played"]),
                "xGF_pg": float(r["xGF_pg"]),
                "xGA_pg": float(r["xGA_pg"]),
            }
        )

    out.sort(key=lambda x: x["team"])
    return out


def league_avg_lambda_from_teams(teams_slim: List[Dict[str, Any]]) -> float:
    if not teams_slim:
        return 0.0
    vals = [t["xGF_pg"] for t in teams_slim if isinstance(t.get("xGF_pg"), (int, float))]
    return float(sum(vals) / len(vals)) if vals else 0.0


# --------------------------- parsing: goalies --------------------------------

def parse_moneypuck_goalies_csv(
    goalies_df: pd.DataFrame,
    *,
    situation: str = "all",
    min_icetime_minutes: float = 200.0,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    required = ["playerId", "name", "team", "situation", "icetime", "xGoals", "goals"]
    missing = [c for c in required if c not in goalies_df.columns]
    if missing:
        raise ValueError(f"goalies.csv missing required columns: {missing}")

    df = goalies_df.copy()
    df["situation"] = df["situation"].astype(str)

    df["playerId"] = pd.to_numeric(df["playerId"], errors="coerce").astype("Int64")
    df["icetime"] = pd.to_numeric(df["icetime"], errors="coerce")
    df["xGoals"] = pd.to_numeric(df["xGoals"], errors="coerce")
    df["goals"] = pd.to_numeric(df["goals"], errors="coerce")

    df = df[df["situation"] == situation].copy()
    df = df.dropna(subset=["playerId", "team", "icetime", "xGoals", "goals"]).copy()
    df = df[df["icetime"] > 0].copy()

    # MoneyPuck seasonSummary icetime is seconds
    df["icetime_sec"] = df["icetime"]
    df["icetime_min"] = df["icetime_sec"] / 60.0

    if min_icetime_minutes and min_icetime_minutes > 0:
        df = df[df["icetime_min"] >= float(min_icetime_minutes)].copy()

    df["xGA_per60"] = (df["xGoals"] / df["icetime_sec"]) * 3600.0
    df["GA_per60"] = (df["goals"] / df["icetime_sec"]) * 3600.0
    df["GSAx_per60"] = ((df["xGoals"] - df["goals"]) / df["icetime_sec"]) * 3600.0

    df = df.sort_values(["playerId", "icetime_sec"], ascending=[True, False]).drop_duplicates("playerId", keep="first")

    records: List[Dict[str, Any]] = []
    for r in df[
        ["playerId", "name", "team", "icetime_min", "xGoals", "goals", "xGA_per60", "GA_per60", "GSAx_per60"]
    ].to_dict(orient="records"):
        records.append(
            {
                "playerId": int(r["playerId"]),
                "name": str(r["name"]),
                "team": str(r["team"]),
                "icetime_min": float(r["icetime_min"]),
                "xGoals": float(r["xGoals"]),
                "goals": float(r["goals"]),
                "xGA_per60": float(r["xGA_per60"]),
                "GA_per60": float(r["GA_per60"]),
                "GSAx_per60": float(r["GSAx_per60"]),
            }
        )

    meta = {
        "situation_used": situation,
        "min_icetime_minutes": float(min_icetime_minutes),
        "goalies_count": len(records),
    }
    return records, meta


# --------------------------- odds: current -----------------------------------

def fetch_odds_current() -> Tuple[Optional[List[Dict[str, Any]]], Dict[str, Any], Optional[str]]:
    api_key = os.environ.get("ODDS_API_KEY", "").strip()
    if not api_key:
        return None, {"ok": False, "reason": "Missing ODDS_API_KEY env var"}, None

    regions = os.environ.get("ODDS_API_REGIONS", "us").strip()
    markets = os.environ.get("ODDS_API_MARKETS", "h2h,totals").strip()
    markets_list = [m.strip() for m in markets.split(",") if m.strip()]

    base_url = "https://api.the-odds-api.com/v4/sports/icehockey_nhl/odds"
    qs = urlencode(
        {
            "apiKey": api_key,
            "regions": regions,
            "markets": ",".join(markets_list),
            "oddsFormat": "american",
            "dateFormat": "iso",
        }
    )
    url = f"{base_url}?{qs}"

    try:
        payload, raw = http_get_json(url)
        return (
            payload,
            {
                "ok": True,
                "meta": {
                    "endpoint": "odds_current",
                    "url": base_url,
                    "regions": regions,
                    "markets": markets_list,
                    "oddsFormat": "american",
                    "dateFormat": "iso",
                },
            },
            sha256_bytes(raw),
        )
    except Exception as e:
        return None, {"ok": False, "error": str(e)}, None


# --------------------------- starters: validations ----------------------------

def validate_starters_schema(starters: Any) -> Tuple[bool, Optional[str]]:
    if starters is None:
        return True, None
    if not isinstance(starters, list):
        return False, "slim.starters must be a list"

    req_top = {"game_key", "date_et", "away", "home", "source"}
    req_side = {"team", "goalie", "status"}
    req_src = {"site", "url", "last_updated_utc"}

    for idx, g in enumerate(starters):
        if not isinstance(g, dict):
            return False, f"slim.starters[{idx}] must be an object"
        missing_top = req_top - set(g.keys())
        if missing_top:
            return False, f"slim.starters[{idx}] missing keys: {sorted(missing_top)}"

        for side_key in ("away", "home"):
            side = g.get(side_key)
            if not isinstance(side, dict):
                return False, f"slim.starters[{idx}].{side_key} must be an object"
            missing_side = req_side - set(side.keys())
            if missing_side:
                return False, f"slim.starters[{idx}].{side_key} missing keys: {sorted(missing_side)}"
            if side.get("status") not in ("confirmed", "projected"):
                return False, f"slim.starters[{idx}].{side_key}.status must be confirmed|projected"

        src = g.get("source")
        if not isinstance(src, dict):
            return False, f"slim.starters[{idx}].source must be an object"
        missing_src = req_src - set(src.keys())
        if missing_src:
            return False, f"slim.starters[{idx}].source missing keys: {sorted(missing_src)}"
        if src.get("site") != "dailyfaceoff":
            return False, f"slim.starters[{idx}].source.site must be dailyfaceoff"

    return True, None


# --------------------------- output writers ----------------------------------

def ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(path)
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)


def write_json(path: str, obj: Any) -> None:
    ensure_parent_dir(path)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",", ":"), sort_keys=False)


def write_yaml(path: str, obj: Any) -> Tuple[bool, Optional[str]]:
    try:
        import yaml  # type: ignore
    except Exception:
        return False, "PyYAML not installed"

    ensure_parent_dir(path)
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(obj, f, sort_keys=False, allow_unicode=True)
    return True, None


# --------------------------- main --------------------------------------------

def main() -> int:
    season = os.environ.get("MP_SEASON", "2025").strip()

    teams_url = f"https://moneypuck.com/moneypuck/playerData/seasonSummary/{season}/regular/teams.csv"
    goalies_url = f"https://moneypuck.com/moneypuck/playerData/seasonSummary/{season}/regular/goalies.csv"

    out_json = os.environ.get("OUT_JSON", "data/nhl_daily_slim.json").strip()
    out_yml = os.environ.get("OUT_YML", "data/nhl_daily_slim.yml").strip()

    fail_if_starters_empty = os.environ.get("FAIL_IF_STARTERS_EMPTY_ON_SLATE", "0").strip() == "1"

    generated_at = utc_now_iso()
    data_date = et_today_date_str()

    source_status: Dict[str, Any] = {}
    validations: Dict[str, Any] = {}
    inputs_hash: Dict[str, Any] = {}
    slim: Dict[str, Any] = {}

    # Odds current
    odds_payload, odds_status, odds_sha = fetch_odds_current()
    source_status["odds_current"] = odds_status
    if odds_sha:
        inputs_hash["odds_current_sha256"] = odds_sha

    if odds_payload is not None:
        slim["odds_current"] = odds_payload
        validations["odds_games_count"] = len(odds_payload)
    else:
        slim["odds_current"] = []
        validations["odds_games_count"] = 0

    odds_games_count = int(validations.get("odds_games_count", 0) or 0)

    # Odds open placeholder
    source_status["odds_open"] = {"ok": False, "reason": "Historical odds not available on current Odds API plan"}

    # Teams
    try:
        teams_df, teams_sha = fetch_csv(teams_url)
        inputs_hash["teams_sha256"] = teams_sha

        teams_slim = parse_moneypuck_teams_csv(teams_df)
        slim["teams"] = teams_slim

        validations["teams_count"] = len(teams_slim)
        source_status["teams"] = {"ok": True, "url": teams_url}
    except Exception as e:
        slim["teams"] = []
        validations["teams_count"] = 0
        source_status["teams"] = {"ok": False, "url": teams_url, "error": str(e)}

    # Goalies
    try:
        goalies_df, goalies_sha = fetch_csv(goalies_url)
        inputs_hash["goalies_sha256"] = goalies_sha

        min_icetime_minutes = float(os.environ.get("GOALIES_MIN_ICETIME_MIN", "200").strip())
        goalies_slim, meta = parse_moneypuck_goalies_csv(
            goalies_df,
            situation="all",
            min_icetime_minutes=min_icetime_minutes,
        )
        slim["goalies"] = goalies_slim
        validations["goalies_count"] = len(goalies_slim)
        source_status["goalies"] = {"ok": True, "url": goalies_url, "meta": meta}
    except Exception as e:
        slim["goalies"] = []
        validations["goalies_count"] = 0
        source_status["goalies"] = {"ok": False, "url": goalies_url, "error": str(e)}

    # Starters (DailyFaceoff)
    starters_list: List[Dict[str, Any]] = []
    dfo_status: Dict[str, Any] = {"ok": False, "url": f"https://www.dailyfaceoff.com/starting-goalies/{data_date}"}

    try:
        # fetch_dailyfaceoff_starters may return either:
        # - list (legacy)
        # - StartersFetchResult (new: has .starters and .status)
        starters_res = fetch_dailyfaceoff_starters(data_date)

        if isinstance(starters_res, list):
            starters_list = starters_res
            dfo_status = {
                "ok": True,
                "url": f"https://www.dailyfaceoff.com/starting-goalies/{data_date}",
                "count": len(starters_list),
            }
        else:
            # expected new shape
            starters_list = list(getattr(starters_res, "starters", []) or [])
            dfo_status = dict(getattr(starters_res, "status", {}) or {})
            dfo_status.setdefault("url", f"https://www.dailyfaceoff.com/starting-goalies/{data_date}")
            dfo_status.setdefault("count", len(starters_list))

    except Exception as e:
        starters_list = []
        dfo_status = {
            "ok": False,
            "url": f"https://www.dailyfaceoff.com/starting-goalies/{data_date}",
            "error": str(e),
        }

    slim["starters"] = starters_list
    validations["starters_count"] = len(starters_list)
    source_status["starters_dailyfaceoff"] = dfo_status

    # If there is a slate but starters are empty, treat that as a failure signal
    if odds_games_count > 0 and len(starters_list) == 0:
        validations["starters_expected_nonempty_on_slate"] = True
        validations["starters_empty_on_slate"] = True

        # Mark DFO as not ok even if legacy code set it ok
        source_status["starters_dailyfaceoff"]["ok"] = False
        source_status["starters_dailyfaceoff"].setdefault(
            "reason",
            "Parsed 0 starters while odds slate has games (likely blocked/JS-rendered/selector drift).",
        )

    ok_schema, schema_err = validate_starters_schema(slim.get("starters"))
    validations["starters_schema_ok"] = bool(ok_schema)
    if not ok_schema:
        validations["starters_schema_error"] = schema_err

    # Optionally fail the run so Actions goes red and you notice immediately
    if fail_if_starters_empty and odds_games_count > 0 and len(starters_list) == 0:
        sys.stderr.write(
            f"[error] starters empty on slate: odds_games_count={odds_games_count}, date_et={data_date}\n"
        )
        return 2

    # League avg lambda proxy (based on teams xGF_pg)
    try:
        slim["league_avg_lambda"] = league_avg_lambda_from_teams(slim.get("teams", []))
    except Exception:
        slim["league_avg_lambda"] = 0.0

    out_obj: Dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": generated_at,
        "data_date_et": data_date,
        "source_status": source_status,
        "validations": validations,
        "inputs_hash": inputs_hash,
        "slim": slim,
    }

    write_json(out_json, out_obj)

    ok_yml, yml_reason = write_yaml(out_yml, out_obj)
    if not ok_yml:
        sys.stderr.write(f"[warn] YAML not written: {yml_reason}\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
