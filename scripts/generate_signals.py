#!/usr/bin/env python3
from __future__ import annotations

import json
import math
import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # type: ignore


DATA_PATH = Path("data/nhl_daily_slim.json")
OUT_MD = Path("data/nhl_signals.md")

REST_GOALS_PER_DAY = 0.08
EDGE_MIN_PP = 2.0
ML_PRICE_MIN = -250
ML_PRICE_MAX = 250


def normal_cdf(x: float) -> float:
    # Standard normal CDF via erf
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def poisson_cdf(k: int, mu: float) -> float:
    # P(X <= k) for Poisson(mu) using stable recurrence
    if k < 0:
        return 0.0
    if mu <= 0:
        return 1.0
    term = math.exp(-mu)
    s = term
    for i in range(1, k + 1):
        term *= mu / i
        s += term
    return min(1.0, max(0.0, s))


def implied_prob_from_american(odds: int) -> Optional[float]:
    if odds == 0:
        return None
    if odds < 0:
        return (-odds) / ((-odds) + 100.0)
    return 100.0 / (odds + 100.0)


def fair_american_from_prob(p: float) -> Optional[int]:
    if p <= 0.0 or p >= 1.0:
        return None
    if p >= 0.5:
        return -round(100.0 * p / (1.0 - p))
    return round(100.0 * (1.0 - p) / p)


def parse_abbrevs_from_game_id(game_id: str) -> Tuple[Optional[str], Optional[str]]:
    # Format: "{AWAY}_vs_{HOME}_{YYYY-MM-DD}"
    if not isinstance(game_id, str) or "_vs_" not in game_id:
        return None, None
    away_part, rest = game_id.split("_vs_", 1)
    if "_" not in rest:
        return away_part.strip(), None
    home_part = rest.split("_", 1)[0]
    return away_part.strip(), home_part.strip()


def to_et_time_str(iso_utc: str) -> str:
    # Expect ISO like 2026-01-10T01:00:00Z
    try:
        s = iso_utc.replace("Z", "+00:00")
        t = dt.datetime.fromisoformat(s)
        if t.tzinfo is None:
            t = t.replace(tzinfo=dt.timezone.utc)
        if ZoneInfo is not None:
            et = t.astimezone(ZoneInfo("America/New_York"))
        else:
            et = t.astimezone(dt.timezone(dt.timedelta(hours=-5)))
        return et.strftime("%-I:%M %p ET")
    except Exception:
        return str(iso_utc)


@dataclass
class MLSig:
    edge_pp: float
    line: str


@dataclass
class TotSig:
    edge_pp: float
    line: str


def main() -> int:
    obj = json.loads(DATA_PATH.read_text(encoding="utf-8"))
    slim = obj["slim"]

    odds_games: List[Dict[str, Any]] = slim.get("odds_current") or []
    teams_list: List[Dict[str, Any]] = slim.get("teams") or []
    rest_list: List[Dict[str, Any]] = slim.get("game_rest") or []
    starters_list: List[Dict[str, Any]] = slim.get("starters") or []

    teams_map: Dict[str, Tuple[float, float]] = {}
    for r in teams_list:
        ab = r.get("team_abbrev")
        xgf = r.get("xGF_pg")
        xga = r.get("xGA_pg")
        if isinstance(ab, str) and isinstance(xgf, (int, float)) and isinstance(xga, (int, float)):
            teams_map[ab] = (float(xgf), float(xga))

    rest_map: Dict[str, int] = {}
    for r in rest_list:
        gid = r.get("id")
        adv = r.get("rest_adv_home")
        if isinstance(gid, str):
            try:
                rest_map[gid] = int(adv) if adv is not None else 0
            except Exception:
                rest_map[gid] = 0

    ml_signals: List[MLSig] = []
    tot_signals: List[TotSig] = []
    skipped: List[str] = []

    for g in odds_games:
        gid = g.get("id")
        if not isinstance(gid, str):
            continue

        commence = g.get("commence_time")
        away_name = g.get("away_team") or ""
        home_name = g.get("home_team") or ""

        away_ab, home_ab = parse_abbrevs_from_game_id(gid)
        if not away_ab or not home_ab:
            skipped.append(f"{gid}: cannot parse abbrevs from id")
            continue

        if away_ab not in teams_map or home_ab not in teams_map:
            skipped.append(f"{gid}: missing team stats for {away_ab} or {home_ab}")
            continue

        away_xgf, away_xga = teams_map[away_ab]
        home_xgf, home_xga = teams_map[home_ab]

        mu_home_base = (home_xgf + away_xga) / 2.0
        mu_away_base = (away_xgf + home_xga) / 2.0

        # Moneyline means: NO rest
        mu_home_ml = mu_home_base
        mu_away_ml = mu_away_base

        mean_diff = mu_home_ml - mu_away_ml
        var_diff = mu_home_ml + mu_away_ml
        sd = math.sqrt(var_diff) if var_diff > 0 else 0.0
        if sd <= 0:
            skipped.append(f"{gid}: sd<=0 for ML")
            continue

        p_home = 1.0 - normal_cdf((0.5 - mean_diff) / sd)
        p_home = min(1.0, max(0.0, p_home))
        p_away = 1.0 - p_home

        # Moneyline prices
        h2h = g.get("h2h") or {}
        best = (h2h.get("best") or {}) if isinstance(h2h, dict) else {}

        home_price = best.get("home_price")
        away_price = best.get("away_price")
        home_book = best.get("home_book")
        away_book = best.get("away_book")

        # Evaluate ML both sides if available
        def maybe_add_ml(side: str, price: Any, book: Any, model_p: float) -> None:
            if not isinstance(price, int) or not isinstance(book, str):
                return
            if price < ML_PRICE_MIN or price > ML_PRICE_MAX:
                return
            imp = implied_prob_from_american(price)
            if imp is None:
                return
            edge_pp = (model_p - imp) * 100.0
            if edge_pp < EDGE_MIN_PP:
                return
            fair = fair_american_from_prob(model_p)
            time_et = to_et_time_str(commence) if isinstance(commence, str) else str(commence)
            line = (
                f"{time_et} {away_ab} @ {home_ab} | {side} ML {price} ({book})"
                f" | model {model_p*100:.1f}% | implied {imp*100:.1f}% | edge +{edge_pp:.1f}pp"
                f" | fair {fair if fair is not None else 'n/a'} | rest_adv_home {rest_map.get(gid,0)} (rest not applied to ML)"
            )
            ml_signals.append(MLSig(edge_pp=edge_pp, line=line))

        maybe_add_ml(home_ab, home_price, home_book, p_home)
        maybe_add_ml(away_ab, away_price, away_book, p_away)

        # Totals (rest-adjusted)
        rest_adv = rest_map.get(gid, 0)
        rest_missing = "rest missing" if gid not in rest_map else "rest applied"

        mu_home_tot = max(0.1, mu_home_base + REST_GOALS_PER_DAY * rest_adv)
        mu_away_tot = max(0.1, mu_away_base - REST_GOALS_PER_DAY * rest_adv)
        mu_total = mu_home_tot + mu_away_tot

        totals = g.get("totals") or {}
        if not isinstance(totals, dict):
            continue

        L = totals.get("line")
        bestt = totals.get("best") or {}
        if not isinstance(bestt, dict) or not isinstance(L, (int, float)):
            skipped.append(f"{gid}: missing totals line/best")
            continue

        Lf = float(L)
        integer_line = abs(Lf - round(Lf)) < 1e-9
        if integer_line:
            k = int(round(Lf))
            model_over = 1.0 - poisson_cdf(k, mu_total)      # >= k+1
            model_under = poisson_cdf(k - 1, mu_total)       # <= k-1
            line_type = f"integer({k})"
        else:
            n = int(math.floor(Lf))
            model_over = 1.0 - poisson_cdf(n, mu_total)      # >= n+1
            model_under = poisson_cdf(n, mu_total)           # <= n
            line_type = f"half({Lf})"

        def maybe_add_total(pick: str, price: Any, book: Any, model_p: float) -> None:
            if not isinstance(price, int) or not isinstance(book, str):
                return
            imp = implied_prob_from_american(price)
            if imp is None:
                return
            edge_pp = (model_p - imp) * 100.0
            if edge_pp < EDGE_MIN_PP:
                return
            fair = fair_american_from_prob(model_p)
            time_et = to_et_time_str(commence) if isinstance(commence, str) else str(commence)
            line = (
                f"{time_et} {away_ab} @ {home_ab} | {pick} {Lf} {price} ({book})"
                f" | model {model_p*100:.1f}% | implied {imp*100:.1f}% | edge +{edge_pp:.1f}pp"
                f" | fair {fair if fair is not None else 'n/a'}"
                f" | mu_total {mu_total:.2f} | line {line_type} | rest_adv_home {rest_adv} ({rest_missing})"
            )
            tot_signals.append(TotSig(edge_pp=edge_pp, line=line))

        over_price = bestt.get("over_price")
        under_price = bestt.get("under_price")
        over_book = bestt.get("over_book")
        under_book = bestt.get("under_book")

        maybe_add_total("Over", over_price, over_book, model_over)
        maybe_add_total("Under", under_price, under_book, model_under)

    ml_signals.sort(key=lambda x: x.edge_pp, reverse=True)
    tot_signals.sort(key=lambda x: x.edge_pp, reverse=True)

    # Build markdown output
    hdr = []
    hdr.append(f"data_date_et: {obj.get('data_date_et')}")
    hdr.append(f"generated_at_utc: {obj.get('generated_at_utc')}")
    hdr.append(f"schema_version: {obj.get('schema_version')}")
    hdr.append("")
    hdr.append(f"counts: odds_games_slim_count={len(odds_games)}, game_rest_count={len(rest_list)}, teams_count={len(teams_list)}, starters_count={len(starters_list)}")
    if len(starters_list) == 0:
        hdr.append("starters: No starters posted yet (signals ignore goalie adjustments).")
    else:
        hdr.append("starters: Starters present but goalie adjustment not implemented in this task.")
    hdr.append("")

    out = []
    out.extend(hdr)

    out.append("MONEYLINE SIGNALS")
    if len(ml_signals) < 3:
        out.append("No qualified signals under current thresholds.")
    else:
        for i, s in enumerate(ml_signals[:10], 1):
            out.append(f"{i}) {s.line}")
    out.append("")

    out.append("TOTALS SIGNALS")
    if len(tot_signals) < 3:
        out.append("No qualified signals under current thresholds.")
    else:
        for i, s in enumerate(tot_signals[:10], 1):
            out.append(f"{i}) {s.line}")
    out.append("")

    out.append("SKIPPED GAMES (short reasons)")
    if not skipped:
        out.append("None")
    else:
        for r in skipped[:25]:
            out.append(f"- {r}")

    OUT_MD.write_text("\n".join(out) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
