# scripts/compute_rest.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, date, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
import json
import urllib.request

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:
    ZoneInfo = None  # type: ignore

ET = ZoneInfo("America/New_York") if ZoneInfo else None


def _http_get_json(url: str, timeout: int = 20) -> Dict[str, Any]:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "nhl-daily-slim/1.0 (rest-compute)",
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8")
    return json.loads(raw)


def _parse_utc(dt_str: str) -> datetime:
    if dt_str.endswith("Z"):
        dt_str = dt_str[:-1] + "+00:00"
    dt = datetime.fromisoformat(dt_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _to_et(dt_utc: datetime) -> datetime:
    if not ET:
        raise RuntimeError("ZoneInfo not available, cannot convert to ET")
    return dt_utc.astimezone(ET)


def _et_date_from_commence_time(commence_time_utc: str) -> date:
    dt_utc = _parse_utc(commence_time_utc)
    dt_et = _to_et(dt_utc)
    return dt_et.date()


def _month_key(d: date) -> str:
    return f"{d.year:04d}-{d.month:02d}"


def _extract_games(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    if isinstance(payload.get("games"), list):
        return payload["games"]

    if isinstance(payload.get("gameWeek"), list):
        out: List[Dict[str, Any]] = []
        for day in payload["gameWeek"]:
            if isinstance(day, dict) and isinstance(day.get("games"), list):
                out.extend(day["games"])
        return out

    if isinstance(payload.get("dates"), list):
        out: List[Dict[str, Any]] = []
        for day in payload["dates"]:
            if isinstance(day, dict) and isinstance(day.get("games"), list):
                out.extend(day["games"])
        return out

    return []


def _game_start_utc(g: Dict[str, Any]) -> Optional[datetime]:
    for k in ("startTimeUTC", "gameDate", "startTime"):
        v = g.get(k)
        if isinstance(v, str):
            try:
                return _parse_utc(v)
            except Exception:
                continue
    return None


def _team_abbrev(obj: Any) -> Optional[str]:
    if not isinstance(obj, dict):
        return None
    for k in ("abbrev", "triCode", "abbreviation"):
        v = obj.get(k)
        if isinstance(v, str) and len(v) == 3:
            return v.upper()
    return None


def _game_has_team(g: Dict[str, Any], team: str) -> bool:
    team = team.upper()
    ht = _team_abbrev(g.get("homeTeam"))
    at = _team_abbrev(g.get("awayTeam"))
    if ht == team or at == team:
        return True

    ht2 = _team_abbrev(g.get("homeTeam", {}).get("team"))
    at2 = _team_abbrev(g.get("awayTeam", {}).get("team"))
    return (ht2 == team) or (at2 == team)


@dataclass(frozen=True)
class TeamRest:
    rest_days: int
    b2b: bool
    prev_game_et_date: Optional[str]
    missing_prev_game: bool


class RestComputer:
    def __init__(self) -> None:
        self._cache: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}

    def _fetch_team_month(self, team: str, yyyy_mm: str) -> List[Dict[str, Any]]:
        key = (team.upper(), yyyy_mm)
        if key in self._cache:
            return self._cache[key]

        url = f"https://api-web.nhle.com/v1/club-schedule/{team.upper()}/month/{yyyy_mm}"
        payload = _http_get_json(url)
        games = _extract_games(payload)

        games = [g for g in games if _game_has_team(g, team)]
        self._cache[key] = games
        return games

    def _get_candidates(self, team: str, game_day: date, lookback_days: int) -> List[Dict[str, Any]]:
        start_day = game_day - timedelta(days=lookback_days)
        months = {_month_key(game_day), _month_key(start_day)}
        out: List[Dict[str, Any]] = []
        for m in sorted(months):
            out.extend(self._fetch_team_month(team, m))
        return out

    def compute_team_rest(self, team: str, commence_time_utc: str) -> TeamRest:
        dt_utc = _parse_utc(commence_time_utc)
        dt_et = _to_et(dt_utc)
        game_day = dt_et.date()

        prev_game_start_et: Optional[datetime] = None

        for lookback in (14, 30):
            candidates = self._get_candidates(team, game_day, lookback)

            best: Optional[datetime] = None
            for g in candidates:
                g_start_utc = _game_start_utc(g)
                if not g_start_utc:
                    continue
                g_start_et = _to_et(g_start_utc)
                if g_start_et < dt_et:
                    if best is None or g_start_et > best:
                        best = g_start_et

            if best:
                prev_game_start_et = best
                break

        if not prev_game_start_et:
            return TeamRest(
                rest_days=0,
                b2b=False,
                prev_game_et_date=None,
                missing_prev_game=True,
            )

        prev_day = prev_game_start_et.date()
        off_days = (game_day - prev_day).days - 1
        if off_days < 0:
            off_days = 0

        return TeamRest(
            rest_days=int(off_days),
            b2b=(off_days == 0),
            prev_game_et_date=prev_day.isoformat(),
            missing_prev_game=False,
        )


def build_slim_rest(games: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rc = RestComputer()
    out: List[Dict[str, Any]] = []

    for g in games:
        home = str(g["home_team"]).upper()
        away = str(g["away_team"]).upper()
        ct = str(g["commence_time_utc"])

        dt_et = _et_date_from_commence_time(ct)

        away_rest = rc.compute_team_rest(away, ct)
        home_rest = rc.compute_team_rest(home, ct)

        out.append(
            {
                "game_key": g["game_key"],
                "date_et": dt_et.isoformat(),
                "away_team": away,
                "home_team": home,
                "away_rest_days": away_rest.rest_days,
                "home_rest_days": home_rest.rest_days,
                "away_b2b": bool(away_rest.b2b),
                "home_b2b": bool(home_rest.b2b),
                "rest_advantage": int(home_rest.rest_days - away_rest.rest_days),
                "away_rest_missing": bool(away_rest.missing_prev_game),
                "home_rest_missing": bool(home_rest.missing_prev_game),
            }
        )

    return out
