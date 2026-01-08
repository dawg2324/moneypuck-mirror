# scripts/team_map.py
from __future__ import annotations

TEAM_TO_ABBR: dict[str, str] = {
    "Anaheim Ducks": "ANA",
    "Boston Bruins": "BOS",
    "Buffalo Sabres": "BUF",
    "Calgary Flames": "CGY",
    "Carolina Hurricanes": "CAR",
    "Chicago Blackhawks": "CHI",
    "Colorado Avalanche": "COL",
    "Columbus Blue Jackets": "CBJ",
    "Dallas Stars": "DAL",
    "Detroit Red Wings": "DET",
    "Edmonton Oilers": "EDM",
    "Florida Panthers": "FLA",
    "Los Angeles Kings": "LAK",
    "Minnesota Wild": "MIN",
    "Montreal Canadiens": "MTL",
    "Nashville Predators": "NSH",
    "New Jersey Devils": "NJD",
    "New York Islanders": "NYI",
    "New York Rangers": "NYR",
    "Ottawa Senators": "OTT",
    "Philadelphia Flyers": "PHI",
    "Pittsburgh Penguins": "PIT",
    "San Jose Sharks": "SJS",
    "Seattle Kraken": "SEA",
    "St. Louis Blues": "STL",
    "Tampa Bay Lightning": "TBL",
    "Toronto Maple Leafs": "TOR",
    "Utah Hockey Club": "UTA",
    "Vancouver Canucks": "VAN",
    "Vegas Golden Knights": "VGK",
    "Washington Capitals": "WSH",
    "Winnipeg Jets": "WPG",
}

TEAM_NAME_ALIASES: dict[str, str] = {
    "St Louis Blues": "St. Louis Blues",
    "LA Kings": "Los Angeles Kings",
    "Utah": "Utah Hockey Club",
    "Utah HC": "Utah Hockey Club",
    # legacy franchise label that might still appear in some sources
    "Arizona Coyotes": "Utah Hockey Club",
}

def normalize_team_name(name: str) -> str:
    n = (name or "").strip()
    if not n:
        return n
    return TEAM_NAME_ALIASES.get(n, n)

def team_abbr_from_any_label(label: str) -> str:
    canonical = normalize_team_name(label)
    return TEAM_TO_ABBR[canonical]
