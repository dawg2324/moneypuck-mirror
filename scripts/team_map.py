# scripts/team_map.py
from __future__ import annotations

# Single source of truth mapping: full team name -> NHL abbreviation
# Reuse everywhere (odds, moneyPuck, DFO, etc.)
TEAM_TO_ABBR: dict[str, str] = {
    "Anaheim Ducks": "ANA",
    "Arizona Coyotes": "ARI",  # legacy, keep if any sources still use it
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
    "Utah Hockey Club": "UTA",  # current franchise name
    "Vancouver Canucks": "VAN",
    "Vegas Golden Knights": "VGK",
    "Washington Capitals": "WSH",
    "Winnipeg Jets": "WPG",
}

# DailyFaceoff sometimes uses slightly different labels in nav/other contexts.
# Keep aliases here and normalize via `normalize_team_name`.
TEAM_NAME_ALIASES: dict[str, str] = {
    # Common punctuation variants
    "St Louis Blues": "St. Louis Blues",
    "LA Kings": "Los Angeles Kings",
    "New York Isles": "New York Islanders",
    # Franchise naming drift
    "Utah": "Utah Hockey Club",
    "Utah HC": "Utah Hockey Club",
    # If any source still outputs these
    "Phoenix Coyotes": "Arizona Coyotes",
}


def normalize_team_name(name: str) -> str:
    """Normalize a team label into the canonical full team name used in TEAM_TO_ABBR."""
    n = (name or "").strip()
    if not n:
        return n
    return TEAM_NAME_ALIASES.get(n, n)


def team_abbr_from_any_label(label: str) -> str:
    """Convert a label (canonical or alias) into abbreviation, or raise KeyError."""
    canonical = normalize_team_name(label)
    return TEAM_TO_ABBR[canonical]
