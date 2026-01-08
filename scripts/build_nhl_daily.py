import pandas as pd

def filter_teams_to_all_row(df: pd.DataFrame) -> pd.DataFrame:
    # Normalize column names just in case
    cols = {c.lower(): c for c in df.columns}

    # MoneyPuck commonly uses these split columns
    situation_col = cols.get("situation")
    score_col = cols.get("score")

    filtered = df.copy()

    # Prefer the true overall row: situation == "all" (and score == "all" if present)
    if situation_col:
        filtered = filtered[filtered[situation_col].astype(str).str.lower().eq("all")]

    if score_col:
        filtered = filtered[filtered[score_col].astype(str).str.lower().eq("all")]

    # If still duplicated (or those cols do not exist), fall back to “most-games” row per team.
    # Identify the team key
    team_col = cols.get("team") or cols.get("teamname") or cols.get("team_name")
    if not team_col:
        raise ValueError(f"Could not find team column. Columns: {list(df.columns)}")

    # Try to find a games column for deterministic pick
    gp_col = (
        cols.get("gp")
        or cols.get("games_played")
        or cols.get("games")
        or cols.get("g")
    )

    if gp_col and gp_col in filtered.columns:
        filtered[gp_col] = pd.to_numeric(filtered[gp_col], errors="coerce").fillna(0)
        filtered = (
            filtered.sort_values([team_col, gp_col], ascending=[True, False])
                    .drop_duplicates(subset=[team_col], keep="first")
        )
    else:
        # Last resort deterministic drop
        filtered = filtered.sort_values([team_col]).drop_duplicates(subset=[team_col], keep="first")

    return filtered.reset_index(drop=True)
