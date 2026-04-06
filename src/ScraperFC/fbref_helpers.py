""" Helper functions for use in multiple functions in the FBref module
"""
from bs4 import BeautifulSoup, Comment, Tag, NavigableString


# ==================================================================================================
def _get_player_id_from_url(url: str) -> str:
    pieces = url.split("/")
    players_idx = None
    for i, piece in enumerate(pieces):
        if piece == "players":
            players_idx = i
    if players_idx is None:
        raise ValueError(f"'players' chunk not found in split URL {pieces}")
    return pieces[players_idx + 1]

# ==================================================================================================
def _get_team_id_from_url(url: str) -> str:
    pieces = url.split("/")
    squads_idx = None
    for i, piece in enumerate(pieces):
        if piece == "squads":
            squads_idx = i
    if squads_idx is None:
        raise ValueError(f"'squads' chunk not found in split URL {pieces}")
    return pieces[squads_idx + 1]

# ==================================================================================================
def _find_commented_out_tables(soup: BeautifulSoup) -> list[str]:
    comments = soup.find_all(string = lambda el: isinstance(el, Comment))
    table_comments = [c for c in comments if 'table' in c and '<div' in c]
    return table_comments

# ==================================================================================================
def _get_ids_from_table(table_tag: Tag, table_type: str) -> list[str]:
    valid_types = ["team", "player"]
    if table_type not in valid_types:
        raise ValueError(f"Invalid table type: {table_type}. Valid types are {valid_types}")
    
    # Use tbody and tfoot to skip header rows and match pd.read_html output
    body = table_tag.find("tbody")
    footer = table_tag.find("tfoot")
    rows = []
    if body:
        rows.extend(body.find_all("tr"))
    if footer:
        rows.extend(footer.find_all("tr"))
    
    if not rows:
        # Fallback if no tbody/tfoot, skip thead explicitly
        rows = [tr for tr in table_tag.find_all("tr") if tr.parent.name != "thead"]

    urls = [el.find("a")["href"] for el in rows if el.find("a")]
    
    if table_type == "team":
        ids = [_get_team_id_from_url(url) for url in urls]
    elif table_type == "player":
        ids = [_get_player_id_from_url(url) for url in urls]
    return ids

# ==================================================================================================
def _get_stats_table_tag(soup: BeautifulSoup, soup_find_args: dict) -> Tag | NavigableString | None:
    """ Find a stats table in the soup from an FBref page

    If no table is explicity found, will search for a commented out tables. (Champions League
    comments out the player stats table until the user clicks to show the player stats.)

    Params:
        soup
        soup_find_args: dict passed to soup.find(). Will probably be {'name': str, 'attrs': dict}
    """
    table_tag = soup.find(**soup_find_args)

    # If no tag was found, try looking in commented out tables
    if table_tag is None:
        # Try to find commented out table
        table_comments = _find_commented_out_tables(soup)
        for comment in table_comments:
            comment_soup = BeautifulSoup(comment, "html.parser")
            table_tag = comment_soup.find(**soup_find_args)
            if table_tag is not None:
                return table_tag

    return table_tag


# ==================================================================================================
def _get_age_mask(df: "pd.DataFrame") -> "pd.Series":
    """ Safely creates a mask for the 'Age' column, handling MultiIndex and flat columns.
    """
    import pandas as pd
    if isinstance(df.columns, pd.MultiIndex):
        # Check if "Age" exists in level 1
        if "Age" in df.columns.get_level_values(1):
            return ~df.xs("Age", level=1, axis=1).isna().any(axis=1)
    
    # Fallback for flat columns
    if "Age" in df.columns:
        return df["Age"].notna()
    
    # If columns are called "('Age', '')" or similar
    age_cols = [c for c in df.columns if isinstance(c, str) and "Age" in c]
    if age_cols:
        return df[age_cols[0]].notna()
    
    return pd.Series(True, index=df.index)
