""" Helper functions for fbref.scrape_match()
"""
from bs4 import BeautifulSoup
from io import StringIO
import re
import pandas as pd
from .fbref_helpers import _get_ids_from_table, _get_age_mask, _get_stats_table_tag, _get_all_stats_table_tags

# ==================================================================================================
def _get_date(soup: BeautifulSoup) -> str:
    """ Gets match date """
    # Method 1: Strong tag in scorebox_meta
    scorebox_meta_tag = soup.find("div", {"class": "scorebox_meta"})
    if scorebox_meta_tag:
        strong_tag = scorebox_meta_tag.find("strong")
        if strong_tag:
            return strong_tag.text.strip()
        # Fallback: second div in scorebox_meta
        divs = scorebox_meta_tag.find_all("div")
        if len(divs) > 0:
            return divs[0].text.strip()

    # Method 2: venuetime span (old method, with fallback)
    venue_time = soup.find("span", {"class": "venuetime"})
    if venue_time:
        if venue_time.has_attr("data-venue-date"):
            return venue_time["data-venue-date"]
        return venue_time.text.strip()

    # Method 3: Title of the page
    title = soup.find("title")
    if title:
        # Match Report - City vs Arsenal, March 31, 2024
        match = re.search(r",\s*([A-Z][a-z]+ \d{1,2}, \d{4})", title.text)
        if match:
            return match.group(1)

    return "Unknown Date"

# ==================================================================================================
def _get_stage(soup: BeautifulSoup) -> str:
    """ Gets the stage description """
    main_div = soup.find("div", {"role": "main"})
    if not main_div:
        return "Unknown Stage"
    
    first_div = main_div.find("div")
    if first_div:
        return first_div.text.strip()
    return "Unknown Stage"

# ==================================================================================================
def _get_team_names(soup: BeautifulSoup) -> tuple[str, str]:
    """ Gets home and away team names """
    scorebox = soup.find("div", {"class": "scorebox"})
    if not scorebox:
        return "Home", "Away"
    
    team_els = scorebox.find_all("div", recursive=False)
    if len(team_els) < 2:
        # Try finding team names via links
        links = scorebox.find_all("a", {"itemprop": "name"})
        if len(links) >= 2:
            return links[0].text.strip(), links[1].text.strip()
        return "Home", "Away"

    home_name = team_els[0].find("div").text.strip()
    away_name = team_els[1].find("div").text.strip()
    return home_name, away_name

# ==================================================================================================
def _get_team_ids(soup: BeautifulSoup) -> tuple[str, str]:
    """ Gets home and away team IDs """
    scorebox = soup.find("div", {"class": "scorebox"})
    if not scorebox:
        return "unknown", "unknown"
    
    team_els = scorebox.find_all("div", recursive=False)
    if len(team_els) < 2:
        return "unknown", "unknown"
    
    def extract_id(el):
        link = el.find("a", href=re.compile(r"/en/squads/"))
        if link:
            parts = link["href"].split("/")
            if len(parts) >= 4:
                return parts[3]
        return "unknown"

    home_id = extract_id(team_els[0])
    away_id = extract_id(team_els[1])
    return home_id, away_id

# ==================================================================================================
def _get_goals(soup: BeautifulSoup) -> tuple[str, str]:
    """ Gets home and away team goals """
    scorebox = soup.find("div", {"class": "scorebox"})
    if not scorebox:
        return "0", "0"
    
    team_els = scorebox.find_all("div", recursive=False)
    if len(team_els) < 2:
        return "0", "0"
    
    home_goals = team_els[0].find("div", {"class": "score"}).text if team_els[0].find("div", {"class": "score"}) else "0"
    away_goals = team_els[1].find("div", {"class": "score"}).text if team_els[1].find("div", {"class": "score"}) else "0"
    
    return home_goals.strip(), away_goals.strip()

# ==================================================================================================
def _get_player_stats(soup: BeautifulSoup) -> dict[str, dict[str, pd.DataFrame]]:
    """ Gets player stats for home and away teams
    """
    home_id, away_id = _get_team_ids(soup)

    home_tables = _get_all_stats_table_tags(soup, {"name": "table", "id": re.compile(f"stats_{home_id}")})
    home_player_stats = dict()
    for table in home_tables:
        key = table["id"].replace(f"stats_{home_id}", "").strip("_")
        df = pd.read_html(StringIO(str(table)))[0].copy()
        ids = _get_ids_from_table(table, "player")
        not_nan_mask = _get_age_mask(df)
        ids_aligned = pd.Series(ids, index=df.index[not_nan_mask][:len(ids)])
        df.loc[ids_aligned.index, "Player ID"] = ids_aligned
        home_player_stats[key] = df

    away_tables = _get_all_stats_table_tags(soup, {"name": "table", "id": re.compile(f"stats_{away_id}")})
    away_player_stats = dict()
    for table in away_tables:
        key = table["id"].replace(f"stats_{away_id}", "").strip("_")
        df = pd.read_html(StringIO(str(table)))[0].copy()
        ids = _get_ids_from_table(table, "player")
        not_nan_mask = _get_age_mask(df)
        ids_aligned = pd.Series(ids, index=df.index[not_nan_mask][:len(ids)])
        df.loc[ids_aligned.index, "Player ID"] = ids_aligned
        away_player_stats[key] = df

    return {"home": home_player_stats, "away": away_player_stats}

# ==================================================================================================
def _get_shots(soup: BeautifulSoup) -> dict[str, pd.DataFrame]:
    """ Gets shot data
    """
    home_id, away_id = _get_team_ids(soup)
    all_el = _get_stats_table_tag(soup, {"name": "table", "id": "shots_all"})
    all_shots = pd.read_html(StringIO(str(all_el)))[0] if all_el else pd.DataFrame()

    home_el = _get_stats_table_tag(soup, {"name": "table", "id": re.compile(f"shots_{home_id}")})
    home_shots = pd.read_html(StringIO(str(home_el)))[0] if home_el else pd.DataFrame()

    away_el = _get_stats_table_tag(soup, {"name": "table", "id": re.compile(f"shots_{away_id}")})
    away_shots = pd.read_html(StringIO(str(away_el)))[0] if away_el else pd.DataFrame()

    return {"all": all_shots, "home": home_shots, "away": away_shots}

# ==================================================================================================
def _get_officials(soup: BeautifulSoup) -> dict[str, str]:
    """ Gets officials' names
    """
    return_dict = {"Referee": "", "AR1": "", "AR2": "", "4th": "", "VAR": ""}

    strong_officials_tag = soup.find("strong", string="Officials")
    if not strong_officials_tag:
        return return_dict

    officials_tag = strong_officials_tag.parent
    if not officials_tag:
        return return_dict

    referee_tag = officials_tag.find(string=re.compile("Referee"))
    if referee_tag:
        referee = referee_tag.text
        referee = referee.replace("\xa0", " ").replace(" (Referee)", "")
        return_dict["Referee"] = referee

    ar1_tag = officials_tag.find(string=re.compile("AR1"))
    if ar1_tag:
        ar1 = ar1_tag.text
        ar1 = ar1.replace("\xa0", " ").replace(" (AR1)", "")
        return_dict["AR1"] = ar1

    ar2_tag = officials_tag.find(string=re.compile("AR2"))
    if ar2_tag:
        ar2 = ar2_tag.text
        ar2 = ar2.replace("\xa0", " ").replace(" (AR2)", "")
        return_dict["AR2"] = ar2

    fourth_tag = officials_tag.find(string=re.compile("4th"))
    if fourth_tag:
        fourth = fourth_tag.text
        fourth = fourth.replace("\xa0", " ").replace(" (4th)", "")
        return_dict["4th"] = fourth

    var_tag = officials_tag.find(string=re.compile("VAR"))
    if var_tag:
        var = var_tag.text
        var = var.replace("\xa0", " ").replace(" (VAR)", "")
        return_dict["VAR"] = var

    return return_dict
