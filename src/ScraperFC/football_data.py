import pandas as pd
import requests
from io import StringIO
from typing import Optional, Union, List, Dict
from .utils.get_module_comps import get_module_comps
from .scraperfc_exceptions import InvalidLeagueException, InvalidYearException

class FootballData:
    # Full column descriptions based on https://www.football-data.co.uk/notes.txt
    COLUMN_DESCRIPTIONS = {
        "Div": "League Division",
        "Date": "Match Date (dd/mm/yy)",
        "Time": "Time of match kick off",
        "HomeTeam": "Home Team",
        "AwayTeam": "Away Team",
        "FTHG": "Full Time Home Team Goals",
        "FTAG": "Full Time Away Team Goals",
        "FTR": "Full Time Result (H=Home Win, D=Draw, A=Away Win)",
        "HTHG": "Half Time Home Team Goals",
        "HTAG": "Half Time Away Team Goals",
        "HTR": "Half Time Result (H=Home Win, D=Draw, A=Away Win)",
        "Attendance": "Crowd Attendance",
        "Referee": "Match Referee",
        "HS": "Home Team Shots",
        "AS": "Away Team Shots",
        "HST": "Home Team Shots on Target",
        "AST": "Away Team Shots on Target",
        "HHW": "Home Team Hit Woodwork",
        "AHW": "Away Team Hit Woodwork",
        "HC": "Home Team Corners",
        "AC": "Away Team Corners",
        "HF": "Home Team Fouls Committed",
        "AF": "Away Team Fouls Committed",
        "HFKC": "Home Team Free Kicks Conceded",
        "AFKC": "Away Team Free Kicks Conceded",
        "HO": "Home Team Offsides",
        "AO": "Away Team Offsides",
        "HY": "Home Team Yellow Cards",
        "AY": "Away Team Yellow Cards",
        "HR": "Home Team Red Cards",
        "AR": "Away Team Red Cards",
        "HBP": "Home Team Bookings Points (10=yellow, 25=red)",
        "ABP": "Away Team Bookings Points (10=yellow, 25=red)",
        # Betting odds descriptions (general)
        "B365": "Bet365", "BW": "Bet&Win", "IW": "Interwetten", "PS": "Pinnacle",
        "VC": "VC Bet", "WH": "William Hill",
        "Max": "Market Maximum", "Avg": "Market Average"
    }

    # Mapping for clean column names
    CLEAN_COLUMNS = {
        "FTHG": "home_goals", "FTAG": "away_goals", "FTR": "result",
        "HTHG": "home_goals_half", "HTAG": "away_goals_half", "HTR": "result_half",
        "HS": "home_shots", "AS": "away_shots", "HST": "home_shots_on_target",
        "AST": "away_shots_on_target", "HC": "home_corners", "AC": "away_corners",
        "HF": "home_fouls", "AF": "away_fouls", "HY": "home_yellow_cards",
        "AY": "away_yellow_cards", "HR": "home_red_cards", "AR": "away_red_cards"
    }

    def __init__(self) -> None:
        self.base_url = "https://www.football-data.co.uk"
        self.comps = get_module_comps("FOOTBALL_DATA")

    def _get_season_string(self, year: int) -> str:
        y1 = year % 100
        y2 = (y1 + 1) % 100
        return f"{y1:02d}{y2:02d}"

    def scrape_matches(self, league: str, year: int, clean_columns: bool = True) -> pd.DataFrame:
        """ Scrapes match data from football-data.co.uk.

        :param league: League name
        :param year: Season start year
        :param clean_columns: If True, renames cryptic columns (like HST, HC) to readable ones.
        """
        if league not in self.comps:
            raise InvalidLeagueException(league, "FootballData", list(self.comps.keys()))
        
        league_id = self.comps[league]["FOOTBALL_DATA"]
        season_str = self._get_season_string(year)
        url = f"{self.base_url}/mmz4281/{season_str}/{league_id}.csv"
        
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(f"Failed to download data from {url}")

        try:
            content = response.content.decode("utf-8-sig" if int(season_str) >= 2425 else "latin-1")
        except:
            content = response.content.decode("latin-1", errors="ignore")

        df = pd.read_csv(StringIO(content), on_bad_lines="warn")
        df = df.dropna(subset=["HomeTeam", "AwayTeam"], how="all")
        
        # Initial renaming for core fields
        core_rename = {
            "Div": "league", "Date": "date", "Time": "time",
            "HomeTeam": "home_team", "AwayTeam": "away_team", "Referee": "referee"
        }
        df = df.rename(columns=core_rename)

        if clean_columns:
            df = df.rename(columns=self.CLEAN_COLUMNS)

        # Time handling
        if "time" not in df.columns: df["time"] = "12:00"
        df["time"] = df["time"].fillna("12:00")
        try:
            df["date"] = pd.to_datetime(df["date"] + " " + df["time"], dayfirst=True, format="mixed")
        except:
            df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors='coerce')
            
        return df

    def get_column_info(self, column_name: str) -> str:
        """ Returns the description of a column based on notes.txt """
        return self.COLUMN_DESCRIPTIONS.get(column_name, "No description available.")
