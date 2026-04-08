import pandas as pd
import hashlib
import json
import os
import yaml
import re
from typing import Optional, Union, List, Any
from .fbref import FBref
from .sofascore import Sofascore
from .transfermarkt import Transfermarkt
from .understat import Understat
from .capology import Capology
from .clubelo import ClubElo
from .football_data import FootballData
from .utils.cache_manager import CacheManager
from rapidfuzz import process, fuzz

class ScraperFC:
    def __init__(self, use_cache: bool = True, cache_dir: str = ".scraperfc_cache"):
        # Load comps.yaml directly from disk
        comps_path = os.path.join(os.path.dirname(__file__), "comps.yaml")
        if os.path.exists(comps_path):
            with open(comps_path, "r", encoding="utf-8") as f:
                self.comps = yaml.safe_load(f)
        else:
            from .utils.load_comps import load_comps
            self.comps = load_comps()

        self.use_cache = use_cache
        self.cache = CacheManager(cache_dir) if use_cache else None
        
        # Lazy loading of modules
        self._fbref = None
        self._sofascore = None
        self._transfermarkt = None
        self._understat = None
        self._capology = None
        self._clubelo = None
        self._football_data = None

    @property
    def fbref(self) -> FBref:
        if self._fbref is None: self._fbref = FBref()
        return self._fbref

    @property
    def sofascore(self) -> Sofascore:
        if self._sofascore is None: self._sofascore = Sofascore()
        return self._sofascore

    @property
    def transfermarkt(self) -> Transfermarkt:
        if self._transfermarkt is None: self._transfermarkt = Transfermarkt()
        return self._transfermarkt

    @property
    def understat(self) -> Understat:
        if self._understat is None: self._understat = Understat()
        return self._understat

    @property
    def capology(self) -> Capology:
        if self._capology is None: self._capology = Capology()
        return self._capology

    @property
    def clubelo(self) -> ClubElo:
        if self._clubelo is None: self._clubelo = ClubElo()
        return self._clubelo

    @property
    def football_data(self) -> FootballData:
        if self._football_data is None: self._football_data = FootballData()
        return self._football_data

    def _match_league(self, league: str, source: str) -> str:
        """ Finds the best matching league name for a given source. """
        s_upper = source.upper().replace("-", "_")
        available_leagues = [name for name, data in self.comps.items() if s_upper in data]
        
        if not available_leagues:
            available_leagues = list(self.comps.keys())

        # Handle common abbreviations
        abbr_map = {"EPL": "England Premier League", "RPL": "Russia Premier League"}
        league = abbr_map.get(league.upper(), league)

        match = process.extractOne(league, available_leagues, scorer=fuzz.token_set_ratio)
        if match and match[1] > 70:
            return match[0]
        raise ValueError(f"League '{league}' not found for source {source}. Available: {available_leagues[:5]}...")

    def _standardize_year(self, year: Union[int, str], source: str) -> str:
        """ Converts numeric year to source-specific string format. """
        s = source.lower().replace("-", "_")
        if isinstance(year, int):
            if s == "fbref":
                return f"{year}-{year+1}"
            if s == "sofascore":
                y1 = year % 100
                y2 = (y1 + 1) % 100
                return f"{y1:02d}/{y2:02d}"
        return str(year)

    def _flatten_sofascore(self, data: Any) -> pd.DataFrame:
        """ Universal flattener for Sofascore nested JSON data. """
        if data is None: return pd.DataFrame()
        if isinstance(data, pd.DataFrame):
            if data.empty: return data
            # Check if columns contain dicts
            if not any(isinstance(x, dict) for x in data.iloc[0] if pd.notnull(x)):
                return data
            data_list = data.to_dict('records')
        else:
            data_list = data if isinstance(data, list) else [data]

        flat_list = []
        for row in data_list:
            item = {}
            for k, v in row.items():
                if isinstance(v, dict):
                    prefix = "" if k in ["statistics", "player", "team"] else f"{k}_"
                    # If it's statistics, we can merge directly, otherwise prefix
                    for sub_k, sub_v in v.items():
                        new_key = sub_k if k == "statistics" else f"{prefix}{sub_k}"
                        item[new_key] = sub_v
                else:
                    item[k] = v
            flat_list.append(item)
        return pd.DataFrame(flat_list)

    def _get_sofascore_match_id(self, link: str) -> str:
        """ Resolves numeric match ID from Sofascore link or returns the link if it's already an ID. """
        if str(link).isdigit(): return str(link)
        
        # Try extracting from URL slug
        match = re.search(r'/([^/]+)$', link.strip('/'))
        slug = match.group(1) if match else link
        if slug.isdigit(): return slug

        # Fetch page to find the real ID
        from .utils.botasaurus_getters import botasaurus_request_get_soup
        soup = botasaurus_request_get_soup(link)
        script = soup.find('script', string=re.compile('initialState'))
        if script:
            id_match = re.search(r'"event":\{"id":(\d+)', script.string)
            if id_match: return id_match.group(1)
            # Fallback search
            id_match = re.search(r'"id":(\d+)', script.string)
            if id_match: return id_match.group(1)
        return slug # Fallback to slug if nothing found

    def get_league_stats(self, league: str, year: int, source: str = "fbref", stat_type: str = "standard", bypass_cache: bool = False) -> pd.DataFrame:
        """ Main method to get league statistics. """
        if self.use_cache and not bypass_cache:
            cached = self.cache.get("get_league_stats", league, year, source, stat_type)
            if cached is not None: return cached

        matched_name = self._match_league(league, source)
        year_str = self._standardize_year(year, source)
        s = source.lower().replace("-", "_")
        
        result = None
        if s == "fbref":
            stats_dict = self.fbref.scrape_stats(year_str, matched_name, stat_type)
            result = stats_dict.get("player")
        elif s == "understat":
            tables = self.understat.scrape_league_tables(year_str, matched_name)
            result = tables[0] if isinstance(tables, (tuple, list)) else tables
        elif s == "football_data":
            result = self.football_data.scrape_matches(matched_name, year)
        elif s == "sofascore":
            raw = self.sofascore.scrape_player_league_stats(year_str, matched_name)
            result = self._flatten_sofascore(raw)
        
        if result is not None and self.use_cache:
            self.cache.set(result, "get_league_stats", league, year, source, stat_type)
        return result

    def get_match_stats(self, link: str, source: str = "fbref", type: str = "stats", bypass_cache: bool = False) -> Any:
        """ Main method to get match-specific statistics. """
        if self.use_cache and not bypass_cache:
            cached = self.cache.get("get_match_stats", link, source, type)
            if cached is not None: return cached

        s = source.lower().replace("-", "_")
        result = None

        if s == "fbref":
            result = self.fbref.scrape_match(link)
        elif s == "sofascore":
            match_id = self._get_sofascore_match_id(link)
            if type == "stats": result = self.sofascore.scrape_team_match_stats(match_id)
            elif type == "shots": result = self._flatten_sofascore(self.sofascore.scrape_match_shots(match_id))
            elif type == "momentum": result = self.sofascore.scrape_match_momentum(match_id)
            elif type == "players": result = self._flatten_sofascore(self.sofascore.scrape_player_match_stats(match_id))
            elif type == "positions": result = self.sofascore.scrape_player_average_positions(match_id)
            elif type == "heatmaps": result = self.sofascore.scrape_heatmaps(match_id)

        if result is not None and self.use_cache:
            self.cache.set(result, "get_match_stats", link, source, type)
        return result

    def close(self):
        if self._fbref: self._fbref.close()
