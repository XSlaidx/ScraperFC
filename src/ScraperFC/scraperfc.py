import pandas as pd
import hashlib
import json
import os
import yaml
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
        # Load comps.yaml directly from disk to ensure we have the latest updates
        comps_path = os.path.join(os.path.dirname(__file__), "comps.yaml")
        if os.path.exists(comps_path):
            with open(comps_path, "r", encoding="utf-8") as f:
                self.comps = yaml.safe_load(f)
        else:
            from .utils.load_comps import load_comps
            self.comps = load_comps()

        self.use_cache = use_cache
        self.cache = CacheManager(cache_dir) if use_cache else None
        
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
        available_leagues = [name for name, data in self.comps.items() if s_upper in data or source.upper() in data]
        
        print(f"DEBUG: Found {len(available_leagues)} leagues for source {source}")
        
        if not available_leagues:
            print(f"DEBUG: Source key '{s_upper}' not found in any league. First 3 leagues: {list(self.comps.items())[:3]}")
            available_leagues = list(self.comps.keys())

        match = process.extractOne(league, available_leagues, scorer=fuzz.token_set_ratio)
        if match and match[1] > 70:
            return match[0]
        raise ValueError(f"League '{league}' not found for source {source}. Available: {available_leagues[:5]}...")

    def get_league_stats(self, league: str, year: int, source: str = "fbref", stat_type: str = "standard", bypass_cache: bool = False) -> pd.DataFrame:
        """ Unified method to get league stats with caching. """
        if self.use_cache and not bypass_cache:
            cached_data = self.cache.get("get_league_stats", league, year, source, stat_type)
            if cached_data is not None:
                print(f"      📡 Cache HIT for {league} {year} ({source})")
                return cached_data

        matched_name = self._match_league(league, source)
        s = source.lower().replace("-", "_")
        year_str = str(year) # FBref expects string for year (e.g. "2023-2024" or "2023")
        
        # Standardize year for FBref (it often uses YYYY-YYYY format)
        if s == "fbref" and len(year_str) == 4:
            year_str = f"{year_str}-{int(year_str)+1}"

        result = None
        if s == "fbref":
            # scrape_stats returns a dict with 'squad', 'opponent', 'player'
            stats_dict = self.fbref.scrape_stats(year_str, matched_name, stat_type)
            # Return player stats by default as it's most common
            result = stats_dict.get("player")
        elif s == "understat":
            result = self.understat.scrape_league_table(matched_name, year)
        elif s == "football_data":
            result = self.football_data.scrape_matches(matched_name, year)
        
        if result is None:
            raise NotImplementedError(f"Stats for {source} not yet unified in Facade.")

        if self.use_cache:
            self.cache.set(result, "get_league_stats", league, year, source, stat_type)
        
        return result

    def get_match_stats(self, link: str, source: str = "fbref", bypass_cache: bool = False) -> Union[pd.DataFrame, dict]:
        """ Unified match stats getter with caching. """
        if self.use_cache and not bypass_cache:
            cached_data = self.cache.get("get_match_stats", link, source)
            if cached_data is not None:
                print(f"      📡 Cache HIT for match {link}")
                return cached_data

        result = None
        s = source.lower().replace("-", "_")
        if s == "fbref":
            result = self.fbref.scrape_match(link)
        elif s == "sofascore":
            result = self.sofascore.get_match_stats(link)
        
        if result is None:
            raise NotImplementedError(f"Match stats for {source} not yet unified.")

        if self.use_cache:
            self.cache.set(result, "get_match_stats", link, source)
        
        return result

    def close(self):
        """ Closes all underlying drivers. """
        if self._fbref: self._fbref.close()
