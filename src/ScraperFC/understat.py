from .scraperfc_exceptions import InvalidLeagueException, InvalidYearException
import json
import pandas as pd
from tqdm import tqdm
import requests
from bs4 import BeautifulSoup
import warnings
import re
from ScraperFC.utils import get_module_comps

comps = get_module_comps("UNDERSTAT")

def _json_from_script(text: str) -> dict:
    if not text:
        return {}
    # Попробовать старый формат
    pattern = r"JSON\.parse\(['\"](.*?)['\"]\)"
    match = re.search(pattern, text, re.DOTALL)
    if match:
        try:
            encoded = match.group(1)
            decoded = encoded.encode('utf-8').decode('unicode_escape')
            return json.loads(decoded)
        except:
            pass
    
    # Fallback to original split logic
    try:
        data_str = text.split('JSON.parse(\'')[1].split('\')')[0].encode('utf-8').decode('unicode_escape')
        return json.loads(data_str)
    except (IndexError, KeyError):
        return {}


class Understat:

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json, text/javascript, */*',
            'X-Requested-With': 'XMLHttpRequest',
        })

    def _fetch_api_data(self, endpoint: str, params: dict = None) -> dict:
        """Вспомогательный метод для запросов к новому API"""
        url = f"https://understat.com/{endpoint}"
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            warnings.warn(f"API request failed for {endpoint}: {e}")
            return {}

    # ==============================================================================================
    def get_season_link(self, year: str, league: str) -> str:
        """ Gets Understat URL of the chosen league season.

        :param year: .. include:: ./arg_docstrings/year_understat.rst
        :type year: str
        :param league: .. include:: ./arg_docstrings/league.rst
        :type league: str
        :raises TypeError: If any of the parameters are the wrong type
        :raises InvalidLeagueException: If the league is not a valid league for this module.
        :raises InvalidYearException: If the year is not a valid year for this league.
        :return: URL to the Understat page of the chosen league season.
        :rtype: str
        """
        if not isinstance(year, str):
            raise TypeError('`year` must be a string.')
        if not isinstance(league, str):
            raise TypeError('`league` must be a string.')
        if league not in comps.keys():
            raise InvalidLeagueException(league, 'Understat', list(comps.keys()))
        valid_seasons = self.get_valid_seasons(league)
        if year not in valid_seasons:
            raise InvalidYearException(year, league, valid_seasons)

        return f'{comps[league]["UNDERSTAT"]}/{year.split("/")[0]}'

    # ==============================================================================================
    def get_valid_seasons(self, league: str) -> list[str]:
        """ Returns valid season strings for the chosen league.

        :param league: .. include:: ./arg_docstrings/league.rst
        :type league: str
        :raises InvalidLeagueException: If the league is not a valid league for this module.
        :return: List of valid year strings for this league
        :rtype: list[str]
        """
        if league not in comps.keys():
            raise InvalidLeagueException(league, 'Understat', list(comps.keys()))

        soup = BeautifulSoup(self.session.get(comps[league]["UNDERSTAT"]).content, 'html.parser')
        select = soup.find('select', {'name': 'season'})
        if not select:
            return []
        valid_seasons = [x.get('value') for x in select.find_all('option') if x.get('value')]
        return valid_seasons

    # ==============================================================================================
    def get_match_links(self, year: str, league: str) -> list[str]:
        """ Gets all of the match links for the chosen league season

        :param year: .. include:: ./arg_docstrings/year_understat.rst
        :type year: str
        :param league: .. include:: ./arg_docstrings/league.rst
        :type league: str
        :return: List of match links of the chosen league season
        :rtype: list[str]
        """
        matches_data, _, _ = self.scrape_season_data(year, league)
        # Поддержка обеих структур данных
        if isinstance(matches_data, dict):
            return [f'https://understat.com/match/{x["id"]}' for x in matches_data.values() 
                    if isinstance(x, dict) and x.get('isResult')]
        elif isinstance(matches_data, list):
            return [f'https://understat.com/match/{x["id"]}' for x in matches_data 
                    if x.get('isResult')]
        return []

    # ==============================================================================================
    def get_team_links(self, year: str, league: str) -> list[str]:
        """ Gets all of the team links for the chosen league season

        :param year: .. include:: ./arg_docstrings/year_understat.rst
        :type year: str
        :param league: .. include:: ./arg_docstrings/league.rst
        :type league: str
        :return: List of team links of the chosen league season
        :rtype: list[str]
        """
        _, teams_data, _ = self.scrape_season_data(year, league)
        if isinstance(teams_data, dict):
            return [
                f'https://understat.com/team/{x["title"].replace(" ", "_")}/{year.split("/")[0]}'
                for x in teams_data.values()
                if isinstance(x, dict) and 'title' in x
            ]
        return []

    # ==============================================================================================
    def scrape_season_data(self, year: str, league: str) -> tuple[dict, dict, dict]:
        """ Scrapes data for chosen Understat league season.

        :param year: .. include:: ./arg_docstrings/year_understat.rst
        :type year: str
        :param league: .. include:: ./arg_docstrings/league.rst
        :type league: str
        :return: Tuple of (matches_data, teams_data, players_data)
        :rtype: tuple[dict, dict, dict]
        """
        # Извлекаем URL-slug из comps (например 'EPL' из 'https://understat.com/league/EPL')
        league_slug = comps[league]["UNDERSTAT"].split('/')[-1]
        year_slug = year.split('/')[0] if '/' in year else year
        
        # Новый эндпоинт API
        data = self._fetch_api_data(f"getLeagueData/{league_slug}/{year_slug}")
        
        if not data:
            # Fallback to old method if API fails
            return self._scrape_season_data_legacy(year, league)
        
        # Новая структура: dates (list), teams (dict), players (list)
        matches_data = {str(m['id']): m for m in data.get('dates', [])} if isinstance(data.get('dates'), list) else data.get('dates', {})
        teams_data = data.get('teams', {})
        players_data = {str(p.get('id', i)): p for i, p in enumerate(data.get('players', []))} if isinstance(data.get('players'), list) else data.get('players', {})
        
        return matches_data, teams_data, players_data

    def _scrape_season_data_legacy(self, year: str, league: str) -> tuple[dict, dict, dict]:
        """Legacy method for backward compatibility with old HTML structure."""
        season_link = self.get_season_link(year, league)
        soup = BeautifulSoup(self.session.get(season_link).content, 'html.parser')
        scripts = soup.find_all('script')
        
        try:
            dates_data_tag = [x for x in scripts if 'datesData' in x.text][0]
            teams_data_tag = [x for x in scripts if 'teamsData' in x.text][0]
            players_data_tag = [x for x in scripts if 'playersData' in x.text][0]
            
            return (_json_from_script(dates_data_tag.text),
                    _json_from_script(teams_data_tag.text),
                    _json_from_script(players_data_tag.text))
        except (IndexError, KeyError):
            return {}, {}, {}

    # ==============================================================================================
    def scrape_league_tables(self, year: str, league: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """ Scrapes the league table for the chosen league season.

        :param year: .. include:: ./arg_docstrings/year_understat.rst
        :type year: str
        :param league: .. include:: ./arg_docstrings/league.rst
        :type league: str
        :return: Tuple of league table, home table, and away table DataFrames
        :rtype: tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]
        """
        _, teams_data, _ = self.scrape_season_data(year, league)

        if not teams_data:
            warnings.warn("No team data available")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

        df = pd.DataFrame()
        for x in teams_data.values():
            if not isinstance(x, dict) or 'history' not in x:
                continue
            # Create matches df for each team
            matches = pd.DataFrame.from_dict(x['history'])
            if matches.empty:
                continue
            newcols = list()
            for c in matches.columns:
                if isinstance(matches.loc[0, c], dict):
                    newcols.append(matches[c].apply(pd.Series).add_prefix(f'{c}_'))
                else:
                    newcols.append(matches[c])  # type: ignore
            matches = pd.concat(newcols, axis=1)
            matches['id'] = [x['id'],] * matches.shape[0]
            matches['title'] = [x['title'],] * matches.shape[0]
            df = pd.concat([df, matches], axis=0, ignore_index=True)

        if df.empty:
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

        # Rename columns to match Understat
        colmapping = {
            'title': 'Team', 'wins': 'W', 'draws': 'D', 'loses': 'L', 'scored': 'G', 'missed': 'GA',
            'pts': 'PTS', 'npxG': 'NPxG', 'npxGA': 'NPxGA', 'npxGD': 'NPxGD', 'deep': 'DC',
            'deep_allowed': 'ODC', 'xpts': 'xPTS',
        }
        df = df.rename(columns={k: v for k, v in colmapping.items() if k in df.columns})

        # Added matches played column
        df['M'] = df.get('W', 0) + df.get('D', 0) + df.get('L', 0)

        # Create initiial league, home, and away tables
        lg_tbl = df.groupby('Team', as_index=False).sum()\
            .sort_values('PTS', ascending=False).reset_index(drop=True)
        h_tbl = df[df['h_a'] == 'h'].groupby('Team', as_index=False).sum()\
            .sort_values('PTS', ascending=False).reset_index(drop=True) if 'h_a' in df.columns else lg_tbl.copy()
        a_tbl = df[df['h_a'] == 'a'].groupby('Team', as_index=False).sum()\
            .sort_values('PTS', ascending=False).reset_index(drop=True) if 'h_a' in df.columns else lg_tbl.copy()

        # Now compute PPDA columns
        for tbl in [lg_tbl, h_tbl, a_tbl]:
            if 'ppda_att' in tbl.columns and 'ppda_def' in tbl.columns and tbl['ppda_def'].sum() > 0:
                tbl['PPDA'] = tbl['ppda_att'] / tbl['ppda_def']
            if 'ppda_allowed_att' in tbl.columns and 'ppda_allowed_def' in tbl.columns and tbl['ppda_allowed_def'].sum() > 0:
                tbl['OPPDA'] = tbl['ppda_allowed_att'] / tbl['ppda_allowed_def']

        # Reorder columns to match Understat
        ordered_cols = [c for c in [
            'Team', 'M', 'W', 'D', 'L', 'G', 'GA', 'PTS', 'xG', 'NPxG', 'xGA', 'NPxGA', 'NPxGD',
            'PPDA', 'OPPDA', 'DC', 'ODC', 'xPTS'
        ] if c in lg_tbl.columns]
        
        lg_tbl = lg_tbl[ordered_cols]
        h_tbl = h_tbl[ordered_cols]
        a_tbl = a_tbl[ordered_cols]

        return lg_tbl, h_tbl, a_tbl

    # ==============================================================================================
    def scrape_match(self, link: str, as_df: bool = False) -> tuple[dict | pd.DataFrame, dict | pd.DataFrame, dict | pd.DataFrame]:
        """ Scrapes a single match from Understat.

        :param link: URL to the match
        :type link: str
        :param as_df: If True, will return the data as DataFrames. If False, data will be
            returned as dicts. Defaults to False.
        :type as_df: bool
        :raises TypeError: If any of the parameters are the wrong type
        :return: Tuple of (shots_data, match_info, rosters_data)
        :rtype: tuple[dict | pd.DataFrame, dict | pd.DataFrame, dict | pd.DataFrame]
        """
        if not isinstance(link, str):
            raise TypeError('`link` must be a string.')
        if not isinstance(as_df, bool):
            raise TypeError('`as_df` must be a boolean.')

        # Извлекаем ID матча из ссылки
        match_id = link.split('/match/')[-1].split('/')[0]
        
        # Новый API-эндпоинт для матчей
        data = self._fetch_api_data(f"getMatchData/{match_id}")
        
        if not data:
            warnings.warn(f"Could not fetch match data for {link}")
            return ({}, {}, {}) if not as_df else (pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
        
        shots_data = data.get('shots', {})
        match_info = data.get('tmpl', {})
        rosters_data = data.get('rosters', {})
        
        if as_df:
            # Handle shots_data
            if isinstance(shots_data, list):
                shots_data = pd.DataFrame(shots_data)
            elif isinstance(shots_data, dict):
                shots_list = []
                for team_key in ['h', 'a']:
                    if team_key in shots_data and isinstance(shots_data[team_key], list):
                        shots_list.extend(shots_data[team_key])
                shots_data = pd.DataFrame(shots_list) if shots_list else pd.DataFrame()

            # Handle rosters_data
            if isinstance(rosters_data, dict):
                roster_list = []
                for team_key in ['h', 'a']:
                    if team_key in rosters_data and isinstance(rosters_data[team_key], dict):
                        roster_list.extend(rosters_data[team_key].values())
                rosters_data = pd.DataFrame(roster_list) if roster_list else pd.DataFrame()
            
            # Handle match_info
            if match_info and isinstance(match_info, dict):
                match_info = pd.Series(match_info).to_frame().T
            else:
                match_info = pd.DataFrame()
        
        return shots_data, match_info, rosters_data

    # ==============================================================================================
    def scrape_matches(self, year: str, league: str, as_df: bool = False) -> dict:
        """ Scrapes all of the matches from the chosen league season.

        Gathers all match links from the chosen league season and then calls scrape_match() on each
        one.

        :param year: .. include:: ./arg_docstrings/year_understat.rst
        :type year: str
        :param league: .. include:: ./arg_docstrings/league.rst
        :type league: str
        :param as_df: If True, the data for each match will be returned as DataFrames. If False,
            individual match data will be returned as dicts. Defaults to False.
        :type as_df: bool
        :return: Dictionary of match data, where each key is a match link and the value is a dict
            of match data.
        :rtype: dict
        """
        links = self.get_match_links(year, league)

        matches = dict()
        for link in tqdm(links, desc=f'{year} {league} matches'):
            shots, info, rosters = self.scrape_match(link, as_df)
            matches[link] = {'shots_data': shots, 'match_info': info, 'rosters_data': rosters}

        return matches

    # ==============================================================================================
    def scrape_team_data(self, team_link: str, as_df: bool = False) -> tuple[dict | pd.DataFrame, dict | pd.DataFrame, dict | pd.DataFrame]:
        """ Scrapes team data from a team's Understat link

        Note that for Understat, team links are season-specific.

        :param team_link: URL to the team's Understat page
        :type team_link: str
        :param as_df: If True, data will be returned as dataframes. If False, dicts. Defaults
            to False.
        :type as_df: bool
        :return: Tuple of (matches_data, team_data, player_data)
        :raises TypeError: If any of the parameters are the wrong type
        :rtype: tuple[dict | pd.DataFrame, dict | pd.DataFrame, dict | pd.DataFrame]
        """
        if not isinstance(team_link, str):
            raise TypeError('`team_link` must be a string.')
        if not isinstance(as_df, bool):
            raise TypeError('`as_df` must be a boolean.')

        scripts = BeautifulSoup(self.session.get(team_link).content, 'html.parser').find_all('script')

        try:
            dates_data_tag = [x for x in scripts if 'datesData' in x.text][0]
            stats_data_tag = [x for x in scripts if 'statisticsData' in x.text][0]
            player_data_tag = [x for x in scripts if 'playersData' in x.text][0]

            matches_data = _json_from_script(dates_data_tag.text)
            team_data = _json_from_script(stats_data_tag.text)
            player_data = _json_from_script(player_data_tag.text)
        except (IndexError, KeyError):
            warnings.warn(f"Could not find team data in scripts for {team_link}")
            return ({}, {}, {}) if not as_df else (pd.DataFrame(), pd.DataFrame(), pd.DataFrame())

        if as_df:
            matches_data = pd.DataFrame.from_dict(matches_data)  # type: ignore
            if not matches_data.empty:
                newcols = list()
                for c in matches_data.columns:  # type: ignore
                    if isinstance(matches_data.loc[0, c], dict):  # type: ignore
                        newcols.append(matches_data[c].apply(pd.Series).add_prefix(f'{c}_'))
                    else:
                        newcols.append(matches_data[c])  # type: ignore
                matches_data = pd.concat(newcols, axis=1)

            for key, value in team_data.items():
                if not isinstance(value, dict):
                    continue
                table = list()
                for k, v in value.items():
                    if not isinstance(v, dict):
                        continue
                    # Drop against because it contains dicts
                    temp = pd.DataFrame.from_dict([v,]).drop(columns='against', errors='ignore')  # type: ignore
                    # Make the against dict into it's own DF and the concat it to temp
                    if 'against' in v and isinstance(v['against'], dict):
                        temp = pd.concat(
                            [
                                temp,
                                pd.DataFrame.from_dict([v['against'],]).add_suffix('_against')  # type: ignore
                            ],
                            axis=1
                        )
                    temp['stat'] = [k,]
                    table.append(temp)
                if table:
                    team_data[key] = pd.concat(table, axis=0, ignore_index=True)

            player_data = pd.DataFrame.from_dict(player_data)  # type: ignore

        return matches_data, team_data, player_data

    # ==============================================================================================
    def scrape_all_teams_data(self, year: str, league: str, as_df: bool = False) -> dict:
        """ Scrapes data for all teams in the given league season.

        :param year: .. include:: ./arg_docstrings/year_understat.rst
        :type year: str
        :param league: .. include:: ./arg_docstrings/league.rst
        :type league: str
        :param as_df: If True, each team's data will be returned as dataframes. If False,
            return dicts. Defaults to False.
        :type as_df: bool
        :return: Dictionary of team data, where each key is a team link and the value is a dict of
            team data.
        :rtype: dict
        """
        team_links = self.get_team_links(year, league)
        return_package = dict()
        for team_link in tqdm(team_links, desc=f'{year} {league} teams'):
            matches, team, players = self.scrape_team_data(team_link, as_df)
            return_package[team_link] = {
                'matches': matches, 'team_data': team, 'players_data': players
            }
        return return_package

    # ==============================================================================================
    def scrape_shot_xy(self, year: str, league: str, as_df: bool = False) -> None:
        """ Deprecated. Use `scrape_matches()` instead.
        """
        raise NotImplementedError(
            'Deprecated. This data is included in the output of `scrape_matches()` now.'
        )

    # ==============================================================================================
    def scrape_home_away_tables(self, year: str, league: str, normalize: bool = False) -> None:
        """ Deprecated. Use `scrape_league_tables()` instead.
        """
        raise NotImplementedError(
            'Deprecated. Home and away tables are output by `scrape_league_tables()` now.'
        )
