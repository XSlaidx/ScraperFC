import yaml
from rapidfuzz import process, fuzz
from .botasaurus_getters import botasaurus_request_get_soup, botasaurus_request_get_json, botasaurus_browser_get_json
from bs4 import BeautifulSoup
import os
import time
import re

try:
    from seleniumbase import Driver
except ImportError:
    Driver = None

class MetaScraper:
    def __init__(self, comps_path="comps.yaml"):
        # Use absolute path if possible or keep as provided
        self.comps_path = comps_path
        if os.path.exists(comps_path):
            with open(comps_path, 'r', encoding='utf-8') as f:
                self.comps = yaml.safe_load(f) or {}
        else:
            self.comps = {}
        self.driver = None

    def _get_fbref_soup(self, url):
        """ Uses SeleniumBase to get FBref soup. """
        if not self.driver:
            if Driver is None:
                raise ImportError("seleniumbase is not installed.")
            self.driver = Driver(browser="chrome", uc=True, headless=True)
        self.driver.get(url)
        time.sleep(5) 
        return BeautifulSoup(self.driver.page_source, "html.parser")

    def save(self):
        with open(self.comps_path, 'w', encoding='utf-8') as f:
            yaml.dump(self.comps, f, allow_unicode=True, sort_keys=True)

    def scrape_fbref_leagues(self):
        url = "https://fbref.com/en/comps/"
        soup = self._get_fbref_soup(url)
        tables = soup.find_all("table")
        leagues = []
        for table in tables:
            rows = table.find("tbody").find_all("tr") if table.find("tbody") else []
            for row in rows:
                name_cell = row.find("td", {"data-stat": "league_name"}) or row.find("th", {"data-stat": "league_name"})
                if name_cell and name_cell.find("a"):
                    league_name = name_cell.text.strip()
                    href = name_cell.find("a")["href"]
                    if not href.startswith("/en/comps/"): continue
                    match = re.search(r'/en/comps/(\d+)/', href)
                    if match:
                        comp_id = match.group(1)
                        history_url = f"https://fbref.com/en/comps/{comp_id}/history/"
                        finder = href.split("/")[-1].replace("-Stats", "")
                        leagues.append({
                            "name": league_name,
                            "fbref": {"history url": history_url, "finders": [finder]}
                        })
        return leagues

    def scrape_sofascore_leagues(self):
        categories_url = "https://api.sofascore.com/api/v1/category"
        try:
            categories_data = botasaurus_browser_get_json(categories_url)
        except: return []
        
        all_tournaments = []
        if categories_data and "categories" in categories_data:
            for cat in categories_data["categories"]:
                cat_id = cat.get("id")
                cat_name = cat.get("name")
                tour_url = f"https://api.sofascore.com/api/v1/category/{cat_id}/unique-tournaments"
                try:
                    tour_data = botasaurus_browser_get_json(tour_url)
                    if tour_data and "groups" in tour_data:
                        for group in tour_data["groups"]:
                            for tour in group.get("uniqueTournaments", []):
                                all_tournaments.append({
                                    "name": f"{cat_name} {tour.get('name')}",
                                    "sofascore_id": tour.get("id")
                                })
                except: continue
        return all_tournaments

    def scrape_transfermarkt_leagues(self):
        continents = ["europa", "asien", "amerika", "afrika", "ozeanien"]
        all_leagues = []
        for cont in continents:
            url = f"https://www.transfermarkt.us/wettbewerbe/{cont}"
            try:
                soup = botasaurus_request_get_soup(url)
                table = soup.find("table", {"class": "items"})
                if table:
                    for row in table.find_all("tr", {"class": ["odd", "even"]}):
                        name_cell = row.find("td", {"class": "hauptlink"})
                        if name_cell and name_cell.find("a"):
                            league_name = name_cell.text.strip()
                            league_url = "https://www.transfermarkt.us" + name_cell.find("a")["href"]
                            all_leagues.append({"name": league_name, "transfermarkt": league_url})
            except: continue
        return all_leagues

    def scrape_understat_leagues(self):
        url = "https://understat.com/"
        try:
            soup = botasaurus_request_get_soup(url)
            leagues = []
            nav = soup.find("ul", {"class": "navbar-nav"})
            if nav:
                for link in nav.find_all("a", {"class": "desktop-link"}):
                    if "league" in link["href"]:
                        leagues.append({"name": link.text.strip(), "understat": "https://understat.com/" + link["href"].lstrip("/")})
            return leagues
        except: return []

    def update_leagues(self):
        # FBref
        try:
            print("Scraping FBref...")
            fbref_leagues = self.scrape_fbref_leagues()
            print(f"Found {len(fbref_leagues)} on FBref.")
            for lg in fbref_leagues:
                if lg["name"] not in self.comps: self.comps[lg["name"]] = {}
                self.comps[lg["name"]]["FBREF"] = lg["fbref"]
        except Exception as e: print(f"FBref error: {e}")
        finally:
            if self.driver: self.driver.quit()

        # Understat
        try:
            print("Scraping Understat...")
            for lg in self.scrape_understat_leagues():
                match = process.extractOne(lg["name"], list(self.comps.keys()), scorer=fuzz.token_set_ratio)
                if match and match[1] > 90: self.comps[match[0]]["UNDERSTAT"] = lg["understat"]
                else:
                    if lg["name"] not in self.comps: self.comps[lg["name"]] = {}
                    self.comps[lg["name"]]["UNDERSTAT"] = lg["understat"]
        except Exception as e: print(f"Understat error: {e}")

        # Transfermarkt
        try:
            print("Scraping Transfermarkt...")
            for lg in self.scrape_transfermarkt_leagues():
                match = process.extractOne(lg["name"], list(self.comps.keys()), scorer=fuzz.token_set_ratio)
                if match and match[1] > 85: self.comps[match[0]]["TRANSFERMARKT"] = lg["transfermarkt"]
                else:
                    if lg["name"] not in self.comps: self.comps[lg["name"]] = {}
                    self.comps[lg["name"]]["TRANSFERMARKT"] = lg["transfermarkt"]
        except Exception as e: print(f"TM error: {e}")

        # Sofascore
        try:
            print("Scraping Sofascore (slow)...")
            for lg in self.scrape_sofascore_leagues():
                match = process.extractOne(lg["name"], list(self.comps.keys()), scorer=fuzz.token_set_ratio)
                if match and match[1] > 85: self.comps[match[0]]["SOFASCORE"] = lg["sofascore_id"]
                else:
                    if lg["name"] not in self.comps: self.comps[lg["name"]] = {}
                    self.comps[lg["name"]]["SOFASCORE"] = lg["sofascore_id"]
        except Exception as e: print(f"Sofascore error: {e}")

        self.save()
        print(f"Done! Total leagues: {len(self.comps)}")

if __name__ == "__main__":
    import sys
    path = sys.argv[1] if len(sys.argv) > 1 else "D:/WebUI_Server/venv/Lib/site-packages/ScraperFC/comps.yaml"
    ms = MetaScraper(path)
    ms.update_leagues()
