import yaml
import os

def update_comps():
    path = "D:/WebUI_Server/venv/Lib/site-packages/ScraperFC/comps.yaml"
    with open(path, 'r', encoding='utf-8') as f:
        comps = yaml.safe_load(f)

    # Dictionary of best matches between our names and FD codes
    fd_map = {
        "England Premier League": "E0",
        "England EFL Championship": "E1",
        "England League 1": "E2",
        "England League 2": "E3",
        "England Conference": "EC",
        "Scotland Premier League": "SC0",
        "Scotland Championship": "SC1",
        "Germany Bundesliga": "D1",
        "Germany 2.Bundesliga": "D2",
        "Italy Serie A": "I1",
        "Italy Serie B": "I2",
        "Spain La Liga": "SP1",
        "Spain La Liga 2": "SP2",
        "France Ligue 1": "F1",
        "France Ligue 2": "F2",
        "Netherlands Eredivisie": "N1",
        "Belgium Pro League": "B1",
        "Portugal Primeira Liga": "P1",
        "Turkey Super Lig": "T1",
        "Greece Super League": "G1",
        "Argentina Liga Profesional": "ARG",
        "Brazil Serie A": "BRA",
        "Chinese Super League": "CHN",
        "J1 League": "JPN",
        "Mexico Liga MX": "MEX",
        "Norway Eliteserien": "NOR",
        "Poland Ekstraklasa": "POL",
        "Romania Liga I": "RO",
        "Russia Premier League": "RUS",
        "South African Premiership": "SA",
        "Sweden Allsvenskan": "SWE",
        "Swiss Super League": "SWZ",
        "USA MLS": "USA"
    }

    count = 0
    for name, code in fd_map.items():
        if name in comps:
            comps[name]["FOOTBALL_DATA"] = code
            count += 1
        else:
            # Try fuzzy match if exact name not found
            from rapidfuzz import process, fuzz
            match = process.extractOne(name, list(comps.keys()), scorer=fuzz.token_set_ratio)
            if match and match[1] > 85:
                comps[match[0]]["FOOTBALL_DATA"] = code
                count += 1

    with open(path, 'w', encoding='utf-8') as f:
        yaml.dump(comps, f, allow_unicode=True, sort_keys=True)
    
    print(f"Updated {count} leagues with Football-Data codes.")

if __name__ == "__main__":
    update_comps()
