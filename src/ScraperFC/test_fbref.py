from ScraperFC import ScraperFC
import pandas as pd

def test_fbref():
    print("--- Тестирование FBref через Facade ---")
    sfc = ScraperFC(use_cache=True)
    
    try:
        # Тестируем загрузку статистики игроков АПЛ 2023/24
        # Используем "England Premier League" для точности
        print("Загрузка данных с FBref (может занять до 1 минуты, обход Cloudflare)...")
        df = sfc.get_league_stats("England Premier League", 2023, source="fbref", stat_type="standard")
        
        if df is not None and not df.empty:
            print(f"✅ Успешно! Загружено {len(df)} строк статистики игроков.")
            print("Первые 5 игроков:")
            print(df[['Player', 'Squad', 'MP', 'Gls', 'Ast']].head())
        else:
            print("❌ Ошибка: Получен пустой DataFrame.")
            
    except Exception as e:
        print(f"❌ Произошла ошибка при парсинге FBref: {e}")
    finally:
        sfc.close()

if __name__ == "__main__":
    test_fbref()
