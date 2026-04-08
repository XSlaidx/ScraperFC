from ScraperFC import ScraperFC
import pandas as pd

def test_everything():
    sfc = ScraperFC(use_cache=True)
    
    print("--- Тест 1: Football-Data (Результаты и Коэффициенты) ---")
    try:
        # Используем полное название для гарантии
        df_fd = sfc.get_league_stats("England Premier League", 2023, source="football-data")
        print(f"Успешно загружено {len(df_fd)} матчей из Football-Data")
        print("Колонки (первые 10):", df_fd.columns[:10].tolist())
        # Проверка режима clean_columns (он включен по умолчанию в facade)
        if "home_goals" in df_fd.columns:
            print("✅ Режим clean_columns работает (FTHG -> home_goals)")
    except Exception as e:
        print(f"❌ Ошибка в Football-Data: {e}")

    print("\n--- Тест 2: Проверка Кэширования ---")
    import time
    start = time.time()
    # Повторный запрос того же самого
    df_cached = sfc.get_league_stats("England Premier League", 2023, source="football-data")
    end = time.time()
    print(f"Время повторного запроса (из кэша): {end - start:.4f} сек")
    if (end - start) < 0.1:
        print("✅ Кэширование работает корректно (Cache HIT)")

    print("\n--- Тест 3: Understat (Fuzzy Matching) ---")
    try:
        # Пробуем неточное название "la liga"
        df_us = sfc.get_league_stats("la liga", 2023, source="understat")
        print(f"Успешно загружена таблица {len(df_us)} команд из Understat")
    except Exception as e:
        print(f"❌ Ошибка в Understat: {e}")

    sfc.close()
    print("\nВсе тесты завершены!")

if __name__ == "__main__":
    test_everything()
