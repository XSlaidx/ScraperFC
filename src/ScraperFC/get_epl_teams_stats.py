import os
import sys
import pandas as pd
# Добавляем путь к ScraperFC
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ScraperFC import ScraperFC

def get_epl_teams():
    s = ScraperFC()
    print("Собираем статистику команд EPL за сезон 23/24...")
    
    # Прямой вызов метода сбора статистики команд через модуль sofascore
    # 23/24 соответствует 2023 году в ScraperFC
    df = s.sofascore.scrape_team_league_stats("23/24", "England Premier League")
    
    if not df.empty:
        print(f"\nДанные собраны. Всего команд: {len(df)}")
        print("\n--- ВСЕ ДОСТУПНЫЕ КОЛОНКИ (всего {}): ---".format(len(df.columns)))
        # Выводим колонки группами по 5 для читаемости
        cols = df.columns.tolist()
        for i in range(0, len(cols), 5):
            print(", ".join(cols[i:i+5]))
            
        print("\n--- ПРИМЕР ДАННЫХ (Первые 5 команд, избранные колонки): ---")
        # Выберем интересные колонки для предпросмотра
        sample_cols = ['teamName', 'goalsScored', 'goalsConceded', 'bigChances', 'bigChancesMissed', 'ballPossession', 'rating']
        # Проверим, все ли они есть в наличии
        actual_sample_cols = [c for c in sample_cols if c in df.columns]
        print(df[actual_sample_cols].head())
        
        # Сохраним в CSV для вас, если нужно будет скачать
        df.to_csv("EPL_23_24_Team_Stats.csv", index=False)
        print("\nПолные данные сохранены в файл: EPL_23_24_Team_Stats.csv")
    else:
        print("Не удалось получить данные.")

if __name__ == "__main__":
    get_epl_teams()
