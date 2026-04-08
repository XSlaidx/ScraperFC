from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime

class PlayerStats(BaseModel):
    player_name: str
    team: str
    minutes_played: int
    goals: int = 0
    assists: int = 0
    expected_goals: Optional[float] = None
    expected_assists: Optional[float] = None
    rating: Optional[float] = None

class MatchStats(BaseModel):
    match_id: str
    date: datetime
    home_team: str
    away_team: str
    home_score: int
    away_score: int
    home_stats: Dict[str, Any]
    away_stats: Dict[str, Any]
    lineups: Optional[Dict[str, List[str]]] = None

class LeagueTableEntry(BaseModel):
    position: int
    team_name: str
    played: int
    won: int
    drawn: int
    lost: int
    goals_for: int
    goals_against: int
    points: int
    expected_goals_for: Optional[float] = None
    expected_goals_against: Optional[float] = None
