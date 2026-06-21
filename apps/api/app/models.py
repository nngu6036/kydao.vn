from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field


class PlayerInitialLevel(str, Enum):
    A2_LEVEL = "a2_level"
    A1_LEVEL = "a1_level"
    NATIONAL_MASTER = "national_master"
    INTL_MASTER = "international_master"
    INTL_GMASTER = "international_grand_master"


class GameResult(str, Enum):
    WIN = "win"
    LOSE = "lose"
    DRAW = "draw"


class Player(BaseModel):
    model_config = {"use_enum_values": True}

    id: str
    created_date: datetime | None = None
    updated_date: datetime | None = None
    name: str
    url: str | None = None
    kydao_id: str | None = None
    title: str | None = None
    nationality: str | None = None
    location: str | None = None
    initial_level: PlayerInitialLevel | None = None
    year_of_birth: int | str | None = None
    yearOfBirth: int | str | None = None
    birth_year: int | str | None = None
    gender: str | None = None
    elo: float | None = None
    rating: int | None = None
    change: int | None = None
    win: int = 0
    draw: int = 0
    lose: int = 0
    active_games: int = 0
    starting_rating: int = 0


class Tournament(BaseModel):
    id: str
    created_date: datetime | None = None
    updated_date: datetime | None = None
    name: str
    url: str | None = None
    status: str | None = None
    date: str | None = None
    country: str | None = None
    location: str | None = None
    participants: int | None = None
    games: int | None = None
    elo_board: str | None = None
    eloBoard: str | None = None
    elo_weight: float | None = None
    eloWeight: float | None = None


class Game(BaseModel):
    model_config = {"use_enum_values": True}

    id: str
    created_date: datetime | None = None
    updated_date: datetime | None = None
    red_id: str | None = None
    red_player_id: str | None = None
    red_name: str | None = None
    black_id: str | None = None
    black_player_id: str | None = None
    black_name: str | None = None
    result: GameResult | str | None = None
    tournament_id: str | None = None
    event_id: str | None = None
    tournament_name: str | None = None
    opening_id: str | None = None
    opening: str | None = None
    date: str | None = None
    matchDate: str | None = None
    moves: int | None = None
    move_list: str | list[str] | None = None
    raw_move_list: str | None = None
    begin_fen: str | None = None
    start_color: str | None = None
    analyzed: bool = False
    eloExcluded: bool | None = None
    url: str | None = None
    parsed_date: datetime | None = None
    red_player_key: str | None = None
    black_player_key: str | None = None
    tournament_key: str | None = None
    red_score: float | None = None


class Page(BaseModel):
    items: list[dict]
    total: int
    skip: int = Field(ge=0)
    limit: int = Field(ge=1)
    page: int = Field(ge=1)
    page_size: int = Field(ge=1)
    pages: int = Field(ge=0)


class User(BaseModel):
    id: str
    created_date: datetime | None = None
    updated_date: datetime | None = None
    name: str
    username: str | None = None
    password: str | None = None
