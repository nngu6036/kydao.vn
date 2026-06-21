from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from math import pow
from typing import Any

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from app.celery_app import celery_app
from app.config import get_settings
from app.models import Game, Player, Tournament, PlayerInitialLevel


logger = logging.getLogger("chess_elo.tasks")


INITIAL_ELO = 1500
MINIMUM_MATCHES = 9
FEMALE_ELO_OFFSET = 200
ACTIVE_PERIOD_DAYS = 360 * 2
INITIAL_LEVEL_ELO = {
    PlayerInitialLevel.INTL_GMASTER.value: 2450,
    PlayerInitialLevel.INTL_MASTER.value: 2350,
    PlayerInitialLevel.NATIONAL_MASTER.value: 2300,
    PlayerInitialLevel.A1_LEVEL.value: 2200,
    PlayerInitialLevel.A2_LEVEL.value: 1900,
}


def _stringify_object_ids(value: Any) -> Any:
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, dict) and set(value.keys()) == {"$oid"}:
        return str(value["$oid"])
    if isinstance(value, list):
        return [_stringify_object_ids(item) for item in value]
    if isinstance(value, dict):
        return {key: _stringify_object_ids(item) for key, item in value.items()}
    return value


def _model_data(document: dict[str, Any]) -> dict[str, Any]:
    data = _stringify_object_ids(document)
    if "_id" in data:
        data["id"] = data.pop("_id")
    return data


def _player_from_document(document: dict[str, Any]) -> Player:
    return Player(**_model_data(document))


def _tournament_from_document(document: dict[str, Any]) -> Tournament:
    return Tournament(**_model_data(document))


def _game_from_document(document: dict[str, Any]) -> Game:
    return Game(**_model_data(document))


def _as_object_id(value: Any) -> ObjectId | None:
    if isinstance(value, ObjectId):
        return value
    if isinstance(value, dict):
        value = value.get("$oid")
    if isinstance(value, str) and ObjectId.is_valid(value):
        return ObjectId(value)
    return None


def _id_values(value: Any) -> list[Any]:
    object_id = _as_object_id(value)
    if object_id:
        return [object_id, str(object_id)]
    if isinstance(value, str):
        return [value]
    return [value]


def _player_key(value: Any) -> str | None:
    object_id = _as_object_id(value)
    if object_id:
        return str(object_id)
    if value in (None, ""):
        return None
    return str(value)


def _parse_game_date(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if not isinstance(value, str) or not value.strip():
        return None

    date_text = value.strip().split(" - ", 1)[0].strip()
    for date_format in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            parsed = datetime.strptime(date_text, date_format)
        except ValueError:
            continue
        if 2015 <= parsed.year <= 2050:
            return parsed
    return None


def _initial_rating(player: Player) -> int:
    current_rating = player.rating
    if isinstance(current_rating, int) and current_rating > 0:
        return current_rating

    initial_level = player.initial_level
    if isinstance(initial_level, str):
        return INITIAL_LEVEL_ELO.get(initial_level, INITIAL_ELO)
    return INITIAL_ELO


def _birth_year(player: Player) -> int | None:
    for value in (player.year_of_birth, player.yearOfBirth, player.birth_year):
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
    return None


def _is_vn_player(player: Player) -> bool:
    nationality = player.nationality
    return isinstance(nationality, str) and nationality.casefold() == "vn"


def _red_score(result: Any) -> float | None:
    if result in ("win", "W"):
        return 1.0
    if result in ("draw", "D"):
        return 0.5
    if result in ("lose", "L"):
        return 0.0
    return None


def _expected_score(player_rating: float, opponent_rating: float) -> float:
    return 1 / (1 + pow(10, (opponent_rating - player_rating) / 400))


def _k_factor(player: Player, game_date: datetime | None) -> int:
    birth_year = _birth_year(player)
    elo = float(player.elo or 0)
    if game_date and birth_year and game_date.year - birth_year <= 18 and elo < 2300:
        return 40

    total_games = player.win + player.draw + player.lose
    if total_games <= 30:
        return 40
    if elo <= 2400:
        return 20
    return 10


def _tournament_weight(tournament: Tournament | None) -> float:
    if not tournament:
        return 1.0
    value = tournament.elo_weight or tournament.eloWeight
    if isinstance(value, int | float):
        return float(value)
    return 1.0


def _update_game_stats(red: Player, black: Player, score: float) -> None:
    if score == 1.0:
        red.win += 1
        black.lose += 1
    elif score == 0.5:
        red.draw += 1
        black.draw += 1
    else:
        red.lose += 1
        black.win += 1


async def _participant_count_for_tournament(db: AsyncIOMotorDatabase, tournament_id: Any) -> int:
    players: set[str] = set()
    cursor = db["games"].find(
        {"tournament_id": {"$in": _id_values(tournament_id)}},
        {"red_id": 1, "red_player_id": 1, "black_id": 1, "black_player_id": 1},
    )

    async for document in cursor:
        game = _game_from_document(document)
        for player_id_value in (game.red_player_id, game.red_id, game.black_player_id, game.black_id):
            player_id = _player_key(player_id_value)
            if player_id:
                players.add(player_id)

    return len(players)


async def _game_count_for_tournament(db: AsyncIOMotorDatabase, tournament_id: Any) -> int:
    return await db["games"].count_documents({"tournament_id": {"$in": _id_values(tournament_id)}})


async def _update_tournament_participants() -> dict[str, int | str]:
    settings = get_settings()
    client = AsyncIOMotorClient(
        settings.mongodb_uri,
        appname=f"{settings.mongodb_app_name}-celery",
    )

    try:
        await client.admin.command("ping")
        db = client[settings.mongodb_database]
        updated = 0
        checked = 0
        now = datetime.now(UTC)

        async for document in db["tournaments"].find({}, {"_id": 1, "name": 1}):
            tournament = _tournament_from_document(document)
            checked += 1
            participants = await _participant_count_for_tournament(db, tournament.id)
            result = await db["tournaments"].update_one(
                {"_id": {"$in": _id_values(tournament.id)}},
                {"$set": {"participants": participants, "updated_date": now}},
            )
            updated += result.modified_count

        return {
            "status": "ok",
            "checked": checked,
            "updated": updated,
            "ran_at": now.isoformat(),
        }
    finally:
        client.close()


async def _update_tournament_games() -> dict[str, int | str]:
    settings = get_settings()
    client = AsyncIOMotorClient(
        settings.mongodb_uri,
        appname=f"{settings.mongodb_app_name}-celery",
    )

    try:
        await client.admin.command("ping")
        db = client[settings.mongodb_database]
        updated = 0
        checked = 0
        now = datetime.now(UTC)

        async for document in db["tournaments"].find({}, {"_id": 1, "name": 1}):
            tournament = _tournament_from_document(document)
            checked += 1
            games = await _game_count_for_tournament(db, tournament.id)
            result = await db["tournaments"].update_one(
                {"_id": {"$in": _id_values(tournament.id)}},
                {"$set": {"games": games, "updated_date": now}},
            )
            updated += result.modified_count

        return {
            "status": "ok",
            "checked": checked,
            "updated": updated,
            "ran_at": now.isoformat(),
        }
    finally:
        client.close()


async def _load_players(db: AsyncIOMotorDatabase) -> dict[str, Player]:
    players: dict[str, Player] = {}
    async for document in db["players"].find({}):
        player = _player_from_document(document)
        player_id = _player_key(player.id)
        if not player_id:
            continue
        player.starting_rating = player.rating if isinstance(player.rating, int) else 0
        player.elo = float(_initial_rating(player))
        player.win = 0
        player.draw = 0
        player.lose = 0
        player.active_games = 0
        players[player_id] = player
    return players


async def _load_tournaments(db: AsyncIOMotorDatabase) -> dict[str, Tournament]:
    tournaments: dict[str, Tournament] = {}
    async for document in db["tournaments"].find({}):
        tournament = _tournament_from_document(document)
        elo_board = tournament.eloBoard or tournament.elo_board
        if elo_board and elo_board != "M":
            continue
        tournament_id = _player_key(tournament.id)
        if tournament_id:
            tournaments[tournament_id] = tournament
    return tournaments


async def _load_elo_games(
    db: AsyncIOMotorDatabase,
    tournaments: dict[str, Tournament],
) -> list[Game]:
    tournament_ids = [ObjectId(id) if ObjectId.is_valid(id) else id for id in tournaments]
    tournament_ids.extend(tournaments.keys())
    games: list[Game] = []
    cursor = db["games"].find(
        {
            "tournament_id": {"$in": tournament_ids},
            "$or": [{"eloExcluded": {"$exists": False}}, {"eloExcluded": False}],
        }
    )

    async for document in cursor:
        game = _game_from_document(document)
        game_date = _parse_game_date(game.date or game.matchDate)
        red_id = _player_key(game.red_player_id or game.red_id)
        black_id = _player_key(game.black_player_id or game.black_id)
        tournament_id = _player_key(game.tournament_id)
        score = _red_score(game.result)
        if not game_date or not red_id or not black_id or not tournament_id or score is None:
            continue
        game.parsed_date = game_date
        game.red_player_key = red_id
        game.black_player_key = black_id
        game.tournament_key = tournament_id
        game.red_score = score
        games.append(game)

    return sorted(games, key=lambda game: game.parsed_date or datetime.min)


def _apply_elo_game(
    game: Game,
    players: dict[str, Player],
    tournaments: dict[str, Tournament],
    now: datetime,
) -> None:
    if not game.red_player_key or not game.black_player_key or not game.tournament_key:
        return

    red = players.get(game.red_player_key)
    black = players.get(game.black_player_key)
    if not red or not black or game.red_score is None:
        return

    score = game.red_score
    weight = _tournament_weight(tournaments.get(game.tournament_key))
    red_rating = float(red.elo or 0)
    black_rating = float(black.elo or 0)
    red.elo = red_rating + _k_factor(red, game.parsed_date) * (score - _expected_score(red_rating, black_rating)) * weight
    black.elo = black_rating + _k_factor(black, game.parsed_date) * (
        1 - score - _expected_score(black_rating, red_rating)
    ) * weight
    _update_game_stats(red, black, score)

    if game.parsed_date and (now.replace(tzinfo=None) - game.parsed_date).days <= ACTIVE_PERIOD_DAYS:
        red.active_games += 1
        black.active_games += 1


def _final_rating(player: Player) -> int:
    total_games = player.win + player.draw + player.lose
    if total_games < MINIMUM_MATCHES or player.active_games < MINIMUM_MATCHES:
        return 0

    rating = round(float(player.elo or 0))
    gender = player.gender
    if rating and isinstance(gender, str) and gender.casefold() == "f":
        rating -= FEMALE_ELO_OFFSET
    return max(0, rating)


async def _update_vn_player_elo() -> dict[str, int | str]:
    settings = get_settings()
    client = AsyncIOMotorClient(
        settings.mongodb_uri,
        appname=f"{settings.mongodb_app_name}-celery",
    )

    try:
        await client.admin.command("ping")
        db = client[settings.mongodb_database]
        players = await _load_players(db)
        tournaments = await _load_tournaments(db)
        games = await _load_elo_games(db, tournaments)
        now = datetime.now(UTC)

        for game in games:
            _apply_elo_game(game, players, tournaments, now)

        checked = 0
        updated = 0
        for player in sorted(players.values(), key=lambda item: -float(item.elo or 0)):
            if not _is_vn_player(player):
                continue

            checked += 1
            rating = _final_rating(player)
            result = await db["players"].update_one(
                {"_id": {"$in": _id_values(player.id)}},
                {
                    "$set": {
                        "elo": rating,
                        "rating": rating,
                        "change": rating - int(player.starting_rating or 0),
                        "win": player.win,
                        "draw": player.draw,
                        "lose": player.lose,
                        "updated_date": now,
                    }
                },
            )
            updated += result.modified_count

        return {
            "status": "ok",
            "players_checked": checked,
            "players_updated": updated,
            "games_processed": len(games),
            "ran_at": now.isoformat(),
        }
    finally:
        client.close()


@celery_app.task(name="app.tasks.update_tournament_participants")
def update_tournament_participants() -> dict[str, int | str]:
    logger.info("Updating tournament participant counts")
    result = asyncio.run(_update_tournament_participants())
    logger.info("Updated tournament participant counts: %s", result)
    return result


@celery_app.task(name="app.tasks.update_tournament_games")
def update_tournament_games() -> dict[str, int | str]:
    logger.info("Updating tournament game counts")
    result = asyncio.run(_update_tournament_games())
    logger.info("Updated tournament game counts: %s", result)
    return result


@celery_app.task(name="app.tasks.update_vn_player_elo")
def update_vn_player_elo() -> dict[str, int | str]:
    logger.info("Updating VN player Elo ratings")
    result = asyncio.run(_update_vn_player_elo())
    logger.info("Updated VN player Elo ratings: %s", result)
    return result
