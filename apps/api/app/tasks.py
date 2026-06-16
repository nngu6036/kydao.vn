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


logger = logging.getLogger("chess_elo.tasks")


INITIAL_ELO = 1500
MINIMUM_MATCHES = 9
FEMALE_ELO_OFFSET = 200
ACTIVE_PERIOD_DAYS = 360 * 2
INITIAL_LEVEL_ELO = {
    "international_grand_master": 2450,
    "international_master": 2350,
    "national_master": 2300,
    "a1_level": 2200,
    "a2_level": 1900,
}


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


def _initial_rating(player: dict[str, Any]) -> int:
    current_rating = player.get("rating")
    if isinstance(current_rating, int) and current_rating > 0:
        return current_rating

    initial_level = player.get("initial_level")
    if isinstance(initial_level, str):
        return INITIAL_LEVEL_ELO.get(initial_level, INITIAL_ELO)
    return INITIAL_ELO


def _birth_year(player: dict[str, Any]) -> int | None:
    for key in ("year_of_birth", "yearOfBirth", "birth_year"):
        value = player.get(key)
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
    return None


def _is_vn_player(player: dict[str, Any]) -> bool:
    nationality = player.get("nationality")
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


def _k_factor(player: dict[str, Any], game_date: datetime | None) -> int:
    birth_year = _birth_year(player)
    if game_date and birth_year and game_date.year - birth_year <= 18 and player["elo"] < 2300:
        return 40

    total_games = player["win"] + player["draw"] + player["lose"]
    if total_games <= 30:
        return 40
    if player["elo"] <= 2400:
        return 20
    return 10


def _tournament_weight(tournament: dict[str, Any] | None) -> float:
    if not tournament:
        return 1.0
    value = tournament.get("elo_weight") or tournament.get("eloWeight")
    if isinstance(value, int | float):
        return float(value)
    return 1.0


def _update_game_stats(red: dict[str, Any], black: dict[str, Any], score: float) -> None:
    if score == 1.0:
        red["win"] += 1
        black["lose"] += 1
    elif score == 0.5:
        red["draw"] += 1
        black["draw"] += 1
    else:
        red["lose"] += 1
        black["win"] += 1


async def _participant_count_for_tournament(db: AsyncIOMotorDatabase, tournament_id: Any) -> int:
    players: set[str] = set()
    cursor = db["games"].find(
        {"tournament_id": {"$in": _id_values(tournament_id)}},
        {"red_id": 1, "red_player_id": 1, "black_id": 1, "black_player_id": 1},
    )

    async for game in cursor:
        for key in ("red_player_id", "red_id", "black_player_id", "black_id"):
            player_id = _player_key(game.get(key))
            if player_id:
                players.add(player_id)

    return len(players)


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

        async for tournament in db["tournaments"].find({}, {"_id": 1}):
            checked += 1
            participants = await _participant_count_for_tournament(db, tournament["_id"])
            result = await db["tournaments"].update_one(
                {"_id": tournament["_id"]},
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


async def _load_players(db: AsyncIOMotorDatabase) -> dict[str, dict[str, Any]]:
    players: dict[str, dict[str, Any]] = {}
    async for player in db["players"].find({}):
        player_id = _player_key(player.get("_id"))
        if not player_id:
            continue
        player["id"] = player_id
        player["starting_rating"] = player.get("rating") if isinstance(player.get("rating"), int) else 0
        player["elo"] = float(_initial_rating(player))
        player["win"] = 0
        player["draw"] = 0
        player["lose"] = 0
        player["active_games"] = 0
        players[player_id] = player
    return players


async def _load_tournaments(db: AsyncIOMotorDatabase) -> dict[str, dict[str, Any]]:
    tournaments: dict[str, dict[str, Any]] = {}
    async for tournament in db["tournaments"].find({}):
        elo_board = tournament.get("eloBoard") or tournament.get("elo_board")
        if elo_board and elo_board != "M":
            continue
        tournament_id = _player_key(tournament.get("_id"))
        if tournament_id:
            tournaments[tournament_id] = tournament
    return tournaments


async def _load_elo_games(
    db: AsyncIOMotorDatabase,
    tournaments: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    tournament_ids = [ObjectId(id) if ObjectId.is_valid(id) else id for id in tournaments]
    tournament_ids.extend(tournaments.keys())
    games: list[dict[str, Any]] = []
    cursor = db["games"].find(
        {
            "tournament_id": {"$in": tournament_ids},
            "$or": [{"eloExcluded": {"$exists": False}}, {"eloExcluded": False}],
        }
    )

    async for game in cursor:
        game_date = _parse_game_date(game.get("date") or game.get("matchDate"))
        red_id = _player_key(game.get("red_player_id") or game.get("red_id"))
        black_id = _player_key(game.get("black_player_id") or game.get("black_id"))
        tournament_id = _player_key(game.get("tournament_id"))
        score = _red_score(game.get("result"))
        if not game_date or not red_id or not black_id or not tournament_id or score is None:
            continue
        game["parsed_date"] = game_date
        game["red_player_key"] = red_id
        game["black_player_key"] = black_id
        game["tournament_key"] = tournament_id
        game["red_score"] = score
        games.append(game)

    return sorted(games, key=lambda game: game["parsed_date"])


def _apply_elo_game(
    game: dict[str, Any],
    players: dict[str, dict[str, Any]],
    tournaments: dict[str, dict[str, Any]],
    now: datetime,
) -> None:
    red = players.get(game["red_player_key"])
    black = players.get(game["black_player_key"])
    if not red or not black:
        return

    score = game["red_score"]
    weight = _tournament_weight(tournaments.get(game["tournament_key"]))
    red_rating = red["elo"]
    black_rating = black["elo"]
    red["elo"] += _k_factor(red, game["parsed_date"]) * (score - _expected_score(red_rating, black_rating)) * weight
    black["elo"] += _k_factor(black, game["parsed_date"]) * (
        1 - score - _expected_score(black_rating, red_rating)
    ) * weight
    _update_game_stats(red, black, score)

    if (now.replace(tzinfo=None) - game["parsed_date"]).days <= ACTIVE_PERIOD_DAYS:
        red["active_games"] += 1
        black["active_games"] += 1


def _final_rating(player: dict[str, Any]) -> int:
    total_games = player["win"] + player["draw"] + player["lose"]
    if total_games < MINIMUM_MATCHES or player["active_games"] < MINIMUM_MATCHES:
        return 0

    rating = round(player["elo"])
    gender = player.get("gender")
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
        for player in sorted(players.values(), key=lambda item: -item["elo"]):
            if not _is_vn_player(player):
                continue

            checked += 1
            rating = _final_rating(player)
            result = await db["players"].update_one(
                {"_id": {"$in": _id_values(player["id"])}},
                {
                    "$set": {
                        "rating": rating,
                        "change": rating - int(player["starting_rating"] or 0),
                        "win": player["win"],
                        "draw": player["draw"],
                        "lose": player["lose"],
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


@celery_app.task(name="app.tasks.update_vn_player_elo")
def update_vn_player_elo() -> dict[str, int | str]:
    logger.info("Updating VN player Elo ratings")
    result = asyncio.run(_update_vn_player_elo())
    logger.info("Updated VN player Elo ratings: %s", result)
    return result
