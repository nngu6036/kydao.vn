#!/usr/bin/env python3
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pymongo import MongoClient, ReturnDocument
from pymongo.errors import PyMongoError

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

PROGRESS_EVERY = 100


RESULT_MAP = {
    "WIN": "win",
    "LOSE": "lose",
    "LOSS": "lose",
    "DRAW": "draw",
    "win": "win",
    "lose": "lose",
    "loss": "lose",
    "draw": "draw",
}
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
    rating: int | None = None
    change: int | None = None


class Tournament(BaseModel):
    id: str
    created_date: datetime | None = None
    updated_date: datetime | None = None
    name: str
    url: str | None = None
    status: str | None = None
    date: str | None = None
    location: str | None = None
    participants: int | None = None


class Game(BaseModel):
    model_config = {"use_enum_values": True}

    id: str
    created_date: datetime | None = None
    updated_date: datetime | None = None
    red_id: str | None = None
    red_name: str | None = None
    black_id: str | None = None
    black_name: str | None = None
    result: GameResult | None = None
    tournament_id: str | None = None
    tournament_name: str | None = None
    opening_id: str | None = None
    opening: str | None = None
    date: str | None = None
    moves: int | None = None
    move_list: str | list[str] | None = None
    raw_move_list: str | None = None
    begin_fen: str | None = None
    start_color: str | None = None
    analyzed: bool = False
    url: str | None = None




def import_tournament(args) -> None:
    datafile = Path(args.datafile)
    if not datafile.is_file():
        raise FileNotFoundError(f"Data file not found: {datafile}")

    with datafile.open("r", encoding="utf-8") as file:
        records = json.load(file)

    if not isinstance(records, list):
        raise ValueError("Data file must contain a JSON array of game records")

    uri = args.mongo_uri
    dbname = os.environ.get("MONGO_DB", "kydao")
    client = MongoClient(uri)
    db = client[dbname]

    imported = 0
    skipped = 0
    total = len(records)
    tournament_cache: dict[str, dict[str, Any] | None] = {}
    for index, record in enumerate(records):
        if not isinstance(record, dict):
            skipped += 1
            logging.warning("Skipping non-object record at index %s", index)
            continue
        if not clean_string(record.get("tournamentName")):
            skipped += 1
            logging.info("Skipping record without tournamentName at index %s", index)
            continue

        try:
            upsert_tournament_record(db, record, tournament_cache)
            imported += 1
        except Exception as exc:
            skipped += 1
            logging.warning("Skipping record at index %s: %s", index, exc)
        log_progress("tournaments", index + 1, total, imported, skipped)

    logging.info("Finished tournaments import: imported %s, skipped %s, total %s", imported, skipped, total)


def import_player(args) -> None:
    datafile = Path(args.datafile)
    if not datafile.is_file():
        raise FileNotFoundError(f"Data file not found: {datafile}")

    with datafile.open("r", encoding="utf-8") as file:
        records = json.load(file)

    if not isinstance(records, list):
        raise ValueError("Data file must contain a JSON array of records")

    uri = args.mongo_uri
    dbname = os.environ.get("MONGO_DB", "kydao")
    client = MongoClient(uri)
    db = client[dbname]

    imported = 0
    skipped = 0
    total = len(records)
    player_cache: dict[str, dict[str, Any] | None] = {}
    for index, record in enumerate(records):
        if not isinstance(record, dict):
            skipped += 1
            logging.warning("Skipping non-object record at index %s", index)
            continue

        player_names = [
            clean_string(record.get("playerName") or record.get("player_name") or record.get("name")),
            clean_string(record.get("redPlayer") or record.get("red_player") or record.get("red_name")),
            clean_string(record.get("blackPlayer") or record.get("black_player") or record.get("black_name")),
        ]
        player_names = [name for name in player_names if name]
        if not player_names:
            skipped += 1
            logging.info("Skipping record without player name at index %s", index)
            continue

        try:
            for player_name in dict.fromkeys(player_names):
                player_record = {**record, "playerName": player_name}
                upsert_player_record(db, player_record, player_cache)
                imported += 1
        except Exception as exc:
            skipped += 1
            logging.warning("Skipping record at index %s: %s", index, exc)
        log_progress("players", index + 1, total, imported, skipped)

    logging.info("Finished players import: imported %s, skipped %s, total %s", imported, skipped, total)


def import_game(args) -> None:
    datafile = Path(args.datafile)
    if not datafile.is_file():
        raise FileNotFoundError(f"Data file not found: {datafile}")

    with datafile.open("r", encoding="utf-8") as file:
        records = json.load(file)

    if not isinstance(records, list):
        raise ValueError("Data file must contain a JSON array of game records")

    uri = args.mongo_uri
    dbname = os.environ.get("MONGO_DB", "kydao")
    client = MongoClient(uri)
    db = client[dbname]

    imported = 0
    skipped = 0
    total = len(records)
    tournament_cache: dict[str, dict[str, Any] | None] = {}
    player_cache: dict[str, dict[str, Any] | None] = {}
    for index, record in enumerate(records):
        if not isinstance(record, dict):
            skipped += 1
            logging.warning("Skipping non-object record at index %s", index)
            continue

        try:
            upsert_game_record(db, record, tournament_cache, player_cache)
            imported += 1
        except Exception as exc:
            skipped += 1
            logging.warning("Skipping record at index %s: %s", index, exc)
        log_progress("games", index + 1, total, imported, skipped)

    logging.info("Finished games import: imported %s, skipped %s, total %s", imported, skipped, total)


def parse_json(args) -> None:
    import_game(args)


def log_progress(kind: str, processed: int, total: int, imported: int, skipped: int) -> None:
    if processed != total and processed % PROGRESS_EVERY != 0:
        return

    percent = (processed / total * 100) if total else 100
    logging.info(
        "Importing %s: %s/%s processed (%.1f%%), imported=%s, skipped=%s",
        kind,
        processed,
        total,
        percent,
        imported,
        skipped,
    )


def upsert_tournament_record(
    db,
    record: dict[str, Any],
    tournament_cache: dict[str, dict[str, Any] | None] | None = None,
) -> None:
    tournament_name = clean_string(
        record.get("tournamentName")
        or record.get("tournament_name")
        or record.get("name")
    )
    if not tournament_name:
        raise ValueError("record must include tournamentName")

    now = datetime.now(timezone.utc)
    tournament = Tournament(
        id=clean_string(record.get("id")) or "",
        created_date=parse_datetime(record.get("createdDate") or record.get("created_date")) or now,
        updated_date=parse_datetime(record.get("updatedDate") or record.get("updated_date")) or now,
        name=tournament_name,
        url=clean_string(record.get("tournamentUrl") or record.get("url")),
        status=clean_string(record.get("status")),
        date=clean_string(record.get("date") or record.get("tournamentDate")),
        location=clean_string(record.get("location")),
        participants=parse_int(record.get("participants") or record.get("participantCount")),
    )

    if tournament_cache is not None and tournament.name in tournament_cache:
        logging.info("Tournament already exists, skipping: %s", tournament.name)
        return

    existing_tournament = db.tournaments.find_one({"name": tournament.name}, {"_id": 1, "name": 1})
    if existing_tournament:
        if tournament_cache is not None:
            tournament_cache[tournament.name] = existing_tournament
        logging.info("Tournament already exists, skipping: %s", tournament.name)
        return

    document = tournament.model_dump(exclude={"id"}, exclude_none=True)
    result = db.tournaments.insert_one(document)
    if tournament_cache is not None:
        tournament_cache[tournament.name] = {"_id": result.inserted_id, "name": tournament.name}
    logging.info("Imported tournament: %s", tournament.name)

def upsert_player_record(
    db,
    record: dict[str, Any],
    player_cache: dict[str, dict[str, Any] | None] | None = None,
) -> None:
    player_name = clean_string(
        record.get("playerName")
        or record.get("player_name")
        or record.get("name")
    )
    if not player_name:
        raise ValueError("record must include playerName")

    now = datetime.now(timezone.utc)
    player = Player(
        id=clean_string(record.get("id")) or "",
        created_date=parse_datetime(record.get("createdDate") or record.get("created_date")) or now,
        updated_date=parse_datetime(record.get("updatedDate") or record.get("updated_date")) or now,
        name=player_name,
        url=clean_string(record.get("playerUrl") or record.get("url")),
        kydao_id=clean_string(record.get("kydaoId") or record.get("kydao_id")),
        title=clean_string(record.get("title")),
        nationality=clean_string(record.get("Nationality") or record.get("nationality") or record.get("country")),
        location=clean_string(record.get("location")),
        initial_level=normalize_initial_level(record.get("initialLevel") or record.get("initial_level")),
        rating=parse_int(record.get("rating")),
        change=parse_int(record.get("change")),
    )

    if player_cache is not None and player.name in player_cache:
        logging.info("Player already exists, skipping: %s", player.name)
        return

    existing_player = db.players.find_one({"name": player.name}, {"_id": 1, "name": 1})
    if existing_player:
        if player_cache is not None:
            player_cache[player.name] = existing_player
        logging.info("Player already exists, skipping: %s", player.name)
        return

    document = player.model_dump(exclude={"id"}, exclude_none=True)
    result = db.players.insert_one(document)
    if player_cache is not None:
        player_cache[player.name] = {"_id": result.inserted_id, "name": player.name}
    logging.info("Imported player: %s", player.name)


def normalize_initial_level(value: Any) -> PlayerInitialLevel | None:
    value = clean_string(value)
    if value is None:
        return None
    try:
        return PlayerInitialLevel(value)
    except ValueError:
        logging.warning("Unknown player initial level ignored: %s", value)
        return None

def upsert_game_record(
    db,
    record: dict[str, Any],
    tournament_cache: dict[str, dict[str, Any] | None] | None = None,
    player_cache: dict[str, dict[str, Any] | None] | None = None,
) -> None:
    url = clean_string(record.get("url"))
    external_id = clean_string(record.get("externalId") or record.get("external_id") or record.get("key"))
    red_name = clean_string(record.get("redPlayer") or record.get("red_player") or record.get("red_name"))
    black_name = clean_string(record.get("blackPlayer") or record.get("black_player") or record.get("black_name"))

    if not url and not external_id:
        raise ValueError("record must include url or externalId")
    if not red_name or not black_name:
        raise ValueError("record must include redPlayer and blackPlayer")

    red_doc = upsert_player(db, red_name, player_cache)
    black_doc = upsert_player(db, black_name, player_cache)
    tournament_name = clean_string(
        record.get("tournamentName")
        or record.get("tournament_name")
        or record.get("event")
        or record.get("eventName")
    )
    tournament_doc = find_tournament_by_name(db, tournament_name, tournament_cache)
    now = datetime.now(timezone.utc)

    game_doc = {
        "red_player_id": red_doc["_id"],
        "red_name": red_name,
        "black_player_id": black_doc["_id"],
        "black_name": black_name,
        "tournament_id": tournament_doc["_id"] if tournament_doc else None,
        "tournament_name": tournament_doc["name"] if tournament_doc else tournament_name,
        "result": normalize_result(record.get("result")),
        "move_list": clean_string(record.get("strMoveList") or record.get("move_list")),
        "begin_fen": clean_string(record.get("beginFEN") or record.get("begin_fen")),
        "start_color": clean_string(record.get("startColor") or record.get("start_color")),
        "url": url,
        "external_id": external_id,
        "source": clean_string(record.get("source")),
        "crawl_status": clean_string(record.get("crawlStatus")),
        "crawl_error": record.get("crawlError"),
        "crawled_at": parse_datetime(record.get("crawledAt")),
        "key": clean_string(record.get("key")),
        "position": record.get("position"),
        "result_raw": clean_string(record.get("resultRaw")),
        "updated_date": now,
    }
    game_doc = {key: value for key, value in game_doc.items() if value is not None}

    insert_defaults = {"created_date": now, "analyzed": False}
    filter_ = {"url": url} if url else {"external_id": external_id}

    db.games.update_one(
        filter_,
        {
            "$set": game_doc,
            "$setOnInsert": insert_defaults,
        },
        upsert=True,
    )


def find_tournament_by_name(
    db,
    name: str | None,
    tournament_cache: dict[str, dict[str, Any] | None] | None = None,
) -> dict[str, Any] | None:
    if not name:
        return None
    if tournament_cache is not None and name in tournament_cache:
        return tournament_cache[name]

    tournament = db.tournaments.find_one({"name": name}, {"_id": 1, "name": 1})
    if tournament_cache is not None:
        tournament_cache[name] = tournament
    if not tournament:
        logging.info("Tournament not found for game, keeping name only: %s", name)
    return tournament


def upsert_player(
    db,
    name: str,
    player_cache: dict[str, dict[str, Any] | None] | None = None,
) -> dict[str, Any]:
    if player_cache is not None and name in player_cache:
        return player_cache[name]

    now = datetime.now(timezone.utc)
    player = db.players.find_one_and_update(
        {"name": name},
        {
            "$set": {"name": name, "updated_date": now},
            "$setOnInsert": {"created_date": now},
        },
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )
    if player_cache is not None:
        player_cache[name] = player
    return player


def clean_string(value: Any) -> str | None:
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def normalize_result(value: Any) -> str | None:
    value = clean_string(value)
    if value is None:
        return None
    return RESULT_MAP.get(value, RESULT_MAP.get(value.upper(), value.lower()))


def parse_datetime(value: Any) -> datetime | None:
    value = clean_string(value)
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def parse_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Import kydao JSON game records into MongoDB.")
    parser.add_argument(
        "--type",
        choices=("tournament", "player", "game"),
        default="game",
        help="Record type to import from the JSON file",
    )
    parser.add_argument(
        "--mongo-uri",
        default=os.environ.get("MONGO_URI", "mongodb://localhost:27017"),
        help="MongoDB connection URI. Defaults to MONGO_URI or mongodb://localhost:27017",
    )
    parser.add_argument("--datafile", required=True, help="JSON file containing an array of game records")
    args = parser.parse_args()

    if args.type == "tournament":
        import_tournament(args)
    elif args.type == "player":
        import_player(args)
    else:
        import_game(args)



if __name__ == "__main__":
    main()
