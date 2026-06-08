#!/usr/bin/env python3
from __future__ import annotations

import json
import logging
import os
import re
import base64
import binascii
from datetime import datetime, timezone
from typing import Any, Optional, Union

from bson import ObjectId
from pymongo import MongoClient

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
MongoDoc = dict[str, Any]
MOVE_PATTERN = re.compile(r"[A-Z].?\d.\d")
MOVE_FULL_PATTERN = re.compile(r"^[A-Z].?\d.\d$")

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

class GameResult(str, Enum):
    WIN = "win"
    LOSE = "lose"
    DRAW = "draw"


VALID_GAME_RESULTS = {result.value for result in GameResult}

class Game(BaseModel):
    model_config = {"use_enum_values": True}

    id: str
    created_date: Optional[datetime] = None
    updated_date: Optional[datetime] = None
    red_id: Optional[str] = None
    red_name: Optional[str] = None
    black_id: Optional[str] = None
    black_name: Optional[str] = None
    result: Optional[GameResult] = None
    tournament_id: Optional[str] = None
    tournament_name: Optional[str] = None
    opening_id: Optional[str] = None
    opening: Optional[str] = None
    date: Optional[str] = None
    moves: Optional[int] = None
    move_list: Optional[Union[str, list[str]]] = None
    raw_move_list: Optional[str] = None
    begin_fen: Optional[str] = None
    start_color: Optional[str] = None
    analyzed: bool = False
    url: Optional[str] = None


def normalize_game(args) -> None:
    uri = args.mongo_uri
    dbname = os.environ.get("MONGO_DB", "kydao")
    client = MongoClient(uri)
    db = client[dbname]

    invalid_records: list[MongoDoc] = []
    scanned = 0
    result_unchanged = 0
    result_fixed = 0
    move_list_unchanged = 0
    move_list_fixed = 0

    cursor = db.games.find({})
    for record in cursor:
        scanned += 1
        update_fields: MongoDoc = {}
        invalid_reasons: list[str] = []
        result = clean_string(record.get("result"))

        if result in VALID_GAME_RESULTS:
            result_unchanged += 1
        else:
            normalized_result = normalize_result(result)
            if normalized_result in VALID_GAME_RESULTS:
                update_fields["result"] = normalized_result
                result_fixed += 1
            else:
                invalid_reasons.append(f"invalid result: {result}")

        move_list = record.get("move_list")
        key = clean_string(record.get("key"))
        if isinstance(move_list, str):
            normalized_move_list, invalid_moves = normalize_move_list(move_list, key)
            if invalid_moves:
                invalid_reasons.append(f"invalid moves: {', '.join(invalid_moves)}")
            elif normalized_move_list is None:
                move_list_unchanged += 1
            else:
                update_fields["move_list"] = normalized_move_list
                move_list_fixed += 1
        else:
            move_list_unchanged += 1

        if invalid_reasons:
            invalid_record = public_json_record(record)
            invalid_record["normalize_errors"] = invalid_reasons
            invalid_records.append(invalid_record)

        if update_fields:
            update_fields["updated_date"] = datetime.now(timezone.utc)
            db.games.update_one({"_id": record["_id"]}, {"$set": update_fields})

    if invalid_records:
        with open(args.invalid_result_file, "w", encoding="utf-8") as file:
            json.dump(invalid_records, file, ensure_ascii=False, indent=2)

    logging.info(
        "Finished game normalization: scanned=%s result_unchanged=%s result_fixed=%s move_list_unchanged=%s move_list_fixed=%s invalid=%s invalid_file=%s",
        scanned,
        result_unchanged,
        result_fixed,
        move_list_unchanged,
        move_list_fixed,
        len(invalid_records),
        args.invalid_result_file if invalid_records else None,
    )


def normalize_move_list(move_list: str, key: Optional[str]) -> tuple[Optional[str], list[str]]:
    move_list = move_list.strip()
    if not move_list or "," in move_list:
        return None, []

    if key and key in move_list:
        encoded_move_list = move_list.replace(key, "")
        try:
            move_list = base64.b64decode(encoded_move_list, validate=True).decode("utf-8")
        except (binascii.Error, UnicodeDecodeError) as exc:
            return None, [f"base64 decode failed: {exc}"]

    moves = [match.group(0) for match in MOVE_PATTERN.finditer(move_list)]
    if not moves or "".join(moves) != move_list:
        return None, [move_list]

    invalid_moves = [move for move in moves if not MOVE_FULL_PATTERN.fullmatch(move)]
    if invalid_moves:
        return None, invalid_moves

    normalized_moves = [move.replace(".", "+").replace("C", "B") for move in moves]
    return ",".join(normalized_moves), []


def clean_string(value: Any) -> Optional[str]:
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def normalize_result(value: Any) -> Optional[str]:
    value = clean_string(value)
    if value is None:
        return None
    return RESULT_MAP.get(value, RESULT_MAP.get(value.upper()))


def public_json_record(record: MongoDoc) -> MongoDoc:
    return {key: json_safe(value) for key, value in record.items()}


def json_safe(value: Any) -> Any:
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, list):
        return [json_safe(item) for item in value]
    if isinstance(value, dict):
        return {key: json_safe(item) for key, item in value.items()}
    return value

def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Import kydao JSON game records into MongoDB.")
    parser.add_argument(
        "--mongo-uri",
        default=os.environ.get("MONGO_URI", "mongodb://localhost:27017"),
        help="MongoDB connection URI. Defaults to MONGO_URI or mongodb://localhost:27017",
    )
    parser.add_argument(
        "--invalid-result-file",
        default="invalid-game-results.json",
        help="JSON file where game records with unresolved result values are written",
    )

    args = parser.parse_args()
    normalize_game(args)


if __name__ == "__main__":
    main()
