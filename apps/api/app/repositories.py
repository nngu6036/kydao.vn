from __future__ import annotations

import base64
import re
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any
from urllib.parse import unquote

from bson import ObjectId
from pymongo import ReturnDocument
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.models import GameResult


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


def _public_doc(document: dict[str, Any]) -> dict[str, Any]:
    item = _stringify_object_ids(document)
    item["id"] = item.pop("_id")
    return item


def _object_id_filter(id: str) -> dict[str, Any]:
    filters: list[dict[str, Any]] = [{"_id": id}]
    if ObjectId.is_valid(id):
        filters.append({"_id": ObjectId(id)})
    return {"$or": filters}


def _text_filter(query: str, fields: Iterable[str]) -> dict[str, Any]:
    query = query.strip()
    if not query:
        return {}
    return {"$or": [{field: {"$regex": query, "$options": "i"}} for field in fields]}


def _sort_public_items(items: list[dict[str, Any]], sort_by: str, sort_dir: int) -> list[dict[str, Any]]:
    def sort_value(item: dict[str, Any]) -> tuple[int, Any]:
        value = item.get(sort_by)
        if value is None:
            return (1, "")
        if isinstance(value, str):
            return (0, value.casefold())
        return (0, value)

    return sorted(items, key=sort_value, reverse=sort_dir < 0)


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


def _utc_now() -> datetime:
    return datetime.now(UTC)


class MongoRepository:
    collection_name: str
    search_fields: tuple[str, ...] = ("name",)
    default_sort: tuple[str, int] | None = ("created_date", -1)

    def __init__(self, db: AsyncIOMotorDatabase) -> None:
        self.db = db

    @property
    def collection(self):
        return self.db[self.collection_name]

    async def list(
        self,
        *,
        query: str = "",
        skip: int = 0,
        limit: int = 50,
        sort_by: str | None = None,
        sort_dir: int = 1,
    ) -> tuple[list[dict[str, Any]], int]:
        filter_ = _text_filter(query, self.search_fields)
        cursor = self.collection.find(filter_)
        sort = (sort_by, sort_dir) if sort_by else self.default_sort
        if sort:
            cursor = cursor.sort(*sort)
        cursor = cursor.skip(skip).limit(limit)
        items = [_public_doc(document) async for document in cursor]
        total = await self.collection.count_documents(filter_)
        return items, total

    async def get(self, id: str) -> dict[str, Any] | None:
        document = await self.collection.find_one(_object_id_filter(id))
        return _public_doc(document) if document else None

    async def create(self, payload: dict[str, Any]) -> dict[str, Any]:
        payload = self._write_payload(payload)
        payload = self._create_payload(payload)
        result = await self.collection.insert_one(payload)
        return await self.get(str(result.inserted_id)) or {"id": str(result.inserted_id), **payload}

    async def update(self, id: str, payload: dict[str, Any]) -> dict[str, Any] | None:
        payload = self._write_payload(payload)
        document = await self._find_one_and_update(self.collection, id, payload)
        return _public_doc(document) if document else None

    async def delete(self, id: str) -> bool:
        result = await self.collection.delete_one(_object_id_filter(id))
        return result.deleted_count > 0

    def _write_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        return {key: value for key, value in payload.items() if key not in {"id", "_id"}}

    def _create_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        now = _utc_now()
        payload["created_date"] = now
        payload["updated_date"] = now
        return payload

    async def _find_one_and_update(self, collection, id: str, payload: dict[str, Any]) -> dict[str, Any] | None:
        current = await collection.find_one(_object_id_filter(id), {"_id": 1, "created_date": 1})
        if not current:
            return None

        update = self._update_document(payload, set_created_date=current.get("created_date") is None)
        return await collection.find_one_and_update(
            {"_id": current["_id"]},
            update,
            return_document=ReturnDocument.AFTER,
        )

    def _update_document(self, payload: dict[str, Any], *, set_created_date: bool = False) -> dict[str, Any]:
        now = _utc_now()
        update_payload = {key: value for key, value in payload.items() if key not in {"created_date", "updated_date"}}
        update_payload["updated_date"] = now
        if set_created_date:
            update_payload["created_date"] = now
        return {"$set": update_payload}


class PlayerRepository(MongoRepository):
    collection_name = "players"
    search_fields = ("name", "title", "location", "url")
    default_sort = ("name", 1)
    computed_sort_fields = ("kydao_id", "elo")

    async def list(
        self,
        *,
        query: str = "",
        skip: int = 0,
        limit: int = 50,
        sort_by: str | None = None,
        sort_dir: int = 1,
    ) -> tuple[list[dict[str, Any]], int]:
        if sort_by in self.computed_sort_fields:
            filter_ = _text_filter(query, self.search_fields)
            cursor = self.collection.find(filter_)
            items = [self._map_player(_public_doc(document)) async for document in cursor]
            total = len(items)
            items = _sort_public_items(items, sort_by, sort_dir)
            return items[skip : skip + limit], total

        items, total = await super().list(
            query=query,
            skip=skip,
            limit=limit,
            sort_by=sort_by,
            sort_dir=sort_dir,
        )
        return [self._map_player(item) for item in items], total

    async def list_elo_rankings(
        self,
        *,
        query: str = "",
        skip: int = 0,
        limit: int = 50,
    ) -> tuple[list[dict[str, Any]], int]:
        filter_ = self._vn_player_filter(query)
        cursor = self.collection.find(filter_)
        items = [self._map_player(_public_doc(document)) async for document in cursor]
        total = len(items)
        items = sorted(items, key=self._elo_ranking_sort_key)
        return items[skip : skip + limit], total

    async def get(self, id: str) -> dict[str, Any] | None:
        item = await super().get(id)
        return self._map_player(item) if item else None

    async def search_by_name(self, name: str, limit: int = 10) -> list[dict[str, Any]]:
        name = name.strip()
        if not name:
            return []

        safe_limit = min(max(limit, 1), 10)
        cursor = (
            self.collection.find({"name": {"$regex": re.escape(name), "$options": "i"}})
            .sort("name", 1)
            .limit(safe_limit)
        )
        return [self._map_player(_public_doc(document)) async for document in cursor]

    async def create(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._map_player(await super().create(payload))

    async def update(self, id: str, payload: dict[str, Any]) -> dict[str, Any] | None:
        should_update_game_names = "name" in payload
        item = await super().update(id, payload)
        if item and should_update_game_names:
            await self._update_game_player_names(item["id"], item["name"])
        return self._map_player(item) if item else None

    async def _update_game_player_names(self, player_id: str, name: str) -> None:
        id_values = _id_values(player_id)
        await self.db["games"].update_many(
            {"$or": [{"red_player_id": {"$in": id_values}}, {"red_id": {"$in": id_values}}]},
            {"$set": {"red_name": name, "updated_date": _utc_now()}},
        )
        await self.db["games"].update_many(
            {"$or": [{"black_player_id": {"$in": id_values}}, {"black_id": {"$in": id_values}}]},
            {"$set": {"black_name": name, "updated_date": _utc_now()}},
        )

    def _map_player(self, item: dict[str, Any]) -> dict[str, Any]:
        item["kydao_id"] = item.get("kydao_id") or self._kydao_id(item.get("url"))
        item["elo"] = item.get("elo") or item.get("rating")
        return item

    def _vn_player_filter(self, query: str) -> dict[str, Any]:
        filters: list[dict[str, Any]] = [{"nationality": "vn"}]
        text_filter = _text_filter(query, self.search_fields)
        if text_filter:
            filters.append(text_filter)
        return {"$and": filters}

    def _elo_ranking_sort_key(self, item: dict[str, Any]) -> tuple[int, float, str]:
        elo = item.get("elo")
        if not isinstance(elo, int | float):
            return (1, 0, str(item.get("name") or "").casefold())
        return (0, -float(elo), str(item.get("name") or "").casefold())

    def _kydao_id(self, url: str | None) -> str | None:
        if not url:
            return None
        parts = [part for part in unquote(url).split("/") if part]
        return parts[-1] if parts and parts[-1].isdigit() else None


class TournamentRepository(MongoRepository):
    collection_name = "tournaments"
    search_fields = ("name", "status", "country", "location")
    default_sort = ("date", -1)

    @property
    def fallback_collection(self):
        return self.db["events"]

    async def _uses_fallback(self) -> bool:
        return await self.collection.estimated_document_count() == 0

    async def list(
        self,
        *,
        query: str = "",
        skip: int = 0,
        limit: int = 50,
        sort_by: str | None = None,
        sort_dir: int = 1,
    ) -> tuple[list[dict[str, Any]], int]:
        if not await self._uses_fallback():
            return await super().list(
                query=query,
                skip=skip,
                limit=limit,
                sort_by=sort_by,
                sort_dir=sort_dir,
            )

        filter_ = _text_filter(query, ("name",))
        sort = (sort_by, sort_dir) if sort_by else self.default_sort
        cursor = self.fallback_collection.find(filter_)
        if sort:
            cursor = cursor.sort(*sort)
        cursor = cursor.skip(skip).limit(limit)
        items = [_public_doc(document) async for document in cursor]
        total = await self.fallback_collection.count_documents(filter_)
        return items, total

    async def get(self, id: str) -> dict[str, Any] | None:
        document = await self.collection.find_one(_object_id_filter(id))
        if document:
            return _public_doc(document)

        fallback_document = await self.fallback_collection.find_one(_object_id_filter(id))
        return _public_doc(fallback_document) if fallback_document else None

    async def search_by_name(self, name: str, limit: int = 10) -> list[dict[str, Any]]:
        name = name.strip()
        if not name:
            return []

        safe_limit = min(max(limit, 1), 10)
        collection = self.fallback_collection if await self._uses_fallback() else self.collection
        cursor = (
            collection.find({"name": {"$regex": re.escape(name), "$options": "i"}})
            .sort("name", 1)
            .limit(safe_limit)
        )
        return [_public_doc(document) async for document in cursor]

    async def update(self, id: str, payload: dict[str, Any]) -> dict[str, Any] | None:
        should_update_game_names = "name" in payload
        payload = self._write_payload(payload)
        document = await self._find_one_and_update(self.collection, id, payload)
        if document:
            item = _public_doc(document)
            if should_update_game_names:
                await self._update_game_tournament_names(item["id"], item["name"])
            return item

        fallback_document = await self._find_one_and_update(self.fallback_collection, id, payload)
        if not fallback_document:
            return None

        item = _public_doc(fallback_document)
        if should_update_game_names:
            await self._update_game_tournament_names(item["id"], item["name"])
        return item

    async def _update_game_tournament_names(self, tournament_id: str, name: str) -> None:
        id_values = _id_values(tournament_id)
        await self.db["games"].update_many(
            {"$or": [{"tournament_id": {"$in": id_values}}, {"event_id": {"$in": id_values}}]},
            {"$set": {"tournament_name": name, "updated_date": _utc_now()}},
        )

    async def delete(self, id: str) -> bool:
        result = await self.collection.delete_one(_object_id_filter(id))
        if result.deleted_count > 0:
            return True

        fallback_result = await self.fallback_collection.delete_one(_object_id_filter(id))
        return fallback_result.deleted_count > 0


class GameRepository(MongoRepository):
    collection_name = "games"
    search_fields = ("url", "result")
    computed_sort_fields = ("red_name", "black_name", "tournament_name", "moves")

    async def list(
        self,
        *,
        query: str = "",
        skip: int = 0,
        limit: int = 50,
        sort_by: str | None = None,
        sort_dir: int = 1,
    ) -> tuple[list[dict[str, Any]], int]:
        filter_ = _text_filter(query, self.search_fields)
        cursor = self.collection.find(filter_)
        if sort_by in self.computed_sort_fields:
            documents = [document async for document in cursor]
            items = await self._enrich_many(documents)
            total = len(items)
            items = _sort_public_items(items, sort_by, sort_dir)
            return items[skip : skip + limit], total

        if sort_by:
            cursor = cursor.sort(sort_by, sort_dir)
        cursor = cursor.skip(skip).limit(limit)
        documents = [document async for document in cursor]
        total = await self.collection.count_documents(filter_)
        return await self._enrich_many(documents), total

    async def list_by_tournament(
        self,
        tournament_id: str,
        *,
        skip: int = 0,
        limit: int = 50,
    ) -> tuple[list[dict[str, Any]], int]:
        id_values = _id_values(tournament_id)
        filter_ = {"$or": [{"tournament_id": {"$in": id_values}}, {"event_id": {"$in": id_values}}]}
        return await self._list_by_filter(filter_, skip=skip, limit=limit)

    async def list_by_player(
        self,
        player_id: str,
        *,
        skip: int = 0,
        limit: int = 50,
    ) -> tuple[list[dict[str, Any]], int]:
        id_values = _id_values(player_id)
        filter_ = {
            "$or": [
                {"red_player_id": {"$in": id_values}},
                {"red_id": {"$in": id_values}},
                {"black_player_id": {"$in": id_values}},
                {"black_id": {"$in": id_values}},
            ]
        }
        return await self._list_by_filter(filter_, skip=skip, limit=limit)

    async def _list_by_filter(
        self,
        filter_: dict[str, Any],
        *,
        skip: int,
        limit: int,
    ) -> tuple[list[dict[str, Any]], int]:
        cursor = self.collection.find(filter_).sort("date", -1).skip(skip).limit(limit)
        documents = [document async for document in cursor]
        total = await self.collection.count_documents(filter_)
        return await self._enrich_many(documents), total

    async def get(self, id: str) -> dict[str, Any] | None:
        document = await self.collection.find_one(_object_id_filter(id))
        if not document:
            return None
        return (await self._enrich_many([document]))[0]

    async def create(self, payload: dict[str, Any]) -> dict[str, Any]:
        payload = self._write_payload(payload)
        payload = self._create_payload(payload)
        self._coerce_relation_ids(payload)
        result = await self.collection.insert_one(payload)
        created = await self.get(str(result.inserted_id))
        return created or {"id": str(result.inserted_id), **_stringify_object_ids(payload)}

    async def update(self, id: str, payload: dict[str, Any]) -> dict[str, Any] | None:
        payload = self._write_payload(payload)
        self._coerce_relation_ids(payload)
        document = await self._find_one_and_update(self.collection, id, payload)
        if not document:
            return None
        return (await self._enrich_many([document]))[0]

    def _write_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        payload = super()._write_payload(payload)
        if "result" in payload and payload["result"] not in (None, ""):
            try:
                payload["result"] = GameResult(payload["result"]).value
            except ValueError as exc:
                allowed = ", ".join(result.value for result in GameResult)
                raise ValueError(f"result must be one of: {allowed}") from exc
        return payload

    def _coerce_relation_ids(self, payload: dict[str, Any]) -> None:
        for key in ("red_player_id", "black_player_id", "red_id", "black_id", "event_id", "tournament_id"):
            value = _as_object_id(payload.get(key))
            if value:
                payload[key] = value

    async def _enrich_many(self, documents: list[dict[str, Any]]) -> list[dict[str, Any]]:
        player_ids: set[ObjectId] = set()
        tournament_ids: set[ObjectId] = set()

        for document in documents:
            for key in ("red_player_id", "black_player_id", "red_id", "black_id"):
                value = _as_object_id(document.get(key))
                if value:
                    player_ids.add(value)
            tournament_id = _as_object_id(document.get("event_id") or document.get("tournament_id"))
            if tournament_id:
                tournament_ids.add(tournament_id)

        players = await self._lookup_by_ids("players", player_ids)
        tournaments = await self._lookup_by_ids("tournaments", tournament_ids)
        if not tournaments:
            tournaments = await self._lookup_by_ids("events", tournament_ids)

        return [self._map_game(document, players, tournaments) for document in documents]

    async def _lookup_by_ids(self, collection_name: str, ids: set[ObjectId]) -> dict[str, dict[str, Any]]:
        if not ids:
            return {}
        object_ids = list(ids)
        string_ids = [str(id) for id in object_ids]
        cursor = self.db[collection_name].find({"$or": [{"_id": {"$in": object_ids}}, {"_id": {"$in": string_ids}}]})

        documents: dict[str, dict[str, Any]] = {}
        async for document in cursor:
            id = _as_object_id(document.get("_id"))
            if id:
                documents[str(id)] = document
        return documents

    def _map_game(
        self,
        document: dict[str, Any],
        players: dict[str, dict[str, Any]],
        tournaments: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        item = _public_doc(document)
        red_id = item.pop("red_player_id", item.get("red_id", None))
        black_id = item.pop("black_player_id", item.get("black_id", None))
        tournament_id = item.pop("event_id", item.get("tournament_id", None))

        red = players.get(str(red_id), {})
        black = players.get(str(black_id), {})
        tournament = tournaments.get(str(tournament_id), {})

        item["red_id"] = str(red_id) if red_id else None
        item["red_name"] = item.get("red_name") or red.get("name")
        item["black_id"] = str(black_id) if black_id else None
        item["black_name"] = item.get("black_name") or black.get("name")
        item["tournament_id"] = str(tournament_id) if tournament_id else None
        item["tournament_name"] = item.get("tournament_name") or tournament.get("name")
        item["raw_move_list"] = item.get("move_list") if isinstance(item.get("move_list"), str) else None
        item["move_list"] = self._decode_move_list(item.get("move_list"), item.get("url"))
        item["moves"] = item.get("moves") or self._move_count(item.get("move_list"))
        item["analyzed"] = bool(item.get("analyzed", False))
        return item

    def _decode_move_list(self, move_list: str | list[str] | None, url: str | None) -> str | list[str] | None:
        if not isinstance(move_list, str) or not url:
            return move_list

        token = self._game_token(url)
        if not token or token not in move_list:
            return move_list

        encoded = move_list.replace(token, "")
        try:
            return base64.b64decode(encoded, validate=True).decode("utf-8")
        except (ValueError, UnicodeDecodeError):
            return move_list

    def _game_token(self, url: str) -> str | None:
        parts = [part for part in unquote(url).split("/") if part]
        if len(parts) < 2:
            return None
        return parts[-2]

    def _move_count(self, move_list: str | list[str] | None) -> int | None:
        if isinstance(move_list, list):
            return len(move_list)
        if isinstance(move_list, str):
            return len([move for move in move_list.split(",") if move.strip()])
        return None
