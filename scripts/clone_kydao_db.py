#!/usr/bin/env python3
"""
Fetch + parse kydao player page.

- Downloads HTML from a URL (handles unicode URLs, timeouts, retries)
- Parses game links inside: <div class="game"> ... <a href="..."> ... </a>
- Returns absolute game URLs + (optional) anchor text

Install:
  pip install requests beautifulsoup4

Run:
  python fetch_kydao_games.py "https://kydao.net/ky-thu/L%E1%BA%A1i%20L%C3%BD%20Huynh/1013"
"""

from __future__ import annotations

import sys
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Iterator
from urllib.parse import urljoin
import re
import os
import requests
from bs4 import BeautifulSoup
import logging
import base64

logging.basicConfig(level=logging.INFO)

@dataclass
class Player:
    name: str
    url: str
    id: Optional[str] = None

@dataclass
class Event:
    name: str
    id: Optional[str] = None

@dataclass
class PlayerGame:
    red_player: str
    black_player: str
    event: str
    url: str
    result: str
    id: Optional[str] = None
    red_player_id: Optional[str] = None
    black_player_id: Optional[str] = None
    event_id: Optional[str] = None
    move_list: Optional[List[str]] = None
    begin_fen: Optional[str] = None
    start_color: Optional[str] = None

@dataclass
class GameList:
    html: str
    url: str

players: Dict[str, Player] = {}
games: Set[tuple] = set()
events: Dict[str, Event] = {}
player_queue: List[Player] = []

def fetch_html(
    url: str,
    *,
    timeout: int = 20,
    retries: int = 3,
    backoff: float = 1.5,
) -> str:
    """
    Fetch HTML content from a URL with retries + sane headers.
    Raises requests.HTTPError on non-2xx after retries.
    """
    session = requests.Session()
    headers = {
        # A realistic UA helps avoid some basic bot blocks.
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-AU,en;q=0.9,vi;q=0.8",
        "Connection": "keep-alive",
    }

    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, headers=headers, timeout=timeout, allow_redirects=True)
            resp.raise_for_status()
            # Let requests infer encoding; fall back to apparent encoding if needed.
            if not resp.encoding:
                resp.encoding = resp.apparent_encoding
            return resp.text
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(backoff ** (attempt - 1))
            else:
                raise

    # Should never reach here
    raise RuntimeError(f"Failed to fetch {url}") from last_err

def save_game(game: PlayerGame, mongo_client=None):
    """
    Persist a PlayerGame and related Player/Event records to MongoDB.

    Uses MONGO_URI and MONGO_DB environment variables (defaults to mongodb://localhost:27017 and db 'kydao').
    If pymongo is not installed, the function logs a warning and returns without failing.
    """
    try:
        from pymongo import MongoClient, ReturnDocument
        from pymongo.errors import PyMongoError
    except Exception as e:
        logging.warning("pymongo not available; skipping saving game %s: %s", game.url, e)
        return

    uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
    dbname = os.environ.get("MONGO_DB", "kydao")
    client = mongo_client or MongoClient(uri)
    db = client[dbname]

    players_col = db.players
    events_col = db.events
    games_col = db.games

    try:
        # Upsert red player
        red_doc = players_col.find_one_and_update(
            {"name": game.red_player},
            {"$setOnInsert": {"name": game.red_player, "url": players.get(game.red_player).url if players.get(game.red_player) else ""}},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        game.red_player_id = str(red_doc["_id"])

        # Upsert black player
        black_doc = players_col.find_one_and_update(
            {"name": game.black_player},
            {"$setOnInsert": {"name": game.black_player, "url": players.get(game.black_player).url if players.get(game.black_player) else ""}},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        game.black_player_id = str(black_doc["_id"])

        # Upsert event
        event_doc = events_col.find_one_and_update(
            {"name": game.event},
            {"$setOnInsert": {"name": game.event}},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        game.event_id = str(event_doc["_id"])

        # Upsert game by URL
        game_doc = {
            "red_player_id": red_doc["_id"],
            "black_player_id": black_doc["_id"],
            "event_id": event_doc["_id"],
            "url": game.url,
            "result": game.result,
            "move_list": game.move_list,
            "begin_fen": game.begin_fen,
            "start_color": game.start_color,
        }
        res = games_col.update_one({"url": game.url}, {"$set": game_doc}, upsert=True)
        if res.upserted_id:
            game.id = str(res.upserted_id)
        else:
            existing = games_col.find_one({"url": game.url})
            if existing and existing.get("_id"):
                game.id = str(existing["_id"])

        # Update in-memory caches
        if game.red_player in players:
            players[game.red_player].id = game.red_player_id
        if game.black_player in players:
            players[game.black_player].id = game.black_player_id
        if game.event not in events:
            events[game.event] = Event(name=game.event, id=game.event_id)

        logging.info("Saved game %s", game.id)
    except PyMongoError as e:
        logging.warning("MongoDB error while saving game %s: %s", game.url, e)


def parse_game(html: str, base_url: str, game: PlayerGame) -> PlayerGame:
    """
    Parse all content inside frame elements with id="game".
    Returns the updated PlayerGame.
    """
    soup = BeautifulSoup(html, "html.parser")
    game_frame = soup.select_one("#game")
    if not game_frame or not game_frame.get("src"):
        raise ValueError(f"No #game iframe with src found on {base_url}")
    frame_url = game_frame.get("src").strip()
    frame_abs_url = urljoin(base_url, frame_url)
    frame_html = fetch_html(frame_abs_url)

    # strMoveList may be single- or double-quoted; allow both
    m = re.search(r"strMoveList\s*=\s*['\"]([^'\"]+)['\"]", frame_html)
    if m:
        moves_str = m.group(1).strip()
        nonce_match = re.search(r"StartBoard\('([^']+)'", frame_html)
        nonce  = nonce_match.group(1) if nonce_match else ""
        moves_str = moves_str.replace("", nonce)
        game.move_list = moves_str

    else:
        game.move_list = None

    m = re.search(r"beginFEN\s*=\s*['\"]([^'\"]+)['\"]", frame_html)
    if m:
        game.begin_fen = m.group(1)

    m = re.search(r"startColor\s*=\s*['\"]([^'\"]+)['\"]", frame_html)
    if m:
        game.start_color = m.group(1)
    save_game(game)
    return game

def parse_game_links(base_url: str) -> Iterator[PlayerGame]:
    """
    Parse all <a href="..."> links inside elements with class="game".
    Yields PlayerGame objects with parsed details.
    """
    html = fetch_html(base_url)
    soup = BeautifulSoup(html, "html.parser")
    for game_div in soup.select("div.game"):
        a_red = game_div.select_one("div.red > a")
        a_black = game_div.select_one("div.black > a")
        a_result = game_div.select_one("div.result > a")
        a_event = game_div.select_one("div.event > a")
        if not (a_red and a_black and a_result and a_event):
            logging.warning("Skipping malformed game entry on %s", base_url)
            continue

        red_player = a_red.get_text(" ", strip=True)
        black_player = a_black.get_text(" ", strip=True)
        result = a_result.get_text(" ", strip=True)
        game_url = a_result.get("href", "").strip()
        if not game_url:
            logging.warning("Game link missing href; skipping.")
            continue

        abs_game_url = urljoin(base_url, game_url)
        event = a_event.get_text(" ", strip=True)
        game = PlayerGame(red_player=red_player, black_player=black_player, event=event, url=abs_game_url, result=result)
        try:
            game_html = fetch_html(abs_game_url)
            parse_game(game_html, abs_game_url, game)
        except Exception as e:
            logging.warning("Failed to parse game %s: %s", abs_game_url, e)
        yield game

def parse_pagination_links(base_url: str, max_pages: int = 100) -> Iterator[GameList]:
    """
    Parse all pages using the pager with id="Content_pager_lblnext".
    Yields GameList objects (html + url). Stops at max_pages or when no next link found.
    """
    html = fetch_html(base_url)
    soup = BeautifulSoup(html, "html.parser")
    pages_fetched = 1
    yield GameList(html=html, url=base_url)
    seen = {base_url}
    while True:
        a = soup.select_one("#Content_pager_lblnext > a")
        if not a:
            break
        href = a.get("href", "").strip()
        if not href:
            break
        abs_url = urljoin(base_url, href)
        if abs_url in seen or pages_fetched >= max_pages:
            logging.info("Reached seen page or max_pages (%d). Stopping pagination.", max_pages)
            break
        try:
            html = fetch_html(abs_url)
        except Exception as e:
            logging.warning("Failed to fetch page %s: %s", abs_url, e)
            break
        yield GameList(html=html, url=abs_url)
        pages_fetched += 1
        seen.add(abs_url)
        soup = BeautifulSoup(html, "html.parser")


def parse_home_page(base_url: str) -> None:
    """
    Parse the homepage for player links and walk players to find their games.
    """
    html = fetch_html(base_url)
    soup = BeautifulSoup(html, "html.parser")
    for game_div in soup.select("div.game"):
        a_red = game_div.select_one("div.red > a")
        a_black = game_div.select_one("div.black > a")
        if not (a_red and a_black):
            logging.debug("Skipping malformed player entry on %s", base_url)
            continue
        red_player = a_red.get_text(" ", strip=True)
        black_player = a_black.get_text(" ", strip=True)
        red_url = a_red.get("href", "").strip()
        black_url = a_black.get("href", "").strip()
        if red_player not in players:
            logging.info("Add player %s", red_player)
            players[red_player] = Player(name=red_player, url=urljoin(base_url, red_url) if red_url else "")
            player_queue.append(players[red_player])
        if black_player not in players:
            logging.info("Add player %s", black_player)
            players[black_player] = Player(name=black_player, url=urljoin(base_url, black_url) if black_url else "")
            player_queue.append(players[black_player])

    while player_queue:
        player = player_queue.pop(0)
        logging.info("Fetching player games %s", player.name)
        if not player.url:
            continue
        for page in parse_pagination_links(player.url):
            games_on_page = parse_game_links(page.url)
            for g in games_on_page:
                game_id = (g.red_player, g.black_player, g.event, g.url)
                if game_id not in games:
                    games.add(game_id)
                    logging.info("Found game: %s vs %s Event: %s URL: %s", g.red_player, g.black_player, g.event, g.url)
                    try:
                        save_game(g)
                    except Exception as e:
                        logging.warning("Failed to save game %s: %s", g.url, e)
                if g.red_player not in players:
                    logging.info("Add player %s", g.red_player)
                    players[g.red_player] = Player(name=g.red_player, url="")
                    player_queue.append(players[g.red_player])
                if g.black_player not in players:
                    logging.info("Add player %s", g.black_player)
                    players[g.black_player] = Player(name=g.black_player, url="")
                    player_queue.append(players[g.black_player])
                if g.event not in events:
                    logging.info("Add event %s", g.event)
                    events[g.event] = Event(name=g.event)

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Fetch and parse kydao player pages.")
    parser.add_argument("url", nargs="?", default="https://kydao.net", help="Player page URL")
    parser.add_argument("--max-pages", type=int, default=100, help="Maximum pagination pages to fetch")
    args = parser.parse_args()
    parse_home_page(args.url)


if __name__ == "__main__":
    main()
