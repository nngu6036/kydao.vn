import { Injectable, inject } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { map, shareReplay } from 'rxjs/operators';
import { type Observable } from 'rxjs';
import type { GameItem, OpeningItem, PlayerItem, RankingItem, TournamentItem } from '../models/content.models';
import { environment } from './environment';

export interface ContentData {
  tournaments: TournamentItem[];
  players: PlayerItem[];
  games: GameItem[];
  openings: OpeningItem[];
  rankings: RankingItem[];
}

interface PageResponse<T> {
  items: T[];
  total: number;
  skip: number;
  limit: number;
  page: number;
  page_size: number;
  pages: number;
}

type ApiPlayer = Partial<PlayerItem> & {
  id: string;
  name: string;
  elo?: number | null;
  win?: number | null;
  draw?: number | null;
  lose?: number | null;
};

type ApiTournament = Partial<TournamentItem> & {
  id: string;
  name: string;
};

type ApiGame = Partial<Omit<GameItem, 'move_list'>> & {
  id: string;
  move_list?: string | string[] | null;
};

@Injectable({ providedIn: 'root' })
export class ContentService {
  private readonly http = inject(HttpClient);
  private readonly baseUrl = environment.apiBaseUrl;

  readonly players$ = this.fetchPage<ApiPlayer>('players').pipe(
    map(items => items.map(item => this.toPlayer(item))),
    shareReplay(1)
  );

  readonly tournaments$ = this.fetchPage<ApiTournament>('tournaments').pipe(
    map(items => items.map(item => this.toTournament(item))),
    shareReplay(1)
  );

  readonly games$ = this.fetchPage<ApiGame>('games').pipe(
    map(items => items.map(item => this.toGame(item))),
    shareReplay(1)
  );

  readonly openings$ = this.games$.pipe(
    map(games => this.toOpenings(games)),
    shareReplay(1)
  );

  readonly rankings$ = this.fetchPage<ApiPlayer>('players/elo-rankings').pipe(
    map(items => this.toRankings(items)),
    shareReplay(1)
  );

  getTournamentById(id: string | null): Observable<TournamentItem | null> {
    if (!id) {
      return this.tournaments$.pipe(map(() => null));
    }
    return this.http.get<ApiTournament>(`${this.baseUrl}/tournaments/${id}`).pipe(
      map(item => this.toTournament(item))
    );
  }

  getPlayerById(id: string | null): Observable<PlayerItem | null> {
    if (!id) {
      return this.players$.pipe(map(() => null));
    }
    return this.http.get<ApiPlayer>(`${this.baseUrl}/players/${id}`).pipe(
      map(item => this.toPlayer(item))
    );
  }

  getGameById(id: string | null): Observable<GameItem | null> {
    if (!id) {
      return this.games$.pipe(map(() => null));
    }
    return this.http.get<ApiGame>(`${this.baseUrl}/games/${id}`).pipe(
      map(item => this.toGame(item))
    );
  }

  getOpeningById(id: string | null): Observable<OpeningItem | null> {
    return this.openings$.pipe(map(items => items.find(item => item.id === id) ?? null));
  }

  getGamesByTournamentId(tournamentId: string | null): Observable<GameItem[]> {
    if (!tournamentId) {
      return this.games$.pipe(map(() => []));
    }
    return this.fetchGames(`tournaments/${tournamentId}/games`);
  }

  getGamesByPlayerId(playerId: string | null): Observable<GameItem[]> {
    if (!playerId) {
      return this.games$.pipe(map(() => []));
    }
    return this.fetchGames(`players/${playerId}/games`);
  }

  getGamesByOpening(openingName: string | null): Observable<GameItem[]> {
    return this.games$.pipe(
      map(items => items.filter(item => item.opening === openingName || item.opening_id === openingName))
    );
  }

  private fetchPage<T>(kind: 'players' | 'players/elo-rankings' | 'tournaments' | 'games'): Observable<T[]> {
    return this.http.get<PageResponse<T>>(`${this.baseUrl}/${kind}`, {
      params: { page: 1, page_size: 200 },
    }).pipe(map(page => page.items));
  }

  private fetchGames(path: string): Observable<GameItem[]> {
    return this.http.get<PageResponse<ApiGame>>(`${this.baseUrl}/${path}`, {
      params: { page: 1, page_size: 200 },
    }).pipe(map(page => page.items.map(item => this.toGame(item))));
  }

  private toPlayer(item: ApiPlayer): PlayerItem {
    return {
      id: item.id,
      name: item.name,
      title: item.title ?? '',
      location: item.location ?? '',
      nationality: item.nationality ?? '',
      sexuality: item.sexuality ?? '',
      rating: item.elo ?? item.rating ?? 0,
      change: item.change ?? 0,
    };
  }

  private toTournament(item: ApiTournament): TournamentItem {
    return {
      id: item.id,
      name: item.name,
      status: item.status ?? '',
      date: item.date ?? '',
      location: item.location ?? '',
      participants: item.participants ?? 0,
    };
  }

  private toGame(item: ApiGame): GameItem {
    const moveList = Array.isArray(item.move_list)
      ? item.move_list.join(',')
      : item.move_list ?? '';
    const opening = item.opening ?? '';

    return {
      id: item.id,
      red_id: item.red_id ?? '',
      red_name: item.red_name ?? '',
      black_id: item.black_id ?? '',
      black_name: item.black_name ?? '',
      result: this.toDisplayResult(item.result ?? ''),
      tournament_id: item.tournament_id ?? '',
      tournament_name: item.tournament_name ?? '',
      opening_id: item.opening_id ?? this.toOpeningId(opening),
      date: item.date ?? '',
      moves: item.moves ?? this.countMoves(moveList),
      move_list: moveList,
      opening,
      analyzed: item.analyzed ?? false,
    };
  }

  private toOpenings(games: GameItem[]): OpeningItem[] {
    const grouped = new Map<string, { name: string; id: string; games: GameItem[] }>();

    for (const game of games) {
      if (!game.opening) {
        continue;
      }
      const id = game.opening_id || this.toOpeningId(game.opening);
      const group = grouped.get(id) ?? { id, name: game.opening, games: [] };
      group.games.push(game);
      grouped.set(id, group);
    }

    return [...grouped.values()].map(group => ({
      id: group.id,
      name: group.name,
      games: group.games.length,
      winRate: this.toWinRate(group.games),
    }));
  }

  private toRankings(players: ApiPlayer[]): RankingItem[] {
    return players
      .sort((a, b) => (b.elo ?? b.rating ?? 0) - (a.elo ?? a.rating ?? 0) || a.name.localeCompare(b.name, 'vi', { sensitivity: 'base' }))
      .map((player, index) => ({
        id: player.id,
        rank: index + 1,
        player_id: player.id,
        player_name: player.name,
        rating: player.elo ?? player.rating ?? 0,
        change: player.change ?? 0,
        games: this.playerGames(player),
      }));
  }

  private playerGames(player: ApiPlayer): number {
    return (player.win ?? 0) + (player.draw ?? 0) + (player.lose ?? 0);
  }

  private toWinRate(games: GameItem[]): OpeningItem['winRate'] {
    if (!games.length) {
      return { red: 0, draw: 0, black: 0 };
    }

    const counts = games.reduce(
      (acc, game) => {
        if (game.result === '1-0') {
          acc.red++;
        } else if (game.result === '0-1') {
          acc.black++;
        } else {
          acc.draw++;
        }
        return acc;
      },
      { red: 0, draw: 0, black: 0 }
    );

    return {
      red: Math.round((counts.red / games.length) * 100),
      draw: Math.round((counts.draw / games.length) * 100),
      black: Math.round((counts.black / games.length) * 100),
    };
  }

  private toDisplayResult(result: string): string {
    if (result === 'win') {
      return '1-0';
    }
    if (result === 'lose') {
      return '0-1';
    }
    if (result === 'draw') {
      return '1/2-1/2';
    }
    return result;
  }

  private countMoves(moveList: string): number {
    return moveList.split(',').map(move => move.trim()).filter(Boolean).length;
  }

  private toOpeningId(opening: string): string {
    return opening
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-|-$/g, '');
  }
}
