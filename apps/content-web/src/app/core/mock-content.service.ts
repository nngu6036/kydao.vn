import { Injectable, inject } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { map, shareReplay } from 'rxjs/operators';
import type { Observable } from 'rxjs';
import type { GameItem, OpeningItem, PlayerItem, RankingItem, TournamentItem } from '../models/content.models';

export interface MockContentData {
  tournaments: TournamentItem[];
  players: PlayerItem[];
  games: GameItem[];
  openings: OpeningItem[];
  rankings: RankingItem[];
}

@Injectable({ providedIn: 'root' })
export class MockContentService {
  private readonly http = inject(HttpClient);

  readonly data$ = this.http
    .get<MockContentData>('assets/mock/content.json')
    .pipe(shareReplay(1));

  readonly tournaments$ = this.data$.pipe(map(data => data.tournaments));
  readonly players$ = this.data$.pipe(map(data => data.players));
  readonly games$ = this.data$.pipe(map(data => data.games));
  readonly openings$ = this.data$.pipe(map(data => data.openings));
  readonly rankings$ = this.data$.pipe(map(data => data.rankings));

  getTournamentById(id: string | null): Observable<TournamentItem | null> {
    return this.tournaments$.pipe(map(items => items.find(item => item.id === id) ?? null));
  }

  getPlayerById(id: string | null): Observable<PlayerItem | null> {
    return this.players$.pipe(map(items => items.find(item => item.id === id) ?? null));
  }

  getGameById(id: string | null): Observable<GameItem | null> {
    return this.games$.pipe(map(items => items.find(item => item.id === id) ?? null));
  }

  getOpeningById(id: string | null): Observable<OpeningItem | null> {
    return this.openings$.pipe(map(items => items.find(item => item.id === id) ?? null));
  }

  getGamesByTournamentId(tournamentId: string | null): Observable<GameItem[]> {
    return this.games$.pipe(map(items => items.filter(item => item.tournament_id === tournamentId)));
  }

  getGamesByPlayerId(playerId: string | null): Observable<GameItem[]> {
    return this.games$.pipe(
      map(items => items.filter(item => item.red_id === playerId || item.black_id === playerId))
    );
  }

  getGamesByOpening(openingName: string | null): Observable<GameItem[]> {
    return this.games$.pipe(
      map(items => items.filter(item => item.opening === openingName))
    );
  }
}
