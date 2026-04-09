import { AsyncPipe, Location, NgFor, NgIf } from '@angular/common';
import { Component, inject } from '@angular/core';
import { ActivatedRoute, RouterLink } from '@angular/router';
import { combineLatest } from 'rxjs';
import { map } from 'rxjs/operators';
import { FooterComponent } from '../components/footer.component';
import { HeaderComponent } from '../components/header.component';
import { MockContentService } from '../core/mock-content.service';
import type { GameItem } from '../models/content.models';

@Component({
  standalone: true,
  imports: [AsyncPipe, NgFor, NgIf, RouterLink, HeaderComponent, FooterComponent],
  template: `
    <div class="homepage">
      <app-header></app-header>
      <section class="search-center search-center--compact">
        <div class="search-container">
          <div class="page-with-back">
            <button class="back-link" type="button" (click)="goBack()" aria-label="Go back">← Quay lai</button>

            <div class="page-main">
              <h1 class="search-title">Kết quả tìm kiếm</h1>

              <div class="search-result-query" *ngIf="searchState$ | async as state">
                <p *ngIf="state.query">Tu khoa: <strong>{{ state.query }}</strong></p>
              </div>

              <div class="content-block list-page-block">
                <div class="list-page-list" *ngIf="results$ | async as games">
                  <p *ngIf="!games.length" class="search-empty">
                    Không tìm thấy ván cờ phù hợp với bộ lọc hiện tại.
                  </p>

                  <div *ngFor="let game of games" class="game-card">
                    <div class="game-main">
                      <div class="game-players">
                        <div class="player-row game-player-red">
                          <span class="player-color-indicator red"></span>
                          <a class="player-name player-link" [routerLink]="['/players', game.red_id]">{{ game.red_name }}</a>
                        </div>
                        <div class="game-result">{{ game.result }}</div>
                        <div class="player-row game-player-black">
                          <span class="player-color-indicator black"></span>
                          <a class="player-name player-link" [routerLink]="['/players', game.black_id]">{{ game.black_name }}</a>
                        </div>
                      </div>
                      <div class="game-meta">
                        <span class="meta-item game-meta-tournament">
                          <a class="entity-link" [routerLink]="['/tournaments', game.tournament_id]">{{ game.tournament_name }}</a>
                        </span>
                        <span class="meta-item game-meta-date">{{ game.date }}</span>
                        <span class="meta-item game-meta-moves">{{ game.moves }} nước</span>
                        <span class="meta-item opening-tag game-meta-opening">
                          <a class="entity-link opening-link" [routerLink]="['/openings', game.opening_id]">{{ game.opening }}</a>
                        </span>
                      </div>
                    </div>
                    <div class="game-actions">
                      <span *ngIf="game.analyzed" class="analyzed-badge">Đã phần tích</span>
                      <a class="play-btn" [routerLink]="['/games', game.id]">Xem</a>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </section>
      <app-footer></app-footer>
    </div>
  `
})
export class SearchPage {
  private readonly route = inject(ActivatedRoute);
  private readonly location = inject(Location);
  private readonly mockContent = inject(MockContentService);

  readonly searchState$ = this.route.queryParamMap.pipe(
    map(params => ({
      query: (params.get('q') || '').trim(),
      playerId: params.get('players') || '',
      tournamentId: params.get('tournaments') || '',
      openingId: params.get('openings') || '',
    }))
  );

  readonly results$ = combineLatest([this.mockContent.games$, this.searchState$]).pipe(
    map(([games, state]) => games.filter(game => this.matchesGame(game, state)))
  );

  goBack(): void {
    this.location.back();
  }

  private matchesGame(
    game: GameItem,
    state: { query: string; playerId: string; tournamentId: string; openingId: string }
  ): boolean {
    if (state.playerId && game.red_id !== state.playerId && game.black_id !== state.playerId) {
      return false;
    }

    if (state.tournamentId && game.tournament_id !== state.tournamentId) {
      return false;
    }

    if (state.openingId && game.opening_id !== state.openingId) {
      return false;
    }

    if (!state.query) {
      return true;
    }

    const keyword = state.query.toLocaleLowerCase();
    return [
      game.red_name,
      game.black_name,
      game.tournament_name,
      game.opening,
    ].some(value => value.toLocaleLowerCase().includes(keyword));
  }
}
