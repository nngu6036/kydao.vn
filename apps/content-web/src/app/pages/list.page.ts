import { Component, inject } from '@angular/core';
import { AsyncPipe, DecimalPipe, Location, NgClass, NgFor, NgIf } from '@angular/common';
import { ActivatedRoute, RouterLink } from '@angular/router';
import { map } from 'rxjs/operators';
import { FooterComponent } from '../components/footer.component';
import { HeaderComponent } from '../components/header.component';
import { ContentService } from '../core/content.service';

@Component({
  template: `
    <div class="homepage">
      <app-header></app-header>
      <section class="search-center search-center--compact">
        <div class="search-container">
          <div class="page-with-back">
            <button class="back-link" type="button" (click)="goBack()" aria-label="Quay lại">← Quay lại</button>
            <div class="page-main">
              <h1 class="search-title">{{ title }}</h1>
              <div class="content-block list-page-block">
                <div class="list-page-list" *ngIf="kind === 'players'">
                  <article class="list-page-row player-list-row" *ngFor="let player of players$ | async" [routerLink]="['/players', player.id]">
                    <div class="list-page-main">
                      <a class="list-page-title player-link" [routerLink]="['/players', player.id]">{{ player.name }}</a>
                      <div class="list-page-meta">{{ player.title }} · {{ player.location }}</div>
                    </div>
                    <div class="list-page-side">
                      <div class="list-page-emphasis">{{ player.rating }}</div>
                      <div class="rating-change" [class.positive]="player.change > 0" [class.negative]="player.change < 0">
                        {{ player.change > 0 ? '▲' : player.change < 0 ? '▼' : '•' }} {{ player.change > 0 ? '+' : '' }}{{ player.change }}
                      </div>
                    </div>
                  </article>
                </div>

                <div class="game-list" *ngIf="kind === 'games'">
                  <article class="game-card" *ngFor="let game of games$ | async">
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
                        <span class="meta-item game-meta-tournament">🏆 <a class="entity-link" [routerLink]="['/tournaments', game.tournament_id]">{{ game.tournament_name }}</a></span>
                        <span class="meta-item game-meta-date">📅 {{ game.date }}</span>
                        <span class="meta-item game-meta-moves">{{ game.moves }} nước</span>
                        <span class="meta-item opening-tag game-meta-opening"><a class="entity-link opening-link" [routerLink]="['/openings', game.opening_id]">{{ game.opening }}</a></span>
                      </div>
                    </div>
                    <div class="game-actions">
                      <span *ngIf="game.analyzed" class="analyzed-badge">Đã phần tích</span>
                      <button class="play-btn" type="button" [routerLink]="['/games', game.id]">Xem</button>
                    </div>
                  </article>
                </div>

                <div class="list-page-list" *ngIf="kind === 'tournaments'">
                  <article class="tournament-card" *ngFor="let tournament of tournaments$ | async" [routerLink]="['/tournaments', tournament.id]">
                    <div class="tournament-info">
                      <div class="tournament-name">{{ tournament.name }}</div>
                      <div class="tournament-meta">
                        <span class="meta-item tournament-meta-date">📅 {{ tournament.date }}</span>
                        <span class="meta-item tournament-meta-location">📍 {{ tournament.location }}</span>
                      <span class="meta-item tournament-meta-participants">{{ tournament.participants }} kỳ thủ</span>
                      </div>
                    </div>
                    <span
                      class="status-badge"
                      [class.status-active]="tournament.status === 'Đang diễn ra'"
                      [class.status-upcoming]="tournament.status === 'Sắp diễn ra'"
                      [class.status-finished]="tournament.status === 'Đã kết thúc'"
                    >
                      {{ tournament.status }}
                    </span>
                  </article>
                </div>

                <div class="opening-list" *ngIf="kind === 'openings'">
                  <article class="opening-card" *ngFor="let opening of openings$ | async" [routerLink]="['/openings', opening.id]">
                    <div class="opening-info">
                      <div class="opening-name">{{ opening.name }}</div>
                      <div class="opening-stats">{{ opening.games | number }} ván cờ</div>
                    </div>
                    <div class="opening-winrate">
                      <div class="winrate-bars">
                        <div class="winrate-bar red" [style.width.%]="opening.winRate.red"></div>
                        <div class="winrate-bar draw" [style.width.%]="opening.winRate.draw"></div>
                        <div class="winrate-bar black" [style.width.%]="opening.winRate.black"></div>
                      </div>
                      <div class="winrate-labels">
                        <span class="label red">Đỏ {{ opening.winRate.red }}%</span>
                        <span class="label draw">Hòa {{ opening.winRate.draw }}%</span>
                        <span class="label black">Đen {{ opening.winRate.black }}%</span>
                      </div>
                    </div>
                  </article>
                </div>

                <div class="ranking-list" *ngIf="kind === 'rankings'">
                  <article class="ranking-row" *ngFor="let player of rankings$ | async" [routerLink]="['/players', player.player_id]">
                    <div class="rank-position">
                      <span class="rank-number" [class.top-three]="player.rank <= 3">{{ player.rank }}</span>
                      <span class="rank-change" [ngClass]="{up: player.change > 0, down: player.change < 0, same: player.change === 0}">
                        {{ player.change > 0 ? '▲' : player.change < 0 ? '▼' : '•' }}
                      </span>
                    </div>
                    <div class="rank-player">
                      <a class="rank-name player-link" [routerLink]="['/players', player.player_id]">{{ player.player_name }}</a>
                      <div class="rank-games">{{ player.games }} ván</div>
                      <div class="rank-rating">{{ player.rating }}</div>
                    </div>
                  </article>
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
export class ListPage {
  private readonly route = inject(ActivatedRoute);
  private readonly location = inject(Location);
  private readonly content = inject(ContentService);

  readonly kind = this.route.snapshot.data['kind'] as 'tournaments' | 'players' | 'games' | 'openings' | 'rankings';
  readonly title = this.route.snapshot.data['title'] || 'Danh sách';

  readonly tournaments$ = this.content.tournaments$.pipe(
    map(items => [...items].sort((a, b) => this.parseDisplayDate(b.date) - this.parseDisplayDate(a.date)))
  );
  readonly players$ = this.content.players$.pipe(
    map(items => [...items].sort((a, b) => a.name.localeCompare(b.name, 'vi', { sensitivity: 'base' })))
  );
  readonly games$ = this.content.games$.pipe(
    map(items => [...items].sort((a, b) => this.parseDisplayDate(a.date) - this.parseDisplayDate(b.date)))
  );
  readonly openings$ = this.content.openings$.pipe(
    map(items => [...items].sort((a, b) => b.games - a.games))
  );
  readonly rankings$ = this.content.rankings$;

  goBack(): void {
    this.location.back();
  }

  private parseDisplayDate(value: string): number {
    const firstPart = value.split('-')[0].trim();
    const [day, month, year] = firstPart.split('/').map(part => Number.parseInt(part, 10));
    return new Date(year, month - 1, day).getTime();
  }
}
