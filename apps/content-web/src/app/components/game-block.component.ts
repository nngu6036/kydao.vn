import { Component, inject } from '@angular/core';
import { AsyncPipe, NgFor, NgIf } from '@angular/common';
import { RouterLink } from '@angular/router';
import { map } from 'rxjs/operators';
import { MockContentService } from '../core/mock-content.service';

@Component({
  selector: 'app-game-block',
  template: `
    <section class="content-block game-block">
      <div class="block-header">
        <div class="block-title-wrapper"><span class="block-icon">⚔</span><h2 class="block-title">Ván Đấu Mới Nhất</h2></div>
        <a class="view-more-btn" routerLink="/games">Xem thêm</a>
      </div>
      <div class="game-list">
        <article class="game-card" *ngFor="let game of games$ | async">
          <div class="game-main">
            <div class="game-players">
              <div class="player-row game-player-red"><span class="player-color-indicator red"></span><a class="player-name player-link" [routerLink]="['/players', game.red_id]">{{ game.red_name }}</a></div>
              <div class="game-result">{{ game.result }}</div>
              <div class="player-row game-player-black"><span class="player-color-indicator black"></span><a class="player-name player-link" [routerLink]="['/players', game.black_id]">{{ game.black_name }}</a></div>
            </div>
            <div class="game-meta">
              <span class="meta-item game-meta-tournament">🏆 <a class="entity-link" [routerLink]="['/tournaments', game.tournament_id]">{{ game.tournament_name }}</a></span>
              <span class="meta-item game-meta-date">📅 {{ game.date }}</span>
              <span class="meta-item game-meta-moves">{{ game.moves }} nước</span>
              <span class="meta-item opening-tag game-meta-opening"><a class="entity-link opening-link" [routerLink]="['/openings', game.opening_id]">{{ game.opening }}</a></span>
            </div>
          </div>
          <div class="game-actions">
            <span *ngIf="game.analyzed" class="analyzed-badge">Đã phân tích</span>
            <button class="play-btn" type="button" [routerLink]="['/games', game.id]">Xem</button>
          </div>
        </article>
      </div>
    </section>
  `
})
export class GameBlockComponent {
  private readonly mockContent = inject(MockContentService);
  readonly games$ = this.mockContent.games$.pipe(map(items => items.slice(0, 3)));
}
