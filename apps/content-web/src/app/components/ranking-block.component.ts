import { Component, inject } from '@angular/core';
import { AsyncPipe, NgClass, NgFor } from '@angular/common';
import { RouterLink } from '@angular/router';
import { map } from 'rxjs/operators';
import { MockContentService } from '../core/mock-content.service';

@Component({
  selector: 'app-ranking-block',
  standalone: true,
  imports: [NgFor, NgClass, AsyncPipe, RouterLink],
  template: `
    <section class="content-block ranking-block">
      <div class="block-header">
        <div class="block-title-wrapper"><span class="block-icon">🏅</span><h2 class="block-title">Bảng Xếp Hạng Quốc Gia</h2></div>
        <a class="view-more-btn" routerLink="/rankings">Xem thêm</a>
      </div>
      <div class="ranking-list">
        <div class="ranking-row" *ngFor="let player of rankings$ | async">
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
        </div>
      </div>
    </section>
  `
})
export class RankingBlockComponent {
  private readonly mockContent = inject(MockContentService);
  readonly rankings$ = this.mockContent.rankings$.pipe(map(items => items.slice(0, 7)));
}
