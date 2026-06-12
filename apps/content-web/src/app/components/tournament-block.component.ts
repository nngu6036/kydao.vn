import { Component, inject } from '@angular/core';
import { AsyncPipe, NgFor } from '@angular/common';
import { RouterLink } from '@angular/router';
import { map } from 'rxjs/operators';
import { ContentService } from '../core/content.service';

@Component({
  selector: 'app-tournament-block',
  template: `
    <section class="content-block tournament-block">
      <div class="block-header">
        <div class="block-title-wrapper"><span class="block-icon">🏆</span><h2 class="block-title">Giải Đấu</h2></div>
        <a class="view-more-btn" routerLink="/tournaments">Xem thêm</a>
      </div>
      <div class="tournament-list">
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
    </section>
  `
})
export class TournamentBlockComponent {
  private readonly content = inject(ContentService);
  readonly tournaments$ = this.content.tournaments$.pipe(map(items => items.slice(0, 3)));
}
