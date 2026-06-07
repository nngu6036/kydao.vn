import { Component, inject } from '@angular/core';
import { AsyncPipe, NgFor } from '@angular/common';
import { RouterLink } from '@angular/router';
import { map } from 'rxjs/operators';
import { MockContentService } from '../core/mock-content.service';

@Component({
  selector: 'app-player-block',
  template: `
    <section class="content-block player-block">
      <div class="block-header">
        <div class="block-title-wrapper"><span class="block-icon">👤</span><h2 class="block-title">Kỳ Thủ Nổi Bật</h2></div>
        <a routerLink="/players">Xem tất cả →</a>
      </div>
      <div class="player-list">
        <article class="player-card" *ngFor="let player of players$ | async" [routerLink]="['/players', player.id]">
          <div class="player-info">
            <div class="player-avatar">{{ initials(player.name) }}</div>
            <div class="player-details">
              <a class="player-name player-link" [routerLink]="['/players', player.id]">{{ player.name }}</a>
              <div class="player-title">{{ player.title }}</div>
              <div class="player-location">{{ player.location }}</div>
            </div>
          </div>
          <div class="player-stats">
            <div class="player-rating">{{ player.rating }}</div>
            <div class="rating-change" [class.positive]="player.change > 0" [class.negative]="player.change < 0">
              {{ player.change > 0 ? '▲' : player.change < 0 ? '▼' : '•' }} {{ player.change > 0 ? '+' : '' }}{{ player.change }}
            </div>
          </div>
        </article>
      </div>
    </section>
  `
})
export class PlayerBlockComponent {
  private readonly mockContent = inject(MockContentService);
  readonly players$ = this.mockContent.players$.pipe(map(items => items.slice(0, 4)));

  initials(name: string): string {
    return name.split(' ').map(part => part[0]).slice(0, 2).join('').toUpperCase();
  }
}
