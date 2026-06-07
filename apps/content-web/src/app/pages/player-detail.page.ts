import { Component, inject } from '@angular/core';
import { AsyncPipe, Location, NgFor, NgIf } from '@angular/common';
import { ActivatedRoute, RouterLink } from '@angular/router';
import { map, switchMap } from 'rxjs/operators';
import { HeaderComponent } from '../components/header.component';
import { FooterComponent } from '../components/footer.component';
import { MockContentService } from '../core/mock-content.service';

@Component({
  template: `
    <div class="homepage">
      <app-header></app-header>
      <section class="search-center search-center--compact">
        <h1 class="search-title">Dữ liệu kỳ thủ</h1>
        <div class="search-container" *ngIf="player$ | async as player">
          <div class="page-with-back">
            <button class="back-link" type="button" (click)="goBack()" aria-label="Quay lại">← Quay lại</button>

            <div class="page-main">
              <div class="content-block detail-page-block">
                <div class="detail-page-header">
                  <div class="detail-page-avatar">{{ initials(player.name) }}</div>
                  <div>
                    <h4 class="detail-page-name">{{ player.name }}</h4>
                    <p class="detail-page-subtitle">{{ player.title }} · {{ player.location }}</p>
                  </div>
                </div>
                <div class="detail-page-grid">
                  <div><strong>Hệ số ELO:</strong> {{ player.rating }}</div>
                </div>



                <div class="game-list">
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
                      <span *ngIf="game.analyzed" class="analyzed-badge">Đã phân tích</span>
                      <button class="play-btn" type="button" [routerLink]="['/games', game.id]">Xem</button>
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
export class PlayerDetailPage {
  private readonly route = inject(ActivatedRoute);
  private readonly location = inject(Location);
  private readonly mockContent = inject(MockContentService);

  readonly playerId$ = this.route.paramMap.pipe(map(params => params.get('id')));
  readonly player$ = this.playerId$.pipe(
    switchMap(playerId => this.mockContent.getPlayerById(playerId))
  );
  readonly games$ = this.playerId$.pipe(
    switchMap(playerId => this.mockContent.getGamesByPlayerId(playerId))
  );

  initials(name: string): string {
    return name.split(' ').map(part => part[0]).slice(0, 2).join('').toUpperCase();
  }

  goBack(): void {
    this.location.back();
  }
}
