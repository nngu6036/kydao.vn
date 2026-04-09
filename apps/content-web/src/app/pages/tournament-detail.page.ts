import { Component, inject } from '@angular/core';
import { AsyncPipe, Location, NgFor, NgIf } from '@angular/common';
import { ActivatedRoute, RouterLink } from '@angular/router';
import { FooterComponent } from '../components/footer.component';
import { HeaderComponent } from '../components/header.component';
import { MockContentService } from '../core/mock-content.service';

@Component({
  standalone: true,
  imports: [NgFor, NgIf, AsyncPipe, RouterLink, HeaderComponent, FooterComponent],
  template: `
    <div class="homepage">
      <app-header></app-header>
      <section class="search-center search-center--compact">
        <div class="search-container" *ngIf="tournament$ | async as tournament">
          <div class="page-with-back">
            <button class="back-link" type="button" (click)="goBack()" aria-label="Go back">← Quay lại</button>

            <div class="page-main">
              <div class="content-block detail-page-block">
                <div class="detail-page-header">
                  <div class="detail-page-avatar">🏆</div>
                  <div>
                    <h4 class="detail-page-name">{{ tournament.name }}</h4>
                    <p class="detail-page-subtitle">
                      <span
                        class="status-badge"
                        [class.status-active]="tournament.status === 'Đang diễn ra'"
                        [class.status-upcoming]="tournament.status === 'Sắp diễn ra'"
                        [class.status-finished]="tournament.status === 'Đã kết thúc'"
                      >
                        {{ tournament.status }}
                      </span>
                    </p>
                  </div>
                </div>

                  <div class="tournament-info">
                    <div class="tournament-meta">
                      <span class="meta-item tournament-meta-date">📅 {{ tournament.date }}</span>
                      <span class="meta-item tournament-meta-location">📍 {{ tournament.location }}</span>
                      <span class="meta-item tournament-meta-participants">{{ tournament.participants }} kỳ thủ</span>
                    </div>
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
export class TournamentDetailPage {
  private readonly route = inject(ActivatedRoute);
  private readonly location = inject(Location);
  private readonly mockContent = inject(MockContentService);
  private readonly tournamentId = this.route.snapshot.paramMap.get('id');

  readonly tournament$ = this.mockContent.getTournamentById(this.tournamentId);
  readonly games$ = this.mockContent.getGamesByTournamentId(this.tournamentId);

  goBack(): void {
    this.location.back();
  }
}
