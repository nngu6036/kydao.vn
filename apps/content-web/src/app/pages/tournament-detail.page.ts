import { Component, inject } from '@angular/core';
import { AsyncPipe, Location, NgFor, NgIf } from '@angular/common';
import { ActivatedRoute, RouterLink } from '@angular/router';
import { FooterComponent } from '../components/footer.component';
import { HeaderComponent } from '../components/header.component';
import { ContentService } from '../core/content.service';

@Component({
  template: `
    <div class="homepage">
      <app-header></app-header>
      <section class="search-center search-center--compact">
        <h1 class="search-title">Dữ liệu giải đấu</h1>
        <div class="search-container" *ngIf="tournament$ | async as tournament">
          <div class="page-with-back">
            <button class="back-link" type="button" (click)="goBack()" aria-label="Quay lại">← Quay lại</button>

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
                      <span class="meta-item tournament-meta-date">📅 <span class="meta-label">Ngày:</span> {{ tournament.date }}</span>
                      <span class="meta-item tournament-meta-location">📍 <span class="meta-label">Địa điểm:</span> {{ tournament.location }}</span>
                      <span class="meta-item tournament-meta-participants"><span class="meta-label">Kỳ thủ:</span> {{ tournament.participants }}</span>
                    </div>
                  </div>



                <div class="tournament-table-wrap">
                  <table class="tournament-table game-table">
                    <thead>
                      <tr>
                        <th scope="col">STT</th>
                        <th scope="col">Kỳ thủ đỏ</th>
                        <th scope="col">Kỳ thủ đen</th>
                        <th scope="col">Kết quả</th>
                        <th scope="col">Số nước</th>
                      </tr>
                    </thead>
                    <tbody>
                      <tr *ngFor="let game of games$ | async; let i = index">
                        <td>{{ i + 1 }}</td>
                        <td>
                          <a class="tournament-table-link player-link" [routerLink]="['/players', game.red_id]">{{ game.red_name }}</a>
                        </td>
                        <td>
                          <a class="tournament-table-link player-link" [routerLink]="['/players', game.black_id]">{{ game.black_name }}</a>
                        </td>
                        <td>
                          <a class="tournament-table-link player-link" [routerLink]="['/games', game.id]">{{ game.result }}</a>
                        </td>
                        <td>{{ game.moves }}</td>
                      </tr>
                    </tbody>
                  </table>
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
  private readonly content = inject(ContentService);
  private readonly tournamentId = this.route.snapshot.paramMap.get('id');

  readonly tournament$ = this.content.getTournamentById(this.tournamentId);
  readonly games$ = this.content.getGamesByTournamentId(this.tournamentId);

  goBack(): void {
    this.location.back();
  }
}
