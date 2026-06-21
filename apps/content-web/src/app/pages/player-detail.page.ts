import { Component, inject } from '@angular/core';
import { AsyncPipe, Location, NgFor, NgIf } from '@angular/common';
import { ActivatedRoute, RouterLink } from '@angular/router';
import { map, switchMap } from 'rxjs/operators';
import { HeaderComponent } from '../components/header.component';
import { FooterComponent } from '../components/footer.component';
import { ContentService } from '../core/content.service';

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



                <div class="tournament-table-wrap">
                  <table class="tournament-table game-table">
                    <thead>
                      <tr>
                        <th scope="col">STT</th>
                        <th scope="col">Kỳ thủ đỏ</th>
                        <th scope="col">Kỳ thủ đen</th>
                        <th scope="col">Kết quả</th>
                        <th scope="col">Giải đấu</th>
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
                        <td>
                          <a class="tournament-table-link player-link" [routerLink]="['/tournaments', game.tournament_id]">{{ game.tournament_name }}</a>
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
export class PlayerDetailPage {
  private readonly route = inject(ActivatedRoute);
  private readonly location = inject(Location);
  private readonly content = inject(ContentService);

  readonly playerId$ = this.route.paramMap.pipe(map(params => params.get('id')));
  readonly player$ = this.playerId$.pipe(
    switchMap(playerId => this.content.getPlayerById(playerId))
  );
  readonly games$ = this.playerId$.pipe(
    switchMap(playerId => this.content.getGamesByPlayerId(playerId))
  );

  initials(name: string): string {
    return name.split(' ').map(part => part[0]).slice(0, 2).join('').toUpperCase();
  }

  goBack(): void {
    this.location.back();
  }
}
