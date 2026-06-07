import { Component, inject } from '@angular/core';
import { AsyncPipe, DecimalPipe, Location, NgFor, NgIf } from '@angular/common';
import { ActivatedRoute, RouterLink } from '@angular/router';
import { map, switchMap } from 'rxjs/operators';
import { FooterComponent } from '../components/footer.component';
import { HeaderComponent } from '../components/header.component';
import { MockContentService } from '../core/mock-content.service';

@Component({
  template: `
    <div class="homepage">
      <app-header></app-header>
      <section class="search-center search-center--compact">
        <h1 class="search-title">Dữ liệu khai cuộc</h1>
        <div class="search-container" *ngIf="opening$ | async as opening">
          <div class="page-with-back">
            <button class="back-link" type="button" (click)="goBack()" aria-label="Quay lại">← Quay lại</button>

            <div class="page-main">
              <div class="content-block detail-page-block">
                <div class="detail-page-header">
                  <div class="detail-page-avatar">📖</div>
                  <div>
                    <h4 class="detail-page-name">{{ opening.name }}</h4>
                    <p class="detail-page-subtitle">{{ opening.games | number }} ván cờ</p>
                  </div>
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



                <div class="game-list" *ngIf="games$ | async as games">
                  <article class="game-card" *ngFor="let game of games">
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
export class OpeningDetailPage {
  private readonly route = inject(ActivatedRoute);
  private readonly location = inject(Location);
  private readonly mockContent = inject(MockContentService);

  readonly opening$ = this.mockContent.getOpeningById(this.route.snapshot.paramMap.get('id'));
  readonly games$ = this.opening$.pipe(
    switchMap(opening => this.mockContent.getGamesByOpening(opening?.name ?? null)),
    map(items => [...items].sort((a, b) => this.parseDisplayDate(a.date) - this.parseDisplayDate(b.date)))
  );

  goBack(): void {
    this.location.back();
  }

  private parseDisplayDate(value: string): number {
    const firstPart = value.split('-')[0].trim();
    const [day, month, year] = firstPart.split('/').map(part => Number.parseInt(part, 10));
    return new Date(year, month - 1, day).getTime();
  }
}
