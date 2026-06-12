import { Component, inject } from '@angular/core';
import { AsyncPipe, DecimalPipe, NgFor } from '@angular/common';
import { RouterLink } from '@angular/router';
import { map } from 'rxjs/operators';
import { ContentService } from '../core/content.service';

@Component({
  selector: 'app-opening-block',
  template: `
    <section class="content-block opening-block">
      <div class="block-header">
        <div class="block-title-wrapper"><span class="block-icon">📖</span><h2 class="block-title">Thư Viện Khai Cuộc</h2></div>
        <a class="view-more-btn" routerLink="/openings">Xem thêm</a>
      </div>
      <div class="opening-list">
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
    </section>
  `
})
export class OpeningBlockComponent {
  private readonly content = inject(ContentService);
  readonly openings$ = this.content.openings$.pipe(map(items => items.slice(0, 3)));
}
