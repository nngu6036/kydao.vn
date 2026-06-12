import { AsyncPipe, NgFor } from '@angular/common';
import { Component, inject } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { ContentService } from '../core/content.service';

@Component({
  selector: 'app-search-center',
  template: `
    <section class="search-center">
      <div class="search-container">
        <h1 class="search-title">Khám Phá Cờ Tướng Việt Nam</h1>
        <div class="search-box">
          <input
            type="text"
            [(ngModel)]="query"
            placeholder="Tìm kiếm kỳ thủ, giải đấu, ván cờ, khai cuộc..."
            class="search-input"
            (keydown.enter)="submit()"
          />
          <button
            class="search-btn"
            type="button"
            (click)="submit()"
            [disabled]="!canSubmit"
            aria-label="Tìm kiếm"
          >
            <span class="search-btn-icon" aria-hidden="true">&#8981;</span>
          </button>
        </div>
        <div class="search-filters">
          <span class="filter-label">Bộ lọc:</span>
          <select class="filter-select" [(ngModel)]="playerFilter">
            <option value="">Kỳ thủ</option>
            <option *ngFor="let player of players$ | async" [value]="player.id">{{ player.name }}</option>
          </select>
          <select class="filter-select" [(ngModel)]="tournamentFilter">
            <option value="">Giải đấu</option>
            <option *ngFor="let tournament of tournaments$ | async" [value]="tournament.id">{{ tournament.name }}</option>
          </select>
          <select class="filter-select" [(ngModel)]="openingFilter">
            <option value="">Khai cuộc</option>
            <option *ngFor="let opening of openings$ | async" [value]="opening.id">{{ opening.name }}</option>
          </select>
        </div>
      </div>
    </section>
  `
})
export class SearchCenterComponent {
  private readonly content = inject(ContentService);

  query = '';
  playerFilter = '';
  tournamentFilter = '';
  openingFilter = '';

  readonly players$ = this.content.players$;
  readonly tournaments$ = this.content.tournaments$;
  readonly openings$ = this.content.openings$;

  constructor(private router: Router) {}

  get canSubmit(): boolean {
    return !!(
      this.query.trim() ||
      this.playerFilter ||
      this.tournamentFilter ||
      this.openingFilter
    );
  }

  submit(): void {
    if (!this.canSubmit) {
      return;
    }

    this.router.navigate(['/search'], {
      queryParams: {
        q: this.query || '',
        players: this.playerFilter || null,
        tournaments: this.tournamentFilter || null,
        openings: this.openingFilter || null,
      }
    });
  }
}
