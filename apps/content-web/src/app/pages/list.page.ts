import { Component, inject } from '@angular/core';
import { AsyncPipe, Location, NgClass, NgFor, NgIf } from '@angular/common';
import { ActivatedRoute, RouterLink } from '@angular/router';
import { BehaviorSubject, Observable, combineLatest } from 'rxjs';
import { map } from 'rxjs/operators';
import { FooterComponent } from '../components/footer.component';
import { HeaderComponent } from '../components/header.component';
import { ContentService } from '../core/content.service';
import type { GameItem, OpeningItem, PlayerItem, RankingItem, TournamentItem } from '../models/content.models';

interface PagedList<T> {
  items: T[];
  total: number;
  page: number;
  pages: number;
}

interface OpeningTreeNode {
  opening: OpeningItem;
  depth: number;
}

@Component({
  template: `
    <div class="homepage">
      <app-header></app-header>
      <section class="search-center search-center--compact">
        <div class="search-container">
          <div class="page-with-back">
            <button class="back-link" type="button" (click)="goBack()" aria-label="Quay lại">← Quay lại</button>
            <div class="page-main">
              <h1 class="search-title">{{ title }}</h1>
              <div class="content-block list-page-block">
                <div class="player-table-section" *ngIf="kind === 'players' && playersPage$ | async as page">
                  <div class="list-filter" role="radiogroup" aria-label="Lọc kỳ thủ theo quốc tịch">
                    <label>
                      <input
                        type="radio"
                        name="playerNationality"
                        value="all"
                        [checked]="playerNationalityFilter === 'all'"
                        (change)="setPlayerNationalityFilter('all')"
                      />
                      <span>Tất cả</span>
                    </label>
                    <label>
                      <input
                        type="radio"
                        name="playerNationality"
                        value="vn"
                        [checked]="playerNationalityFilter === 'vn'"
                        (change)="setPlayerNationalityFilter('vn')"
                      />
                      <span>Việt Nam</span>
                    </label>
                    <label>
                      <input
                        type="radio"
                        name="playerNationality"
                        value="non-vn"
                        [checked]="playerNationalityFilter === 'non-vn'"
                        (change)="setPlayerNationalityFilter('non-vn')"
                      />
                      <span>ngoài Việt Nam</span>
                    </label>
                  </div>

                  <div class="tournament-table-wrap">
                    <table class="tournament-table">
                      <thead>
                        <tr>
                          <th scope="col">STT</th>
                          <th scope="col">Tên</th>
                          <th scope="col">Giới tính</th>
                        </tr>
                      </thead>
                      <tbody>
                        <tr *ngFor="let player of page.items; let i = index">
                          <td>{{ (page.page - 1) * pageSize + i + 1 }}</td>
                          <td>
                            <a class="tournament-table-link player-link" [routerLink]="['/players', player.id]">{{ player.name }}</a>
                          </td>
                          <td>{{ displaySexuality(player.sexuality) }}</td>
                        </tr>
                      </tbody>
                    </table>
                  </div>
                </div>

                <div class="tournament-table-wrap" *ngIf="kind === 'games' && gamesPage$ | async as page">
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
                      <tr *ngFor="let game of page.items; let i = index">
                        <td>{{ (page.page - 1) * pageSize + i + 1 }}</td>
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

                <div class="tournament-table-wrap" *ngIf="kind === 'tournaments' && tournamentsPage$ | async as page">
                  <table class="tournament-table">
                    <thead>
                      <tr>
                        <th scope="col">STT</th>
                        <th scope="col">Tên</th>
                        <th scope="col">Ngày</th>
                      </tr>
                    </thead>
                    <tbody>
                      <tr *ngFor="let tournament of page.items; let i = index">
                        <td>{{ (page.page - 1) * pageSize + i + 1 }}</td>
                        <td>
                          <a class="tournament-table-link player-link" [routerLink]="['/tournaments', tournament.id]">{{ tournament.name }}</a>
                        </td>
                        <td>{{ tournament.date }}</td>
                      </tr>
                    </tbody>
                  </table>
                </div>

                <div class="opening-tree" *ngIf="kind === 'openings' && openingsTree$ | async as nodes">
                  <a
                    class="opening-tree-row"
                    *ngFor="let node of nodes"
                    [routerLink]="['/openings', node.opening.id]"
                    [style.padding-left.px]="node.depth * 24"
                  >
                    <span class="opening-tree-connector" aria-hidden="true"></span>
                    <span class="opening-tree-name">{{ node.opening.name }}</span>
                    <span class="opening-tree-code" *ngIf="node.opening.code">{{ node.opening.code }}</span>
                  </a>
                </div>

                <div class="ranking-list" *ngIf="kind === 'rankings' && rankingsPage$ | async as page">
                  <article class="ranking-row" *ngFor="let player of page.items" [routerLink]="['/players', player.player_id]">
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
                  </article>
                </div>

                <div class="list-page-pagination list-page-pagination-bottom" *ngIf="kind !== 'openings' && activePage$ | async as page">
                  <ngb-pagination
                    [collectionSize]="page.total"
                    [page]="page.page"
                    [pageSize]="pageSize"
                    [boundaryLinks]="true"
                    [rotate]="true"
                    [maxSize]="5"
                    (pageChange)="goToPage($event, page.pages)"
                  ></ngb-pagination>
                  <span>Trang {{ page.page }} / {{ page.pages || 1 }} - {{ page.total }} mục</span>
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
export class ListPage {
  private readonly route = inject(ActivatedRoute);
  private readonly location = inject(Location);
  private readonly content = inject(ContentService);
  private readonly pageSubject = new BehaviorSubject(1);
  private readonly playerNationalityFilterSubject = new BehaviorSubject<'all' | 'vn' | 'non-vn'>('all');
  readonly pageSize = 20;
  playerNationalityFilter: 'all' | 'vn' | 'non-vn' = 'all';

  readonly kind = this.route.snapshot.data['kind'] as 'tournaments' | 'players' | 'games' | 'openings' | 'rankings';
  readonly title = this.route.snapshot.data['title'] || 'Danh sách';

  private readonly tournaments$ = this.content.tournaments$.pipe(
    map(items => [...items].sort((a, b) => this.compareLatestDates(a.date, b.date)))
  );
  private readonly players$ = combineLatest([this.content.players$, this.playerNationalityFilterSubject]).pipe(
    map(([items, nationality]) =>
      items
        .filter(item => nationality === 'all' || item.nationality === nationality)
        .sort((a, b) => a.name.localeCompare(b.name, 'vi', { sensitivity: 'base' }))
    )
  );
  private readonly games$ = this.content.games$.pipe(
    map(items => [...items].sort((a, b) => this.compareLatestDates(a.date, b.date)))
  );
  private readonly openings$ = this.content.openings$.pipe(
    map(items => [...items].sort((a, b) => a.name.localeCompare(b.name, 'vi', { sensitivity: 'base' })))
  );
  private readonly rankings$ = this.content.rankings$;

  readonly tournamentsPage$ = this.paginate(this.tournaments$);
  readonly playersPage$ = this.paginate(this.players$);
  readonly gamesPage$ = this.paginate(this.games$);
  readonly openingsPage$ = this.paginate(this.openings$);
  readonly openingsTree$ = this.openings$.pipe(map(items => this.toOpeningTree(items)));
  readonly rankingsPage$ = this.paginate(this.rankings$);
  readonly activePage$ = this.activePage();

  goBack(): void {
    this.location.back();
  }

  goToPage(requestedPage: number, pages: number): void {
    const lastPage = Math.max(1, pages || 1);
    this.pageSubject.next(Math.min(Math.max(1, Math.trunc(requestedPage)), lastPage));
  }

  setPlayerNationalityFilter(value: 'all' | 'vn' | 'non-vn'): void {
    this.playerNationalityFilter = value;
    this.playerNationalityFilterSubject.next(value);
    this.pageSubject.next(1);
  }

  displaySexuality(value: string): string {
    if (value === 'male') {
      return 'Nam';
    }
    if (value === 'female') {
      return 'Nữ';
    }
    return value || '-';
  }

  private paginate<T>(source$: Observable<T[]>): Observable<PagedList<T>> {
    return combineLatest([source$, this.pageSubject]).pipe(
      map(([items, requestedPage]) => {
        const pages = Math.ceil(items.length / this.pageSize);
        const page = Math.min(Math.max(1, requestedPage), pages || 1);
        const start = (page - 1) * this.pageSize;
        return {
          items: items.slice(start, start + this.pageSize),
          total: items.length,
          page,
          pages,
        };
      })
    );
  }

  private activePage(): Observable<PagedList<TournamentItem | PlayerItem | GameItem | OpeningItem | RankingItem>> {
    if (this.kind === 'players') {
      return this.playersPage$;
    }
    if (this.kind === 'games') {
      return this.gamesPage$;
    }
    if (this.kind === 'openings') {
      return this.openingsPage$;
    }
    if (this.kind === 'rankings') {
      return this.rankingsPage$;
    }
    return this.tournamentsPage$;
  }

  private toOpeningTree(items: OpeningItem[]): OpeningTreeNode[] {
    const byId = new Map(items.map(item => [item.id, item]));
    const childrenByParent = new Map<string, OpeningItem[]>();

    for (const item of items) {
      const parentId = item.parent_id && byId.has(item.parent_id) ? item.parent_id : '';
      const children = childrenByParent.get(parentId) ?? [];
      children.push(item);
      childrenByParent.set(parentId, children);
    }

    for (const children of childrenByParent.values()) {
      children.sort((a, b) => a.name.localeCompare(b.name, 'vi', { sensitivity: 'base' }));
    }

    const result: OpeningTreeNode[] = [];
    const visit = (opening: OpeningItem, depth: number, ancestors: Set<string>) => {
      if (ancestors.has(opening.id)) {
        return;
      }
      result.push({ opening, depth });
      const nextAncestors = new Set(ancestors);
      nextAncestors.add(opening.id);
      for (const child of childrenByParent.get(opening.id) ?? []) {
        visit(child, depth + 1, nextAncestors);
      }
    };

    for (const root of childrenByParent.get('') ?? []) {
      visit(root, 0, new Set());
    }

    return result;
  }

  private compareLatestDates(a: string, b: string): number {
    const aTime = this.parseTournamentDate(a);
    const bTime = this.parseTournamentDate(b);

    if (aTime === null && bTime === null) {
      return 0;
    }
    if (aTime === null) {
      return 1;
    }
    if (bTime === null) {
      return -1;
    }
    return bTime - aTime;
  }

  private parseTournamentDate(value: string): number | null {
    const dateText = this.datePrefix(value.trim());
    const fullIsoMatch = dateText.match(/^(\d{4})[-/](\d{1,2})[-/](\d{1,2})$/);
    if (fullIsoMatch) {
      const [, year, month, day] = fullIsoMatch.map(part => Number.parseInt(part, 10));
      return this.validDateTime(year, month, day);
    }

    const fullDisplayMatch = dateText.match(/^(\d{1,2})[-/](\d{1,2})[-/](\d{4})$/);
    if (fullDisplayMatch) {
      const [, day, month, year] = fullDisplayMatch.map(part => Number.parseInt(part, 10));
      return this.validDateTime(year, month, day);
    }

    const yearMonthMatch = dateText.match(/^(\d{4})[-/](\d{1,2})$/);
    if (yearMonthMatch) {
      const [, year, month] = yearMonthMatch.map(part => Number.parseInt(part, 10));
      return this.validDateTime(year, month, 1);
    }

    const monthYearMatch = dateText.match(/^(\d{1,2})[-/](\d{4})$/);
    if (monthYearMatch) {
      const [, month, year] = monthYearMatch.map(part => Number.parseInt(part, 10));
      return this.validDateTime(year, month, 1);
    }

    const yearOnlyMatch = dateText.match(/^\d{4}$/);
    if (yearOnlyMatch) {
      return new Date(Number.parseInt(dateText, 10), 0, 1).getTime();
    }

    return null;
  }

  private validDateTime(year: number, month: number, day: number): number | null {
    const date = new Date(year, month - 1, day);
    if (date.getFullYear() !== year || date.getMonth() !== month - 1 || date.getDate() !== day) {
      return null;
    }
    return date.getTime();
  }

  private datePrefix(value: string): string {
    const rangeStart = value.split(/\s+-\s+/, 1)[0].trim();
    return (
      rangeStart.match(/^(\d{4}[-/]\d{1,2}[-/]\d{1,2}|\d{1,2}[-/]\d{1,2}[-/]\d{4})/)?.[1] ??
      rangeStart.match(/^(\d{4}[-/]\d{1,2}|\d{1,2}[-/]\d{4})/)?.[1] ??
      rangeStart
    );
  }
}
