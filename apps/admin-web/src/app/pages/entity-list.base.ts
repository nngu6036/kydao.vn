import { FormControl } from '@angular/forms';
import {
  BehaviorSubject,
  catchError,
  combineLatest,
  debounceTime,
  distinctUntilChanged,
  map,
  of,
  startWith,
  switchMap,
  tap,
} from 'rxjs';
import {
  AdminContentService,
  ENTITY_CONFIGS,
  EntityConfig,
  EntityKind,
  PageResponse,
} from '../core/admin-content.service';

export interface ListColumn {
  key: string;
  label: string;
  widthPercent?: number;
  sortable?: boolean;
}

interface SortState {
  column?: string;
  direction: 'asc' | 'desc';
}

export const ENTITY_LIST_TEMPLATE = `
  <section class="page-head">
    <div>
      <p class="eyebrow">Quản lý</p>
      <h1>{{ config.label }}</h1>
      <p class="page-copy">{{ config.description }}</p>
    </div>
    <a class="primary-action" [routerLink]="['/', config.kind, 'new']">Tạo {{ config.singular }}</a>
  </section>

  <section class="work-panel entity-list-panel" *ngIf="vm$ | async as vm">
    <div class="table-toolbar">
      <input type="search" [formControl]="search" placeholder="Tìm kiếm bản ghi" />
      <span>{{ vm.page.total }} bản ghi</span>
    </div>

    <div class="data-table" *ngIf="vm.page.items.length; else emptyState">
      <div class="data-row data-row-head" [style.grid-template-columns]="gridTemplateColumns">
        <button
          class="column-sort"
          type="button"
          *ngFor="let column of columns"
          (click)="sortBy(column)"
          [disabled]="column.sortable === false"
        >
          <span>{{ column.label }}</span>
          <span class="sort-indicator" *ngIf="vm.sort.column === column.key">
            {{ vm.sort.direction === 'asc' ? '▲' : '▼' }}
          </span>
        </button>
        <span></span>
      </div>

      <div
        class="data-row data-row-clickable"
        *ngFor="let item of vm.page.items"
        [routerLink]="['/', vm.config.kind, item['id'], 'edit']"
        [style.grid-template-columns]="gridTemplateColumns"
      >
        <span *ngFor="let column of columns">{{ displayColumn(item, column) }}</span>
        <span class="row-actions">
          <a
            class="row-action"
            [routerLink]="['/', vm.config.kind, item['id'], 'edit']"
            (click)="$event.stopPropagation()"
          >
            Sửa
          </a>
          <button class="row-action row-action-danger" type="button" (click)="deleteItem(item); $event.stopPropagation()">
            Xóa
          </button>
        </span>
      </div>
    </div>

    <div class="pagination-bar pagination-bar-bottom">
      <ngb-pagination
        [collectionSize]="vm.page.total"
        [page]="vm.page.page"
        [pageSize]="pageSize"
        [boundaryLinks]="true"
        [rotate]="true"
        [maxSize]="5"
        (pageChange)="goToPage($event, vm.page.pages)"
      ></ngb-pagination>
      <span>Trang {{ vm.page.page }} / {{ vm.page.pages || 1 }}</span>
    </div>

    <ng-template #emptyState>
      <div class="empty-state">
        <strong>Không tìm thấy bản ghi</strong>
        <span>Hãy thử từ khóa khác hoặc tạo mục mới.</span>
      </div>
    </ng-template>
  </section>
`;

export abstract class EntityListBasePage {
  private readonly pageSubject = new BehaviorSubject(1);
  private readonly sortSubject = new BehaviorSubject<SortState>({ direction: 'asc' });
  private readonly refreshSubject = new BehaviorSubject(0);
  private readonly filtersSubject = new BehaviorSubject<Record<string, string>>({});
  readonly pageSize = 25;

  readonly config: EntityConfig;
  readonly columns: ListColumn[];
  readonly gridTemplateColumns: string;
  readonly search = new FormControl('', { nonNullable: true });

  readonly query$ = this.search.valueChanges.pipe(
    startWith(''),
    debounceTime(200),
    distinctUntilChanged(),
    tap(() => this.pageSubject.next(1))
  );

  readonly vm$ = combineLatest([this.query$, this.pageSubject, this.sortSubject, this.refreshSubject, this.filtersSubject]).pipe(
    switchMap(([query, page, sort, , filters]) =>
      this.api.list(this.config.kind, query, page, this.pageSize, sort.column, sort.direction, filters).pipe(
        map((response) => ({ config: this.config, page: this.normalizePage(response, page), sort })),
        catchError(() => of({ config: this.config, page: this.emptyPage(page), sort }))
      )
    )
  );

  protected constructor(
    protected readonly api: AdminContentService,
    kind: EntityKind,
    columns: ListColumn[],
    initialSort?: SortState,
  ) {
    this.config = ENTITY_CONFIGS[kind];
    this.columns = columns;
    this.sortSubject.next(initialSort ?? this.defaultSort());
    this.gridTemplateColumns = this.toGridTemplateColumns(columns);
  }

  protected setListFilters(filters: Record<string, string>): void {
    this.filtersSubject.next(filters);
    if (this.pageSubject.value !== 1) {
      this.pageSubject.next(1);
    }
  }

  goToPage(requestedPage: number, pages: number): void {
    const lastPage = Math.max(1, pages || 1);
    this.pageSubject.next(Math.min(Math.max(1, Math.trunc(requestedPage)), lastPage));
  }

  sortBy(column: ListColumn): void {
    if (column.sortable === false) {
      return;
    }

    const current = this.sortSubject.value;
    const direction = current.column === column.key && current.direction === 'asc' ? 'desc' : 'asc';
    this.sortSubject.next({ column: column.key, direction });
    if (this.pageSubject.value !== 1) {
      this.pageSubject.next(1);
    }
  }

  deleteItem(item: Record<string, unknown>): void {
    const id = item['id'];
    if (typeof id !== 'string' || !id) {
      return;
    }

    const name = this.display(item['name'] ?? item['red_name'] ?? item['tournament_name'] ?? id);
    if (!window.confirm(`Xóa ${this.config.singular} "${name}"?`)) {
      return;
    }

    this.api.delete(this.config.kind, id).subscribe({
      next: () => this.refreshSubject.next(this.refreshSubject.value + 1),
      error: () => window.alert('Không thể xóa bản ghi.'),
    });
  }

  display(value: unknown): string {
    if (value === null || value === undefined || value === '') {
      return '-';
    }
    if (typeof value === 'boolean') {
      return value ? 'Có' : 'Không';
    }
    return String(value);
  }

  displayColumn(item: Record<string, unknown>, column: ListColumn): string {
    const value = item[column.key];
    if (column.key === 'result') {
      return this.displayResult(value);
    }
    if (column.key === 'country' || column.key === 'nationality') {
      return this.displayCountry(value);
    }
    if (column.key === 'initial_level') {
      return this.displayInitialLevel(value);
    }
    if (column.key === 'sexuality') {
      return this.displaySexuality(value);
    }
    return this.display(value);
  }

  private displayCountry(value: unknown): string {
    const countryLabels: Record<string, string> = {
      vn: 'Việt Nam',
      'non-vn': 'ngoài Việt Nam',
    };
    if (typeof value === 'string' && countryLabels[value]) {
      return countryLabels[value];
    }
    return this.display(value);
  }

  private displayResult(value: unknown): string {
    const resultLabels: Record<string, string> = {
      win: 'thắng',
      lose: 'thua',
      draw: 'hòa',
    };
    if (typeof value === 'string' && resultLabels[value]) {
      return resultLabels[value];
    }
    return this.display(value);
  }

  private displayInitialLevel(value: unknown): string {
    const levelLabels: Record<string, string> = {
      a2_level: 'Kỳ thủ A2',
      a1_level: 'Kỳ thủ A1',
      national_master: 'Kiện tướng quốc gia',
      international_master: 'Quốc tế đại sư',
      international_grand_master: 'Đặc cấp Quốc tế đại sư',
    };
    if (typeof value === 'string' && levelLabels[value]) {
      return levelLabels[value];
    }
    return this.display(value);
  }

  private displaySexuality(value: unknown): string {
    const sexualityLabels: Record<string, string> = {
      male: 'Nam',
      female: 'Nữ',
    };
    if (typeof value === 'string' && sexualityLabels[value]) {
      return sexualityLabels[value];
    }
    return this.display(value);
  }

  private toGridTemplateColumns(columns: ListColumn[]): string {
    const actionWidth = '140px';
    if (!columns.some((column) => column.widthPercent !== undefined)) {
      return `repeat(${columns.length}, minmax(140px, 1fr)) ${actionWidth}`;
    }

    const specified = columns.reduce((total, column) => total + (column.widthPercent ?? 0), 0);
    const unspecified = columns.filter((column) => column.widthPercent === undefined).length;
    const fallback = unspecified > 0 ? Math.max(0, 100 - specified) / unspecified : 0;
    const tracks = columns.map((column) => `minmax(120px, ${column.widthPercent ?? fallback}%)`);
    return `${tracks.join(' ')} ${actionWidth}`;
  }

  private defaultSort(): SortState {
    return {
      column: 'created_date',
      direction: 'desc',
    };
  }

  private normalizePage(response: PageResponse, requestedPage: number): PageResponse {
    return {
      ...response,
      page: response.page ?? requestedPage,
      page_size: response.page_size ?? response.limit ?? this.pageSize,
      pages: response.pages ?? (response.total ? Math.ceil(response.total / (response.limit || this.pageSize)) : 0),
    };
  }

  private emptyPage(page: number): PageResponse {
    return {
      items: [],
      total: 0,
      skip: (page - 1) * this.pageSize,
      limit: this.pageSize,
      page,
      page_size: this.pageSize,
      pages: 0,
    };
  }
}
