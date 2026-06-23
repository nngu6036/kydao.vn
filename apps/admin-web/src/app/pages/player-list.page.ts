import { AsyncPipe, NgFor, NgIf } from '@angular/common';
import { Component, ElementRef, ViewChild, inject } from '@angular/core';
import { FormControl, ReactiveFormsModule } from '@angular/forms';
import { RouterLink } from '@angular/router';
import { combineLatest } from 'rxjs';
import { distinctUntilChanged, startWith } from 'rxjs/operators';
import { AdminContentService } from '../core/admin-content.service';
import { EntityListBasePage, ListColumn } from './entity-list.base';

const PLAYER_LIST_TEMPLATE = `
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
      <select [formControl]="nationalityFilter" aria-label="Lọc theo quốc tịch">
        <option value="">Tất cả quốc tịch</option>
        <option value="vn">Việt Nam</option>
        <option value="non-vn">ngoài Việt Nam</option>
      </select>
      <select [formControl]="sexualityFilter" aria-label="Lọc theo giới tính">
        <option value="">Tất cả giới tính</option>
        <option value="male">Nam</option>
        <option value="female">Nữ</option>
      </select>
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
        class="data-row"
        *ngFor="let item of vm.page.items"
        [style.grid-template-columns]="gridTemplateColumns"
      >
        <span
          *ngFor="let column of columns"
          class="inline-edit-cell"
          [class.inline-editing-cell]="isEditingCell(item, column)"
          (click)="startCellEdit(item, column)"
        >
          <ng-container *ngIf="isEditingCell(item, column); else readCell">
            <select
              #cellControl
              *ngIf="column.key === 'nationality'"
              class="inline-edit-control"
              [value]="cellDraft"
              (click)="$event.stopPropagation()"
              (change)="cellDraft = $any($event.target).value"
              (blur)="saveCellEdit(item, column)"
              (keydown.escape)="stopCellEdit()"
            >
              <option value="">Chưa chọn</option>
              <option value="vn">Việt Nam</option>
              <option value="non-vn">ngoài Việt Nam</option>
            </select>

            <select
              #cellControl
              *ngIf="column.key === 'sexuality'"
              class="inline-edit-control"
              [value]="cellDraft"
              (click)="$event.stopPropagation()"
              (change)="cellDraft = $any($event.target).value"
              (blur)="saveCellEdit(item, column)"
              (keydown.escape)="stopCellEdit()"
            >
              <option value="">Chưa chọn</option>
              <option value="male">Nam</option>
              <option value="female">Nữ</option>
            </select>

            <select
              #cellControl
              *ngIf="column.key === 'initial_level'"
              class="inline-edit-control"
              [value]="cellDraft"
              (click)="$event.stopPropagation()"
              (change)="cellDraft = $any($event.target).value"
              (blur)="saveCellEdit(item, column)"
              (keydown.escape)="stopCellEdit()"
            >
              <option value="">Chưa chọn</option>
              <option value="a2_level">Kỳ thủ A2</option>
              <option value="a1_level">Kỳ thủ A1</option>
              <option value="national_master">Kiện tướng quốc gia</option>
              <option value="international_master">Quốc tế đại sư</option>
              <option value="international_grand_master">Đặc cấp Quốc tế đại sư</option>
            </select>

            <input
              #cellControl
              *ngIf="column.key !== 'nationality' && column.key !== 'sexuality' && column.key !== 'initial_level'"
              class="inline-edit-control"
              type="text"
              [value]="cellDraft"
              (click)="$event.stopPropagation()"
              (input)="cellDraft = $any($event.target).value"
              (blur)="saveCellEdit(item, column)"
              (keydown.enter)="saveCellEdit(item, column)"
              (keydown.escape)="stopCellEdit()"
            />
          </ng-container>

          <ng-template #readCell>{{ displayColumn(item, column) }}</ng-template>
        </span>

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

@Component({
  template: PLAYER_LIST_TEMPLATE,
})
export class PlayerListPage extends EntityListBasePage {
  private editingCell?: { id: string; key: string };
  cellDraft = '';
  private savingCell = false;
  readonly nationalityFilter = new FormControl('', { nonNullable: true });
  readonly sexualityFilter = new FormControl('', { nonNullable: true });

  @ViewChild('cellControl') set cellControl(control: ElementRef<HTMLInputElement | HTMLSelectElement> | undefined) {
    if (control) {
      setTimeout(() => control.nativeElement.focus());
    }
  }

  constructor() {
    super(inject(AdminContentService), 'players', [
      { key: 'name', label: 'Tên kỳ thủ', widthPercent: 34 },
      { key: 'initial_level', label: 'Cấp độ ban đầu', widthPercent: 20 },
      { key: 'nationality', label: 'Quốc tịch', widthPercent: 16 },
      { key: 'sexuality', label: 'Giới tính', widthPercent: 12 },
      { key: 'elo', label: 'ELO', widthPercent: 8 },
    ], { column: 'name', direction: 'asc' });

    combineLatest([
      this.nationalityFilter.valueChanges.pipe(startWith(this.nationalityFilter.value), distinctUntilChanged()),
      this.sexualityFilter.valueChanges.pipe(startWith(this.sexualityFilter.value), distinctUntilChanged()),
    ]).subscribe(([nationality, sexuality]) => {
      this.setListFilters({ nationality, sexuality });
    });
  }

  startCellEdit(item: Record<string, unknown>, column: ListColumn): void {
    if (this.savingCell || !this.isEditableColumn(column)) {
      return;
    }

    const id = this.recordId(item);
    if (!id) {
      return;
    }

    this.editingCell = { id, key: column.key };
    this.cellDraft = this.toDraftString(item[column.key]);
  }

  isEditingCell(item: Record<string, unknown>, column: ListColumn): boolean {
    const id = this.recordId(item);
    return !!id && this.editingCell?.id === id && this.editingCell.key === column.key;
  }

  saveCellEdit(item: Record<string, unknown>, column: ListColumn): void {
    if (!this.isEditingCell(item, column) || this.savingCell) {
      return;
    }

    const id = this.recordId(item);
    if (!id) {
      this.stopCellEdit();
      return;
    }

    const previousValue = item[column.key];
    const nextValue = this.toPayloadValue(column, this.cellDraft);
    if (this.sameValue(previousValue, nextValue)) {
      this.stopCellEdit();
      return;
    }

    this.savingCell = true;
    this.api.update('players', id, { [column.key]: nextValue }).subscribe({
      next: (updated) => {
        Object.assign(item, updated);
        this.savingCell = false;
        this.stopCellEdit();
      },
      error: () => {
        this.savingCell = false;
        window.alert('Không thể lưu bản ghi.');
        this.stopCellEdit();
      },
    });
  }

  stopCellEdit(): void {
    this.editingCell = undefined;
    this.cellDraft = '';
  }

  private toPayloadValue(column: ListColumn, value: string): unknown {
    if (value === '') {
      return null;
    }
    return value;
  }

  private isEditableColumn(column: ListColumn): boolean {
    return column.key !== 'elo';
  }

  private sameValue(previousValue: unknown, nextValue: unknown): boolean {
    if (previousValue === null || previousValue === undefined) {
      return nextValue === null || nextValue === undefined || nextValue === '';
    }
    return String(previousValue) === String(nextValue);
  }

  private recordId(item: Record<string, unknown>): string | undefined {
    const id = item['id'] ?? item['_id'];
    return typeof id === 'string' && id ? id : undefined;
  }

  private toDraftString(value: unknown): string {
    return value === null || value === undefined ? '' : String(value);
  }
}
