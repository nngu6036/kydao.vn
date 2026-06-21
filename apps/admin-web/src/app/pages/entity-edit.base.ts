import { FormControl, FormGroup } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import { Observable, catchError, combineLatest, map, of, switchMap, tap } from 'rxjs';
import {
  AdminContentService,
  ENTITY_CONFIGS,
  EntityConfig,
  EntityField,
  EntityKind,
  EntitySearchOption,
} from '../core/admin-content.service';

export const ENTITY_EDIT_TEMPLATE = `
  <section class="page-head" *ngIf="vm$ | async as vm">
    <div>
      <p class="eyebrow">{{ vm.isNew ? 'Tạo mới' : 'Chỉnh sửa' }}</p>
      <h1>{{ vm.config.singular }}</h1>
    </div>
    <a class="secondary-action" [routerLink]="['/', vm.config.kind]">Quay lại danh sách</a>
  </section>

  <section class="work-panel" *ngIf="activeConfig">
    <form class="edit-form" [formGroup]="form" (ngSubmit)="save()">
      <label *ngFor="let field of activeConfig.fields" [class.checkbox-field]="field.type === 'checkbox'">
        <span>{{ field.label }}</span>

        <textarea
          *ngIf="field.type === 'textarea'; else standardInput"
          [formControlName]="field.key"
          rows="7"
        ></textarea>

        <ng-template #standardInput>
          <div class="entity-search" *ngIf="field.type === 'entity-search'; else selectInput">
            <input
              type="search"
              [formControl]="searchControls[field.key]"
              placeholder="Nhập tên để tìm kiếm"
              autocomplete="off"
              (focus)="openEntitySearch(field)"
              (input)="onEntitySearchInput(field, $any($event.target).value)"
              (blur)="cancelEntitySearch(field)"
              (keydown.escape)="cancelEntitySearch(field)"
            />
            <div class="entity-search-menu" *ngIf="isEntitySearchOpen(field)">
              <button
                type="button"
                class="entity-search-option"
                *ngFor="let option of searchOptions[field.key] ?? []"
                (pointerdown)="selectEntityOption(field, option); $event.preventDefault()"
              >
                <strong>{{ option.name || option.id }}</strong>
                <span>{{ option.id }}</span>
              </button>
              <div class="entity-search-empty" *ngIf="!(searchOptions[field.key] ?? []).length">
                Không tìm thấy kết quả
              </div>
            </div>
          </div>

          <ng-template #selectInput>
            <select
              *ngIf="field.type === 'select'; else textInput"
              [formControlName]="field.key"
            >
              <option value="">Chưa chọn</option>
              <option *ngFor="let option of field.options ?? []" [value]="option.value">{{ option.label }}</option>
            </select>

            <ng-template #textInput>
              <div *ngIf="field.type === 'date'; else standardTextInput" class="date-input-row">
                <input
                  type="text"
                  [attr.placeholder]="inputPlaceholder(field)"
                  [formControlName]="field.key"
                />
                <input
                  class="date-picker-input"
                  type="date"
                  [value]="datePickerValue(field)"
                  (change)="onDatePickerChange(field, $any($event.target).value)"
                />
              </div>
              <ng-template #standardTextInput>
                <input
                  *ngIf="field.type !== 'checkbox'"
                  [type]="inputType(field)"
                  [attr.placeholder]="inputPlaceholder(field)"
                  [formControlName]="field.key"
                />
              </ng-template>
            </ng-template>

            <input *ngIf="field.type === 'checkbox'" type="checkbox" [formControlName]="field.key" />
          </ng-template>
        </ng-template>
      </label>

      <div class="form-actions">
        <div class="form-action-buttons">
        <button class="primary-action" type="submit" [disabled]="saving">
          {{ saving ? 'Đang lưu...' : 'Lưu thay đổi' }}
        </button>
        <button
          class="secondary-action danger-action"
          type="button"
          *ngIf="activeId"
          [disabled]="saving"
          (click)="deleteCurrent()"
        >
          Xóa
        </button>
        </div>
        <span class="save-state" *ngIf="message">{{ message }}</span>
      </div>
    </form>
  </section>
`;

export abstract class EntityEditBasePage {
  form = new FormGroup<Record<string, FormControl>>({});
  activeConfig?: EntityConfig;
  activeId?: string;
  saving = false;
  message = '';
  searchControls: Record<string, FormControl<string>> = {};
  searchOptions: Record<string, EntitySearchOption[]> = {};
  private committedSearchValues: Record<string, { id: string; name: string }> = {};
  private openSearchKey?: string;

  readonly config: EntityConfig;
  readonly vm$: Observable<{ config: EntityConfig; id: string | undefined; isNew: boolean }>;

  protected constructor(
    protected readonly route: ActivatedRoute,
    protected readonly router: Router,
    protected readonly api: AdminContentService,
    kind: EntityKind,
  ) {
    this.config = ENTITY_CONFIGS[kind];
    this.vm$ = combineLatest([this.route.paramMap]).pipe(
      map(([params]) => {
        const id = params.get('id') ?? undefined;
        return { config: this.config, id, isNew: !id };
      }),
      switchMap((state) => {
        this.buildForm(state.config);
        this.activeConfig = state.config;
        this.activeId = state.id;

        if (!state.id) {
          return of(state);
        }

        return this.api.get(state.config.kind, state.id).pipe(
          tap((item) => {
            this.form.patchValue(this.toFormValue(item));
            this.setSearchLabels(item);
            this.afterFormDataLoaded();
          }),
          map(() => state),
          catchError(() => {
            this.message = 'Không thể tải bản ghi này.';
            return of(state);
          })
        );
      })
    );
  }

  save(): void {
    if (!this.activeConfig) {
      return;
    }

    this.saving = true;
    this.message = '';
    const payload = this.toPayload(this.form.getRawValue());
    const kind = this.activeConfig.kind;
    const request = this.activeId
      ? this.api.update(kind, this.activeId, payload)
      : this.api.create(kind, payload);

    const isCreate = !this.activeId;

    request.subscribe({
      next: (item) => {
        this.saving = false;
        this.message = 'Đã lưu';
        if (isCreate) {
          const id = this.recordId(item);
          if (id) {
            this.activeId = id;
            this.router.navigate(['/', kind, id, 'edit'], { replaceUrl: true });
            return;
          }
        }

        if (this.navigateToListAfterSave()) {
          this.router.navigate(['/', kind]);
        }
      },
      error: () => {
        this.saving = false;
        this.message = 'Lưu thất bại. Hãy kiểm tra kết nối API.';
      },
    });
  }

  deleteCurrent(): void {
    if (!this.activeConfig || !this.activeId) {
      return;
    }

    if (!window.confirm(`Xóa ${this.activeConfig.singular} này?`)) {
      return;
    }

    this.saving = true;
    this.message = '';
    const kind = this.activeConfig.kind;
    this.api.delete(kind, this.activeId).subscribe({
      next: () => {
        this.saving = false;
        this.router.navigate(['/', kind]);
      },
      error: () => {
        this.saving = false;
        this.message = 'Không thể xóa bản ghi.';
      },
    });
  }

  openEntitySearch(field: EntityField): void {
    this.openSearchKey = field.key;
    this.searchEntityOptions(field, this.searchControls[field.key]?.value ?? '');
  }

  isEntitySearchOpen(field: EntityField): boolean {
    return this.openSearchKey === field.key;
  }

  searchEntityOptions(field: EntityField, value: string): void {
    if (field.type !== 'entity-search' || !field.searchKind) {
      return;
    }

    const query = value.trim();
    if (!query) {
      this.searchOptions[field.key] = [];
      return;
    }

    this.api.searchByName(field.searchKind, query).subscribe({
      next: (options) => {
        this.searchOptions[field.key] = options;
      },
      error: () => {
        this.searchOptions[field.key] = [];
      },
    });
  }

  onEntitySearchInput(field: EntityField, value: string): void {
    this.form.controls[field.key]?.setValue('');
    this.setRelatedName(field.key, '');
    this.searchEntityOptions(field, value);
  }

  cancelEntitySearch(field: EntityField): void {
    setTimeout(() => {
      if (this.openSearchKey !== field.key) {
        return;
      }

      this.restoreCommittedSearchValue(field);
      this.searchOptions[field.key] = [];
      this.openSearchKey = undefined;
    }, 100);
  }

  selectEntityOption(field: EntityField, option: EntitySearchOption): void {
    const name = this.optionName(option);
    this.commitSearchValue(field, option.id, name);
    this.searchOptions[field.key] = [];
    this.openSearchKey = undefined;
  }

  inputType(field: EntityField): string {
    if (field.type === 'number') {
      return field.type;
    }
    return 'text';
  }

  inputPlaceholder(field: EntityField): string | null {
    if (field.type === 'date') {
      return 'YYYY, MM/YYYY, or DD/MM/YYYY';
    }
    return null;
  }

  datePickerValue(field: EntityField): string {
    if (field.type !== 'date') {
      return '';
    }

    const value = this.form.controls[field.key]?.value;
    if (typeof value !== 'string') {
      return '';
    }

    const normalized = this.normalizeDateInput(value);
    return typeof normalized === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(normalized) ? normalized : '';
  }

  onDatePickerChange(field: EntityField, value: string): void {
    if (field.type !== 'date') {
      return;
    }

    this.form.controls[field.key]?.setValue(value);
    this.form.controls[field.key]?.markAsDirty();
    this.form.controls[field.key]?.updateValueAndValidity();
  }

  protected afterFormBuilt(): void {
    // Subclasses can attach streams to the freshly-created form controls.
  }

  protected afterFormDataLoaded(): void {
    // Subclasses can react after existing entity data has been patched into the form.
  }

  protected navigateToListAfterSave(): boolean {
    return true;
  }

  private buildForm(config: EntityConfig): void {
    const controls: Record<string, FormControl> = {};
    this.searchControls = {};
    this.searchOptions = {};
    this.committedSearchValues = {};

    for (const field of config.fields) {
      controls[field.key] = new FormControl(field.type === 'checkbox' ? false : '');
      if (field.type === 'entity-search') {
        this.searchControls[field.key] = new FormControl('', { nonNullable: true });
      }
    }
    this.form = new FormGroup(controls);
    this.afterFormBuilt();
  }

  private setSearchLabels(item: Record<string, unknown>): void {
    for (const field of this.activeConfig?.fields ?? []) {
      if (field.type !== 'entity-search') {
        continue;
      }

      const labelKey = this.searchLabelKey(field.key);
      const id = item[field.key];
      const label = item[labelKey] ?? id;
      this.commitSearchValue(field, id ? String(id) : '', label ? String(label) : '');
    }
  }

  private searchLabelKey(key: string): string {
    if (key === 'red_id') {
      return 'red_name';
    }
    if (key === 'black_id') {
      return 'black_name';
    }
    if (key === 'tournament_id') {
      return 'tournament_name';
    }
    return key;
  }

  private setRelatedName(key: string, name: string): void {
    const nameKey = this.searchLabelKey(key);
    if (nameKey !== key) {
      this.form.controls[nameKey]?.setValue(name);
      this.form.controls[nameKey]?.markAsDirty();
      this.form.controls[nameKey]?.updateValueAndValidity();
    }
  }

  private commitSearchValue(field: EntityField, id: string, name: string): void {
    this.committedSearchValues[field.key] = { id, name };
    this.form.controls[field.key]?.setValue(id);
    this.form.controls[field.key]?.markAsDirty();
    this.form.controls[field.key]?.updateValueAndValidity();
    this.searchControls[field.key]?.setValue(name);
    this.setRelatedName(field.key, name);
  }

  private optionName(option: EntitySearchOption): string {
    const name = option.name;
    return typeof name === 'string' && name.trim() ? name : option.id;
  }

  private restoreCommittedSearchValue(field: EntityField): void {
    const value = this.committedSearchValues[field.key] ?? { id: '', name: '' };
    this.form.controls[field.key]?.setValue(value.id);
    this.searchControls[field.key]?.setValue(value.name);
    this.setRelatedName(field.key, value.name);
  }

  private toFormValue(item: Record<string, unknown>): Record<string, unknown> {
    const value: Record<string, unknown> = {};
    for (const key of Object.keys(this.form.controls)) {
      value[key] = item[key] ?? (typeof item[key] === 'boolean' ? false : '');
    }
    return value;
  }

  private toPayload(value: Record<string, unknown>): Record<string, unknown> {
    const payload: Record<string, unknown> = {};
    for (const field of this.activeConfig?.fields ?? []) {
      const raw = value[field.key];
      const key = field.payloadKey ?? field.key;
      payload[key] = this.toPayloadValue(field, raw);
    }
    return payload;
  }

  private toPayloadValue(field: EntityField, raw: unknown): unknown {
    if (raw === '') {
      return null;
    }
    if (field.type === 'date') {
      return this.normalizeDateInput(raw);
    }
    return raw;
  }

  private normalizeDateInput(raw: unknown): unknown {
    if (typeof raw !== 'string') {
      return raw;
    }

    const value = raw.trim();
    if (!value) {
      return null;
    }

    const yearOnly = value.match(/^(\d{4})$/);
    if (yearOnly) {
      return `${yearOnly[1]}-01-01`;
    }

    const yearMonth = value.match(/^(\d{4})[-/](\d{1,2})$/);
    if (yearMonth) {
      return this.formatDateParts(yearMonth[1], yearMonth[2], '1') ?? value;
    }

    const monthYear = value.match(/^(\d{1,2})[-/](\d{4})$/);
    if (monthYear) {
      return this.formatDateParts(monthYear[2], monthYear[1], '1') ?? value;
    }

    const yearMonthDay = value.match(/^(\d{4})[-/](\d{1,2})[-/](\d{1,2})$/);
    if (yearMonthDay) {
      return this.formatDateParts(yearMonthDay[1], yearMonthDay[2], yearMonthDay[3]) ?? value;
    }

    const dayMonthYear = value.match(/^(\d{1,2})[-/](\d{1,2})[-/](\d{4})$/);
    if (dayMonthYear) {
      return this.formatDateParts(dayMonthYear[3], dayMonthYear[2], dayMonthYear[1]) ?? value;
    }

    return value;
  }

  private formatDateParts(yearValue: string, monthValue: string, dayValue: string): string | null {
    const year = Number.parseInt(yearValue, 10);
    const month = Number.parseInt(monthValue, 10);
    const day = Number.parseInt(dayValue, 10);
    if (!Number.isInteger(year) || !Number.isInteger(month) || !Number.isInteger(day)) {
      return null;
    }
    if (year < 1000 || year > 9999 || month < 1 || month > 12 || day < 1 || day > 31) {
      return null;
    }

    const date = new Date(year, month - 1, day);
    if (date.getFullYear() !== year || date.getMonth() !== month - 1 || date.getDate() !== day) {
      return null;
    }

    return `${yearValue.padStart(4, '0')}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
  }

  private recordId(item: Record<string, unknown>): string | undefined {
    const id = item['id'] ?? item['_id'];
    return typeof id === 'string' && id ? id : undefined;
  }
}
