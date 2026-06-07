import { Component, ViewChild } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { BehaviorSubject, Observable, of } from 'rxjs';
import { map, startWith, switchMap } from 'rxjs/operators';
import {
  XiangqiBoardComponent,
  XiangqBoardUtils,
  type XiangqiMove,
} from '@chess-elo/shared-ui/xiangqi-board';
import { AdminContentService } from '../core/admin-content.service';
import { EntityEditBasePage } from './entity-edit.base';

const GAME_EDIT_TEMPLATE = `
  <section class="page-head" *ngIf="vm$ | async as vm">
    <div>
      <p class="eyebrow">{{ vm.isNew ? 'Tạo mới' : 'Chỉnh sửa' }}</p>
      <h1>{{ vm.config.singular }}</h1>
    </div>
    <a class="secondary-action" [routerLink]="['/', vm.config.kind]">Quay lại danh sách</a>
  </section>

  <section class="work-panel game-edit-panel" *ngIf="activeConfig">
    <form class="edit-form game-edit-form" [formGroup]="form" (ngSubmit)="save()">
      <label
        *ngFor="let field of activeConfig.fields"
        [class.checkbox-field]="field.type === 'checkbox'"
        [class.full-width-field]="field.key === 'move_list'"
      >
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
              <input
                *ngIf="field.type !== 'checkbox'"
                [type]="inputType(field)"
                [formControlName]="field.key"
              />
            </ng-template>

            <input *ngIf="field.type === 'checkbox'" type="checkbox" [formControlName]="field.key" />
          </ng-template>
        </ng-template>
      </label>

      <div class="form-actions">
        <button class="primary-action" type="submit" [disabled]="saving">
          {{ saving ? 'Đang lưu...' : 'Lưu thay đổi' }}
        </button>
        <span class="save-state" *ngIf="message">{{ message }}</span>
      </div>
    </form>

    <div class="game-board-column">
      <div class="game-board-preview">
        <div class="game-board-layout">
          <div class="game-board-stack">
            <xiangqi-board
              #xiangqiBoard
              class="game-board"
              [position]="boardPosition()"
              (pieceMove)="movePiece($event)"
            ></xiangqi-board>
          </div>

          <div class="move-list-pane">
            <select
              class="move-list-box"
              [value]="selectedMoveIndex"
              (change)="selectMove($any($event.target).value)"
              size="16"
            >
              <ng-container *ngIf="moveTokens$ | async as moveTokens">
                <option *ngFor="let move of moveTokens; let idx = index" [value]="idx" [selected]="idx === selectedMoveIndex">
                  {{ moveOptionLabel(idx, move) }}
                </option>
              </ng-container>
            </select>
          </div>
        </div>
      </div>

      <div class="game-board-toolbar" role="toolbar" aria-label="Board navigation">
        <button
          class="icon-action"
          type="button"
          aria-label="First move"
          title="First move"
          [disabled]="!canGoFirst()"
          (click)="goFirst()"
        >&#x23EE;</button>
        <button
          class="icon-action"
          type="button"
          aria-label="Previous move"
          title="Previous move"
          [disabled]="!canGoPrevious()"
          (click)="goPrevious()"
        >&#x25C0;</button>
        <button
          class="icon-action"
          type="button"
          aria-label="Next move"
          title="Next move"
          [disabled]="!canGoNext()"
          (click)="goNext()"
        >&#x25B6;</button>
        <button
          class="icon-action"
          type="button"
          aria-label="Last move"
          title="Last move"
          [disabled]="!canGoLast()"
          (click)="goLast()"
        >&#x23ED;</button>
        <button
          class="icon-action icon-action-danger"
          type="button"
          aria-label="Remove selected move"
          title="Remove selected move"
          [disabled]="!canRemoveMove()"
          (click)="removeSelectedMove()"
        >&#x2715;</button>
      </div>
    </div>
  </section>
`;

@Component({
  template: GAME_EDIT_TEMPLATE,
})
export class GameEditPage extends EntityEditBasePage {
  @ViewChild('xiangqiBoard') private xiangqiBoard?: XiangqiBoardComponent;

  selectedMoveIndex = -1;
  readonly moveTokens$: Observable<string[]>;

  private readonly moveListControlReceiver = new BehaviorSubject(this.form.controls['move_list']);

  constructor(route: ActivatedRoute, router: Router, api: AdminContentService) {
    super(route, router, api, 'games');
    this.moveTokens$ = this.moveListControlReceiver.pipe(
      switchMap((control) => {
        if (!control) {
          return of([]);
        }

        return control.valueChanges.pipe(
          startWith(control.value),
          map((value) => this.toMoveTokens(value))
        );
      })
    );
  }

  boardPosition(): string {
    const value = this.form.controls['begin_fen']?.value;
    return typeof value === 'string' && value.trim() ? value.trim() : 'start';
  }

  private boardMoves():XiangqiMove[] {
    const fen = this.form.controls['begin_fen']?.value;
    const move_list = this.form.controls['move_list']?.value;
    return XiangqBoardUtils.parseMoveNotationList(move_list, fen);
  }


  selectMove(index: number | string): void {
    const moveIndex = Number(index);
    if (moveIndex <0) {
      this.xiangqiBoard?.reset();
      this.selectedMoveIndex = moveIndex;
      return;
    }      
    const moves = this.boardMoves();
    if (Number.isNaN(moveIndex) || moveIndex < 0) {
      return;
    }
    const move = moves[moveIndex];
    if (!move) {
      return;
    }
    this.selectedMoveIndex = moveIndex;
    this.xiangqiBoard?.takeMove(move);
  }

  canGoFirst(): boolean {
    return this.selectedMoveIndex > 0;
  }

  canGoPrevious(): boolean {
    return this.selectedMoveIndex >= 0;
  }

  canGoNext(): boolean {
    return this.selectedMoveIndex < this.moveCount() - 1;
  }

  canGoLast(): boolean {
    const lastMoveIndex = this.moveCount() - 1;
    return lastMoveIndex >= 0 && this.selectedMoveIndex !== lastMoveIndex;
  }

  canRemoveMove(): boolean {
    return this.selectedMoveIndex >= 0 && this.selectedMoveIndex < this.moveCount();
  }

  goFirst(): void {
    this.selectMove(-1);
  }

  goPrevious(): void {
    this.selectedMoveIndex--;
    this.selectMove(this.selectedMoveIndex);
  }

  goNext(): void {
    const nextIndex = this.selectedMoveIndex + 1;
    if (nextIndex >= this.moveCount()) {
      return;
    }

    this.selectMove(nextIndex);
  }

  goLast(): void {
    const lastMoveIndex = this.moveCount() - 1;
    if (lastMoveIndex < 0) {
      return;
    }

    this.selectMove(lastMoveIndex);
  }

  removeSelectedMove(): void {
    if (!this.canRemoveMove()) {
      return;
    }
    const raw = this.form.controls['move_list']?.value.trim() ?? '';
    const moveTokens = this.toMoveTokens(raw);
    moveTokens.splice(this.selectedMoveIndex)
    this.form.controls['move_list']?.setValue(moveTokens.join(', '));

    const nextSelectedIndex = Math.min(this.selectedMoveIndex, moveTokens.length - 1);
    this.selectMove(nextSelectedIndex);
  }

  moveOptionLabel(index: number, move: string): string {
    const label = index % 2 === 0 ? String(Math.floor(index / 2) + 1) : '';
    const paddedLabel = label.padStart(2, '\u00A0');
    return `${paddedLabel} ${move}`;
  }

  movePiece(event:XiangqiMove): void {
    if (this.selectedMoveIndex ==-1) {
      this.form.controls['move_list']?.setValue('')
    };
    const raw = this.form.controls['move_list']?.value.trim() ?? '';
    const  moveList = raw.split(',').map((m:string) => m.trim()).filter(Boolean);
    moveList.push(event.Notation);
    this.form.controls['move_list']?.setValue(moveList.join(', '));
    this.selectMove(moveList.length - 1);
  }

  protected override afterFormBuilt(): void {
    this.moveListControlReceiver.next(this.form.controls['move_list']);
  }

  protected override afterFormDataLoaded(): void {
     if (this.moveCount() === 0) {
      this.selectedMoveIndex = -1;
      return;
    }

    setTimeout(() => {
      this.selectMove(this.moveCount()-1);
    });
  }

  private toMoveTokens(value: unknown): string[] {
    const moveList = Array.isArray(value) ? value.join(',') : typeof value === 'string' ? value : '';
    return moveList
      .split(',')
      .map((move) => move.trim())
      .filter(Boolean);
  }

  private moveCount(): number {
    return this.currentMoveTokens().length;
  }

  private currentMoveTokens(): string[] {
    return this.toMoveTokens(this.form.controls['move_list']?.value);
  }

}
