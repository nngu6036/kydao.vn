import { Component, ViewChild, inject } from '@angular/core';
import { AsyncPipe, Location, NgFor, NgIf } from '@angular/common';
import { ActivatedRoute, RouterLink } from '@angular/router';
import { map } from 'rxjs/operators';
import { FooterComponent } from '../components/footer.component';
import { HeaderComponent } from '../components/header.component';
import {
  XiangqiBoardComponent,
  XiangqBoardUtils,
  type XiangqiMove,
} from '@chess-elo/shared-ui/xiangqi-board';
import { ContentService } from '../core/content.service';

@Component({
  template: `
    <div class="homepage">
      <app-header></app-header>
      <section class="search-center search-center--compact">
        <h1 class="search-title">Dữ liệu ván đấu</h1>
        <div class="search-container" *ngIf="game$ | async as game">
          <div class="page-with-back">
            <button class="back-link" type="button" (click)="goBack()" aria-label="Quay lại">← Quay lại</button>

            <div class="page-main">
              <div class="content-block detail-page-block">
                <div class="detail-page-header">
                  <div class="detail-page-avatar">X</div>
                  <div>
                    <h4 class="detail-page-name">{{ game.tournament_name }}</h4>
                  </div>
                </div>

                <div class="game-main">
                  <div class="game-players detail-game-players">
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
                  <div class="game-meta detail-game-meta">
                    <span class="meta-item game-meta-tournament"><a class="entity-link" [routerLink]="['/tournaments', game.tournament_id]">{{ game.tournament_name }}</a></span>
                    <span class="meta-item game-meta-date">{{ game.date }}</span>
                    <span class="meta-item game-meta-moves">{{ game.moves }} nước</span>
                    <span class="meta-item opening-tag game-meta-opening"><a class="entity-link opening-link" [routerLink]="['/openings', game.opening_id]">{{ game.opening }}</a></span>
                    <span *ngIf="game.analyzed" class="analyzed-badge">Đã phần tích</span>
                  </div>
                </div>

                <div class="game-board-column">
                  <div class="game-board-preview">
                    <div class="game-board-layout">
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
                      </div>

                      <div class="game-board-stack">
                        <xiangqi-board
                          #xiangqiBoard
                          class="game-board"
                          [attr.data-board-id]="boardComponentId"
                          [componentId]="boardComponentId"
                          [position]="boardPosition()"
                          [viewOnly]="true"
                        ></xiangqi-board>
                      </div>

                      <div class="move-list-pane" *ngIf="game.moveTokens.length">
                        <select
                          class="move-list-box"
                          [value]="selectedMoveIndex"
                          (change)="selectMove($any($event.target).value)"
                          size="16"
                        >
                          <option
                            *ngFor="let move of game.moveTokens; let idx = index"
                            [value]="idx"
                            [selected]="idx === selectedMoveIndex"
                          >
                            {{ moveOptionLabel(idx, move) }}
                          </option>
                        </select>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </section>
      <app-footer></app-footer>
    </div>
  `,
})
export class GameDetailPage {
  @ViewChild('xiangqiBoard') private xiangqiBoard?: XiangqiBoardComponent;

  readonly boardComponentId = `game-view-board-${Math.random().toString(36).slice(2)}`;
  private readonly route = inject(ActivatedRoute);
  private readonly location = inject(Location);
  private readonly content = inject(ContentService);

  selectedMoveIndex = -1;
  private currentBoardMoves: XiangqiMove[] = [];
  private currentMoveTokens: string[] = [];

  readonly game$ = this.content.getGameById(this.route.snapshot.paramMap.get('id')).pipe(
    map((game) => {
      if (!game) {
        return null;
      }

      const moveTokens = this.toMoveTokens(game.move_list);
      const boardMoves = this.toBoardMoves(game.move_list);
      this.currentMoveTokens = moveTokens;
      this.currentBoardMoves = boardMoves;
      this.selectedMoveIndex = -1;
      return {
        ...game,
        boardMoves,
        moveTokens
      };
    })
  );

  goBack(): void {
    this.location.back();
  }

  boardPosition(): string {
    return 'start';
  }

  selectMove(index: number | string): void {
    const moveIndex = Number(index);
    if (moveIndex < 0) {
      this.xiangqiBoard?.reset();
      this.selectedMoveIndex = moveIndex;
      return;
    }
    if (Number.isNaN(moveIndex) || moveIndex < 0) {
      return;
    }
    const move = this.currentBoardMoves[moveIndex];
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

  moveOptionLabel(index: number, move: string): string {
    const label = index % 2 === 0 ? String(Math.floor(index / 2) + 1) : '';
    const paddedLabel = label.padStart(2, '\u00A0');
    return `${paddedLabel} ${move}`;
  }

  private toMoveTokens(moveList: string): string[] {
    return moveList
      .split(',')
      .map((move) => move.trim())
      .filter(Boolean);
  }

  private toBoardMoves(moveList: string): XiangqiMove[] {
    return XiangqBoardUtils.parseMoveNotationList(moveList);
  }

  private moveCount(): number {
    return this.currentMoveTokens.length;
  }
}
