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
} from '../components/xiangqi-board.standalone.component';
import { MockContentService } from '../core/mock-content.service';

@Component({
  standalone: true,
  imports: [AsyncPipe, NgFor, NgIf, RouterLink, HeaderComponent, FooterComponent, XiangqiBoardComponent],
  template: `
    <div class="homepage">
      <app-header></app-header>
      <section class="search-center search-center--compact">
        <div class="search-container" *ngIf="game$ | async as game">
          <div class="page-with-back">
            <button class="back-link" type="button" (click)="goBack()" aria-label="Go back"><- Quay lai</button>

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

                <div class="game-board-layout">
                  <xiangqi-board
                    #xiangqiBoard
                    class="game-board"
                    position="start"
                    [moves]="game.boardMoves"
                    [viewOnly]="true"
                  ></xiangqi-board>

                  <div class="move-list-panel" *ngIf="game.moveTokens.length">
                    <select
                      class="move-list-box"
                      [value]="selectedMoveIndex"
                      (change)="selectMove($any($event.target).value)"
                      size="16"
                    >
                      <option *ngFor="let move of game.moveTokens; let idx = index" [value]="idx">
                        {{ moveOptionLabel(idx, move) }}
                      </option>
                    </select>
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

  private readonly route = inject(ActivatedRoute);
  private readonly location = inject(Location);
  private readonly mockContent = inject(MockContentService);

  selectedMoveIndex = -1;

  readonly game$ = this.mockContent.getGameById(this.route.snapshot.paramMap.get('id')).pipe(
    map((game) => {
      if (!game) {
        return null;
      }

      const moveTokens = this.toMoveTokens(game.move_list);
      const boardMoves = this.toBoardMoves(game.move_list);
      return {
        ...game,
        boardMoves,
        moveTokens: moveTokens
      };
    })
  );

  goBack(): void {
    this.location.back();
  }

  selectMove(index: number): void {
    const moveIndex = Number(index);
    if (Number.isNaN(moveIndex) || moveIndex < 0) {
      return;
    }
    this.selectedMoveIndex = moveIndex;
    this.xiangqiBoard?.boardCtrl.setMoveCursor(moveIndex);
  }

  moveOptionLabel(index: number, move: string): string {
    const label = index % 2 === 0 ? String(Math.floor(index / 2) + 1) : '';
    const paddedLabel = label.padStart(2, '\u00A0');
    return `${paddedLabel} ${move}`;
  }

  private toBoardMoves(moveList: string): XiangqiMove[] {
    try {
      return XiangqBoardUtils.parseMoveList(moveList, 'start');
    } catch {
      return [];
    }
  }

  private toMoveTokens(moveList: string): string[] {
    return moveList
      .split(',')
      .map((move) => move.trim())
      .filter(Boolean);
  }
}
