import {
  AfterViewInit,
  Component,
  ElementRef,
  Injectable,
  Input,
  OnChanges,
  OnDestroy,
  OnInit,
  QueryList,
  SimpleChanges,
  ViewChild,
  ViewChildren,
} from '@angular/core';
import { CommonModule } from '@angular/common';
import { Observable, Subject, Subscription } from 'rxjs';

/*
 * Self-contained Xiangqi board extracted from the original project.
 *
 * Bundled into this single file:
 * - XiangqiBoardComponent
 * - XiangqiPieceComponent
 * - XiangqiBoardController
 * - XiangqiBoard / XiangqiPiece models
 * - board config + board/ui utils
 */

export const ROW_TOP = 9;
export const ROW_LOW = 0;
export const ROW_LENGTH = ROW_TOP - ROW_LOW + 1;
export const COLUMNS = 'abcdefghi'.split('');
export const START_FEN = 'rnbakabnr/9/1c5c1/p1p1p1p1p/9/9/P1P1P1P1P/1C5C1/9/RNBAKABNR w';
export const OUTER_BOARD_WIDTH = 560;
export const OUTER_BOARD_HEIGHT = 644;
export const INNER_BOARD_WIDTH = 448;
export const INNER_BOARD_HEIGHT = 504;
export const SQUARE_WIDTH = INNER_BOARD_WIDTH / (COLUMNS.length - 1);
export const SQUARE_HEIGHT = INNER_BOARD_HEIGHT / (ROW_LENGTH - 1);
export const PIECES = 'RNBAKABNRCCPPPPP';
export type XiangqiMove = { from: string; to: string; code?: string; target?: string; position?: string };

export interface XiangqiBoardContext {
  orientation: string;
  scaleX: number;
  scaleY: number;
}

export class XiangqiPiece {
  hidden = false;
  pos = '';
  code = '';
  id = '';

  get Color(): string {
    return this.code[0];
  }

  isBlack(): boolean {
    return this.code.startsWith('b');
  }

  isRed(): boolean {
    return this.code.startsWith('r');
  }
}

export class XiangqiBoard implements Iterable<XiangqiPiece> {
  pieces: XiangqiPiece[] = [];
  position: Record<string, string> = {};
  firstPlayer: 'r' | 'b' = 'r';

  constructor() {
    const pieces = PIECES.split('');
    for (let i = 0; i < pieces.length; i++) {
      const redPiece = new XiangqiPiece();
      redPiece.id = XiangqBoardUtils.uuid();
      redPiece.code = `r${pieces[i]}`;
      this.pieces.push(redPiece);

      const blackPiece = new XiangqiPiece();
      blackPiece.id = XiangqBoardUtils.uuid();
      blackPiece.code = `b${pieces[i]}`;
      this.pieces.push(blackPiece);
    }
    this.reset();
  }

  erase(): void {
    for (const piece of this.pieces) {
      piece.pos = '';
      piece.hidden = true;
    }
  }

  reset(): void {
    this.erase();
    this.CurrentPosition = 'start';
  }

  private applyPosition(): void {
    this.erase();

    for (const [key, val] of Object.entries(this.position)) {
      for (const piece of this.pieces) {
        if (piece.code === val && !piece.pos) {
          piece.pos = key;
          piece.hidden = false;
          break;
        }
      }
    }
  }

  set CurrentPosition(value: string | Record<string, string>) {
    this.firstPlayer = 'r';

    if (value === 'start') {
      value = START_FEN;
    }

    if (typeof value === 'object' && value !== null) {
      this.position = value;
      this.applyPosition();
      return;
    }
    if (XiangqBoardUtils.validFen(value)) {
      this.position = XiangqBoardUtils.fenToObj(value);
      if (value.endsWith('w') || value.endsWith('r')) this.firstPlayer = 'r';
      else if (value.endsWith('b')) this.firstPlayer = 'b';
    }

    this.applyPosition();
  }

  get CurrentPosition(): string {
    const positions: Record<string, string> = {};
    for (const piece of this.pieces) {
      if (!piece.hidden) positions[piece.pos] = piece.code;
    }
    return XiangqBoardUtils.objToFen(positions);
  }

  getPieceAtPos(pos: string): XiangqiPiece | null {
    for (const piece of this.pieces) {
      if (piece.pos === pos && !piece.hidden) return piece;
    }
    return null;
  }

  getPieceById(id: string): XiangqiPiece | undefined {
    return this.pieces.find((piece) => piece.id === id);
  }

  [Symbol.iterator](): Iterator<XiangqiPiece> {
    let index = 0;
    const board = this;
    return {
      next(): IteratorResult<XiangqiPiece> {
        if (index < board.pieces.length) {
          return { value: board.pieces[index++], done: false };
        }
        return { value: undefined as never, done: true };
      },
    };
  }
}

export class XiangqBoardUtils {
  private static readonly notationToPieceCode: Record<string, string> = {
    P: 'C',
    M: 'N',
    X: 'R',
    V: 'B',
    S: 'A',
    T: 'K',
  };

  static uuid(): string {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
      const r = (Math.random() * 16) | 0;
      const v = c === 'x' ? r : (r & 0x3) | 0x8;
      return v.toString(16);
    });
  }

  static expandFenEmptySquares(fen: string): string {
    return fen
      .replace(/9/g, '111111111')
      .replace(/8/g, '11111111')
      .replace(/7/g, '1111111')
      .replace(/6/g, '111111')
      .replace(/5/g, '11111')
      .replace(/4/g, '1111')
      .replace(/3/g, '111')
      .replace(/2/g, '11');
  }

  static squeezeFenEmptySquares(fen: string): string {
    return fen
      .replace(/111111111/g, '9')
      .replace(/11111111/g, '8')
      .replace(/1111111/g, '7')
      .replace(/111111/g, '6')
      .replace(/11111/g, '5')
      .replace(/1111/g, '4')
      .replace(/111/g, '3')
      .replace(/11/g, '2');
  }

  static validFen(fen: string): boolean {
    if (!fen) return false;
    fen = fen.replace(/ .+$/, '');
    fen = XiangqBoardUtils.expandFenEmptySquares(fen);
    const chunks = fen.split('/');
    if (chunks.length !== ROW_LENGTH) return false;

    for (let i = 0; i < ROW_LENGTH; i++) {
      if (chunks[i].length !== COLUMNS.length || chunks[i].search(/[^kabnrcpKABNRCP1]/) !== -1) {
        console.log(`Invalid FEN chunk at row ${i}:`, chunks[i]);
        return false;
      }
    }
    return true;
  }

  static fenToPieceCode(piece: string): string {
    if (piece.toLowerCase() === piece) {
      return 'b' + piece.toUpperCase();
    }
    return 'r' + piece.toUpperCase();
  }

  static fenToObj(fen: string): Record<string, string> {
    if (!XiangqBoardUtils.validFen(fen)) {
      throw new Error('Invalid FEN');
    }

    fen = fen.replace(/ .+$/, '');
    const rows = fen.split('/');
    const position: Record<string, string> = {};

    let currentRow = ROW_TOP;
    for (let i = 0; i < ROW_LENGTH; i++) {
      const row = rows[i].split('');
      let colIdx = 0;

      for (let j = 0; j < row.length; j++) {
        if (/[1-9]/.test(row[j])) {
          colIdx += Number.parseInt(row[j], 10);
        } else {
          const square = COLUMNS[colIdx] + currentRow;
          position[square] = XiangqBoardUtils.fenToPieceCode(row[j]);
          colIdx += 1;
        }
      }
      currentRow -= 1;
    }
    return position;
  }

  static pieceCodeToFen(pieceId: string): string {
    const pieceCodeLetters = pieceId.split('');
    if (pieceCodeLetters[0] === 'b') {
      return pieceCodeLetters[1].toLowerCase();
    }
    return pieceCodeLetters[1].toUpperCase();
  }

  static isValidPos(pos: string): boolean {
    try {
      const col = pos[0].charCodeAt(0) - 'a'.charCodeAt(0);
      const row = Number.parseInt(pos[1], 10);
      if (ROW_LENGTH - row - 1 < 0) return false;
      if (COLUMNS.length - 1 - col < 0) return false;
    } catch {
      return false;
    }
    return true;
  }

  static objToFen(obj: Record<string, string>): string {
    let fen = '';
    let currentRow = ROW_TOP;
    for (let i = 0; i < ROW_LENGTH; i++) {
      for (let j = 0; j < COLUMNS.length; j++) {
        const square = COLUMNS[j] + currentRow;
        fen += obj[square] ? XiangqBoardUtils.pieceCodeToFen(obj[square]) : '1';
      }
      if (i !== ROW_TOP) {
        fen += '/';
      }
      currentRow -= 1;
    }
    return XiangqBoardUtils.squeezeFenEmptySquares(fen);
  }

  static parseMoveList(
    moveList: string,
    initialPosition: string | Record<string, string> = 'start'
  ): XiangqiMove[] {
    const board = new XiangqiBoard();
    board.CurrentPosition = initialPosition;
    const moves: XiangqiMove[] = [];
    const tokens = moveList
      .split(',')
      .map((token) => token.trim())
      .filter(Boolean);

    for (let index = 0; index < tokens.length; index++) {
      const player: 'r' | 'b' = index % 2 === 0 ? 'r' : 'b';
      const move = this.parseNotationMove(board, tokens[index], player);
      console.log('Parsed move:', tokens[index], move); 
      if (!move) {
        continue;
      }

      const targetPiece = board.getPieceAtPos(move.to);
      if (targetPiece) {
        targetPiece.hidden = true;
        move.target = targetPiece.code;
      }

      const piece = board.getPieceAtPos(move.from);
      if (!piece) {
        continue;
      }

      move.code = piece.code;
      piece.pos = move.to;
      moves.push(move);
    }
    return moves;
  }

  private static parseNotationMove(board: XiangqiBoard, notation: string, player: 'r' | 'b'): XiangqiMove | null {
    const normalized = notation.replace(/\s+/g, '').toUpperCase();
    const match = normalized.match(/^([PMXBST])(\d)([.+-])(\d)$/);
    if (!match) {
      console.log('Invalid move notation:', notation);
      return null;
    }

    const [, pieceLetter, sourceFileText, operator, targetText] = match;
    const pieceCode = this.notationToPieceCode[pieceLetter];
    if (!pieceCode) {
      console.log('Invalid piece letter in notation:', pieceLetter);
      return null;
    }

    const sourceColumn = this.fileToColumnIndex(Number.parseInt(sourceFileText, 10));
    if (sourceColumn < 0) {
      console.log('Invalid source file in notation:', sourceFileText);
      return null;
    }

    const candidates = [...board]
      .filter((piece) => !piece.hidden && piece.code === `${player}${pieceCode}`)
      .filter((piece) => piece.pos[0] === COLUMNS[sourceColumn]);

    if (candidates.length === 0) {
      console.log('No piece found for move notation:', notation);
      return null;
    }

    for (const piece of candidates) {
      const to = this.resolveDestination(piece, operator, Number.parseInt(targetText, 10), player);
      if (!to) {
        console.log('Invalid destination for move notation:', notation);

        continue;
      }

      return { from: piece.pos, to };
    }
    console.log('No valid piece found for move notation:', notation);
    return null;
  }

  private static resolveDestination(
    piece: XiangqiPiece,
    operator: string,
    target: number,
    player: 'r' | 'b'
  ): string | null {
    const col = piece.pos[0].charCodeAt(0) - 'a'.charCodeAt(0);
    const row = Number.parseInt(piece.pos.slice(1), 10);
    const forward = player === 'r' ? 1 : -1;

    if (operator === '.') {
      const targetColumn = this.fileToColumnIndex(target);
      if (targetColumn < 0) {
        return null;
      }

      if (piece.code.endsWith('R') || piece.code.endsWith('C') || piece.code.endsWith('K')) {
        return `${COLUMNS[targetColumn]}${row}`;
      }

      if (piece.code.endsWith('N')) {
        const rowDelta = Math.abs(targetColumn - col) === 1 ? 2 : Math.abs(targetColumn - col) === 2 ? 1 : 0;
        console.log('Calculated row delta for Knight move:', piece, operator, target, player, row, col ,rowDelta );
        if (!rowDelta) {
          return null;
        }

        const primaryRow = row + forward * rowDelta;
        if (this.isValidSquare(targetColumn, primaryRow)) {
          return `${COLUMNS[targetColumn]}${primaryRow}`;
        }

        const alternateRow = row - forward * rowDelta;
        return this.isValidSquare(targetColumn, alternateRow) ? `${COLUMNS[targetColumn]}${alternateRow}` : null;
      }

      if (piece.code.endsWith('B')) {
        const candidateRows = [row + forward * 2, row - forward * 2];
        for (const candidateRow of candidateRows) {
          if (Math.abs(targetColumn - col) === 2 && this.isValidSquare(targetColumn, candidateRow)) {
            return `${COLUMNS[targetColumn]}${candidateRow}`;
          }
        }
        return null;
      }

      if (piece.code.endsWith('A')) {
        const candidateRows = [row + forward, row - forward];
        for (const candidateRow of candidateRows) {
          if (Math.abs(targetColumn - col) === 1 && this.isValidSquare(targetColumn, candidateRow)) {
            return `${COLUMNS[targetColumn]}${candidateRow}`;
          }
        }
      }

      return null;
    }

    const distance = Number.parseInt(String(target), 10);
    if (!distance) {
      return null;
    }

    const direction = operator === '+' ? forward : -forward;

    if (piece.code.endsWith('R') || piece.code.endsWith('C') || piece.code.endsWith('K') || piece.code.endsWith('P')) {
      const targetRow = row + direction * distance;
      return this.isValidSquare(col, targetRow) ? `${COLUMNS[col]}${targetRow}` : null;
    }

    if (piece.code.endsWith('N')) {
      const targetColumn = this.fileToColumnIndex(target);
      const rowDelta = Math.abs(targetColumn - col) === 1 ? 2 : Math.abs(targetColumn - col) === 2 ? 1 : 0;
      const targetRow = row + direction * rowDelta;
      return this.isValidSquare(targetColumn, targetRow) ? `${COLUMNS[targetColumn]}${targetRow}` : null;
    }

    if (piece.code.endsWith('B')) {
      const targetColumn = this.fileToColumnIndex(target);
      const targetRow = row + direction * 2;
      return this.isValidSquare(targetColumn, targetRow) ? `${COLUMNS[targetColumn]}${targetRow}` : null;
    }

    if (piece.code.endsWith('A')) {
      const targetColumn = this.fileToColumnIndex(target);
      const targetRow = row + direction;
      return this.isValidSquare(targetColumn, targetRow) ? `${COLUMNS[targetColumn]}${targetRow}` : null;
    }

    return null;
  }

  private static fileToColumnIndex(file: number): number {
    if (file < 1 || file > COLUMNS.length) {
      return -1;
    }

    return COLUMNS.length - file;
  }

  private static isValidSquare(col: number, row: number): boolean {
    return col >= 0 && col < COLUMNS.length && row >= ROW_LOW && row <= ROW_TOP;
  }
}

export class XiangqiBoardUIUtils {
  static posToCoordinate(context: XiangqiBoardContext, pos: string) {
    const col = pos[0].charCodeAt(0) - 'a'.charCodeAt(0);
    const row = Number.parseInt(pos[1], 10);
    let top = SQUARE_HEIGHT * row;
    let left = SQUARE_WIDTH * (COLUMNS.length - 1 - col);

    if (context.orientation === 'red') {
      top = SQUARE_HEIGHT * (ROW_LENGTH - row - 1);
      left = SQUARE_WIDTH * col;
    }

    top -= SQUARE_HEIGHT / 2;
    left -= SQUARE_WIDTH / 2;

    return {
      col,
      row,
      top: XiangqiBoardUIUtils.transformY(context, top),
      left: XiangqiBoardUIUtils.transformX(context, left),
    };
  }

  static transformX(context: XiangqiBoardContext, x: number): number {
    const offsetX = (OUTER_BOARD_WIDTH - INNER_BOARD_WIDTH) / 2;
    return Math.floor((x + offsetX) * context.scaleX);
  }

  static transformY(context: XiangqiBoardContext, y: number): number {
    const offsetY = (OUTER_BOARD_HEIGHT - INNER_BOARD_HEIGHT) / 2;
    return Math.floor((y + offsetY) * context.scaleY);
  }

  static coordinateToPos(context: XiangqiBoardContext, x: number, y: number): string {
    const offsetX = (OUTER_BOARD_WIDTH - SQUARE_WIDTH * COLUMNS.length) / 2;
    const offsetY = (OUTER_BOARD_HEIGHT - SQUARE_HEIGHT * ROW_LENGTH) / 2;
    const col = COLUMNS[Math.floor((x / context.scaleX - offsetX) / SQUARE_WIDTH)];
    const row = ROW_LENGTH - Math.floor((y / context.scaleY - offsetY) / SQUARE_HEIGHT) - 1;
    return col + row;
  }
}

@Injectable()
export class XiangqiBoardController {
  board: XiangqiBoard;
  moves: Array<{ from: string; to: string; code?: string; target?: string; position?: string }> = [];
  moveCursor = -1;
  mode: 'edit' | 'play' = 'edit';
  originalPosition = START_FEN;

  private pieceMoveReceiver = new Subject<unknown>();
  pieceMove: Observable<unknown> = this.pieceMoveReceiver.asObservable();

  private pieceCapturedReceiver = new Subject<unknown>();
  pieceCaptured: Observable<unknown> = this.pieceCapturedReceiver.asObservable();

  private positionChangeReceiver = new Subject<void>();
  positionChange: Observable<void> = this.positionChangeReceiver.asObservable();

  constructor() {
    this.board = new XiangqiBoard();
    this.reset();
  }

  fireEvents(): void {
    this.positionChangeReceiver.next();
  }

  setUp(
    position: string | Record<string, string>,
    moves?: Array<{ from: string; to: string; code?: string; target?: string; position?: string }>
  ): void {
    if (position === 'start') position = START_FEN;

    this.board.erase();
    this.board.CurrentPosition = position;
    this.originalPosition = this.board.CurrentPosition;
    this.moves = [];
    this.moveCursor = -1;
    console.log('Board set up with moves:',moves);
    if (moves) {
      for (const move of moves) {
        this.takeMove({ ...move });
      }
    }

    this.resetMoveCursor();
    this.fireEvents();
  }

  reset(): void {
    this.setUp('start', []);
    this.mode = 'edit';
    this.fireEvents();
  }

  setEditMode(): void {
    this.mode = 'edit';
  }

  setPlayMode(): void {
    this.mode = 'play';
  }

  canMovePiece(pieceId: string): boolean {
    if (this.mode === 'edit') return true;
    const piece = this.board.getPieceById(pieceId);
    if (!piece) return false;
    return this.nextPlayer() === piece.Color;
  }

  canTakeMove(move: { from: string; to: string; code?: string }): boolean {
    if (this.mode === 'edit') return true;
    const fromPiece = this.board.getPieceAtPos(move.from);
    if (!fromPiece) return false;
    if (!this.canMovePiece(fromPiece.id)) return false;
    return true;
  }

  takeMove(move: { from: string; to: string; code?: string; target?: string; position?: string }): void {
    if (!this.canTakeMove(move)) return;

    this.moves = this.moves.slice(0, this.moveCursor + 1);
    this.moves.push(move);
    this.moveCursor++;

    for (const piece of this.board) {
      if (piece.pos === move.to && !piece.hidden) {
        move.target = piece.code;
        piece.hidden = true;
        this.pieceCapturedReceiver.next(piece);
      }
      if (piece.pos === move.from && !piece.hidden) {
        move.code = piece.code;
        piece.pos = move.to;
        this.pieceMoveReceiver.next(move);
      }
    }

    move.position = this.currentFen();
    this.fireEvents();
  }

  setMoveCursor(idx: number): void {
    if (idx < 0) {
      this.moveCursor = -1;
      this.board.CurrentPosition = this.originalPosition;
    } else {
      const move = this.moves[idx];
      if (!move) {
        this.moveCursor = this.moves.length - 1;
        this.board.CurrentPosition =
          this.moveCursor >= 0 ? this.moves[this.moveCursor].position ?? this.originalPosition : this.originalPosition;
      } else {
        this.moveCursor = idx;
        this.board.CurrentPosition = move.position ?? this.originalPosition;
      }
    }
    this.fireEvents();
  }

  resetMoveCursor(): void {
    this.setMoveCursor(this.moves.length - 1);
  }

  nextPlayer(): 'r' | 'b' {
    if (this.moveCursor < 0) return this.board.firstPlayer;
    return this.moveCursor % 2 === 0 ? (this.board.firstPlayer === 'r' ? 'b' : 'r') : this.board.firstPlayer;
  }

  currentFen(): string {
    return this.board.CurrentPosition;
  }
}

@Component({
  selector: 'xiangqi-piece',
  standalone: true,
  imports: [CommonModule],
  template: `
    <img
      [src]="'/assets/theme/images/chess/pieces/' + piece.code + '.svg'"
      [hidden]="!piece || piece.hidden"
      [width]="width"
      [height]="height"
      class="xiangqi-piece"
      [style.top.px]="top"
      [style.left.px]="left"
      [ngClass]="{ border: selected }"
      alt=""
    />
  `,
  styles: [
    `
      .xiangqi-piece {
        position: absolute;
      }

      .border {
        border-style: solid;
        border-width: 1px;
        border-color: yellow;
      }
    `,
  ],
})
export class XiangqiPieceComponent {
  top = 0;
  left = 0;
  width = 16;
  height = 16;
  selected = false;
  context!: XiangqiBoardContext;

  @Input() componentId = '';

  piece: XiangqiPiece = new XiangqiPiece();

  render(context: XiangqiBoardContext, pieceInfo: XiangqiPiece): void {
    this.context = context;
    this.selected = false;
    this.width = Math.floor(SQUARE_WIDTH * this.context.scaleX);
    this.height = Math.floor(SQUARE_HEIGHT * this.context.scaleY);
    this.piece = pieceInfo;
  }

  draw(): void {
    if (!this.context || this.piece.hidden || !this.piece.pos) return;

    const coord = XiangqiBoardUIUtils.posToCoordinate(this.context, this.piece.pos);
    this.top = coord.top;
    this.left = coord.left;
  }

  select(): void {
    this.selected = true;
  }

  unselect(): void {
    this.selected = false;
  }
}

@Component({
  selector: 'xiangqi-board',
  standalone: true,
  imports: [CommonModule, XiangqiPieceComponent],
  providers: [XiangqiBoardController],
  template: `
    <div class="xiangqiboard">
      <div class="board" #boardContainer>
        <img
          src="/assets/theme/images/chess/board.png"
          (mousedown)="clickBoard($event)"
          #boardDOM
          alt="Xiangqi board"
        />

        <xiangqi-piece
          *ngFor="let piece of Board"
          [componentId]="piece.id"
          (click)="selectPiece(piece.id); $event.stopPropagation()"
        ></xiangqi-piece>
      </div>
    </div>
  `,
  styles: [
    `
      .xiangqiboard {
        width: 100%;
        padding: 0;
        margin: 0;
      }

      .board {
        width: 100%;
        position: relative;
      }

      .board > img {
        width: 100%;
        display: block;
      }
    `,
  ],
})
export class XiangqiBoardComponent implements AfterViewInit, OnChanges, OnDestroy, XiangqiBoardContext, OnInit {
  @Input() position: string | Record<string, string> = 'start';
  @Input() moves: Array<{ from: string; to: string; code?: string; target?: string; position?: string }> = [];
  @Input() orientation: 'red' | 'black' = 'red';
  @Input() viewOnly = false;

  @ViewChild('boardDOM') boardDOM!: ElementRef<HTMLImageElement>;
  @ViewChild('boardContainer') boardContainer!: ElementRef<HTMLElement>;
  @ViewChildren(XiangqiPieceComponent) pieceComponentList!: QueryList<XiangqiPieceComponent>;

  scaleX = 1;
  scaleY = 1;

  private selectedPieceComponent: XiangqiPieceComponent | null = null;
  private positionChangeSub?: Subscription;
  private pieceListSub?: Subscription;
  private initialized = false;

  private pieceSelectedReceiver = new Subject<string>();
  pieceSelected: Observable<string> = this.pieceSelectedReceiver.asObservable();

  private pieceDeselectedReceiver = new Subject<string>();
  pieceDeselected: Observable<string> = this.pieceDeselectedReceiver.asObservable();

  private boardClickedReceiver = new Subject<string>();
  boardClicked: Observable<string> = this.boardClickedReceiver.asObservable();

  private pieceMoveReceiver = new Subject<unknown>();
  pieceMove: Observable<unknown> = this.pieceMoveReceiver.asObservable();

  constructor(public boardCtrl: XiangqiBoardController) {}

  ngOnInit(): void {
    this.positionChangeSub = this.boardCtrl.positionChange.subscribe(() => {
      this.draw();
    });
  }

  ngAfterViewInit(): void {
    this.pieceListSub = this.pieceComponentList.changes.subscribe(() => {
      if (this.initialized) {
        this.drawBoard();
      }
    });

    queueMicrotask(() => {
      this.initialized = true;
      this.applyInputs();
      this.drawBoard();
    });
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (!this.initialized) return;
    if (changes['position'] || changes['moves'] || changes['orientation'] || changes['viewOnly']) {
      this.applyInputs();
      this.drawBoard();
    }
  }

  ngOnDestroy(): void {
    this.positionChangeSub?.unsubscribe();
    this.pieceListSub?.unsubscribe();
  }

  get Board(): XiangqiBoard {
    return this.boardCtrl.board;
  }

  private applyInputs(): void {
    this.boardCtrl.setUp(this.position, this.moves);
    if (this.viewOnly) {
      this.boardCtrl.setPlayMode();
    }
    this.updateScale();
  }

  private updateScale(): void {
    if (!this.boardDOM?.nativeElement) return;
    this.scaleX = this.boardDOM.nativeElement.offsetWidth / OUTER_BOARD_WIDTH;
    this.scaleY = this.boardDOM.nativeElement.offsetHeight / OUTER_BOARD_HEIGHT;
  }

  private drawBoard(): void {
    this.updateScale();
    const pieceComponents = this.pieceComponentList.toArray();

    for (const pieceComponent of pieceComponents) {
      for (const pieceInfo of this.Board) {
        if (pieceInfo.id === pieceComponent.componentId) {
          pieceComponent.render(this, pieceInfo);
          break;
        }
      }
    }

    this.draw();
  }

  draw(): void {
    for (const piece of this.pieceComponentList.toArray()) {
      piece.draw();
    }
  }

  clickBoard(event: MouseEvent & { offsetX: number; offsetY: number }): void {
    if (this.viewOnly) return;
    const pos = XiangqiBoardUIUtils.coordinateToPos(this, event.offsetX, event.offsetY);
    if (!XiangqBoardUtils.isValidPos(pos)) return;
    this.selectBoardSquare(pos);
  }

  selectBoardSquare(pos: string): void {
    if (this.viewOnly) return;

    if (this.selectedPieceComponent) {
      const move = {
        to: pos,
        from: this.selectedPieceComponent.piece.pos,
        code: this.selectedPieceComponent.piece.code,
      };
      if (!this.boardCtrl.canTakeMove(move)) return;
      this.boardCtrl.takeMove(move);
      this.pieceMoveReceiver.next(move);
      this.unselectPiece();
    } else {
      const pieceComponent = this.getPieceComponentAtPos(pos);
      if (pieceComponent && !pieceComponent.piece.hidden) {
        this.selectedPieceComponent = pieceComponent;
        this.pieceSelectedReceiver.next(this.selectedPieceComponent.componentId);
        this.selectedPieceComponent.select();
      }
    }

    this.boardClickedReceiver.next(pos);
  }

  unselectPiece(): void {
    if (this.selectedPieceComponent) {
      this.pieceDeselectedReceiver.next(this.selectedPieceComponent.componentId);
      this.selectedPieceComponent.unselect();
      this.selectedPieceComponent = null;
    }
  }

  selectPiece(pieceId: string): void {
    if (this.viewOnly) return;

    const pieceComponent = this.getPieceComponentById(pieceId);
    if (!pieceComponent) return;

    if (this.selectedPieceComponent && this.selectedPieceComponent.piece.id === pieceId) {
      this.unselectPiece();
      return;
    }

    if (
      this.selectedPieceComponent &&
      this.selectedPieceComponent.piece.Color !== pieceComponent.piece.Color
    ) {
      const move = {
        to: pieceComponent.piece.pos,
        from: this.selectedPieceComponent.piece.pos,
        code: this.selectedPieceComponent.piece.code,
      };
      if (!this.boardCtrl.canTakeMove(move)) return;
      this.boardCtrl.takeMove(move);
      this.pieceMoveReceiver.next(move);
      this.unselectPiece();
    } else {
      if (!this.boardCtrl.canMovePiece(pieceId)) return;
      if (this.selectedPieceComponent) this.selectedPieceComponent.unselect();
      this.selectedPieceComponent = pieceComponent;
      this.pieceSelectedReceiver.next(this.selectedPieceComponent.componentId);
      this.selectedPieceComponent.select();
    }
  }

  private getPieceComponentAtPos(pos: string): XiangqiPieceComponent | null {
    for (const pieceComponent of this.pieceComponentList.toArray()) {
      if (pieceComponent.piece.pos === pos && !pieceComponent.piece.hidden) {
        return pieceComponent;
      }
    }
    return null;
  }

  private getPieceComponentById(id: string): XiangqiPieceComponent | undefined {
    return this.pieceComponentList.toArray().find((pieceComponent) => pieceComponent.piece.id === id);
  }
}
