import {
  AfterViewInit,
  Component,
  DoCheck,
  EventEmitter,
  Input,
  NgModule,
  OnChanges,
  OnDestroy,
  OnInit,
  Output,
  SimpleChanges,
} from "@angular/core";
import { CommonModule } from "@angular/common";
import { Observable, Subject } from "rxjs";

/*
 * Self-contained Xiangqi board extracted from the original project.
 *
 * Bundled into this single file:
 * - XiangqiBoardComponent
 * - XiangqiPieceComponent
 * - XiangqiBoard / XiangqiPiece models
 * - board config + board/ui utils
 */
export type XiangqiBoardGUIContext = {
  orientation: string;
  scaleX: number;
  scaleY: number;
};

export const ROW_TOP = 9;
export const ROW_LOW = 0;
export const ROW_LENGTH = ROW_TOP - ROW_LOW + 1;
export const COLUMN_LENGTH = 9;
export const RED_COLUMNS: Record<string, number> = {
  "9": 0,
  "8": 1,
  "7": 2,
  "6": 3,
  "5": 4,
  "4": 5,
  "3": 6,
  "2": 7,
  "1": 8,
};
export const BLACK_COLUMNS: Record<string, number> = {
  "1": 0,
  "2": 1,
  "3": 2,
  "4": 3,
  "5": 4,
  "6": 5,
  "7": 6,
  "8": 7,
  "9": 8,
};
export const START_FEN =
  "xmvstsvmx/9/1p5p1/b1b1b1b1b/9/9/B1B1B1B1B/1P5P1/9/XMVSTSVMX r";
export const OUTER_BOARD_WIDTH = 560;
export const OUTER_BOARD_HEIGHT = 644;
export const INNER_BOARD_WIDTH = 448;
export const INNER_BOARD_HEIGHT = 504;
export const SQUARE_WIDTH = INNER_BOARD_WIDTH / (COLUMN_LENGTH - 1);
export const SQUARE_HEIGHT = INNER_BOARD_HEIGHT / (ROW_LENGTH - 1);
export const DEFAULT_POSITION = "start";
export const FIRST_PLAYER = "r";
export const SECOND_PLAYER = "b";
export const PIECES = "XMVSTSVMXPPBBBBB";
export const PIECE_CODE = {
  PAWN: "B",
  CANON: "P",
  ROCK: "X",
  HORSE: "M",
  ELEPHANT: "V",
  ADMINISTRATOR: "S",
  GENERAL: "T",
};
export const OPERATOR = {
  FORWARD: "+",
  BACKWARD: "/",
  LATERAL: "-",
};
export const MODIFIER = {
  FRONT: "t",
  MIDDLE: "g",
  BACK: "s",
};

export interface MoveResolver {
  resolveLateralMove(
    piece: XiangqiPiece,
    dstModifier: number,
    color: string,
    board: XiangqiBoard,
  ): XiangqiMove;
  resolveForwardMove(
    piece: XiangqiPiece,
    dstModifier: number,
    color: string,
    board: XiangqiBoard,
  ): XiangqiMove;
  resolveBackwardMove(
    piece: XiangqiPiece,
    dstModifier: number,
    color: string,
    board: XiangqiBoard,
  ): XiangqiMove;
}

class RockMoveResolver implements MoveResolver {
  resolveLateralMove(
    piece: XiangqiPiece,
    dstModifier: number,
    color: string,
    board: XiangqiBoard,
  ): XiangqiMove {
    // Expect notation like "3-5" where numbers are file numbers (1-based)
    const srcCol = piece.Col;
    const dstCol =
      color === FIRST_PLAYER
        ? RED_COLUMNS[dstModifier]
        : BLACK_COLUMNS[dstModifier];
    const srcRow = piece.Row;
    const dstRow = piece.Row;
    const row = color === FIRST_PLAYER ? ROW_LOW : ROW_TOP;
    return board.createMove(`${srcCol}${srcRow}`, `${dstCol}${dstRow}`);
  }

  resolveForwardMove(
    piece: XiangqiPiece,
    dstModifier: number,
    color: string,
    board: XiangqiBoard,
  ): XiangqiMove {
    // Expect notation to be a distance number (e.g. "3") or "from:dist" (e.g. "5:3")
    const srcCol = piece.Col;
    const dstCol = piece.Col;
    const srcRow = piece.Row;
    const dstRow =
      color === FIRST_PLAYER ? srcRow - dstModifier : srcRow + dstModifier;
    return board.createMove(`${srcCol}${srcRow}`, `${dstCol}${dstRow}`);
  }

  resolveBackwardMove(
    piece: XiangqiPiece,
    dstModifier: number,
    color: string,
    board: XiangqiBoard,
  ): XiangqiMove {
    // Backward is similar to forward but direction reversed
    const srcCol = piece.Col;
    const dstCol = piece.Col;
    const srcRow = piece.Row;
    const dstRow =
      color === FIRST_PLAYER ? srcRow + dstModifier : srcRow - dstModifier;
    return board.createMove(`${srcCol}${srcRow}`, `${dstCol}${dstRow}`);
  }
}

class CanonMoveResolver implements MoveResolver {
  resolveLateralMove(
    piece: XiangqiPiece,
    dstModifier: number,
    color: string,
    board: XiangqiBoard,
  ): XiangqiMove {
    // Expect notation like "3-5" where numbers are file numbers (1-based)
    const srcCol = piece.Col;
    const dstCol =
      color === FIRST_PLAYER
        ? RED_COLUMNS[dstModifier]
        : BLACK_COLUMNS[dstModifier];
    const srcRow = piece.Row;
    const dstRow = piece.Row;
    const row = color === FIRST_PLAYER ? ROW_LOW : ROW_TOP;
    return board.createMove(`${srcCol}${srcRow}`, `${dstCol}${dstRow}`);
  }

  resolveForwardMove(
    piece: XiangqiPiece,
    dstModifier: number,
    color: string,
    board: XiangqiBoard,
  ): XiangqiMove {
    // Expect notation to be a distance number (e.g. "3") or "from:dist" (e.g. "5:3")
    const srcCol = piece.Col;
    const dstCol = piece.Col;
    const srcRow = piece.Row;
    const dstRow =
      color === FIRST_PLAYER ? srcRow - dstModifier : srcRow + dstModifier;
    return board.createMove(`${srcCol}${srcRow}`, `${dstCol}${dstRow}`);
  }

  resolveBackwardMove(
    piece: XiangqiPiece,
    dstModifier: number,
    color: string,
    board: XiangqiBoard,
  ): XiangqiMove {
    // Backward is similar to forward but direction reversed
    const srcCol = piece.Col;
    const dstCol = piece.Col;
    const srcRow = piece.Row;
    const dstRow =
      color === FIRST_PLAYER ? srcRow + dstModifier : srcRow - dstModifier;
    return board.createMove(`${srcCol}${srcRow}`, `${dstCol}${dstRow}`);
  }
}

class PawnMoveResolver implements MoveResolver {
  resolveLateralMove(
    piece: XiangqiPiece,
    dstModifier: number,
    color: string,
    board: XiangqiBoard,
  ): XiangqiMove {
    // Expect notation like "3-5" where numbers are file numbers (1-based)
    const srcCol = piece.Col;
    const dstCol =
      color === FIRST_PLAYER
        ? RED_COLUMNS[dstModifier]
        : BLACK_COLUMNS[dstModifier];
    const srcRow = piece.Row;
    const dstRow = piece.Row;
    const row = color === FIRST_PLAYER ? ROW_LOW : ROW_TOP;
    return board.createMove(`${srcCol}${srcRow}`, `${dstCol}${dstRow}`);
  }

  resolveForwardMove(
    piece: XiangqiPiece,
    dstModifier: number,
    color: string,
    board: XiangqiBoard,
  ): XiangqiMove {
    // Expect notation to be a distance number (e.g. "3") or "from:dist" (e.g. "5:3")
    const srcCol = piece.Col;
    const dstCol = piece.Col;
    const srcRow = piece.Row;
    const dstRow =
      color === FIRST_PLAYER ? srcRow - dstModifier : srcRow + dstModifier;
    return board.createMove(`${srcCol}${srcRow}`, `${dstCol}${dstRow}`);
  }

  resolveBackwardMove(
    piece: XiangqiPiece,
    dstModifier: number,
    color: string,
    board: XiangqiBoard,
  ): XiangqiMove {
    // Backward is similar to forward but direction reversed
    const srcCol = piece.Col;
    const dstCol = piece.Col;
    const srcRow = piece.Row;
    const dstRow =
      color === FIRST_PLAYER ? srcRow + dstModifier : srcRow - dstModifier;
    return board.createMove(`${srcCol}${srcRow}`, `${dstCol}${dstRow}`);
  }
}

class GeneralMoveResolver implements MoveResolver {
  resolveLateralMove(
    piece: XiangqiPiece,
    dstModifier: number,
    color: string,
    board: XiangqiBoard,
  ): XiangqiMove {
    // Expect notation like "3-5" where numbers are file numbers (1-based)
    const srcCol = piece.Col;
    const dstCol =
      color === FIRST_PLAYER
        ? RED_COLUMNS[dstModifier]
        : BLACK_COLUMNS[dstModifier];
    const srcRow = piece.Row;
    const dstRow = piece.Row;
    const row = color === FIRST_PLAYER ? ROW_LOW : ROW_TOP;
    return board.createMove(`${srcCol}${srcRow}`, `${dstCol}${dstRow}`);
  }

  resolveForwardMove(
    piece: XiangqiPiece,
    dstModifier: number,
    color: string,
    board: XiangqiBoard,
  ): XiangqiMove {
    // Expect notation to be a distance number (e.g. "3") or "from:dist" (e.g. "5:3")
    const srcCol = piece.Col;
    const dstCol = piece.Col;
    const srcRow = piece.Row;
    const dstRow =
      color === FIRST_PLAYER ? srcRow - dstModifier : srcRow + dstModifier;
    return board.createMove(`${srcCol}${srcRow}`, `${dstCol}${dstRow}`);
  }

  resolveBackwardMove(
    piece: XiangqiPiece,
    dstModifier: number,
    color: string,
    board: XiangqiBoard,
  ): XiangqiMove {
    // Backward is similar to forward but direction reversed
    const srcCol = piece.Col;
    const dstCol = piece.Col;
    const srcRow = piece.Row;
    const dstRow =
      color === FIRST_PLAYER ? srcRow + dstModifier : srcRow - dstModifier;
    return board.createMove(`${srcCol}${srcRow}`, `${dstCol}${dstRow}`);
  }
}

class AdministratorMoveResolver implements MoveResolver {
  resolveLateralMove(
    piece: XiangqiPiece,
    dstModifier: number,
    color: string,
    board: XiangqiBoard,
  ): XiangqiMove {
    throw new Error("Administrator cannot move laterally");
  }

  resolveForwardMove(
    piece: XiangqiPiece,
    dstModifier: number,
    color: string,
    board: XiangqiBoard,
  ): XiangqiMove {
    // Expect notation to be a distance number (e.g. "3") or "from:dist" (e.g. "5:3")
    const srcCol = piece.Col;
    const dstCol = piece.Player == FIRST_PLAYER ? RED_COLUMNS[dstModifier]:BLACK_COLUMNS[dstModifier];
    const srcRow = piece.Row;
    const dstRow = color === FIRST_PLAYER ? srcRow - 1 : srcRow + 1;
    return board.createMove(`${srcCol}${srcRow}`, `${dstCol}${dstRow}`);
  }

  resolveBackwardMove(
    piece: XiangqiPiece,
    dstModifier: number,
    color: string,
    board: XiangqiBoard,
  ): XiangqiMove {
    // Backward is similar to forward but direction reversed
    const srcCol = piece.Col;
    const dstCol = piece.Player == FIRST_PLAYER ? RED_COLUMNS[dstModifier]:BLACK_COLUMNS[dstModifier];
    const srcRow = piece.Row;
    const dstRow = color === FIRST_PLAYER ? srcRow + 1 : srcRow - 1;
    return board.createMove(`${srcCol}${srcRow}`, `${dstCol}${dstRow}`);
  }
}

class ElephanMoveResolver implements MoveResolver {
  resolveLateralMove(
    piece: XiangqiPiece,
    dstModifier: number,
    color: string,
    board: XiangqiBoard,
  ): XiangqiMove {
    throw new Error("Elephant cannot move laterally");
  }

  resolveForwardMove(
    piece: XiangqiPiece,
    dstModifier: number,
    color: string,
    board: XiangqiBoard,
  ): XiangqiMove {
    // Expect notation to be a distance number (e.g. "3") or "from:dist" (e.g. "5:3")
    const srcCol = piece.Col;
    const dstCol = piece.Player == FIRST_PLAYER ? RED_COLUMNS[dstModifier]:BLACK_COLUMNS[dstModifier];
    const srcRow = piece.Row;
    const dstRow = color === FIRST_PLAYER ? srcRow - 2 : srcRow + 2;
    return board.createMove(`${srcCol}${srcRow}`, `${dstCol}${dstRow}`);
  }

  resolveBackwardMove(
    piece: XiangqiPiece,
    dstModifier: number,
    color: string,
    board: XiangqiBoard,
  ): XiangqiMove {
    // Backward is similar to forward but direction reversed
    const srcCol = piece.Col;
    const dstCol = piece.Player == FIRST_PLAYER ? RED_COLUMNS[dstModifier]:BLACK_COLUMNS[dstModifier];
    const srcRow = piece.Row;
    const dstRow = color === FIRST_PLAYER ? srcRow + 2 : srcRow - 2;
    return board.createMove(`${srcCol}${srcRow}`, `${dstCol}${dstRow}`);
  }
}

class HorseMoveResolver implements MoveResolver {
  resolveLateralMove(
    piece: XiangqiPiece,
    dstModifier: number,
    color: string,
    board: XiangqiBoard,
  ): XiangqiMove {
    throw new Error("Horse cannot move laterally");
  }

  resolveForwardMove(
    piece: XiangqiPiece,
    dstModifier: number,
    color: string,
    board: XiangqiBoard,
  ): XiangqiMove {
    // Expect notation to be a distance number (e.g. "3") or "from:dist" (e.g. "5:3")
    const srcCol = piece.Col;
    const dstCol = piece.Player == FIRST_PLAYER ? RED_COLUMNS[dstModifier]:BLACK_COLUMNS[dstModifier];
    const deltaRow = 3 - Math.abs(dstCol - piece.Col); // 1 or 2
    const srcRow = piece.Row;
    const dstRow =
      color === FIRST_PLAYER ? srcRow - deltaRow : srcRow + deltaRow;
    return board.createMove(`${srcCol}${srcRow}`, `${dstCol}${dstRow}`);
  }

  resolveBackwardMove(
    piece: XiangqiPiece,
    dstModifier: number,
    color: string,
    board: XiangqiBoard,
  ): XiangqiMove {
    // Backward is similar to forward but direction reversed
    const srcCol = piece.Col;
    const dstCol = piece.Player == FIRST_PLAYER ? RED_COLUMNS[dstModifier]:BLACK_COLUMNS[dstModifier];
    const deltaRow = 3 - Math.abs(dstCol - piece.Col); // 1 or 2
    const srcRow = piece.Row;
    const dstRow =
      color === FIRST_PLAYER ? srcRow + deltaRow : srcRow - deltaRow;
    return board.createMove(`${srcCol}${srcRow}`, `${dstCol}${dstRow}`);
  }
}

export class XiangqiPiece {
  Hidden: boolean;
  Pos: string;
  Code: string;
  Id: string;

  constructor(code: string, id: string, pos = "", hidden = false) {
    this.Pos = pos;
    this.Code = code;
    this.Id = id;
    this.Hidden = hidden;
  }

  get Player(): string {
    return this.Code[0];
  }

  get PieceCode(): string {
    return this.Code[1];
  }

  get isSecond(): boolean {
    return this.Player === SECOND_PLAYER;
  }

  get isFirst(): boolean {
    return this.Player === FIRST_PLAYER;
  }

  get Row(): number {
    return Number.parseInt(this.Pos[1], 10);
  }

  get Col(): number {
    return Number.parseInt(this.Pos[0], 10);
  }
}

export class XiangqiMove {
  From = "";
  To = "";
  Notation = "";
  Fen = "";

  get RowFrom(): number {
    return Number.parseInt(this.From[1], 10);
  }

  get RowTo(): number {
    return Number.parseInt(this.To[1], 10);
  }

  get ColFrom(): number {
    return Number.parseInt(this.From[0], 10);
  }

  get ColTo(): number {
    return Number.parseInt(this.To[0], 10);
  }
}

export class XiangqiBoard implements Iterable<XiangqiPiece> {
  private pieces: XiangqiPiece[] = [];

  constructor() {
    for (let i = 0; i < PIECES.length; i++) {
      const redPiece = new XiangqiPiece(
        `${FIRST_PLAYER}${PIECES[i]}`,
        XiangqBoardUtils.uuid(),
      );
      this.pieces.push(redPiece);
      const blackPiece = new XiangqiPiece(
        `${SECOND_PLAYER}${PIECES[i]}`,
        XiangqBoardUtils.uuid(),
      );
      this.pieces.push(blackPiece);
    }
  }

  erase(): void {
    for (const piece of this.pieces) {
      piece.Pos = "";
      piece.Hidden = true;
    }
  }

  set FEN(value: string) {
    if (value === DEFAULT_POSITION) {
      value = START_FEN;
    }
    if (XiangqBoardUtils.validFen(value)) {
      const position = XiangqBoardUtils.fenToObj(value);
      this.applyPosition(position);
    }
  }

  get FEN(): string {
    const positions: Record<string, string> = {};
    for (const piece of this.pieces) {
      if (!piece.Hidden) positions[piece.Pos] = piece.Code;
    }
    return XiangqBoardUtils.objToFen(positions);
  }

  getPieceAtPos(pos: string): XiangqiPiece | null {
    return (
      this.pieces.find((piece) => piece.Pos === pos && !piece.Hidden) || null
    );
  }

  getPieceById(id: string): XiangqiPiece | undefined {
    return this.pieces.find((piece) => piece.Id === id);
  }

  getColByCode(code: string) : number | null  {
    let col = null;
    for (const piece of this.VisiblePieces)
      if (piece.Code == code) {
            if (col && col != piece.Col)
                throw new Error(`Ambiguous col by code ${code}`);
            col = piece.Col;
      }
    return col
  }
    
  get VisiblePieces(): XiangqiPiece[] {
    return this.pieces.filter((piece) => !piece.Hidden);
  }

  getPiecesAtSameColumn(col: number, code: string): XiangqiPiece[] {
    const pieces = this.VisiblePieces.filter(
      (p) => p.Col === col && p.Code === code,
    );
    const color = code[0];
    if (color === FIRST_PLAYER) {
      pieces.sort((a, b) => b.Row - a.Row);
    } else {
      pieces.sort((a, b) => a.Row - b.Row);
    }
    return pieces;
  }

  getPieceIndexWithinSameColumn(piece: XiangqiPiece,pieces: XiangqiPiece[]): string {
    if (pieces.length === 1) {
      return '';
    }
    let index = 0;
    for (const p of pieces) {
      if ((p.Row > piece.Row && piece.Player === FIRST_PLAYER)  || (p.Row < piece.Row && piece.Player === SECOND_PLAYER)) {
        index++;
      }
    }
    if (index ===0)
      return MODIFIER.BACK;
    if (index === pieces.length - 1)
      return MODIFIER.FRONT;
    if (index === 1 && pieces.length === 3)
      return MODIFIER.MIDDLE;
    return '';
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

  reset(): void {
    this.erase();
    this.FEN = DEFAULT_POSITION;
  }

  applyPosition(position: Record<string, string>): void {
    this.erase();
    for (const [key, val] of Object.entries(position)) {
      for (const piece of this.pieces) {
        if (piece.Code === val && !piece.Pos) {
          piece.Pos = key;
          piece.Hidden = false;
          break;
        }
      }
    }
  }

  applyMove(move: XiangqiMove): void {
    this.FEN = move.Fen;
    const srcPiece = this.getPieceAtPos(move.From);
    const dstPiece = this.getPieceAtPos(move.To);
    if (!srcPiece)
      throw new Error(`No piece found at source position ${move.From}`);
    if (dstPiece) dstPiece.Hidden = true;
    srcPiece.Pos = move.To;
  }

  createMove(from: string, to: string): XiangqiMove {
    let move = new XiangqiMove();
    move.From = from;
    move.To = to;
    move.Fen = this.FEN;
    move.Notation = XiangqBoardUtils.getMoveNotation(move, this); 
    return move;
  }
}

export class XiangqBoardUtils {
  static uuid(): string {
    return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (c) => {
      const r = (Math.random() * 16) | 0;
      const v = c === "x" ? r : (r & 0x3) | 0x8;
      return v.toString(16);
    });
  }

  static validFen(fen: string): boolean {
    if (!fen) return false;
    fen = fen.replace(/ .+$/, "");
    fen = XiangqBoardUtils.expandFenEmptySquares(fen);
    const chunks = fen.split("/");
    if (chunks.length !== ROW_LENGTH) return false;
    for (let i = 0; i < ROW_LENGTH; i++) {
      if (
        chunks[i].length !== COLUMN_LENGTH ||
        chunks[i].search(/[^tsvmxpbTSVMXPB1]/) !== -1
      ) {
        return false;
      }
    }
    return true;
  }

  static isValidPos(pos: string): boolean {
    try {
      const col = Number.parseInt(pos[0], 10);
      const row = Number.parseInt(pos[1], 10);
      if (ROW_LENGTH - row - 1 < 0) return false;
      if (COLUMN_LENGTH - 1 - col < 0) return false;
    } catch {
      return false;
    }
    return true;
  }

  static fenToObj(fen: string): Record<string, string> {
    if (!XiangqBoardUtils.validFen(fen)) {
      throw new Error("Invalid FEN");
    }
    fen = fen.replace(/ .+$/, "");
    const rows = fen.split("/");
    const position: Record<string, string> = {};

    let currentRow = 0;
    for (let i = 0; i < ROW_LENGTH; i++) {
      const row = rows[i].split("");
      let colIdx = 0;

      for (let j = 0; j < row.length; j++) {
        if (/[1-9]/.test(row[j])) {
          colIdx += Number.parseInt(row[j], 10);
        } else {
          const square = `${colIdx}${currentRow}`;
          position[square] = XiangqBoardUtils.fenToPieceCode(row[j]);
          colIdx += 1;
        }
      }
      currentRow += 1;
    }
    return position;
  }

  static objToFen(obj: Record<string, string>): string {
    let fen = "";
    let currentRow = 0;
    for (let i = 0; i < ROW_LENGTH; i++) {
      for (let j = 0; j < COLUMN_LENGTH; j++) {
        const square = `${j}${currentRow}`;
        fen += obj[square] ? XiangqBoardUtils.pieceCodeToFen(obj[square]) : "1";
      }
      if (i !== ROW_TOP) {
        fen += "/";
      }
      currentRow += 1;
    }
    return XiangqBoardUtils.squeezeFenEmptySquares(fen);
  }

  private static expandFenEmptySquares(fen: string): string {
    return fen
      .replace(/9/g, "111111111")
      .replace(/8/g, "11111111")
      .replace(/7/g, "1111111")
      .replace(/6/g, "111111")
      .replace(/5/g, "11111")
      .replace(/4/g, "1111")
      .replace(/3/g, "111")
      .replace(/2/g, "11");
  }

  private static squeezeFenEmptySquares(fen: string): string {
    return fen
      .replace(/111111111/g, "9")
      .replace(/11111111/g, "8")
      .replace(/1111111/g, "7")
      .replace(/111111/g, "6")
      .replace(/11111/g, "5")
      .replace(/1111/g, "4")
      .replace(/111/g, "3")
      .replace(/11/g, "2");
  }

  private static fenToPieceCode(piece: string): string {
    if (piece.toLowerCase() === piece) {
      return SECOND_PLAYER + piece.toUpperCase();
    }
    return FIRST_PLAYER + piece.toUpperCase();
  }

  private static pieceCodeToFen(pieceId: string): string {
    const pieceCodeLetters = pieceId.split("");
    if (pieceCodeLetters[0] === SECOND_PLAYER) {
      return pieceCodeLetters[1].toLowerCase();
    }
    return pieceCodeLetters[1].toUpperCase();
  }

  static getMoveNotation(move: XiangqiMove, board: XiangqiBoard | null): string {
    if (!board) {
      throw new Error("Board is required to get move notation");
    }
    const piece = board.getPieceAtPos(move.From);
    if (!piece) {
      throw new Error(`No piece found at position ${move.From}`);
    }
    const pieces = board.getPiecesAtSameColumn(piece.Col, piece.Code);
    if (pieces.length > 3)
      throw new Error(
        "More than 3 pieces in the same column, move notation is ambiguous",
      );
    let srcModifier  = board.getPieceIndexWithinSameColumn(piece,pieces);
    let operator = "";
    if (move.RowFrom === move.RowTo) {
      operator = OPERATOR.LATERAL;
    } else if (move.RowTo < move.RowFrom) {
      operator =
        piece.Player === FIRST_PLAYER ? OPERATOR.FORWARD : OPERATOR.BACKWARD;
    } else {
      operator =
        piece.Player === FIRST_PLAYER ? OPERATOR.BACKWARD : OPERATOR.FORWARD;
    }
    const srcCol =
      piece.Player === FIRST_PLAYER
        ? COLUMN_LENGTH - move.ColFrom
        : move.ColFrom + 1;
    const dstCol =
      piece.Player === FIRST_PLAYER
        ? COLUMN_LENGTH - move.ColTo
        : move.ColTo + 1;
    let dstModifier = "";
    if (piece.PieceCode === PIECE_CODE.ROCK || piece.PieceCode === PIECE_CODE.CANON) {
      if (move.RowFrom === move.RowTo) dstModifier = `${dstCol}`;
      else dstModifier = Math.abs(move.RowFrom - move.RowTo) + "";
    }
    if (
      piece.PieceCode === PIECE_CODE.ELEPHANT ||
      piece.PieceCode === PIECE_CODE.ADMINISTRATOR ||
      piece.PieceCode === PIECE_CODE.HORSE
    ) {
      dstModifier = `${dstCol}`;
    }
    if (piece.PieceCode === PIECE_CODE.GENERAL || piece.PieceCode === PIECE_CODE.PAWN) {
      if (move.RowFrom === move.RowTo) dstModifier = `${dstCol}`;
      else dstModifier = "1";
    }
    return `${piece.PieceCode}${srcModifier}${srcCol}${operator}${dstModifier}`;
  }

  static parseMoveNotationList(
    moveNotationList: unknown,
    startPosition: unknown = DEFAULT_POSITION,
  ): XiangqiMove[] {
    const board = new XiangqiBoard();
    board.FEN =
      typeof startPosition === "string" && startPosition.trim()
        ? startPosition.trim()
        : DEFAULT_POSITION;
    const moves: XiangqiMove[] = [];
    const normalizedMoveList = Array.isArray(moveNotationList)
      ? moveNotationList.join(",")
      : typeof moveNotationList === "string"
        ? moveNotationList
        : "";
    const tokens = normalizedMoveList
      .split(",")
      .map((token) => token.trim())
      .filter(Boolean);
    for (let index = 0; index < tokens.length; index++) {
      const player = index % 2 === 0 ? FIRST_PLAYER : SECOND_PLAYER;
      try {
        const move = this.parseMoveNotation(tokens[index], player, board);
        moves.push(move);
        board.applyMove(move)
      }
      catch (error) {
        console.error("Failed to parse Xiangqi move notation", {
          error,
          index,
          notation: tokens[index],
          player,
          startPosition,
          boardFen: board.FEN,
          visiblePieces: board.VisiblePieces.map((piece) => ({
            code: piece.Code,
            pos: piece.Pos,
          })),
        });
        return moves;
      }
    }
    return moves;
  }


  private static parseMoveNotation(
    notation: string,
    player: string,
    board: XiangqiBoard,
  ): XiangqiMove {
    const normalized = notation.replace(/\s+/g, "");
    const match = normalized.match(/^([BPXMVST])([sgt])?(\d?)([/+-])(\d)$/);
    if (!match) {
      throw new Error(`Invalid move notation: ${notation} : not matching regular expression`);
    }
    let [, pieceCode, srcModifier, srcCol, operator, dstModifier] = match;
    if (!srcModifier && !srcCol) {
      throw new Error(`Invalid move notation: ${notation} modifier and srcCol are both null`);
    }
    if (!srcCol) 
      srcCol = `${board.getColByCode(`${player}${pieceCode}`)}`;
    else
      srcCol = player == FIRST_PLAYER ? `${RED_COLUMNS[srcCol]}`:`${BLACK_COLUMNS[srcCol]}`
    const candidates = board.getPiecesAtSameColumn(
       Number.parseInt(srcCol),
      `${player}${pieceCode}`,
    );
    if (candidates.length === 0) {
      throw new Error(
        `No piece found for move notation: ${notation} with player ${player} and piece code ${pieceCode} at column ${srcCol}`,
      );
    }
    if (candidates.length > 3) {
      throw new Error(
        `Too many piece found for move notation: ${notation} with player ${player} and piece code ${pieceCode}`,
      );
    }
    if (candidates.length > 1 && !srcModifier) {
      throw new Error(
        `Ambiguous piece found for move notation: ${notation} with player ${player} and piece code ${pieceCode}`,
      );
    }
    let piece = candidates[0];
    if (candidates.length > 1) {
      if (srcModifier === MODIFIER.BACK) {
        piece = candidates[0];
      }
      if (srcModifier === MODIFIER.FRONT) {
        piece = candidates[candidates.length - 1];
      }
      if (srcModifier === MODIFIER.MIDDLE) {
        piece = candidates[1];
      }
    }
    const move = this.resolveMove(
      piece,
      operator,
      Number.parseInt(dstModifier, 10),
      board
    );
    return move;
  }

  private static resolveMove(
    piece: XiangqiPiece,
    operator: string,
    dstModifier: number,
    board: XiangqiBoard,
  ): XiangqiMove {
    let resolver: MoveResolver;
    switch (piece.Code[1]) {
      case PIECE_CODE.ROCK:
        resolver = new RockMoveResolver();
        break;
      case PIECE_CODE.CANON:
        resolver = new CanonMoveResolver();
        break;
      case PIECE_CODE.PAWN:
        resolver = new PawnMoveResolver();
        break;
      case PIECE_CODE.GENERAL:
        resolver = new GeneralMoveResolver();
        break;
      case PIECE_CODE.ADMINISTRATOR:
        resolver = new AdministratorMoveResolver();
        break;
      case PIECE_CODE.ELEPHANT:
        resolver = new ElephanMoveResolver();
        break;
      case PIECE_CODE.HORSE:
        resolver = new HorseMoveResolver();
        break;
      default:
        throw new Error(`Unknown piece code: ${piece.Code}`);
    }
    let move: XiangqiMove | null = null;
    if (operator === OPERATOR.LATERAL) {
      move = resolver.resolveLateralMove(piece, dstModifier, piece.Player, board);
    } else if (operator === OPERATOR.FORWARD) {
      move = resolver.resolveForwardMove(piece, dstModifier, piece.Player, board);
    } else if (operator === OPERATOR.BACKWARD) {
      move = resolver.resolveBackwardMove(piece, dstModifier, piece.Player, board);
    } else {
      throw new Error(`Unknown operator: ${operator}`);
    }
    return move;
  }
}

export class XiangqiBoardGuiUtils {
  static posToCoordinate(context: XiangqiBoardGUIContext, pos: string) {
    const col = Number.parseInt(pos[0], 10);
    const row = Number.parseInt(pos[1], 10);
    let top = SQUARE_HEIGHT * row - SQUARE_HEIGHT / 2;
    let left = SQUARE_WIDTH * col -SQUARE_WIDTH / 2;

    return {
      col,
      row,
      top: XiangqiBoardGuiUtils.transformY(context, top),
      left: XiangqiBoardGuiUtils.transformX(context, left),
    };
  }

  static coordinateToPos(
    context: XiangqiBoardGUIContext,
    x: number,
    y: number,
  ): string {
    const offsetX = (OUTER_BOARD_WIDTH - SQUARE_WIDTH * COLUMN_LENGTH) / 2;
    const offsetY = (OUTER_BOARD_HEIGHT - SQUARE_HEIGHT * ROW_LENGTH) / 2;
    const col = Math.floor((x / context.scaleX - offsetX) / SQUARE_WIDTH);
    const row = Math.floor((y / context.scaleY - offsetY) / SQUARE_HEIGHT) ;
    return `${col}${row}`;
  }

  private static transformX(
    context: XiangqiBoardGUIContext,
    x: number,
  ): number {
    const offsetX = (OUTER_BOARD_WIDTH - INNER_BOARD_WIDTH) / 2;
    return Math.floor((x + offsetX) * context.scaleX);
  }

  private static transformY(
    context: XiangqiBoardGUIContext,
    y: number,
  ): number {
    const offsetY = (OUTER_BOARD_HEIGHT - INNER_BOARD_HEIGHT) / 2;
    return Math.floor((y + offsetY) * context.scaleY);
  }
}

@Component({
  selector: "xiangqi-piece",
  template: `
    <img
      *ngIf="piece as renderedPiece"
      [src]="'assets/theme/images/chess/pieces/' + renderedPiece.Code + '.svg'"
      [hidden]="hidden"
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
export class XiangqiPieceComponent implements DoCheck {
  top = 0;
  left = 0;
  width = 16;
  height = 16;
  hidden = true;
  selected = false;

  @Input() componentId = "";

  private context!: XiangqiBoardGUIContext;
  private pieceInfo: XiangqiPiece | null = null;
  private renderedStateKey = "";

  @Input() set renderContext(value: XiangqiBoardGUIContext | null) {
    if (!value) return;
    this.context = value;
    this.updatePosition();
  }

  @Input() set pieceData(value: XiangqiPiece | null) {
    this.piece = value;
  }

  @Input() set selectedPiece(value: boolean) {
    this.selected = value;
  }

  get piece(): XiangqiPiece | null {
    return this.pieceInfo;
  }

  set piece(value: XiangqiPiece | null) {
    this.pieceInfo = value;
    this.updatePosition();
  }

  ngDoCheck(): void {
    const nextStateKey = this.getStateKey();
    if (nextStateKey === this.renderedStateKey) return;

    this.updatePosition();
  }

  render(context: XiangqiBoardGUIContext, pieceInfo: XiangqiPiece): void {
    this.context = context;
    this.selected = false;
    this.width = Math.floor(SQUARE_WIDTH * this.context.scaleX);
    this.height = Math.floor(SQUARE_HEIGHT * this.context.scaleY);
    this.piece = pieceInfo;
  }

  draw(): void {
    this.updatePosition();
  }

  private updatePosition(): void {
    this.renderedStateKey = this.getStateKey();
    this.hidden = !this.pieceInfo || this.pieceInfo.Hidden;
    if (!this.context || this.hidden || !this.pieceInfo?.Pos) return;

    const coord = XiangqiBoardGuiUtils.posToCoordinate(
      this.context,
      this.pieceInfo.Pos,
    );
    this.top = coord.top;
    this.left = coord.left;
  }

  private getStateKey(): string {
    if (!this.pieceInfo) return "none";

    return [
      this.pieceInfo.Id,
      this.pieceInfo.Code,
      this.pieceInfo.Pos,
      this.pieceInfo.Hidden,
      this.context?.orientation,
      this.context?.scaleX,
      this.context?.scaleY,
    ].join("|");
  }

  select(): void {
    this.selected = true;
  }

  unselect(): void {
    this.selected = false;
  }
}

@Component({
  selector: "xiangqi-board",
  template: ``,
  styles: [
    `
      :host {
        display: block;
        width: 100%;
      }

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
export class XiangqiBoardComponent
  implements AfterViewInit, OnChanges, OnDestroy, XiangqiBoardGUIContext, OnInit
{
  @Input() componentId = XiangqBoardUtils.uuid();
  @Input() position: string = DEFAULT_POSITION;
  @Input() orientation: string = FIRST_PLAYER;
  @Input() viewOnly = false;

  scaleX = 1;
  scaleY = 1;
  initialized = false;
  selectedPieceId: string | null = null;
  readonly pieceSelected: Observable<string>;
  readonly pieceDeselected: Observable<string>;
  readonly boardClicked: Observable<string>;
  @Output() readonly pieceMove = new EventEmitter<unknown>();
  private board = new XiangqiBoard();

  private hostElement?: HTMLElement;
  private boardRoot?: HTMLDivElement;
  private boardLayer?: HTMLDivElement;
  private boardImage?: HTMLImageElement;
  private boardContainer?: HTMLElement;
  private pieceImages: HTMLImageElement[] = [];
  private readonly resizeListener = () => this.onWindowResize();
  private lastPlayer:string = '';
  private mode: "edit" | "play" = "edit";
  private pieceSelectedReceiver = new Subject<string>();
  private pieceDeselectedReceiver = new Subject<string>();
  private boardClickedReceiver = new Subject<string>();

  constructor() {
    this.pieceSelected = this.pieceSelectedReceiver.asObservable();
    this.pieceDeselected = this.pieceDeselectedReceiver.asObservable();
    this.boardClicked = this.boardClickedReceiver.asObservable();
    this.setUpBoard(DEFAULT_POSITION, []);
  }

  ngOnInit(): void {
    this.mode = "edit";
  }

  ngAfterViewInit(): void {
    this.createBoardDom();
    window.addEventListener("resize", this.resizeListener);
  }

  boardReady (event?: Event) {
    this.boardImage = (event?.target as HTMLImageElement | null) ?? this.boardImage;
    this.boardContainer = this.boardImage?.parentElement ?? this.boardContainer;
    this.initialized = true;
    this.updateScale();
    this.drawBoard();
  }

  boardImageFailed(event: Event): void {
    this.boardImage = (event.target as HTMLImageElement | null) ?? undefined;
    this.boardContainer = this.boardImage?.parentElement ?? undefined;
    this.initialized = true;
    this.drawBoard();
  }

  onWindowResize(): void {
    if (!this.initialized) return;
    this.updateScale();
    this.drawBoard();
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (!changes["position"]) return;

    this.setUpBoard(this.position || DEFAULT_POSITION);
    this.lastPlayer = "";
    this.unselectPiece();
    if (this.initialized) {
      this.drawBoard();
    }
  }

  ngOnDestroy(): void {
    window.removeEventListener("resize", this.resizeListener);
    this.hostElement?.replaceChildren();
  }

  get Board(): XiangqiBoard {
    return this.board;
  }

  get visiblePieces(): XiangqiPiece[] {
    return this.board.VisiblePieces;
  }

  get pieceWidth(): number {
    return Math.floor(SQUARE_WIDTH * this.scaleX);
  }

  get pieceHeight(): number {
    return Math.floor(SQUARE_HEIGHT * this.scaleY);
  }

  pieceTop(piece: XiangqiPiece): number {
    return XiangqiBoardGuiUtils.posToCoordinate(this, piece.Pos).top;
  }

  pieceLeft(piece: XiangqiPiece): number {
    return XiangqiBoardGuiUtils.posToCoordinate(this, piece.Pos).left;
  }

  reset() {
    this.board.reset();
    this.lastPlayer = '';
    this.renderPieces();
  }

  clickBoard(event: MouseEvent & { offsetX: number; offsetY: number }): void {
    if (this.viewOnly) return;
    const pos = XiangqiBoardGuiUtils.coordinateToPos(
      this,
      event.offsetX,
      event.offsetY,
    );
    if (!XiangqBoardUtils.isValidPos(pos)) return;
    this.selectBoardSquare(pos);
  }

  selectPiece(pieceId: string): void {
    if (this.viewOnly) return;

    const piece = this.board.getPieceById(pieceId);
    if (!piece) return;

    if (this.selectedPieceId === pieceId) {
      this.unselectPiece();
      return;
    }
    if (
      this.selectedPieceId &&
      this.board.getPieceById(this.selectedPieceId)?.Player !== piece.Player
    ) {
      const selectedPiece = this.board.getPieceById(this.selectedPieceId);
      if (!selectedPiece) return;
      const move = this.board.createMove(
        selectedPiece.Pos || "",
        piece.Pos || "",
      );
      if (!move) return;
      if (!this.canTakeMove(move)) return;
      this.takeMove(move);
      this.pieceMove.emit(move);
      this.unselectPiece();
    } else {
      if (!this.canMovePiece(pieceId)) return;
      this.selectedPieceId = pieceId;
      this.pieceSelectedReceiver.next(pieceId);
      this.renderPieces();
    }
  }

  private updateScale(): void {
    const boardImage = this.boardImage;
    if (!boardImage) return;

    const rect = boardImage.getBoundingClientRect();
    const width =
      rect.width ||
      boardImage.offsetWidth ||
      this.boardContainer?.clientWidth ||
      0;
    let height = rect.height || boardImage.offsetHeight || 0;

    if (!height && width) {
      height = width * (OUTER_BOARD_HEIGHT / OUTER_BOARD_WIDTH);
    }

    if (!width || !height) return;

    this.scaleX = width / OUTER_BOARD_WIDTH;
    this.scaleY = height / OUTER_BOARD_HEIGHT;
  }

  drawBoard(): void {
    this.renderPieces();
  }

  private createBoardDom(): void {
    if (this.boardRoot) return;
    const hostElement = this.findHostElement();
    if (!hostElement) {
      return;
    }

    const root = document.createElement("div");
    root.className = "xiangqiboard";
    root.style.width = "100%";
    root.style.padding = "0";
    root.style.margin = "0";

    const board = document.createElement("div");
    board.className = "board";
    board.style.width = "100%";
    board.style.position = "relative";

    const boardImage = document.createElement("img");
    boardImage.src = "assets/theme/images/chess/board.png";
    boardImage.alt = "Xiangqi board";
    boardImage.style.width = "100%";
    boardImage.style.display = "block";
    boardImage.addEventListener("mousedown", (event) =>
      this.clickBoard(event as MouseEvent & { offsetX: number; offsetY: number }),
    );
    boardImage.addEventListener("load", (event) => this.boardReady(event));
    boardImage.addEventListener("error", (event) => this.boardImageFailed(event));

    board.appendChild(boardImage);
    root.appendChild(board);
    hostElement.replaceChildren(root);

    this.hostElement = hostElement;
    this.boardRoot = root;
    this.boardLayer = board;
    this.boardImage = boardImage;
    this.boardContainer = board;

    if (boardImage.complete && boardImage.naturalWidth > 0) {
      this.boardReady({ target: boardImage } as unknown as Event);
    } else {
      this.renderPieces();
    }
  }

  private findHostElement(): HTMLElement | undefined {
    return (
      document.querySelector<HTMLElement>(
        `xiangqi-board[data-board-id="${this.componentId}"]`,
      ) ??
      document.querySelector<HTMLElement>(
        `xiangqi-board[ng-reflect-component-id="${this.componentId}"]`,
      ) ??
      undefined
    );
  }

  private renderPieces(): void {
    if (!this.boardLayer) return;

    for (const pieceImage of this.pieceImages) {
      pieceImage.remove();
    }
    this.pieceImages = [];

    for (const piece of this.board.VisiblePieces) {
      const pieceImage = document.createElement("img");
      const coord = XiangqiBoardGuiUtils.posToCoordinate(this, piece.Pos);
      pieceImage.className = "xiangqi-piece";
      pieceImage.src = `assets/theme/images/chess/pieces/${piece.Code}.svg`;
      pieceImage.alt = "";
      pieceImage.style.position = "absolute";
      pieceImage.style.top = `${coord.top}px`;
      pieceImage.style.left = `${coord.left}px`;
      pieceImage.style.width = `${this.pieceWidth}px`;
      pieceImage.style.height = `${this.pieceHeight}px`;
      if (piece.Id === this.selectedPieceId) {
        pieceImage.style.borderStyle = "solid";
        pieceImage.style.borderWidth = "1px";
        pieceImage.style.borderColor = "yellow";
      }
      pieceImage.addEventListener("click", (event) => {
        event.stopPropagation();
        this.selectPiece(piece.Id);
      });
      this.boardLayer.appendChild(pieceImage);
      this.pieceImages.push(pieceImage);
    }
  }


  private selectBoardSquare(pos: string): void {
    if (this.viewOnly) return;

    if (this.selectedPieceId) {
      const selectedPiece = this.board.getPieceById(this.selectedPieceId);
      if (!selectedPiece) return;
      const move = this.board.createMove(
        selectedPiece.Pos || "",
        pos,
      );
      if (!move) return;
      if (!this.canTakeMove(move)) return;
      this.takeMove(move);
      this.pieceMove.emit(move);
      this.unselectPiece();
    } else {
      const piece = this.board.getPieceAtPos(pos);
      if (piece) {
        this.selectedPieceId = piece.Id;
        this.pieceSelectedReceiver.next(piece.Id);
        this.renderPieces();
      }
    }

    this.boardClickedReceiver.next(pos);
  }

  private unselectPiece(): void {
    if (this.selectedPieceId) {
      this.pieceDeselectedReceiver.next(this.selectedPieceId);
      this.selectedPieceId = null;
      this.renderPieces();
    }
  }

  private setUpBoard(
    position: string = DEFAULT_POSITION,
    moves?: Array<XiangqiMove>,
  ): void {
    if (position === DEFAULT_POSITION) position = START_FEN;
    this.board.FEN = position;
    this.renderPieces();
  }

  private canMovePiece(pieceId: string): boolean {
    const piece = this.board.getPieceById(pieceId);
    if (!piece) return false;
    return this.nextPlayer === piece.Player;
  }

  canTakeMove(move: XiangqiMove): boolean {
    if (this.mode === "edit") return true;
    const fromPiece = this.board.getPieceAtPos(move.From);
    if (!fromPiece) return false;
    if (!this.canMovePiece(fromPiece.Id)) return false;
    return true;
  }

  takeMove(move: XiangqiMove): void {
    if (!this.canTakeMove(move)) return;
    this.board.applyMove(move);
    const piece = this.board.getPieceAtPos(move.To);
    this.lastPlayer = piece?.Player || FIRST_PLAYER;
    this.renderPieces();
  }

  get nextPlayer() {
    if (!this.lastPlayer)
      return FIRST_PLAYER;
    return this.lastPlayer === FIRST_PLAYER ? SECOND_PLAYER : FIRST_PLAYER;
  }

}

@NgModule({
  declarations: [XiangqiBoardComponent, XiangqiPieceComponent],
  imports: [CommonModule],
  exports: [XiangqiBoardComponent],
})
export class XiangqiBoardModule {}
