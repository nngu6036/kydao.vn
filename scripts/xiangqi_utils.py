#!/usr/bin/env python3
from __future__ import annotations

import math
import re
import uuid
from dataclasses import dataclass
from typing import Dict, Iterator, List, Optional, Protocol


ROW_TOP = 9
ROW_LOW = 0
ROW_LENGTH = ROW_TOP - ROW_LOW + 1
COLUMN_LENGTH = 9
RED_COLUMNS: Dict[str, int] = {
    "9": 0,
    "8": 1,
    "7": 2,
    "6": 3,
    "5": 4,
    "4": 5,
    "3": 6,
    "2": 7,
    "1": 8,
}
BLACK_COLUMNS: Dict[str, int] = {
    "1": 0,
    "2": 1,
    "3": 2,
    "4": 3,
    "5": 4,
    "6": 5,
    "7": 6,
    "8": 7,
    "9": 8,
}
START_FEN = "xmvstsvmx/9/1p5p1/b1b1b1b1b/9/9/B1B1B1B1B/1P5P1/9/XMVSTSVMX r"
OUTER_BOARD_WIDTH = 560
OUTER_BOARD_HEIGHT = 644
INNER_BOARD_WIDTH = 448
INNER_BOARD_HEIGHT = 504
SQUARE_WIDTH = INNER_BOARD_WIDTH / (COLUMN_LENGTH - 1)
SQUARE_HEIGHT = INNER_BOARD_HEIGHT / (ROW_LENGTH - 1)
DEFAULT_POSITION = "start"
FIRST_PLAYER = "r"
SECOND_PLAYER = "b"
PIECES = "XMVSTSVMXPPBBBBB"


class PieceCode:
    PAWN = "B"
    CANON = "P"
    ROCK = "X"
    HORSE = "M"
    ELEPHANT = "V"
    ADMINISTRATOR = "S"
    GENERAL = "T"


class Operator:
    FORWARD = "+"
    BACKWARD = "/"
    LATERAL = "-"


class Modifier:
    FRONT = "t"
    MIDDLE = "g"
    BACK = "s"


@dataclass
class XiangqiBoardGUIContext:
    orientation: str = FIRST_PLAYER
    scale_x: float = 1
    scale_y: float = 1


@dataclass
class XiangqiPiece:
    code: str
    id: str
    pos: str = ""
    hidden: bool = False

    @property
    def player(self) -> str:
        return self.code[0]

    @property
    def piece_code(self) -> str:
        return self.code[1]

    @property
    def is_second(self) -> bool:
        return self.player == SECOND_PLAYER

    @property
    def is_first(self) -> bool:
        return self.player == FIRST_PLAYER

    @property
    def row(self) -> int:
        return int(self.pos[1])

    @property
    def col(self) -> int:
        return int(self.pos[0])


@dataclass
class XiangqiMove:
    from_pos: str = ""
    to_pos: str = ""
    notation: str = ""
    fen: str = ""

    @property
    def row_from(self) -> int:
        return int(self.from_pos[1])

    @property
    def row_to(self) -> int:
        return int(self.to_pos[1])

    @property
    def col_from(self) -> int:
        return int(self.from_pos[0])

    @property
    def col_to(self) -> int:
        return int(self.to_pos[0])


class MoveResolver(Protocol):
    def resolve_lateral_move(
        self,
        piece: XiangqiPiece,
        dst_modifier: int,
        color: str,
        board: XiangqiBoard,
    ) -> XiangqiMove:
        ...

    def resolve_forward_move(
        self,
        piece: XiangqiPiece,
        dst_modifier: int,
        color: str,
        board: XiangqiBoard,
    ) -> XiangqiMove:
        ...

    def resolve_backward_move(
        self,
        piece: XiangqiPiece,
        dst_modifier: int,
        color: str,
        board: XiangqiBoard,
    ) -> XiangqiMove:
        ...


def _file_to_col(color: str, file_number: int) -> int:
    columns = RED_COLUMNS if color == FIRST_PLAYER else BLACK_COLUMNS
    return columns[str(file_number)]


class StraightMoveResolver:
    def resolve_lateral_move(
        self,
        piece: XiangqiPiece,
        dst_modifier: int,
        color: str,
        board: XiangqiBoard,
    ) -> XiangqiMove:
        dst_col = _file_to_col(color, dst_modifier)
        return board.create_move(f"{piece.col}{piece.row}", f"{dst_col}{piece.row}")

    def resolve_forward_move(
        self,
        piece: XiangqiPiece,
        dst_modifier: int,
        color: str,
        board: XiangqiBoard,
    ) -> XiangqiMove:
        dst_row = piece.row - dst_modifier if color == FIRST_PLAYER else piece.row + dst_modifier
        return board.create_move(f"{piece.col}{piece.row}", f"{piece.col}{dst_row}")

    def resolve_backward_move(
        self,
        piece: XiangqiPiece,
        dst_modifier: int,
        color: str,
        board: XiangqiBoard,
    ) -> XiangqiMove:
        dst_row = piece.row + dst_modifier if color == FIRST_PLAYER else piece.row - dst_modifier
        return board.create_move(f"{piece.col}{piece.row}", f"{piece.col}{dst_row}")


class RockMoveResolver(StraightMoveResolver):
    pass


class CanonMoveResolver(StraightMoveResolver):
    pass


class PawnMoveResolver(StraightMoveResolver):
    pass


class GeneralMoveResolver(StraightMoveResolver):
    pass


class AdministratorMoveResolver:
    def resolve_lateral_move(
        self,
        piece: XiangqiPiece,
        dst_modifier: int,
        color: str,
        board: XiangqiBoard,
    ) -> XiangqiMove:
        raise ValueError("Administrator cannot move laterally")

    def resolve_forward_move(
        self,
        piece: XiangqiPiece,
        dst_modifier: int,
        color: str,
        board: XiangqiBoard,
    ) -> XiangqiMove:
        dst_col = _file_to_col(piece.player, dst_modifier)
        dst_row = piece.row - 1 if color == FIRST_PLAYER else piece.row + 1
        return board.create_move(f"{piece.col}{piece.row}", f"{dst_col}{dst_row}")

    def resolve_backward_move(
        self,
        piece: XiangqiPiece,
        dst_modifier: int,
        color: str,
        board: XiangqiBoard,
    ) -> XiangqiMove:
        dst_col = _file_to_col(piece.player, dst_modifier)
        dst_row = piece.row + 1 if color == FIRST_PLAYER else piece.row - 1
        return board.create_move(f"{piece.col}{piece.row}", f"{dst_col}{dst_row}")


class ElephantMoveResolver:
    def resolve_lateral_move(
        self,
        piece: XiangqiPiece,
        dst_modifier: int,
        color: str,
        board: XiangqiBoard,
    ) -> XiangqiMove:
        raise ValueError("Elephant cannot move laterally")

    def resolve_forward_move(
        self,
        piece: XiangqiPiece,
        dst_modifier: int,
        color: str,
        board: XiangqiBoard,
    ) -> XiangqiMove:
        dst_col = _file_to_col(piece.player, dst_modifier)
        dst_row = piece.row - 2 if color == FIRST_PLAYER else piece.row + 2
        return board.create_move(f"{piece.col}{piece.row}", f"{dst_col}{dst_row}")

    def resolve_backward_move(
        self,
        piece: XiangqiPiece,
        dst_modifier: int,
        color: str,
        board: XiangqiBoard,
    ) -> XiangqiMove:
        dst_col = _file_to_col(piece.player, dst_modifier)
        dst_row = piece.row + 2 if color == FIRST_PLAYER else piece.row - 2
        return board.create_move(f"{piece.col}{piece.row}", f"{dst_col}{dst_row}")


class HorseMoveResolver:
    def resolve_lateral_move(
        self,
        piece: XiangqiPiece,
        dst_modifier: int,
        color: str,
        board: XiangqiBoard,
    ) -> XiangqiMove:
        raise ValueError("Horse cannot move laterally")

    def resolve_forward_move(
        self,
        piece: XiangqiPiece,
        dst_modifier: int,
        color: str,
        board: XiangqiBoard,
    ) -> XiangqiMove:
        dst_col = _file_to_col(piece.player, dst_modifier)
        delta_row = 3 - abs(dst_col - piece.col)
        dst_row = piece.row - delta_row if color == FIRST_PLAYER else piece.row + delta_row
        return board.create_move(f"{piece.col}{piece.row}", f"{dst_col}{dst_row}")

    def resolve_backward_move(
        self,
        piece: XiangqiPiece,
        dst_modifier: int,
        color: str,
        board: XiangqiBoard,
    ) -> XiangqiMove:
        dst_col = _file_to_col(piece.player, dst_modifier)
        delta_row = 3 - abs(dst_col - piece.col)
        dst_row = piece.row + delta_row if color == FIRST_PLAYER else piece.row - delta_row
        return board.create_move(f"{piece.col}{piece.row}", f"{dst_col}{dst_row}")


class XiangqiBoard:
    def __init__(self) -> None:
        self.pieces: List[XiangqiPiece] = []
        for piece in PIECES:
            self.pieces.append(XiangqiPiece(f"{FIRST_PLAYER}{piece}", XiangqiBoardUtils.uuid()))
            self.pieces.append(XiangqiPiece(f"{SECOND_PLAYER}{piece}", XiangqiBoardUtils.uuid()))

    def __iter__(self) -> Iterator[XiangqiPiece]:
        return iter(self.pieces)

    def erase(self) -> None:
        for piece in self.pieces:
            piece.pos = ""
            piece.hidden = True

    @property
    def FEN(self) -> str:
        positions: Dict[str, str] = {}
        for piece in self.pieces:
            if not piece.hidden:
                positions[piece.pos] = piece.code
        return XiangqiBoardUtils.obj_to_fen(positions)

    @FEN.setter
    def FEN(self, value: str) -> None:
        if value == DEFAULT_POSITION:
            value = START_FEN
        if XiangqiBoardUtils.valid_fen(value):
            self.apply_position(XiangqiBoardUtils.fen_to_obj(value))

    @property
    def visible_pieces(self) -> List[XiangqiPiece]:
        return [piece for piece in self.pieces if not piece.hidden]

    def reset(self) -> None:
        self.erase()
        self.FEN = DEFAULT_POSITION

    def apply_position(self, position: Dict[str, str]) -> None:
        self.erase()
        for pos, code in position.items():
            for piece in self.pieces:
                if piece.code == code and not piece.pos:
                    piece.pos = pos
                    piece.hidden = False
                    break

    def get_piece_at_pos(self, pos: str) -> Optional[XiangqiPiece]:
        return next((piece for piece in self.pieces if piece.pos == pos and not piece.hidden), None)

    def get_piece_by_id(self, id: str) -> Optional[XiangqiPiece]:
        return next((piece for piece in self.pieces if piece.id == id), None)


    def get_col_by_code(self, code: str):
        col = None
        for piece in self.visible_pieces:
            if piece.code == code:
                if col and col != piece.col:
                    raise ValueError(f"Ambiguous col by code {code}")
                col = piece.col
        return col

    def get_pieces_at_same_column(self, col: int, code: str) -> List[XiangqiPiece]:
        pieces = [piece for piece in self.visible_pieces if piece.col == col and piece.code == code]
        if code[0] == FIRST_PLAYER:
            pieces.sort(key=lambda piece: piece.row, reverse=True)
        else:
            pieces.sort(key=lambda piece: piece.row)
        return pieces

    def get_piece_index_within_same_column(self, piece: XiangqiPiece, pieces: List[XiangqiPiece]) -> str:
        if len(pieces) == 1:
            return ""
        index = 0
        for candidate in pieces:
            if (candidate.row > piece.row and piece.player == FIRST_PLAYER) or (
                candidate.row < piece.row and piece.player == SECOND_PLAYER
            ):
                index += 1
        if index == 0:
            return Modifier.BACK
        if index == len(pieces) - 1:
            return Modifier.FRONT
        if index == 1 and len(pieces) == 3:
            return Modifier.MIDDLE
        return ""

    def apply_move(self, move: XiangqiMove) -> None:
        self.FEN = move.fen
        src_piece = self.get_piece_at_pos(move.from_pos)
        dst_piece = self.get_piece_at_pos(move.to_pos)
        if not src_piece:
            raise ValueError(f"No piece found at source position {move.from_pos}")
        if dst_piece:
            dst_piece.hidden = True
        src_piece.pos = move.to_pos

    def create_move(self, from_pos: str, to_pos: str) -> XiangqiMove:
        move = XiangqiMove(from_pos=from_pos, to_pos=to_pos, fen=self.FEN)
        move.notation = XiangqiBoardUtils.get_move_notation(move, self)
        return move


class XiangqiBoardUtils:
    @staticmethod
    def uuid() -> str:
        return str(uuid.uuid4())

    @staticmethod
    def valid_fen(fen: str) -> bool:
        if not fen:
            return False
        fen = re.sub(r" .+$", "", fen)
        fen = XiangqiBoardUtils.expand_fen_empty_squares(fen)
        chunks = fen.split("/")
        if len(chunks) != ROW_LENGTH:
            return False
        for chunk in chunks:
            if len(chunk) != COLUMN_LENGTH or re.search(r"[^tsvmxpbTSVMXPB1]", chunk):
                return False
        return True

    @staticmethod
    def is_valid_pos(pos: str) -> bool:
        try:
            col = int(pos[0])
            row = int(pos[1])
        except (IndexError, ValueError):
            return False
        return ROW_LENGTH - row - 1 >= 0 and COLUMN_LENGTH - 1 - col >= 0

    @staticmethod
    def fen_to_obj(fen: str) -> Dict[str, str]:
        if not XiangqiBoardUtils.valid_fen(fen):
            raise ValueError("Invalid FEN")
        fen = re.sub(r" .+$", "", fen)
        position: Dict[str, str] = {}
        current_row = 0
        for row_text in fen.split("/"):
            col_idx = 0
            for char in row_text:
                if re.match(r"[1-9]", char):
                    col_idx += int(char)
                else:
                    position[f"{col_idx}{current_row}"] = XiangqiBoardUtils.fen_to_piece_code(char)
                    col_idx += 1
            current_row += 1
        return position

    @staticmethod
    def obj_to_fen(obj: Dict[str, str]) -> str:
        fen = ""
        current_row = 0
        for i in range(ROW_LENGTH):
            for col in range(COLUMN_LENGTH):
                square = f"{col}{current_row}"
                fen += XiangqiBoardUtils.piece_code_to_fen(obj[square]) if square in obj else "1"
            if i != ROW_TOP:
                fen += "/"
            current_row += 1
        return XiangqiBoardUtils.squeeze_fen_empty_squares(fen)

    @staticmethod
    def expand_fen_empty_squares(fen: str) -> str:
        for count in range(9, 1, -1):
            fen = fen.replace(str(count), "1" * count)
        return fen

    @staticmethod
    def squeeze_fen_empty_squares(fen: str) -> str:
        for count in range(9, 1, -1):
            fen = fen.replace("1" * count, str(count))
        return fen

    @staticmethod
    def fen_to_piece_code(piece: str) -> str:
        if piece.lower() == piece:
            return SECOND_PLAYER + piece.upper()
        return FIRST_PLAYER + piece.upper()

    @staticmethod
    def piece_code_to_fen(piece_id: str) -> str:
        return piece_id[1].lower() if piece_id[0] == SECOND_PLAYER else piece_id[1].upper()

    @staticmethod
    def fen_to_ascii_board(fen: str) -> str:
        if not fen:
            return ""
        fen = re.sub(r" .+$", "", fen)
        fen = XiangqiBoardUtils.expand_fen_empty_squares(fen)
        rows = [row.replace("1", " ") for row in fen.split("/")]
        width = max((len(row) for row in rows), default=0)
        border = f"+{'-' * width}+"
        bordered_rows = [f"|{row.ljust(width)}|" for row in rows]
        return "\n".join([border, *bordered_rows, border])

    @staticmethod
    def get_move_notation(move: XiangqiMove, board: Optional[XiangqiBoard]) -> str:
        if not board:
            raise ValueError("Board is required to get move notation")
        piece = board.get_piece_at_pos(move.from_pos)
        if not piece:
            raise ValueError(f"No piece found at position {move.from_pos}")
        pieces = board.get_pieces_at_same_column(piece.col, piece.code)
        if len(pieces) > 3:
            raise ValueError("More than 3 pieces in the same column, move notation is ambiguous")
        src_modifier = board.get_piece_index_within_same_column(piece, pieces)
        if move.row_from == move.row_to:
            operator = Operator.LATERAL
        elif move.row_to < move.row_from:
            operator = Operator.FORWARD if piece.player == FIRST_PLAYER else Operator.BACKWARD
        else:
            operator = Operator.BACKWARD if piece.player == FIRST_PLAYER else Operator.FORWARD

        src_col = COLUMN_LENGTH - move.col_from if piece.player == FIRST_PLAYER else move.col_from + 1
        dst_col = COLUMN_LENGTH - move.col_to if piece.player == FIRST_PLAYER else move.col_to + 1
        dst_modifier = ""
        if piece.piece_code in (PieceCode.ROCK, PieceCode.CANON):
            dst_modifier = str(dst_col) if move.row_from == move.row_to else str(abs(move.row_from - move.row_to))
        if piece.piece_code in (PieceCode.ELEPHANT, PieceCode.ADMINISTRATOR, PieceCode.HORSE):
            dst_modifier = str(dst_col)
        if piece.piece_code in (PieceCode.GENERAL, PieceCode.PAWN):
            dst_modifier = str(dst_col) if move.row_from == move.row_to else "1"
        return f"{piece.piece_code}{src_modifier}{src_col}{operator}{dst_modifier}"

    @staticmethod
    def parse_move_notation_list(
        move_notation_list: str,
        start_position: str = DEFAULT_POSITION,
    ) -> List[XiangqiMove]:
        board = XiangqiBoard()
        board.FEN = start_position
        moves: List[XiangqiMove] = []
        tokens = [token.strip() for token in move_notation_list.split(",") if token.strip()]
        for index, token in enumerate(tokens):
            player = FIRST_PLAYER if index % 2 == 0 else SECOND_PLAYER
            move = XiangqiBoardUtils.parse_move_notation(token, player, board)
            moves.append(move)
            board.apply_move(move)
        return moves

    @staticmethod
    def parse_move_notation(notation: str, player: str, board: XiangqiBoard) -> XiangqiMove:
        normalized = re.sub(r"\s+", "", notation).upper()
        match = re.match(r"^([BPXMVST])([SGT])?(\d?)([/+-])(\d)$", normalized)
        if not match:
            raise ValueError(f"Invalid move notation: {notation}")
        piece_code, src_modifier, src_col, operator, dst_modifier = match.groups()
        src_modifier = src_modifier.lower() if src_modifier else None
        if not src_modifier and not src_col:
            raise ValueError(f"Invalid move notation: {notation}")
        if not src_col:
            source_column = board.get_col_by_code(f"{player}{piece_code}")
        else:
            source_column = _file_to_col(player, int(src_col))
        candidates = board.get_pieces_at_same_column(source_column, f"{player}{piece_code}")
        if not candidates:
            raise ValueError(
                f"No piece found for move notation: {notation} with player {player} and piece code {piece_code}"
            )
        if len(candidates) > 3:
            raise ValueError(
                f"Too many pieces found for move notation: {notation} with player {player} and piece code {piece_code}"
            )
        if len(candidates) > 1 and not src_modifier:
            if  piece_code in (PieceCode.ELEPHANT, PieceCode.ADMINISTRATOR):
                if operator == Operator.FORWARD:
                    src_modifier = Modifier.BACK
                if operator == Operator.BACKWARD:
                    src_modifier = Modifier.FRONT
            else:
                raise ValueError(
                    f"Ambiguous piece found for move notation: {notation} with player {player} and piece code {piece_code}"
                )
        piece = candidates[0]
        if len(candidates) > 1:
            if src_modifier == Modifier.BACK:
                piece = candidates[0]
            elif src_modifier == Modifier.FRONT:
                piece = candidates[-1]
            elif src_modifier == Modifier.MIDDLE:
                piece = candidates[1]
        return XiangqiBoardUtils.resolve_move(piece, operator, int(dst_modifier), board)
    

    @staticmethod
    def resolve_move(
        piece: XiangqiPiece,
        operator: str,
        dst_modifier: int,
        board: XiangqiBoard,
    ) -> XiangqiMove:
        resolver_by_piece_code = {
            PieceCode.ROCK: RockMoveResolver(),
            PieceCode.CANON: CanonMoveResolver(),
            PieceCode.PAWN: PawnMoveResolver(),
            PieceCode.GENERAL: GeneralMoveResolver(),
            PieceCode.ADMINISTRATOR: AdministratorMoveResolver(),
            PieceCode.ELEPHANT: ElephantMoveResolver(),
            PieceCode.HORSE: HorseMoveResolver(),
        }
        resolver = resolver_by_piece_code.get(piece.piece_code)
        if not resolver:
            raise ValueError(f"Unknown piece code: {piece.code}")
        if operator == Operator.LATERAL:
            return resolver.resolve_lateral_move(piece, dst_modifier, piece.player, board)
        if operator == Operator.FORWARD:
            return resolver.resolve_forward_move(piece, dst_modifier, piece.player, board)
        if operator == Operator.BACKWARD:
            return resolver.resolve_backward_move(piece, dst_modifier, piece.player, board)
        raise ValueError(f"Unknown operator: {operator}")
