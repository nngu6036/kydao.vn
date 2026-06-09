#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
import base64
import binascii
import re
from datetime import datetime, timezone

MOVE_PATTERN = re.compile(r"[A-Z].?\d?.\d")
MOVE_FULL_PATTERN = re.compile(r"^[A-Z].?\d?.\d$")

try:
    from .xiangqi_utils import DEFAULT_POSITION, XiangqiBoardUtils, XiangqiBoard
except ImportError:
    from xiangqi_utils import DEFAULT_POSITION, XiangqiBoardUtils, XiangqiBoard


def main() -> int:

    move_list = "UDItNU04LjdNMi4zWDktOEM3LjFQOC05TTguN1YzLjVQOC05QzMuMUM3LjFYOC40TTcuOFg4LTNNOC45WDMuMVg5LThNMi40UDUtNEM3LjFWMy41WDMtNFM0LjVNNy42UDQvMVgxLTNQNC0zWDMuNlgxLTRYNC8xQzkuMVA5LTZYNC0yTTYuNVgyLjZNNS82UDktNk02LjdQNi42WDQvM005LjdQNi0zWDguN1AzLjFYMi01TTcuOVAzLTRYMy02TTMuMlg2LThTNS80WDgvMVg1LTdYOC02WDgvNk05LzdQNC02TTcuNVM2LjVYNC41WDcvNFg2LTVYOC4xTTUvNlA2LTlYNS0xVjcuOVgxLTRYNy02WHQuMVg4LTZYNC0yUDktN1gyLjRQNy8xWDIvM0MxLjFNNi44VDUtNlM2LjVQNy42WDItMVY5LjdDNy4xWDYuNEM5LjFDMS4xTTgvOVg2LzJYMS4zVDYuMVgxLzZWNy81WDEtM1Y1LjNYMy40WDYtNVgzLjFUNi8xWDMuMVQ2LjFNOS84WDUtNk04LjZWMy81TTYvNFg2LjJNNC8yWDYvMk0yLjFYNi0VmhLY0JZRHhwSDg95TTEuM1g5LTZYMy0yWDYvMlgyLTNYNi4yTTMuMlg2LTdYMy0xWDcvMVgxLTJWNS43WDIvMlQ2LzFYMi4yVDYuMVY1LjdWNy81VnQvOVg3LjFNMi4xWDcvM00xLzJYNy4zTTIvMVg3LjFNMS8yWDcvM00yLzRWNS43WDIvM1Q2LzFNNC4yVjcvNU0yLjk="
    key = "VmhLY0JZRHhwSDg9"

    try:
        if key in move_list:
            encoded_move_list = move_list.replace(key, "")
        move_list = base64.b64decode(encoded_move_list, validate=True).decode("utf-8")
        moves = [match.group(0) for match in MOVE_PATTERN.finditer(move_list)]
        moves = [move.replace(".", "+").replace("C", "B") for move in moves]
        print(moves)
        parsed_moves = XiangqiBoardUtils.parse_move_notation_list(",".join(moves),)
        board = XiangqiBoard()
        board.FEN = DEFAULT_POSITION
        normalized_notation = []
        for move in parsed_moves:
            notation = XiangqiBoardUtils.get_move_notation(move, board)
            normalized_notation.append(notation)
            board.apply_move(move)    
        print(normalized_notation)
    except Exception as exc:
        print(f"ERROR: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
