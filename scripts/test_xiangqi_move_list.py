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

    move_list = "UDItNU04LjdNMi4zWDktOFgxLTJDNy4xWDIuNk0yLjNNOC43QzMuMVg5LjFWMy41WDktNlM0LjVDNS4xWDEtNFg2LjhUNS00UDguNE03LjZDNS4xQzUuMU0zLjVDNS4xUDUuMkM3LjFYMi00TTYuN1A1LjFQOC03TTUuM1g4LjRNNy41WDgtN1Y3LjVNNy81RTB3c3BrTCsyMUE9WDQtN1AyLTFQOC81UDEuNFA4LTNYNy01TTMuNVAxLTVQMy01UDUuMlM0LjVNNS83TTUvNk0zLjVYNy05VDQtNVM1LjRTNS80TTYuNVM2LjVYOS02UDctOVg2LzNNNy41UzYuNU1zLjdWNS4zVjUvM1g2LTJQOS0zTTUuN003LzZWMy41TTUuMw=="
    key = "RTB3c3BrTCsyMUE9"

    try:
        if key in move_list:
            encoded_move_list = move_list.replace(key, "")
        move_list = base64.b64decode(encoded_move_list, validate=True).decode("utf-8")
        moves = [match.group(0) for match in MOVE_PATTERN.finditer(move_list)]
        moves = [move.replace(".", "+").replace("C", "B") for move in moves]
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
