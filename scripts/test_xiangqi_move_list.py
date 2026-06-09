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

    move_list = "UDItNlA4LTRNMi4zTTguN1gxLTJDNy4xTTguN0MzLjFQOC05UDItM1g5LThDMy4xWDIuNEMzLjFNNy85WDkuMUMzLjFDNy4xWDItM1A0LTZQNi01UDYtNVg4LjZDMy00UDkuNEM0LTVQNS04WDktNFM0LjVDdC02WDMuMlg0LjVDOS4xWDQvMUM5LjFDNi4xTTMvMlgxLjFYOC4zWDEtNFA4LzJDNi4xTTIuM1YzLjN3EwdVUwckwyeTA9FYMy00WHQtN1g4LzdQMy42WDgtN1AzLTJQOS04UzQuNVB0LjNWMS4zUHQtOVg0LjNYNy04VDUtNFg4LjdUNC4xWDgvMVQ0LzFYOC4xVDQuMVg4LzhYNy4yWDQvNVg3LjJYNC8xWDcvMVg0LTJNNy42WDguN1Q0LzFNOS43WDQuNFg4LTdYNC0yWDcuMVQ0LjFYNy8xVDQvMVg3LjFUNC4xWDcvNE02LjdYNy4zVDQvMVg3LjFUNC4xTTcuNlg3LTVTNi41WDItNVQ1LTZNNy42WDcvMVQ0LjFYMi4xWDUvMw=="
    key = "N3EwdVUwckwyeTA9"

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
