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

    move_list = "UDItNU04LjdDMy4xTTIuMU0yLjNDMS4xTTguN00xLjJDNy4xUDIuNVA1LThQOC41VjcuNU0yLjNYMS0yWDktOFg5LThYMS0yUDguM1Y3LjVQOC4yVjUvN1A4LTdYMi45TTcvOFA4LjFDNy4xUDgtMVgyLjlNNy84QzcuMU0zLzJQNy04UDEuMVQ1LjFNMi40QzctNlY3LjVNOC42TTguNk02LjhQMS8yVDUvMU00LzJDNi03TTIuNEM3LTZNNC8yQzYtN00yLjRDNy02TTQvNkM2LTdNdC40QzctNk00LzZDNi03TXMuNE0zLjRNNC4zTTQuNk0zLjJTNi41UzYuNVA4LzFDNS4xUDgtM00yLzRQMy00UDEuMU04LzZQMS0zQzMuMU02LjdDMy4xQzkuMVA0LTZNNC8yUDYuMk03LzZDMy00VjUuM0M3LTZNMi4xTXMuN00xLjNTNS42UDMtNFM0LjVNNi43UDYtOFA0LTFDNi01UDEuMVA4LzRWMy4xUDgvM1M1LjRDNC4xUzQuNUM0LjFNMy4xTTcvNk03LzhNcy44TTguNkNzLjFNNi81QzQtNVM0LzVDNS4xTTUuN002LzdNNy42QzUtNk02LzdUNS00TTcuOFM1LjRNOC82VDQtNVAxLTJNOC82UDItMU02LjhQMS0yTTgvNlAyLTFNNi44UDEtMk03LzZQMi8yTTYuOE02LzRQOC0xTTEvMlM2LzVNNC42UDEuNEMxLjFDMS4xQzEuMVAxLTVWMy81UDUvMkMxLjFNOC82TTIuM1A1LjFDMS0yVDUtNE0zLzRNNi41QzItM0MxLjFDMy4xVjUuM1YxLzNDMS0yTTYvNVA1LTlNNC8zUDkvMU0zLjVDMi4xTXMuM001LjdNNS80TTcvNU0zLjVDMi0zVjMuMVQ0LTVNNC4yUDkuMU0yLjNQOS03TTMuNE01LzdNNC8zVjMvNUMzLTRNNy41TTUvM0MzLjFNdC81QzMuMU0zLzVQNy04VjUuM1Y1LjdNdC82QzMtMkM0LTNNNS42TTYuOFA4LzFNNS42UDgtNVQ1LTRQNS4xUzUuNEMyLTNUNC4xTTYvN004LzZQNS02VDQtNUMzLTJNdC40b3NrUHFPMWZ2S2c9VjMuNU02LjVNNy45TTUuN005LjhNNy82UDYtMVQ1LTRUNS02UzQvNVAxLzNNNi80"
    key = "b3NrUHFPMWZ2S2c9"

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
