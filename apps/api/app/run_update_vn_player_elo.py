from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys

from app.tasks import _update_vn_player_elo


logger = logging.getLogger("chess_elo.run_update_vn_player_elo")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the VN male player Elo update task immediately.")
    parser.add_argument(
        "--compact",
        action="store_true",
        help="Print result JSON on one line.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
        help="Console log level.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )

    logger.info("Starting VN male player Elo update")
    try:
        result = asyncio.run(_update_vn_player_elo())
    except Exception:
        logger.exception("VN male player Elo update failed")
        return 1

    logger.info("Finished VN male player Elo update")
    indent = None if args.compact else 2
    print(json.dumps(result, ensure_ascii=False, indent=indent))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
