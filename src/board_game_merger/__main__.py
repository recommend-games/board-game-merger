import argparse
import logging
import sys
from typing import TYPE_CHECKING

import polars as pl

from board_game_merger.config import MergeConfig
from board_game_merger.merge import merge_files
from board_game_merger.schemas import (
    GAME_ITEM_SCHEMA,
    RATING_ITEM_SCHEMA,
    USER_ITEM_SCHEMA,
)

if TYPE_CHECKING:
    from polars._typing import IntoExpr

LOGGER = logging.getLogger(__name__)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Merge board game data files")

    parser.add_argument(
        "site",
        choices=["bgg"],
        help="Site to merge data from",
    )
    parser.add_argument(
        "--item-type",
        "-t",
        choices=["game", "rating", "user"],
        default="game",
        help="Type of item to merge",
    )
    parser.add_argument(
        "--in-paths",
        "-i",
        nargs="+",
        help="Paths to input files or directories",
    )
    parser.add_argument(
        "--out-path",
        "-o",
        help="Path to output file",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        stream=sys.stdout,
    )

    if args.site == "bgg":
        LOGGER.info("Merging data from BoardGameGeek")
    else:
        raise ValueError(f"Unknown site: {args.site}")

    item_type = args.item_type
    LOGGER.info("Merging %ss", item_type)

    fieldnames_exclude = ["published_at", "scraped_at"]

    if item_type == "game":
        schema = GAME_ITEM_SCHEMA
        key_col: IntoExpr | list[IntoExpr] = "bgg_id"
        fieldnames_exclude += ["updated_at"]

    elif item_type == "rating":
        schema = RATING_ITEM_SCHEMA
        key_col = [pl.col("bgg_user_name").str.to_lowercase(), "bgg_id"]

    elif item_type == "user":
        schema = USER_ITEM_SCHEMA
        key_col = pl.col("bgg_user_name").str.to_lowercase()

    else:
        raise ValueError(f"Unknown item type: {item_type}")

    merge_files(
        merge_config=MergeConfig(
            schema=schema,
            in_paths=args.in_paths,
            out_path=args.out_path,
            key_col=key_col,
            latest_col=pl.col("scraped_at").str.to_datetime(time_zone="UTC"),
            sort_fields=key_col,
            fieldnames_exclude=fieldnames_exclude,
        ),
        drop_empty=True,
        sort_keys=True,
        progress_bar=True,
    )


if __name__ == "__main__":
    main()
