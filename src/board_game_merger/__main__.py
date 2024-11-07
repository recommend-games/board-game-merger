import logging
import sys
from typing import TYPE_CHECKING

import polars as pl

from board_game_merger.merge import merge_files
from board_game_merger.schemas import (
    GAME_ITEM_SCHEMA,
    RATING_ITEM_SCHEMA,
    USER_ITEM_SCHEMA,
)

if TYPE_CHECKING:
    from polars._typing import IntoExpr

LOGGER = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)

    item_type = sys.argv[1] if len(sys.argv) > 1 else "game"
    LOGGER.info("Merging %ss", item_type)

    fieldnames_exclude = ["published_at", "scraped_at"]

    if item_type == "game":
        path = "GameItem"
        schema = GAME_ITEM_SCHEMA
        key_col: IntoExpr | list[IntoExpr] = "bgg_id"
        fieldnames_exclude += ["updated_at"]

    elif item_type == "rating":
        path = "RatingItem"
        schema = RATING_ITEM_SCHEMA
        key_col = [pl.col("bgg_user_name").str.to_lowercase(), "bgg_id"]

    elif item_type == "user":
        path = "UserItem"
        schema = USER_ITEM_SCHEMA
        key_col = pl.col("bgg_user_name").str.to_lowercase()

    else:
        raise ValueError(f"Unknown item type: {item_type}")

    merge_files(
        in_paths=f"feeds/bgg/{path}/",
        schema=schema,
        out_path=f"{item_type}s_merged.jl",
        key_col=key_col,
        latest_col=pl.col("scraped_at").str.to_datetime(time_zone="UTC"),
        sort_fields=key_col,
        fieldnames_exclude=fieldnames_exclude,
        drop_empty=True,
        sort_keys=True,
        progress_bar=True,
    )


if __name__ == "__main__":
    main()
