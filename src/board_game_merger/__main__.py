import argparse
import logging
import sys

from board_game_merger.config import MergeConfig
from board_game_merger.merge import merge_files

LOGGER = logging.getLogger(__name__)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Merge board game data files")

    parser.add_argument(
        "site",
        help="Site to merge data from",
    )
    parser.add_argument(
        "--item-type",
        "-t",
        choices=("GameItem", "RatingItem", "UserItem"),
        default="GameItem",
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
        "--clean-results",
        "-c",
        action="store_true",
        help="Clean results (remove empty fields, sort keys "
        "alphabetically and sort rows)",
    )
    parser.add_argument(
        "--latest-min-days",
        "-m",
        type=float,
        help="Minimum number of days for the latest column",
    )
    parser.add_argument(
        "--overwrite",
        "-W",
        action="store_true",
        help="Overwrite output file if it exists",
    )
    parser.add_argument(
        "--progress-bar",
        "-p",
        action="store_true",
        help="Show progress bar",
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

    merge_configs = (
        tuple(
            MergeConfig.all_sites_config(
                clean_results=args.clean_results, latest_min_days=args.latest_min_days
            )
        )
        if args.site == "all"
        else (
            MergeConfig.site_config(
                site=args.site,
                item=args.item_type,
                in_paths=args.in_paths,
                out_path=args.out_path,
                clean_results=args.clean_results,
                latest_min_days=args.latest_min_days,
            ),
        )
    )

    for merge_config in merge_configs:
        merge_files(
            merge_config=merge_config,
            overwrite=args.overwrite,
            drop_empty=True,
            sort_keys=bool(args.clean_results),
            progress_bar=bool(args.progress_bar),
        )


if __name__ == "__main__":
    main()
