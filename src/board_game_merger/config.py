from __future__ import annotations

import dataclasses
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

import polars as pl

from board_game_merger.schemas import ITEM_TYPE_SCHEMA

if TYPE_CHECKING:
    from polars._typing import IntoExpr

PROJECT_DIR = Path(__file__).resolve().parent.parent.parent
FEEDS_DIR = PROJECT_DIR.parent / "board-game-scraper" / "feeds"
DATA_DIR = PROJECT_DIR.parent / "board-game-data"


@dataclasses.dataclass(frozen=True, kw_only=True)
class MergeConfig:
    schema: pl.Schema

    in_paths: str | Path | list[str] | list[Path]
    out_path: str | Path

    key_col: IntoExpr | list[IntoExpr]
    latest_col: IntoExpr | list[IntoExpr]
    latest_min: Any = None

    sort_fields: IntoExpr | list[IntoExpr] | None = None
    sort_descending: bool = False

    fieldnames_include: IntoExpr | list[IntoExpr] | None = None
    fieldnames_exclude: str | list[str] | None = None

    @classmethod
    def global_defaults(
        cls,
        site: str,
        *,
        item: Literal["GameItem", "UserItem", "RatingItem"] = "GameItem",
        clean_results: bool = False,
        latest_min_days: float | None = None,
        **kwargs: Any,
    ) -> MergeConfig:
        now = datetime.now(timezone.utc)
        now_str = now.strftime("%Y-%m-%dT%H-%M-%S")

        schema = kwargs.get(item) or ITEM_TYPE_SCHEMA.get(item)

        if not schema:
            raise ValueError(f"Unknown item type: {item}")

        kwargs.setdefault("schema", schema)
        kwargs.setdefault("in_paths", FEEDS_DIR / site / item)

        kwargs.setdefault("key_col", f"{site}_id")
        kwargs.setdefault(
            "latest_col", pl.col("scraped_at").str.to_datetime(time_zone="UTC")
        )
        if latest_min_days and latest_min_days > 0:
            kwargs.setdefault("latest_min", now - timedelta(days=latest_min_days))

        if clean_results:
            kwargs.setdefault("out_path", DATA_DIR / "scraped" / f"{site}_{item}.jl")
            kwargs.setdefault("sort_fields", kwargs["key_col"])
            kwargs.setdefault(
                "fieldnames_exclude", ["published_at", "updated_at", "scraped_at"]
            )

        else:
            kwargs.setdefault("out_path", FEEDS_DIR / site / f"{now_str}-merged.jl")

        return cls(**kwargs)
