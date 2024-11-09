from __future__ import annotations

import dataclasses
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

import polars as pl

from board_game_merger.schemas import ITEM_TYPE_SCHEMA

if TYPE_CHECKING:
    from collections.abc import Generator

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
    def with_defaults(  # noqa: PLR0913
        cls,
        site: str,
        *,
        item: Literal["GameItem", "UserItem", "RatingItem"] = "GameItem",
        schema: pl.Schema | None = None,
        in_paths: str | Path | list[str] | list[Path] | None = None,
        out_path: str | Path | None = None,
        key_col: IntoExpr | list[IntoExpr] | None = None,
        latest_col: IntoExpr | list[IntoExpr] | None = None,
        clean_results: bool = False,
        latest_min_days: float | None = None,
        **kwargs: Any,
    ) -> MergeConfig:
        now = datetime.now(timezone.utc)
        now_str = now.strftime("%Y-%m-%dT%H-%M-%S")

        kwargs["schema"] = schema if schema is not None else ITEM_TYPE_SCHEMA.get(item)

        if not kwargs["schema"]:
            raise ValueError(f"Unknown item type: {item}")

        kwargs["in_paths"] = in_paths or (FEEDS_DIR / site / item)

        kwargs["key_col"] = key_col if key_col is not None else f"{site}_id"
        kwargs["latest_col"] = (
            latest_col
            if latest_col is not None
            else pl.col("scraped_at").str.to_datetime(time_zone="UTC")
        )
        if latest_min_days and latest_min_days > 0:
            kwargs.setdefault("latest_min", now - timedelta(days=latest_min_days))

        if clean_results:
            kwargs["out_path"] = out_path or (
                DATA_DIR / "scraped" / f"{site}_{item}.jl"
            )
            kwargs.setdefault("sort_fields", kwargs["key_col"])
            kwargs.setdefault(
                "fieldnames_exclude",
                ["published_at", "updated_at", "scraped_at"],
            )

        else:
            kwargs["out_path"] = out_path or (
                FEEDS_DIR / site / item / f"{now_str}-merged.jl"
            )

        return cls(**kwargs)

    @classmethod
    def site_config(
        cls,
        site: str,
        *,
        item: Literal["GameItem", "UserItem", "RatingItem"] = "GameItem",
        **kwargs: Any,
    ) -> MergeConfig:
        if site == "bgg":
            return cls.bgg_config(item=item, **kwargs)

        if item != "GameItem":
            raise ValueError(f"Unknown item type for site <{site}>: {item}")

        if site == "bgg_hotness":
            return cls.bgg_hotness_config(**kwargs)

        return cls.with_defaults(site=site, item="GameItem", **kwargs)

    @classmethod
    def bgg_config(
        cls,
        *,
        item: Literal["GameItem", "UserItem", "RatingItem"] = "GameItem",
        clean_results: bool = False,
        **kwargs: Any,
    ) -> MergeConfig:
        if item == "GameItem":
            return cls.with_defaults(
                site="bgg",
                item="GameItem",
                clean_results=clean_results,
                **kwargs,
            )

        if item == "UserItem":
            kwargs.setdefault("key_col", pl.col("bgg_user_name").str.to_lowercase())
            if clean_results:
                kwargs.setdefault("fieldnames_exclude", ["published_at", "scraped_at"])
            return cls.with_defaults(
                site="bgg",
                item="UserItem",
                clean_results=clean_results,
                **kwargs,
            )

        if item == "RatingItem":
            kwargs.setdefault(
                "key_col",
                [pl.col("bgg_user_name").str.to_lowercase(), "bgg_id"],
            )
            if clean_results:
                kwargs.setdefault("fieldnames_exclude", ["published_at", "scraped_at"])
            return cls.with_defaults(
                site="bgg",
                item="RatingItem",
                clean_results=clean_results,
                **kwargs,
            )

        raise ValueError(f"Unknown item type: {item}")

    @classmethod
    def bgg_hotness_config(
        cls,
        *,
        clean_results: bool = False,
        **kwargs: Any,
    ) -> MergeConfig:
        kwargs.setdefault(
            "key_col",
            [pl.col("published_at").str.to_datetime(time_zone="UTC"), "bgg_id"],
        )
        kwargs.setdefault(
            "sort_fields",
            [pl.col("published_at").str.to_datetime(time_zone="UTC"), "rank"],
        )
        kwargs.setdefault("fieldnames_exclude", None)

        if clean_results:
            kwargs.setdefault(
                "fieldnames_include",
                [
                    "published_at",
                    "rank",
                    "add_rank",
                    "bgg_id",
                    "name",
                    "year",
                    "image_url",
                ],
            )

        return cls.with_defaults(
            site="bgg_hotness",
            item="GameItem",
            clean_results=clean_results,
            **kwargs,
        )

    @classmethod
    def all_sites_config(
        cls,
        *,
        sites: list[str] | None = None,
        **kwargs: Any,
    ) -> Generator[MergeConfig, None, None]:
        sites = sites or [
            "bgg_hotness",
            "dbpedia",
            "luding",
            "spielen",
            "wikidata",
            "bgg",
        ]
        for site in sites:
            if site == "bgg":
                yield cls.site_config(site=site, item="GameItem", **kwargs)
                yield cls.site_config(site=site, item="UserItem", **kwargs)
                yield cls.site_config(site=site, item="RatingItem", **kwargs)
            else:
                yield cls.site_config(site=site, item="GameItem", **kwargs)
