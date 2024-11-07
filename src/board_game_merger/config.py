from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

    import polars as pl
    from polars._typing import IntoExpr


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
