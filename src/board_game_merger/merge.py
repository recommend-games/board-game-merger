from __future__ import annotations

import json
import logging
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any, Union

import polars as pl
from tqdm import tqdm

if TYPE_CHECKING:
    from polars._typing import IntoExpr

LOGGER = logging.getLogger(__name__)
PATH_LIKE = Union[str, Path]
MAX_DISPLAY_ITEMS = 10


def merge_files(
    *,
    in_paths: PATH_LIKE | list[str] | list[Path],
    schema: pl.Schema,
    out_path: PATH_LIKE,
    key_col: IntoExpr | list[IntoExpr],
    latest_col: IntoExpr | list[IntoExpr],
    latest_min: Any = None,
    sort_fields: IntoExpr | list[IntoExpr] | None = None,
    sort_descending: bool = False,
    fieldnames_include: IntoExpr | list[IntoExpr] | None = None,
    fieldnames_exclude: str | list[str] | None = None,
    drop_empty: bool = False,
    sort_keys: bool = False,
    progress_bar: bool = False,
) -> None:
    """
    Merge files into one. Execute the following steps:

    - Filter out rows older than latest_min
    - For each row with identical keys, keep the latest one
    - Sort the output by keys, latest, or fields
    - Select only specified fields or exclude some fields
    - For each row, remove empty fields and sort keys alphabetically
    """

    if fieldnames_include is not None and fieldnames_exclude is not None:
        msg = "Cannot specify both fieldnames_include and fieldnames_exclude"
        raise ValueError(msg)

    in_paths_iter = in_paths if isinstance(in_paths, list) else [in_paths]
    in_paths = [Path(in_path).resolve() for in_path in in_paths_iter]
    out_path = Path(out_path).resolve()

    LOGGER.info(
        "Merging items from %s into <%s>",
        f"[{len(in_paths)} paths]" if len(in_paths) > MAX_DISPLAY_ITEMS else in_paths,
        out_path,
    )
    data = pl.scan_ndjson(
        source=in_paths,
        schema=schema,
        batch_size=512,
        low_memory=True,
        rechunk=True,
        ignore_errors=True,
    )

    latest_col = latest_col if isinstance(latest_col, list) else [latest_col]
    if latest_min is not None:
        LOGGER.info("Filtering out rows before <%s>", latest_min)
        data = data.filter(latest_col[0] >= latest_min)

    key_col = key_col if isinstance(key_col, list) else [key_col]
    key_col_dict = {f"__key__{i}": key for i, key in enumerate(key_col)}

    LOGGER.info("Merging rows with identical keys: %s", key_col)
    LOGGER.info("Keeping latest by: %s", latest_col)

    data = (
        data.sort(by=latest_col, descending=True, nulls_last=True)
        .with_columns(**key_col_dict)
        .unique(subset=list(key_col_dict), keep="first")
        .drop(key_col_dict.keys())
    )

    if sort_fields is not None:
        LOGGER.info(
            "Sorting data by: %s (%s)",
            sort_fields,
            "descending" if sort_descending else "ascending",
        )
        data = data.sort(sort_fields, descending=sort_descending)

    if fieldnames_include is not None:
        LOGGER.info("Selecting fields: %s", fieldnames_include)
        data = data.select(fieldnames_include)
    elif fieldnames_exclude is not None:
        LOGGER.info("Excluding fields: %s", fieldnames_exclude)
        data = data.select(pl.exclude(fieldnames_exclude))

    LOGGER.info("Collecting results, this may take a while…")
    result = data.collect()
    LOGGER.info("Finished collecting results with shape %dx%d", *result.shape)
    num_rows = len(result)

    if not drop_empty and not sort_keys:
        LOGGER.info("Writing merged data to <%s>", out_path)
        result.write_ndjson(out_path)
        LOGGER.info("Done.")
        return

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_file = Path(temp_dir) / "merged.jl"
        LOGGER.info("Writing merged data to <%s>", temp_file)
        result.write_ndjson(temp_file)
        del result

        LOGGER.info("Writing cleaned data to <%s>", out_path)
        with temp_file.open("r") as in_file, out_path.open("w") as out_file:
            lines = (
                tqdm(
                    in_file,
                    desc="Cleaning data",
                    unit=" rows",
                    total=num_rows,
                )
                if progress_bar
                else in_file
            )
            for line in lines:
                row = json.loads(line)
                if drop_empty:
                    row = {k: v for k, v in row.items() if v}
                json.dump(row, out_file, sort_keys=sort_keys, separators=(",", ":"))
                out_file.write("\n")
    LOGGER.info("Done.")
