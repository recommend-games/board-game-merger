from __future__ import annotations

import json
import logging
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
from tqdm import tqdm

if TYPE_CHECKING:
    from board_game_merger.config import MergeConfig

LOGGER = logging.getLogger(__name__)
MAX_DISPLAY_ITEMS = 10


def merge_files(
    *,
    merge_config: MergeConfig,
    overwrite: bool = False,
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

    if (
        merge_config.fieldnames_include is not None
        and merge_config.fieldnames_exclude is not None
    ):
        msg = "Cannot specify both fieldnames_include and fieldnames_exclude"
        raise ValueError(msg)

    in_paths_iter = (
        merge_config.in_paths
        if isinstance(merge_config.in_paths, list)
        else [merge_config.in_paths]
    )
    in_paths = [Path(in_path).resolve() for in_path in in_paths_iter]
    out_path = Path(merge_config.out_path).resolve()

    LOGGER.info(
        "Merging items from %s into <%s>",
        f"[{len(in_paths)} paths]" if len(in_paths) > MAX_DISPLAY_ITEMS else in_paths,
        out_path,
    )

    if not overwrite and out_path.exists():
        LOGGER.warning("Output file already exists, use overwrite to replace it")
        return

    data = pl.scan_ndjson(
        source=in_paths,
        schema=merge_config.schema,
        batch_size=512,
        low_memory=True,
        rechunk=True,
        ignore_errors=True,
    )

    latest_col = (
        merge_config.latest_col
        if isinstance(merge_config.latest_col, list)
        else [merge_config.latest_col]
    )
    if merge_config.latest_min is not None:
        LOGGER.info("Filtering out rows before <%s>", merge_config.latest_min)
        data = data.filter(latest_col[0] >= merge_config.latest_min)

    key_col = (
        merge_config.key_col
        if isinstance(merge_config.key_col, list)
        else [merge_config.key_col]
    )
    key_col_dict = {f"__key__{i}": key for i, key in enumerate(key_col)}

    LOGGER.info("Merging rows with identical keys: %s", key_col)
    LOGGER.info("Keeping latest by: %s", latest_col)

    data = (
        data.sort(by=latest_col, descending=True, nulls_last=True)
        .with_columns(**key_col_dict)
        .unique(subset=list(key_col_dict), keep="first")
        .drop(key_col_dict.keys())
    )

    if merge_config.sort_fields is not None:
        LOGGER.info(
            "Sorting data by: %s (%s)",
            merge_config.sort_fields,
            "descending" if merge_config.sort_descending else "ascending",
        )
        data = data.sort(
            merge_config.sort_fields,
            descending=merge_config.sort_descending,
        )

    if merge_config.fieldnames_include is not None:
        LOGGER.info("Selecting fields: %s", merge_config.fieldnames_include)
        data = data.select(merge_config.fieldnames_include)
    elif merge_config.fieldnames_exclude is not None:
        LOGGER.info("Excluding fields: %s", merge_config.fieldnames_exclude)
        data = data.select(pl.exclude(merge_config.fieldnames_exclude))

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
