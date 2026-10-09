from __future__ import annotations

import math

import polars as pl


def _clean_value(value):
    """
    Convert Polars/Python values into JSON-safe values.
    """

    if value is None:
        return None

    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None

    if hasattr(value, "isoformat"):
        return value.isoformat()

    return value


def dataframe_to_records(
    df: pl.DataFrame,
    limit: int | None = None,
):
    """
    Convert a Polars DataFrame into JSON-safe dictionaries.
    """

    if limit is not None:
        df = df.head(limit)

    records = df.to_dicts()

    return [
        {
            key: _clean_value(value)
            for key, value in row.items()
        }
        for row in records
    ]


def dataframe_to_dict(
    df: pl.DataFrame,
):
    """
    Convert a one-row analytics DataFrame into a dictionary.
    """

    records = dataframe_to_records(df)

    if not records:
        return {}

    return records[0]

