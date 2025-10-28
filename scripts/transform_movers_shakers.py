#!/usr/bin/env python3
# scripts\transform_movers_shakers.py
"""
transform_movers_shakers.py
Transform pipeline for "Movers & Shakers" Amazon CSV (semicolon-separated).

Usage:
    python transform_movers_shakers.py input.csv output_clean.csv
"""

import sys
import pandas as pd
import numpy as np
import re
from datetime import datetime

SCRAPED_AT = "2025-10-25"


def read_input(path):
    return pd.read_csv(path, sep=";", dtype=str, keep_default_na=False)


def extract_asin(link):
    if not isinstance(link, str):
        return None
    for pattern in [r"/dp/([A-Z0-9]{10})", r"/gp/product/([A-Z0-9]{10})"]:
        m = re.search(pattern, link)
        if m:
            return m.group(1)
    return None


def parse_price(p):
    """Cleans and normalizes Amazon price strings to float."""
    if not isinstance(p, str) or p.strip() == "":
        return np.nan

    s = p.lower().strip()

    s = re.sub(r"\(.*?\)", "", s)
    s = re.sub(
        r"(converted|approx|usd|us\$|from|price|now|only)", "", s, flags=re.IGNORECASE
    )

    s = re.sub(r"[^0-9\.,-]", "", s)
    s = s.replace(",", "")

    m = re.findall(r"\d+\.\d+|\d+", s)
    if not m:
        return np.nan

    try:
        return float(m[-1])
    except:
        return np.nan


def parse_rating(r):
    if not isinstance(r, str) or r.strip() == "":
        return np.nan
    m = re.search(r"(\d+\.\d+)", r)
    return float(m.group(1)) if m else np.nan


def parse_int(s):
    try:
        if not isinstance(s, str) or s.strip() == "":
            return np.nan
        return int(re.sub(r"[^0-9]", "", s))
    except:
        return np.nan


def parse_float(s):
    try:
        if not isinstance(s, str) or s.strip() == "":
            return np.nan
        return float(re.sub(r"[^0-9\.-]", "", s))
    except:
        return np.nan


def parse_category_levels(cat_string):
    if not isinstance(cat_string, str) or cat_string.strip() == "":
        return {"category_full": None, "cat_level_1": None, "cat_level_2": None}
    m = re.search(r"in\s+(.*)", cat_string, flags=re.IGNORECASE)
    cat_full = m.group(1).strip() if m else cat_string.strip()
    parts = re.split(r"[>/›»\|,/]| - | & ", cat_full)
    parts = [p.strip() for p in parts if p]
    return {
        "category_full": cat_full,
        "cat_level_1": parts[0] if len(parts) > 0 else None,
        "cat_level_2": parts[1] if len(parts) > 1 else None,
    }


def price_segment_by_category(df, q_low=0.25, q_high=0.75):
    def seg(series):
        if series.isna().all():
            return pd.Series(["unknown"] * len(series), index=series.index)
        q1, q2 = series.quantile(q_low), series.quantile(q_high)

        def classify(x):
            if pd.isna(x):
                return "unknown"
            if x <= q1:
                return "Low"
            elif x <= q2:
                return "Mid"
            return "High"

        return series.apply(classify)

    df["price_segment"] = "unknown"
    for cat, idx in df.groupby("cat_level_1", dropna=False).groups.items():
        segs = seg(df.loc[idx, "price_norm"])
        df.loc[idx, "price_segment"] = segs.values
    return df


def transform(df):
    df.columns = [c.strip() for c in df.columns]

    colmap = {}
    for c in df.columns:
        lc = c.lower()
        if lc == "page":
            colmap[c] = "page"
        elif lc.startswith("rank") and "sales" not in lc:
            colmap[c] = "rank"
        elif "category" in lc:
            colmap[c] = "category"
        elif "title" in lc:
            colmap[c] = "title"
        elif "link" in lc or "url" in lc:
            colmap[c] = "link"
        elif "rating" in lc:
            colmap[c] = "rating_raw"
        elif "review" in lc:
            colmap[c] = "review_count_raw"
        elif "price" in lc and "move" not in lc:
            colmap[c] = "price_raw"
        elif "move" in lc:
            colmap[c] = "move_pct_raw"
        elif "sales_rank_now" in lc or "rank now" in lc:
            colmap[c] = "sales_rank_now_raw"
        elif "sales_rank_was" in lc or "rank was" in lc:
            colmap[c] = "sales_rank_was_raw"

    df = df.rename(columns=colmap)

    df["rank"] = df["rank"].apply(parse_int)
    df["asin"] = df["link"].apply(extract_asin)
    df["price_norm"] = df["price_raw"].apply(parse_price)
    df["rating"] = df["rating_raw"].apply(parse_rating)
    df["review_count"] = df["review_count_raw"].apply(parse_int)
    df["move_pct"] = df["move_pct_raw"].apply(parse_float)
    df["sales_rank_now"] = df["sales_rank_now_raw"].apply(parse_float)
    df["sales_rank_was"] = df["sales_rank_was_raw"].apply(parse_float)

    for col in ["sales_rank_now", "sales_rank_was", "move_pct"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    cats = df["category"].apply(parse_category_levels).apply(pd.Series)
    df = pd.concat([df, cats], axis=1)

    df["sales_rank_change"] = df.apply(
        lambda r: (
            r["sales_rank_was"] - r["sales_rank_now"]
            if pd.notna(r["sales_rank_was"]) and pd.notna(r["sales_rank_now"])
            else np.nan
        ),
        axis=1,
    )
    df["sales_rank_change_abs"] = df["sales_rank_change"].abs()

    def direction(x):
        if pd.isna(x):
            return None
        if x > 0:
            return "up"
        if x < 0:
            return "down"
        return "no_change"

    df["move_direction"] = df["sales_rank_change"].apply(direction)

    df["review_density"] = df.apply(
        lambda r: (
            r["review_count"] / r["rank"]
            if pd.notna(r["review_count"]) and pd.notna(r["rank"]) and r["rank"] > 0
            else np.nan
        ),
        axis=1,
    )

    df["norm_review_density"] = np.nan
    for cat, idx in df.groupby("cat_level_1", dropna=False).groups.items():
        vals = df.loc[idx, "review_density"].dropna()
        if len(vals) < 2:
            df.loc[idx, "norm_review_density"] = 0.0
            continue
        mn, mx = vals.min(), vals.max()
        df.loc[idx, "norm_review_density"] = (df.loc[idx, "review_density"] - mn) / (
            mx - mn
        )

    df = price_segment_by_category(df)

    df["scraped_at"] = SCRAPED_AT

    keep = [
        "rank",
        "page",
        "category",
        "category_full",
        "cat_level_1",
        "cat_level_2",
        "title",
        "asin",
        "link",
        "rating",
        "review_count",
        "price_raw",
        "price_norm",
        "price_segment",
        "move_pct",
        "sales_rank_now",
        "sales_rank_was",
        "sales_rank_change",
        "sales_rank_change_abs",
        "move_direction",
        "review_density",
        "norm_review_density",
        "scraped_at",
    ]
    return df[keep]


def main(in_path, out_path):
    df = read_input(in_path)
    out = transform(df)
    out.to_csv(out_path, index=False)
    print(f"Saved cleaned movers & shakers to {out_path}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python transform_movers_shakers.py input.csv output.csv")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])
