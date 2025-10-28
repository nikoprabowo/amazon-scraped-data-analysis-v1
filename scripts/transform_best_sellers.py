#!/usr/bin/env python3
# scripts\transform_best_sellers.py
"""
transform_best_sellers.py
Transform pipeline for "Best Sellers" Amazon CSV (semicolon-separated).

Usage:
    python transform_best_sellers.py input.csv output_clean.csv
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
    m = re.search(r"/dp/([A-Z0-9]{10})", link)
    if m:
        return m.group(1)
    m2 = re.search(r"/gp/product/([A-Z0-9]{10})", link)
    if m2:
        return m2.group(1)
    return None


def parse_price(p):
    if pd.isna(p) or p == "" or str(p).lower() in ["nan", "n/a", "none"]:
        return np.nan
    s = str(p)
    s = re.sub(r"\(.*?\)", "", s)
    s = re.sub(r"[^0-9\.,-]", "", s).strip()
    # unify comma/point: if comma thousands, remove commas
    # heuristics: if both '.' and ',' present, decide:
    if "," in s and "." in s:
        # assume comma thousands, remove commas
        s = s.replace(",", "")
    else:
        # if only commas and no dot, treat comma as thousands separator
        if "," in s and "." not in s:
            s = s.replace(",", "")
    try:
        return float(s)
    except:
        return np.nan


def parse_rating(r):
    if pd.isna(r) or r == "":
        return np.nan
    m = re.search(r"(\d+\.\d+)", str(r))
    if m:
        return float(m.group(1))
    return np.nan


def parse_int(s):
    try:
        if pd.isna(s) or s == "":
            return np.nan
        return int(str(s).replace(",", "").strip())
    except:
        return np.nan


def parse_category_levels(cat_string):
    # We extract the portion after "in " (case-insensitive)
    if not isinstance(cat_string, str):
        return {"category_full": None, "cat_level_1": None, "cat_level_2": None}
    s = cat_string
    m = re.search(r"in\s+(.*)", s, flags=re.IGNORECASE)
    if m:
        cat_full = m.group(1).strip()
    else:
        cat_full = s.strip()
    # split by common separators
    # use '>' or '/' or '&' or ' - ' or '›' or '»' or '|' or ','
    parts = re.split(r"[>/›»\|,/]| - | & ", cat_full)
    parts = [p.strip() for p in parts if p and p.strip()]
    lvl1 = parts[0] if len(parts) >= 1 else None
    lvl2 = parts[1] if len(parts) >= 2 else None
    return {"category_full": cat_full, "cat_level_1": lvl1, "cat_level_2": lvl2}


def price_segment_by_category(df, q_low=0.25, q_high=0.75):
    def seg(series):
        if series.isna().all():
            return pd.Series(["unknown"] * len(series), index=series.index)
        q1 = series.quantile(q_low)
        q2 = series.quantile(q_high)

        def f(x):
            if pd.isna(x):
                return "unknown"
            if x <= q1:
                return "Low"
            elif x <= q2:
                return "Mid"
            else:
                return "High"

        return series.apply(f)

    df["price_segment"] = "unknown"
    for cat, grp in df.groupby("cat_level_1", dropna=False):
        segs = seg(grp["price_norm"])
        df.loc[segs.index, "price_segment"] = segs.values
    return df


def transform(df):
    df.columns = [c.strip() for c in df.columns]
    colmap = {}
    for c in df.columns:
        lc = c.lower()
        if "rank" in lc:
            colmap[c] = "rank"
        elif "page" == lc:
            colmap[c] = "page"
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
        elif "price" in lc:
            colmap[c] = "price_raw"
    df = df.rename(columns=colmap)

    df["rank"] = df.get("rank").apply(lambda x: parse_int(x))
    df["asin"] = df.get("link", "").apply(lambda x: extract_asin(x))
    df["price_norm"] = df.get("price_raw", "").apply(parse_price)
    df["rating"] = df.get("rating_raw", "").apply(parse_rating)
    df["review_count"] = df.get("review_count_raw", "").apply(parse_int)

    cats = df.get("category", "").apply(parse_category_levels).apply(pd.Series)
    df = pd.concat([df, cats], axis=1)

    df["scraped_at"] = pd.to_datetime(SCRAPED_AT).date().isoformat()

    # Avoid division by zero: if rank missing, use np.nan
    df["review_density"] = df.apply(
        lambda r: (
            (r["review_count"] / r["rank"])
            if (
                not pd.isna(r["review_count"])
                and not pd.isna(r["rank"])
                and r["rank"] > 0
            )
            else np.nan
        ),
        axis=1,
    )

    # normalized review density (min-max within category level 1) for comparability
    df["norm_review_density"] = np.nan
    for cat, grp_idx in df.groupby("cat_level_1", dropna=False).groups.items():
        vals = df.loc[grp_idx, "review_density"]
        if vals.dropna().empty:
            continue
        mn = vals.min()
        mx = vals.max()
        if mn == mx:
            df.loc[grp_idx, "norm_review_density"] = 0.0
        else:
            df.loc[grp_idx, "norm_review_density"] = (vals - mn) / (mx - mn)

    df = price_segment_by_category(df)

    keep_cols = [
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
        "review_density",
        "norm_review_density",
        "scraped_at",
    ]
    keep_cols = [c for c in keep_cols if c in df.columns]
    return df[keep_cols]


def main(in_path, out_path):
    df = read_input(in_path)
    out = transform(df)
    out.to_csv(out_path, index=False)
    print(f"Saved cleaned file to {out_path}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python transform_best_sellers.py input.csv output.csv")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])
