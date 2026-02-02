#!/usr/bin/env python3
"""Build a CRM-ready list for 16 product HS mapping targets in priority regions."""
from pathlib import Path
import pandas as pd
from urllib.parse import urlparse

from src.processors.hs_mapper import HSMapper

TARGET_REGIONS = {"north_africa", "south_america", "turkey", "central_asia"}

COUNTRY_REGION = {
    "Egypt": "north_africa",
    "Morocco": "north_africa",
    "Tunisia": "north_africa",
    "Algeria": "north_africa",
    "Libya": "north_africa",
    "Brazil": "south_america",
    "Argentina": "south_america",
    "Colombia": "south_america",
    "Peru": "south_america",
    "Ecuador": "south_america",
    "Chile": "south_america",
    "Mexico": "south_america",
    "Turkey": "turkey",
    "Türkiye": "turkey",
    "Uzbekistan": "central_asia",
    "Kazakhstan": "central_asia",
    "Kyrgyzstan": "central_asia",
    "Turkmenistan": "central_asia",
    "Tajikistan": "central_asia",
}

SOCIAL_DOMAINS = {
    "linkedin.com",
    "facebook.com",
    "instagram.com",
    "youtube.com",
    "twitter.com",
    "x.com",
    "tiktok.com",
    "pinterest.com",
    "reddit.com",
    "alibaba.com",
    "aliexpress.com",
    "made-in-china.com",
    "indiamart.com",
    "tradekey.com",
}


def infer_region(row):
    region = row.get("region")
    if isinstance(region, str) and region.strip():
        return region.strip()
    country = row.get("country")
    return COUNTRY_REGION.get(country, "")


def has_website(row):
    value = row.get("website")
    if not value:
        return False
    val = str(value).strip()
    return val not in {"", "[]", "nan", "None", "null"}


def is_social_website(value):
    if not value:
        return False
    try:
        url = str(value).strip()
        if not url or url.lower() in {"nan", "none", "null", "[]"}:
            return False
        if not url.startswith("http"):
            url = f"https://{url}"
        domain = urlparse(url).netloc.lower().replace("www.", "")
        return any(domain == d or domain.endswith(f".{d}") for d in SOCIAL_DOMAINS)
    except Exception:
        return False


def main():
    project_root = Path(__file__).resolve().parents[1]
    src = project_root / "outputs" / "crm" / "targets_master.csv"
    if not src.exists():
        print(f"targets_master.csv not found at {src}")
        return

    df = pd.read_csv(src)

    # Region filter
    df["region"] = df.apply(infer_region, axis=1)
    df = df[df["region"].isin(TARGET_REGIONS)].copy()

    # Require a non-social website (contact channel)
    df = df[df.apply(has_website, axis=1)]
    df = df[~df["website"].apply(is_social_website)]

    # Optional: prefer customer or end-user labels if present
    if "lead_role" in df.columns:
        df = df[(df["lead_role"].isna()) | (df["lead_role"].isin(["CUSTOMER", "END_USER", "end_user", "customer", "UNKNOWN"]))]
    if "entity_type" in df.columns:
        df = df[(df["entity_type"].isna()) | (df["entity_type"].isin(["end_user", "customer", "unknown"]))]
    if "is_machinery_supplier" in df.columns:
        df = df[df["is_machinery_supplier"].astype(str).str.lower().isin(["false", "0", "nan", "none", ""]) ]

    # Assign HS mapping
    mapper = HSMapper()
    hs_primary = []
    hs_secondary = []
    hs_fallback = []
    hs_reason = []
    hs_keywords = []

    for _, row in df.iterrows():
        context = " ".join(
            str(row.get(col, ""))
            for col in ("context", "company", "source", "source_name", "evidence")
        )
        hs_map = mapper.map_text(context)
        hs_primary.append(hs_map.get("hs_primary", ""))
        hs_secondary.append(hs_map.get("hs_secondary", ""))
        hs_fallback.append(",".join(hs_map.get("hs_fallback", []) or [])
        )
        hs_reason.append(hs_map.get("hs_reason", ""))
        hs_keywords.append(",".join(hs_map.get("hs_matched_keywords", []) or []))

    df["hs_primary"] = hs_primary
    df["hs_secondary"] = hs_secondary
    df["hs_fallback"] = hs_fallback
    df["hs_reason"] = hs_reason
    df["hs_matched_keywords"] = hs_keywords

    # Sort by score if present
    if "score" in df.columns:
        df = df.sort_values("score", ascending=False)

    # Output
    out_dir = project_root / "outputs" / "crm"
    out_dir.mkdir(parents=True, exist_ok=True)

    master_out = out_dir / "targets_master_16_products.csv"
    df.to_csv(master_out, index=False)

    top_out = out_dir / "top100_16_products.csv"
    df.head(100).to_csv(top_out, index=False)

    print(f"Saved {len(df)} leads -> {master_out}")
    print(f"Saved top100 -> {top_out}")


if __name__ == "__main__":
    main()
