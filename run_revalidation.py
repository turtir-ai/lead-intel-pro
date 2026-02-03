#!/usr/bin/env python3
"""
Re-Validate Verified Stenter Customers

This script takes the manually verified stenter customer list and:
1. Discovers real company websites (not directories)
2. Extracts contact info (emails, phones)
3. Verifies SCE (Stenter Customer Evidence) from website content
4. Exports a clean, sales-ready customer list

Usage:
    python run_revalidation.py [--limit N] [--test]
    
Example:
    python run_revalidation.py --limit 10 --test  # Test with 10 leads
    python run_revalidation.py                     # Full run
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

# Load .env file FIRST before any other imports
from dotenv import load_dotenv
env_path = Path(__file__).parent.parent / ".env"
if env_path.exists():
    load_dotenv(env_path)
    print(f"Loaded .env from {env_path}")

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd

from src.processors.verified_re_validator import VerifiedReValidator
from src.processors.website_resolver import WebsiteResolver
from src.processors.data_cleaner import DataCleaner
from src.utils.logger import get_logger

logger = get_logger(__name__)


# Default paths
VERIFIED_CSV_PATH = "/Users/dev/Documents/germany/Doğrulama/Onaylanmış Stenter Yedek Parça Müşteri Listesi - Table 1.csv"
OUTPUT_DIR = "outputs/crm"


def load_and_dedupe_verified_list(csv_path: str) -> pd.DataFrame:
    """
    Load verified list and deduplicate by company name.
    
    Args:
        csv_path: Path to verified CSV
        
    Returns:
        Deduplicated DataFrame
    """
    logger.info(f"Loading verified list from: {csv_path}")
    
    df = pd.read_csv(csv_path)
    original_count = len(df)
    
    # Normalize column names
    df.columns = [c.strip() for c in df.columns]
    
    # Map Turkish column names
    col_map = {
        "Şirket Adı": "company",
        "Ülke": "country", 
        "Neden Onaylı? (Kanıt/Makine)": "evidence_reason",
        "Hedef Ürün (HS Kodu)": "hs_code",
        "Kaynak Dosya": "source_file",
        "Sıra": "row_number",
    }
    df = df.rename(columns=col_map)
    
    # Dedupe by normalized company name
    df['company_normalized'] = df['company'].str.lower().str.strip()
    df = df.drop_duplicates(subset=['company_normalized'], keep='first')
    df = df.drop(columns=['company_normalized'])
    
    logger.info(f"Loaded {original_count} rows, deduplicated to {len(df)} unique companies")
    
    return df


def apply_noise_filter(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply noise filtering to remove non-customer entries.
    
    Args:
        df: Verified DataFrame
        
    Returns:
        Filtered DataFrame
    """
    cleaner = DataCleaner()
    
    original_count = len(df)
    valid_mask = []
    
    for _, row in df.iterrows():
        company = str(row.get('company', '')).strip()
        context = str(row.get('evidence_reason', '')).strip()
        
        # Check if noise
        is_noise = cleaner.is_noise(company)
        is_non_customer = cleaner.is_non_customer(company, context)
        
        valid_mask.append(not is_noise and not is_non_customer)
    
    df = df[valid_mask]
    logger.info(f"Noise filter: {original_count} -> {len(df)} leads")
    
    return df


def run_revalidation(
    input_csv: str = VERIFIED_CSV_PATH,
    output_dir: str = OUTPUT_DIR,
    limit: int = None,
    test_mode: bool = False,
) -> str:
    """
    Run the full re-validation pipeline.
    
    Args:
        input_csv: Path to verified customers CSV
        output_dir: Output directory
        limit: Max leads to process
        test_mode: If True, just show stats without processing
        
    Returns:
        Path to output file
    """
    # Ensure output dir exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Step 1: Load and dedupe
    df = load_and_dedupe_verified_list(input_csv)
    
    # Step 2: Apply noise filter
    df = apply_noise_filter(df)
    
    if test_mode:
        logger.info("=== TEST MODE ===")
        logger.info(f"Would process {len(df)} leads")
        logger.info(f"Sample companies:")
        for _, row in df.head(10).iterrows():
            logger.info(f"  - {row['company']} ({row['country']})")
        return None
    
    # Step 3: Run re-validation
    validator = VerifiedReValidator(output_dir=output_dir)
    results_df = validator.validate_batch(df, limit=limit)
    
    # Step 4: Export
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = validator.export_results(results_df, tag=timestamp)
    
    # Step 5: Summary
    print_summary(results_df)
    
    return output_path


def print_summary(df: pd.DataFrame):
    """Print validation summary."""
    logger.info("\n" + "=" * 60)
    logger.info("RE-VALIDATION SUMMARY")
    logger.info("=" * 60)
    
    total = len(df)
    with_website = len(df[df['website'] != ''])
    with_email = len(df[df['emails'] != ''])
    sales_ready = len(df[df['sce_sales_ready'] == True])
    high_conf = len(df[df['sce_confidence'] == 'high'])
    
    logger.info(f"Total processed:     {total}")
    logger.info(f"Website found:       {with_website} ({100*with_website/max(1,total):.1f}%)")
    logger.info(f"Email found:         {with_email} ({100*with_email/max(1,total):.1f}%)")
    logger.info(f"Sales ready (SCE):   {sales_ready} ({100*sales_ready/max(1,total):.1f}%)")
    logger.info(f"High confidence:     {high_conf}")
    logger.info("=" * 60)
    
    # Top sales-ready leads
    sales_ready_df = df[df['sce_sales_ready'] == True].sort_values('sce_total', ascending=False)
    if not sales_ready_df.empty:
        logger.info("\nTop Sales-Ready Leads:")
        for _, row in sales_ready_df.head(10).iterrows():
            logger.info(f"  {row['company']} ({row['country']}) - SCE: {row['sce_total']:.2f}")


def main():
    parser = argparse.ArgumentParser(
        description="Re-validate verified stenter customers"
    )
    parser.add_argument(
        "--input", "-i",
        default=VERIFIED_CSV_PATH,
        help="Path to verified customers CSV"
    )
    parser.add_argument(
        "--output", "-o", 
        default=OUTPUT_DIR,
        help="Output directory"
    )
    parser.add_argument(
        "--limit", "-l",
        type=int,
        default=None,
        help="Max leads to process (for testing)"
    )
    parser.add_argument(
        "--test", "-t",
        action="store_true",
        help="Test mode - show stats without processing"
    )
    
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info("STENTER CUSTOMER RE-VALIDATION PIPELINE")
    logger.info("=" * 60)
    
    output_path = run_revalidation(
        input_csv=args.input,
        output_dir=args.output,
        limit=args.limit,
        test_mode=args.test,
    )
    
    if output_path:
        logger.info(f"\nOutput saved to: {output_path}")


if __name__ == "__main__":
    main()
