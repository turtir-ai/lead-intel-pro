#!/usr/bin/env python3
"""
Final Sales Export - Clean and organized output for sales team.

This script:
1. Takes merged validated output
2. Filters out directory URLs
3. Cleans contact data
4. Creates prioritized sales list
"""

import pandas as pd
import re
from glob import glob
from datetime import datetime
from pathlib import Path

# Additional blocked domains (directories, not company sites)
BLOCKED_DOMAINS = [
    "commonshare.com",
    "opensupplyhub.org",
    "nusalist.com",
    "europages.",
    "mustakbil.com",
    "rehber.corlutso.org.tr",
    "textilegence.com",
    "marketscreener.com",
    "linkedin.com",
    "facebook.com",
    "twitter.com",
    "instagram.com",
    "alibaba.com",
    "made-in-china.com",
    "indiamart.com",
    "tradeindia.com",
    "oeko-tex.com",
    "gots.org",
    "bettercotton.org",
]


def is_real_website(url: str) -> bool:
    """Check if URL is a real company website (not directory)."""
    if not url or pd.isna(url) or str(url).lower() in ['nan', '', 'none']:
        return False
    
    url = str(url).lower()
    
    for blocked in BLOCKED_DOMAINS:
        if blocked in url:
            return False
    
    return True


def clean_emails(emails_raw) -> str:
    """Extract clean email addresses."""
    if not emails_raw or pd.isna(emails_raw) or str(emails_raw).lower() in ['nan', '', '[]', 'none']:
        return ''
    
    emails_str = str(emails_raw)
    
    # Extract all email addresses
    pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    found = re.findall(pattern, emails_str)
    
    # Filter out garbage
    clean = []
    for email in found:
        email_lower = email.lower()
        # Skip known bad patterns
        if any(x in email_lower for x in ['example.com', 'test.com', 'noreply', 'no-reply']):
            continue
        clean.append(email)
    
    return '; '.join(list(set(clean))[:3])  # Max 3 unique emails


def clean_phones(phones_raw) -> str:
    """Extract clean phone numbers."""
    if not phones_raw or pd.isna(phones_raw) or str(phones_raw).lower() in ['nan', '', '[]', 'none']:
        return ''
    
    phones_str = str(phones_raw)
    
    # Extract phone-like patterns
    pattern = r'[\+\d\(\)\s\-]{7,20}'
    found = re.findall(pattern, phones_str)
    
    # Clean up
    clean = []
    for phone in found:
        digits_only = re.sub(r'\D', '', phone)
        if len(digits_only) >= 7 and len(digits_only) <= 15:
            # Skip placeholder numbers
            if digits_only not in ['10000000', '15000000', '00000000']:
                clean.append(phone.strip())
    
    return '; '.join(list(set(clean))[:2])  # Max 2 phones


def get_priority(row) -> int:
    """Calculate sales priority (1=highest, 5=lowest)."""
    score = 5  # Default lowest
    
    # SCE Sales Ready → Priority 1
    if row.get('sce_sales_ready', False):
        return 1
    
    # Has real website + email → Priority 2
    has_website = is_real_website(row.get('website', ''))
    has_email = bool(row.get('emails_clean', ''))
    
    if has_website and has_email:
        return 2
    
    # Has email but not website → Priority 3
    if has_email:
        return 3
    
    # Has real website only → Priority 4
    if has_website:
        return 4
    
    return 5


def main():
    # Find latest merged file
    merged_files = sorted(glob("outputs/crm/verified_merged_*.csv"))
    if not merged_files:
        print("No merged files found!")
        return
    
    latest = merged_files[-1]
    print(f"Loading: {latest}")
    
    df = pd.read_csv(latest)
    print(f"Total leads: {len(df)}")
    
    # Process each lead
    rows = []
    for _, row in df.iterrows():
        website = row.get('website', '')
        
        # Skip if website is directory
        if website and not is_real_website(website):
            website = ''  # Clear it, we need real website
        
        # Clean contacts
        emails_clean = clean_emails(row.get('emails', ''))
        phones_clean = clean_phones(row.get('phones', ''))
        
        # Build clean row
        clean_row = {
            'company': row.get('company', ''),
            'country': row.get('country', ''),
            'website': website if is_real_website(website) else '',
            'emails': emails_clean,
            'phones': phones_clean,
            'evidence': row.get('evidence_reason', ''),
            'sce_score': row.get('sce_total', 0),
            'sce_signals': row.get('sce_signals', ''),
            'sce_ready': row.get('sce_sales_ready', False),
            'pipeline_context': row.get('context', ''),
            'emails_clean': emails_clean,  # For priority calc
        }
        
        # Calculate priority
        clean_row['priority'] = get_priority(clean_row)
        del clean_row['emails_clean']  # Remove temp field
        
        rows.append(clean_row)
    
    result_df = pd.DataFrame(rows)
    
    # Sort by priority, then by SCE score
    result_df = result_df.sort_values(
        by=['priority', 'sce_score'],
        ascending=[True, False]
    )
    
    # Export
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Full export
    output_path = f"outputs/crm/sales_master_{timestamp}.csv"
    result_df.to_csv(output_path, index=False)
    print(f"\nExported {len(result_df)} leads to {output_path}")
    
    # Priority 1-3 only (actionable leads)
    actionable = result_df[result_df['priority'] <= 3]
    action_path = f"outputs/crm/sales_actionable_{timestamp}.csv"
    actionable.to_csv(action_path, index=False)
    print(f"Exported {len(actionable)} actionable leads (P1-P3) to {action_path}")
    
    # Stats
    print("\n" + "=" * 60)
    print("FINAL SALES EXPORT SUMMARY")
    print("=" * 60)
    print(f"Total leads: {len(result_df)}")
    print()
    print("By Priority:")
    for p in range(1, 6):
        count = len(result_df[result_df['priority'] == p])
        pct = 100 * count / len(result_df)
        labels = {
            1: "SCE Sales Ready (website + evidence)",
            2: "Website + Email",
            3: "Email only",
            4: "Website only",
            5: "Needs research",
        }
        print(f"  P{p}: {count:3d} ({pct:5.1f}%) - {labels[p]}")
    
    print()
    print("By Country:")
    country_counts = result_df['country'].value_counts().head(10)
    for country, count in country_counts.items():
        print(f"  {country}: {count}")
    
    print("\n" + "=" * 60)
    print("TOP 20 ACTIONABLE LEADS")
    print("=" * 60)
    for i, (_, row) in enumerate(actionable.head(20).iterrows(), 1):
        print(f"\n{i}. {row['company']} ({row['country']}) [P{row['priority']}]")
        if row['website']:
            print(f"   Website: {row['website']}")
        if row['emails']:
            print(f"   Email: {row['emails']}")
        if row['phones']:
            print(f"   Phone: {row['phones']}")
        if row['sce_ready']:
            print(f"   SCE: {row['sce_score']:.2f} - {row['sce_signals']}")


if __name__ == "__main__":
    main()
