#!/usr/bin/env python3
"""
GPT V3 Pipeline Fix Script
Fixes the critical issues identified in the audit:

1. Schema fix: country=URL -> extract country from address/location
2. Noise cleanup: Remove event/news/nav content 
3. Role reclassification: Proper customer vs supplier split
4. SCE validation: Only mark sales_ready if evidence is strong
5. Export split: customers_end_user / channels_oem_reps / dropped_noise
"""

import os
import re
import pandas as pd
from pathlib import Path

# Add project root to path
import sys
sys.path.insert(0, str(Path(__file__).parent))

from src.utils.logger import get_logger
from src.processors.entity_quality_gate_v2 import EntityQualityGateV2
from src.processors.lead_role_classifier import LeadRoleClassifier
from src.processors.sce_scorer import SCEScorer

logger = get_logger(__name__)


# ============================================================
# PATCH A: Schema Contract - Fix OEKO-TEX country=URL bug
# ============================================================

def fix_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Fix schema issues: country=URL, website=email, etc."""
    
    url_pattern = re.compile(r'https?://|www\.|\.com/|\.org/|\.net/', re.IGNORECASE)
    email_pattern = re.compile(r'@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
    
    # Country extraction from address patterns
    country_patterns = {
        r'\b(Egypt|Mısır)\b': 'Egypt',
        r'\b(Morocco|Fas)\b': 'Morocco', 
        r'\b(Tunisia|Tunus)\b': 'Tunisia',
        r'\b(Brazil|Brasil)\b': 'Brazil',
        r'\b(Argentina|Arjantin)\b': 'Argentina',
        r'\b(Peru)\b': 'Peru',
        r'\b(Colombia|Kolombiya)\b': 'Colombia',
        r'\b(Chile|Şili)\b': 'Chile',
        r'\b(Ecuador|Ekvador)\b': 'Ecuador',
        r'\b(Turkey|Türkiye)\b': 'Turkey',
        r'\b(Pakistan)\b': 'Pakistan',
        r'\b(India|Hindistan)\b': 'India',
        r'\b(Bangladesh|Bangladeş)\b': 'Bangladesh',
    }
    
    def extract_country(row):
        """Extract actual country from address or context if country is invalid."""
        country = str(row.get('country', ''))
        
        # If country looks like URL, try to extract from address
        if url_pattern.search(country):
            # Look in address field
            address = str(row.get('address', ''))
            context = str(row.get('context', ''))
            search_text = f"{address} {context}"
            
            for pattern, country_name in country_patterns.items():
                if re.search(pattern, search_text, re.IGNORECASE):
                    return country_name
            
            # Fallback: if source is oekotex, use source_name hints
            if 'oeko' in str(row.get('source_type', '')).lower():
                return None  # Will need manual review
        
        return country if country and not url_pattern.search(country) else None
    
    def fix_website(row):
        """Fix website field if it contains email."""
        website = str(row.get('website', ''))
        
        if email_pattern.search(website) and not website.startswith('http'):
            # This is an email, not a website
            return None
        
        if url_pattern.search(website):
            return website
        
        return None
    
    # Apply fixes
    df['country_fixed'] = df.apply(extract_country, axis=1)
    df['website_fixed'] = df.apply(fix_website, axis=1)
    
    # Log changes
    country_fixes = (df['country_fixed'] != df['country']).sum()
    website_fixes = (df['website_fixed'] != df['website']).sum()
    logger.info(f"Schema fix: {country_fixes} country fields, {website_fixes} website fields corrected")
    
    # Replace with fixed values
    df['country'] = df['country_fixed'].fillna(df['country'])
    df['website'] = df['website_fixed'].fillna(df['website'])
    df = df.drop(columns=['country_fixed', 'website_fixed'], errors='ignore')
    
    return df


# ============================================================
# PATCH B: Noise Gate - Enhanced filtering
# ============================================================

NOISE_PATTERNS = re.compile(
    r'^(view basket|new expertise in energy|energy|yarn|clients|home_|'
    r'istanbul event|sign in|menu|header|footer|nav|'
    r'textile world fiber|textile machinery|'
    r'the new|the latest|the best|top \d+|'
    r'read more|learn more|click here|subscribe|'
    r'cookie|privacy policy|terms of|about us|contact us|'
    r'page \d|section \d|slide \d|tab|button|link|'
    r'loading|please wait|error|404|403|undefined|null|'
    r'fiber\s*&\s*yarn|garment\s*&\s*fashion|'
    r'sign in textile|view all|see more|show more|'
    r'planet|sustainability report)$|'
    # Article headline patterns - must be exact matches
    r'^new expertise in energy and environmental|'
    r'^spotlight on the montex|'
    r'^introduction to textile|'
    r'^guide to textile|'
    r'^types of stenter|'
    r'^how textile|'
    r'^booming textile|'
    r'^fascinating textile',
    re.IGNORECASE
)

# Single word generic terms
GENERIC_SINGLE_WORDS = {
    'energy', 'yarn', 'textile', 'textiles', 'fabric', 'fabrics',
    'machine', 'machines', 'equipment', 'parts', 'planet',
    'unknown', 'other', 'various', 'clients', 'data',
}

def is_noise(company: str) -> bool:
    """Check if company name is noise/junk."""
    if not company or not company.strip():
        return True
    
    company = company.strip()
    company_lower = company.lower()
    
    # Check noise patterns
    if NOISE_PATTERNS.match(company):
        return True
    
    # Check single word generics
    if company_lower in GENERIC_SINGLE_WORDS:
        return True
    
    # Check if too short
    if len(company) < 3:
        return True
    
    # Check if it's a truncated OEM name
    truncated_oems = ['CKNER', 'NFORTS', 'ANTEX', 'ABCOCK', 'OLLER']
    if any(company.upper().startswith(t) for t in truncated_oems):
        return True
    
    # Check for website navigation content
    nav_keywords = ['view basket', 'checkout', 'home_', 'sign in', 
                   'menu', 'header', 'footer', 'cookie', 'privacy']
    if any(kw in company_lower for kw in nav_keywords):
        return True
    
    # Check for newlines (multi-line content = not a company name)
    if '\n' in company:
        return True
    
    return False


def apply_noise_filter(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Filter out noise entries."""
    noise_mask = df['company'].apply(is_noise)
    
    clean = df[~noise_mask].copy()
    noise = df[noise_mask].copy()
    
    logger.info(f"Noise filter: removed {len(noise)} entries, kept {len(clean)}")
    
    return clean, noise


# ============================================================
# PATCH C: Multilingual Role Classifier Enhancement
# ============================================================

# Additional multilingual customer signals
CUSTOMER_SIGNALS_MULTI = {
    # Portuguese (Brazil)
    'tinturaria', 'acabamento', 'beneficiamento', 'estamparia',
    'tecelagem', 'malharia', 'fábrica têxtil', 'indústria têxtil',
    'rama', 'termofixação', 'alvejamento', 'mercerização',
    'fábrica de tecidos', 'produção têxtil',
    
    # Spanish (LATAM)
    'tintorería', 'acabado', 'tejeduría', 'textil',
    'planta de acabado', 'fábrica textil', 'industria textil',
    'rama', 'termofijado', 'blanqueo', 'teñido',
    
    # German
    'färberei', 'ausrüstung', 'veredlung', 'spannrahmen',
    'textilveredelung', 'weberei', 'strickerei',
    
    # Turkish
    'terbiye', 'boyahane', 'apre', 'ram', 'fikse',
    'dokuma', 'örme', 'iplik', 'boya tesisi',
}

# Additional multilingual supplier/channel signals
CHANNEL_SIGNALS_MULTI = {
    # Portuguese (Brazil)
    'máquinas', 'equipamentos', 'representações', 'distribuidor',
    'revenda', 'assistência técnica', 'automação', 'peças',
    'rolamentos', 'correias', 'manutenção',
    
    # Spanish (LATAM)
    'maquinaria', 'equipos', 'representante', 'distribuidor',
    'servicio técnico', 'repuestos', 'automatización',
    
    # Program suffixes to strip
    '- texbrasil', '- programa texbrasil', '- apex brasil',
}

def enhanced_role_classify(lead: dict) -> str:
    """Enhanced role classification with multilingual support."""
    company = str(lead.get('company', '')).lower()
    context = str(lead.get('context', '')).lower()
    source_type = str(lead.get('source_type', '')).lower()
    
    text = f"{company} {context}"
    
    # Strip program suffixes for better classification
    for suffix in ['- texbrasil', '- programa texbrasil', '- apex brasil']:
        text = text.replace(suffix, '')
    
    # OEM manufacturers are INTERMEDIARY (they sell machines, not buy parts)
    oem_names = ['brückner', 'bruckner', 'monforts', 'krantz', 'santex', 
                 'artos', 'babcock', 'goller', 'thies', 'benninger', 'dilmenler']
    if any(oem in company for oem in oem_names):
        return 'INTERMEDIARY'
    
    # News sites are INTERMEDIARY
    news_sites = ['textileworld', 'fibre2fashion', 'texdata', 'eurotextile', 
                  'textile today', 'just-style', 'fashionunited']
    if any(news in company for news in news_sites):
        return 'INTERMEDIARY'
    
    customer_score = 0
    channel_score = 0
    
    # Check customer signals
    for signal in CUSTOMER_SIGNALS_MULTI:
        if signal in text:
            customer_score += 1
    
    # Check channel signals
    for signal in CHANNEL_SIGNALS_MULTI:
        if signal in text:
            channel_score += 1
    
    # Source type boost
    if source_type in ['gots', 'oekotex', 'known_manufacturer']:
        customer_score += 2
    elif source_type in ['fair_exhibitor', 'directory']:
        customer_score += 1
    
    # Make decision
    if customer_score > channel_score + 1:
        return 'CUSTOMER'
    elif channel_score > customer_score:
        return 'INTERMEDIARY'
    else:
        return lead.get('role', 'UNKNOWN')


# ============================================================
# PATCH D: SCE Sales Ready Validation
# ============================================================

def validate_sce_sales_ready(lead: dict) -> bool:
    """
    Validate SCE sales_ready flag.
    Only mark as sales ready if:
    1. E1 >= 0.4 (direct stenter/OEM evidence) AND role is CUSTOMER
    2. OR (E2 >= 0.5 AND E3 >= 0.4) AND role is CUSTOMER
    
    NOT sales ready if:
    - Role is INTERMEDIARY, BRAND, or NOISE
    - Company is OEM manufacturer (Brückner, Monforts, etc.)
    """
    role = lead.get('role', 'UNKNOWN')
    
    # Exclude non-customers
    if role in ['INTERMEDIARY', 'BRAND']:
        return False
    
    # Exclude OEM manufacturers
    company = str(lead.get('company', '')).lower()
    oem_names = ['brückner', 'bruckner', 'monforts', 'krantz', 'santex', 'artos', 'babcock', 'goller']
    if any(oem in company for oem in oem_names):
        return False
    
    # Check SCE scores
    e1 = float(lead.get('sce_e1', 0))
    e2 = float(lead.get('sce_e2', 0))
    e3 = float(lead.get('sce_e3', 0))
    
    # Stricter criteria
    if e1 >= 0.4 and role == 'CUSTOMER':
        return True
    
    if e2 >= 0.5 and e3 >= 0.4 and role == 'CUSTOMER':
        return True
    
    return False


# ============================================================
# PATCH E: CRM Export Split
# ============================================================

def export_split(df: pd.DataFrame, output_dir: str = "outputs/crm"):
    """Export leads into 3 separate files."""
    
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # 1. End-user customers (sales target)
    customers = df[df['role'] == 'CUSTOMER'].copy()
    customers_path = f"{output_dir}/customers_end_user.csv"
    customers.to_csv(customers_path, index=False)
    logger.info(f"Exported {len(customers)} customers to {customers_path}")
    
    # 2. Channels/OEM/Reps (different strategy)
    channels = df[df['role'].isin(['INTERMEDIARY', 'BRAND'])].copy()
    channels_path = f"{output_dir}/channels_oem_reps.csv"
    channels.to_csv(channels_path, index=False)
    logger.info(f"Exported {len(channels)} channels to {channels_path}")
    
    # 3. Unknown (manual review)
    unknown = df[df['role'] == 'UNKNOWN'].copy()
    unknown_path = f"{output_dir}/unknown_review.csv"
    unknown.to_csv(unknown_path, index=False)
    logger.info(f"Exported {len(unknown)} unknown for review to {unknown_path}")
    
    # 4. Sales-ready subset (highest priority)
    sales_ready = df[df['sce_sales_ready_validated'] == True].copy()
    sales_path = f"{output_dir}/sales_ready_validated.csv"
    sales_ready.to_csv(sales_path, index=False)
    logger.info(f"Exported {len(sales_ready)} sales-ready leads to {sales_path}")
    
    return customers, channels, unknown, sales_ready


# ============================================================
# MAIN: Run all fixes
# ============================================================

def run_gpt_v3_fixes(input_path: str = "outputs/crm/targets_master.csv"):
    """Run all GPT V3 fixes on the current output."""
    
    logger.info("=" * 60)
    logger.info("🔧 GPT V3 Pipeline Fixes")
    logger.info("=" * 60)
    
    # Load data
    if not os.path.exists(input_path):
        logger.error(f"Input file not found: {input_path}")
        return
    
    df = pd.read_csv(input_path)
    logger.info(f"Loaded {len(df)} leads from {input_path}")
    
    # PATCH A: Schema fix
    logger.info("\n📋 PATCH A: Schema Contract Fix")
    df = fix_schema(df)
    
    # PATCH B: Noise filter
    logger.info("\n🧹 PATCH B: Noise Gate Filter")
    df, noise = apply_noise_filter(df)
    
    # Save noise to separate file
    if len(noise) > 0:
        noise.to_csv("outputs/crm/dropped_noise.csv", index=False)
        logger.info(f"Saved {len(noise)} noise entries to dropped_noise.csv")
    
    # PATCH C: Enhanced role classification
    logger.info("\n🏷️ PATCH C: Multilingual Role Classification")
    df['role_enhanced'] = df.apply(enhanced_role_classify, axis=1)
    role_changes = (df['role_enhanced'] != df['role']).sum()
    logger.info(f"Role reclassification: {role_changes} leads changed")
    df['role'] = df['role_enhanced']
    df = df.drop(columns=['role_enhanced'], errors='ignore')
    
    # PATCH D: SCE validation
    logger.info("\n✅ PATCH D: SCE Sales Ready Validation")
    df['sce_sales_ready_validated'] = df.apply(validate_sce_sales_ready, axis=1)
    original_sales_ready = df['sce_sales_ready'].sum() if 'sce_sales_ready' in df.columns else 0
    validated_sales_ready = df['sce_sales_ready_validated'].sum()
    logger.info(f"SCE validation: {original_sales_ready} -> {validated_sales_ready} sales-ready")
    
    # PATCH E: Export split
    logger.info("\n📤 PATCH E: CRM Export Split")
    customers, channels, unknown, sales_ready = export_split(df)
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("📊 GPT V3 FIX SUMMARY")
    logger.info("=" * 60)
    logger.info(f"  Total leads after fix: {len(df)}")
    logger.info(f"  Customers (end-user): {len(customers)}")
    logger.info(f"  Channels (OEM/reps): {len(channels)}")
    logger.info(f"  Unknown (review): {len(unknown)}")
    logger.info(f"  Sales Ready (validated): {len(sales_ready)}")
    logger.info(f"  Noise dropped: {len(noise)}")
    
    # Role distribution
    logger.info("\n  Role Distribution:")
    for role, count in df['role'].value_counts().items():
        logger.info(f"    {role}: {count}")
    
    # Country distribution (top 10)
    logger.info("\n  Country Distribution (top 10):")
    for country, count in df['country'].value_counts().head(10).items():
        if pd.notna(country):
            logger.info(f"    {country}: {count}")
    
    return df


if __name__ == "__main__":
    os.chdir(Path(__file__).parent)
    run_gpt_v3_fixes()
