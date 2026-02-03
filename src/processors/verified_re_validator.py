#!/usr/bin/env python3
"""
Verified Re-Validator Module
Re-validates the manually verified stenter customer list with:
- Website discovery via Brave Search
- Contact extraction (emails, phones)
- SCE evidence verification from actual website content
- Clean, sales-ready export

This is the "gold standard" validator that takes manually curated leads
and enriches them with actual contact data and evidence.
"""

import os
import re
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import pandas as pd

from src.utils.http_client import HttpClient
from src.utils.logger import get_logger

logger = get_logger(__name__)


# ============================================================================
# CONFIGURATION
# ============================================================================

# OEM brands - if found, it's strong E1 evidence
OEM_BRANDS = {
    'monforts', 'montex', 'monfortex', 'monfongs',
    'brückner', 'bruckner', 'brueckner', 'bruekner',
    'krantz', 'santex', 'artos', 'babcock', 'goller',
    'benninger', 'thies', 'fong', 'dilo',
}

# Stenter/finishing keywords (E1)
STENTER_KEYWORDS = {
    'stenter', 'stentor', 'tenter', 'tentor',
    'ramöz', 'ramoz', 'rama', 'ramas', 'ram frame',
    'heat setting', 'heat-setting', 'thermofixation',
    'termofijado', 'termo-fixação', 'термофиксация',
    'chain rail', 'pin chain', 'clip chain', 'needle plate',
}

# Finishing process keywords (E2)
FINISHING_KEYWORDS = {
    'dyeing', 'finishing', 'bleaching', 'mercerizing', 'sanforizing',
    'printing', 'coating', 'laminating', 'calendering',
    'boyama', 'boya', 'terbiye', 'apre', 'boyahane',
    'tinturaria', 'tingimento', 'acabamento', 'estamparia',
    'tintorería', 'teñido', 'acabados', 'blanqueo',
    'färberei', 'veredlung', 'ausrüstung', 'bleicherei',
}

# Directory domains - NOT company websites
DIRECTORY_DOMAINS = {
    'oeko-tex.com', 'services.oeko-tex.com',
    'global-trace-base.org', 'gots.org',
    'abit.org.br', 'texbrasil.com.br', 'febratex.com.br',
    'bettercotton.org', 'wrap.org',
    'linkedin.com', 'facebook.com', 'instagram.com',
    'wikipedia.org', 'youtube.com',
    'emis.com', 'dnb.com', 'kompass.com', 'europages.com',
    'alibaba.com', 'made-in-china.com', 'indiamart.com',
    'zoominfo.com', 'bloomberg.com',
}

# Email regex
EMAIL_REGEX = re.compile(
    r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
    re.IGNORECASE
)

# Phone regex (international formats)
PHONE_REGEX = re.compile(
    r'(?:\+?\d{1,3}[-.\s]?)?\(?\d{2,4}\)?[-.\s]?\d{3,4}[-.\s]?\d{3,4}',
    re.IGNORECASE
)


class VerifiedReValidator:
    """
    Re-validates verified customer list with website discovery and evidence extraction.
    """

    def __init__(
        self,
        brave_api_key: Optional[str] = None,
        output_dir: str = "outputs/crm",
        max_pages_per_site: int = 2,
        delay_between_requests: float = 1.0,
    ):
        """
        Initialize the re-validator.

        Args:
            brave_api_key: Brave Search API key (falls back to env var)
            output_dir: Where to save output files
            max_pages_per_site: Max pages to crawl per website
            delay_between_requests: Seconds between requests
        """
        self.api_key = brave_api_key or os.getenv("BRAVE_API_KEY", "")
        self.output_dir = output_dir
        self.max_pages = max_pages_per_site
        self.delay = delay_between_requests
        
        os.makedirs(output_dir, exist_ok=True)
        
        # HTTP client for fetching pages
        self.http = HttpClient(
            settings={
                "timeout": 15,
                "max_retries": 2,
            },
            policies={}
        )
        
        # Brave search (lazy init)
        self._brave = None
    
    @property
    def brave(self):
        """Lazy init Brave client."""
        if self._brave is None and self.api_key:
            from src.collectors.discovery.brave_search import BraveSearchClient
            self._brave = BraveSearchClient(self.api_key, settings={})
        return self._brave

    def load_verified_list(self, csv_path: str) -> pd.DataFrame:
        """
        Load the manually verified customer list.

        Args:
            csv_path: Path to verified CSV

        Returns:
            DataFrame with verified leads
        """
        df = pd.read_csv(csv_path)
        logger.info(f"Loaded {len(df)} verified leads from {csv_path}")
        
        # Normalize column names
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        
        # Map Turkish column names if present
        col_map = {
            "şirket_adı": "company",
            "ülke": "country",
            "neden_onaylı?_(kanıt/makine)": "evidence_reason",
            "hedef_ürün_(hs_kodu)": "hs_code",
            "kaynak_dosya": "source_file",
        }
        df = df.rename(columns=col_map)
        
        return df

    def search_company_website(self, company: str, country: str) -> Optional[str]:
        """
        Search for company's official website via Brave.

        Args:
            company: Company name
            country: Country name

        Returns:
            Best website URL or None
        """
        if not self.brave:
            logger.warning("Brave API not configured, skipping website search")
            return None

        # Build search query
        # Exclude directories and associations
        query = f'"{company}" "{country}" official website -directory -association -linkedin -facebook'
        
        try:
            results = self.brave.search(query, count=5)
        except Exception as e:
            logger.error(f"Brave search failed for {company}: {e}")
            return None

        if not results:
            return None

        # Score and pick best result
        best_url = None
        best_score = 0

        for item in results:
            url = item.get("url", "")
            if not url:
                continue

            domain = urlparse(url).netloc.lower()

            # Skip directory domains
            if any(bad in domain for bad in DIRECTORY_DOMAINS):
                continue

            # Score based on domain matching company name
            score = self._score_domain_match(company, domain, item.get("title", ""))

            if score > best_score:
                best_score = score
                best_url = url

        if best_score >= 2:
            return best_url
        return None

    def _score_domain_match(self, company: str, domain: str, title: str) -> int:
        """Score how well domain matches company name."""
        score = 0
        company_lower = company.lower()

        # Extract domain base (without TLD)
        domain_base = domain.replace("www.", "").split(".")[0]

        # Direct name in domain
        company_words = re.findall(r'\w+', company_lower)
        for word in company_words:
            if len(word) > 3 and word in domain_base:
                score += 2

        # Company name in title
        for word in company_words:
            if len(word) > 3 and word in title.lower():
                score += 1

        return score

    def fetch_page_content(self, url: str) -> Tuple[str, List[str]]:
        """
        Fetch page and extract text content + internal links.

        Args:
            url: URL to fetch

        Returns:
            Tuple of (text_content, internal_links)
        """
        try:
            resp = self.http.get(url, allow_binary=False)
            if not resp:
                return "", []

            html = resp.text if hasattr(resp, 'text') else str(resp)

            # Extract text
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html, 'html.parser')

            # Remove script/style
            for tag in soup(['script', 'style', 'nav', 'footer', 'header']):
                tag.decompose()

            text = soup.get_text(separator=' ', strip=True)

            # Extract internal links
            base_domain = urlparse(url).netloc
            links = []
            for a in soup.find_all('a', href=True):
                href = a['href']
                full_url = urljoin(url, href)
                if base_domain in urlparse(full_url).netloc:
                    links.append(full_url)

            return text[:50000], links[:20]  # Limit content size

        except Exception as e:
            logger.debug(f"Failed to fetch {url}: {e}")
            return "", []

    def extract_contacts(self, text: str) -> Dict[str, List[str]]:
        """
        Extract emails and phones from text.

        Args:
            text: Text content

        Returns:
            Dict with 'emails' and 'phones' lists
        """
        emails = list(set(EMAIL_REGEX.findall(text)))
        phones = list(set(PHONE_REGEX.findall(text)))

        # Filter out common false positives
        emails = [e for e in emails if not any(x in e.lower() for x in 
                  ['example.com', 'test.com', 'email.com', '@2x.', '.png', '.jpg'])]
        phones = [p for p in phones if len(re.sub(r'\D', '', p)) >= 8]

        return {
            'emails': emails[:5],  # Top 5
            'phones': phones[:3],  # Top 3
        }

    def score_sce_evidence(self, text: str) -> Dict:
        """
        Score SCE (Stenter Customer Evidence) from text content.

        Args:
            text: Website text content

        Returns:
            Dict with e1, e2, e3 scores and signals
        """
        text_lower = text.lower()

        # E1: OEM brands and stenter keywords
        e1_signals = []
        for brand in OEM_BRANDS:
            if brand in text_lower:
                e1_signals.append(f"brand:{brand}")
        for kw in STENTER_KEYWORDS:
            if kw in text_lower:
                e1_signals.append(f"stenter:{kw}")

        # E2: Finishing process keywords
        e2_signals = []
        for kw in FINISHING_KEYWORDS:
            if kw in text_lower:
                e2_signals.append(f"finishing:{kw}")

        # E3: Textile production indicators
        e3_signals = []
        e3_keywords = ['textile', 'fabric', 'mill', 'production', 'manufacturing',
                       'capacity', 'factory', 'plant', 'facility', 'export']
        for kw in e3_keywords:
            if kw in text_lower:
                e3_signals.append(f"production:{kw}")

        # Calculate scores
        e1_score = min(1.0, len(e1_signals) * 0.3)
        e2_score = min(1.0, len(e2_signals) * 0.2)
        e3_score = min(1.0, len(e3_signals) * 0.15)

        # Total score
        total = max(e1_score, (e2_score * 0.6 + e3_score * 0.4))

        # Sales ready if strong evidence
        is_sales_ready = e1_score >= 0.3 or (e2_score >= 0.4 and e3_score >= 0.3)

        return {
            'sce_e1': round(e1_score, 3),
            'sce_e2': round(e2_score, 3),
            'sce_e3': round(e3_score, 3),
            'sce_total': round(total, 3),
            'sce_signals': '; '.join((e1_signals + e2_signals)[:10]),
            'sce_sales_ready': is_sales_ready,
            'sce_confidence': 'high' if e1_score >= 0.5 else ('medium' if total >= 0.3 else 'low'),
        }

    def validate_lead(self, row: pd.Series) -> Dict:
        """
        Validate a single lead.

        Args:
            row: DataFrame row with company info

        Returns:
            Dict with validation results
        """
        company = str(row.get('company', row.get('şirket_adı', ''))).strip()
        country = str(row.get('country', row.get('ülke', ''))).strip()
        evidence_reason = str(row.get('evidence_reason', row.get('neden_onaylı?_(kanıt/makine)', ''))).strip()
        hs_code = str(row.get('hs_code', row.get('hedef_ürün_(hs_kodu)', ''))).strip()

        result = {
            'company': company,
            'country': country,
            'original_evidence': evidence_reason,
            'hs_code': hs_code,
            'website': '',
            'emails': '',
            'phones': '',
            'website_text': '',
            'sce_e1': 0,
            'sce_e2': 0,
            'sce_e3': 0,
            'sce_total': 0,
            'sce_signals': '',
            'sce_sales_ready': False,
            'sce_confidence': 'low',
            'validation_status': 'pending',
            'validated_at': datetime.utcnow().isoformat(),
        }

        if not company:
            result['validation_status'] = 'no_company_name'
            return result

        # Step 1: Search for website
        logger.info(f"Validating: {company} ({country})")
        website = self.search_company_website(company, country)

        if website:
            result['website'] = website
            time.sleep(self.delay)

            # Step 2: Fetch homepage
            text, links = self.fetch_page_content(website)

            if text:
                # Step 3: Extract contacts
                contacts = self.extract_contacts(text)
                result['emails'] = '; '.join(contacts['emails'])
                result['phones'] = '; '.join(contacts['phones'])

                # Step 4: Try to find contact page
                contact_links = [l for l in links if any(x in l.lower() for x in 
                                ['contact', 'kontakt', 'contato', 'iletisim', 'contacto'])]
                
                if contact_links and self.max_pages > 1:
                    time.sleep(self.delay)
                    contact_text, _ = self.fetch_page_content(contact_links[0])
                    if contact_text:
                        text += " " + contact_text
                        more_contacts = self.extract_contacts(contact_text)
                        result['emails'] = '; '.join(set(
                            result['emails'].split('; ') + more_contacts['emails']
                        ))
                        result['phones'] = '; '.join(set(
                            result['phones'].split('; ') + more_contacts['phones']
                        ))

                # Store snippet of website text
                result['website_text'] = text[:500]

                # Step 5: Score SCE evidence
                sce = self.score_sce_evidence(text)
                result.update(sce)

                result['validation_status'] = 'validated'
            else:
                result['validation_status'] = 'website_fetch_failed'
        else:
            result['validation_status'] = 'website_not_found'

        return result

    def validate_batch(self, df: pd.DataFrame, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Validate all leads in batch.

        Args:
            df: DataFrame with verified leads
            limit: Max leads to process (for testing)

        Returns:
            DataFrame with validation results
        """
        results = []
        total = len(df) if limit is None else min(limit, len(df))

        logger.info(f"Starting validation of {total} leads...")

        for i, (_, row) in enumerate(df.iterrows()):
            if limit and i >= limit:
                break

            result = self.validate_lead(row)
            results.append(result)

            # Progress
            if (i + 1) % 10 == 0:
                logger.info(f"Progress: {i + 1}/{total}")

        result_df = pd.DataFrame(results)
        return result_df

    def export_results(self, df: pd.DataFrame, tag: str = "") -> str:
        """
        Export validated results.

        Args:
            df: Validation results DataFrame
            tag: Optional filename tag

        Returns:
            Path to output file
        """
        suffix = f"_{tag}" if tag else ""
        
        # Full export
        full_path = os.path.join(self.output_dir, f"verified_validated{suffix}.csv")
        df.to_csv(full_path, index=False)
        logger.info(f"Exported {len(df)} leads to {full_path}")

        # Sales-ready subset
        sales_ready = df[df['sce_sales_ready'] == True]
        if not sales_ready.empty:
            sr_path = os.path.join(self.output_dir, f"verified_sales_ready{suffix}.csv")
            sales_ready.to_csv(sr_path, index=False)
            logger.info(f"Exported {len(sales_ready)} sales-ready leads to {sr_path}")

        # Summary report
        summary = {
            'total_processed': len(df),
            'websites_found': len(df[df['website'] != '']),
            'emails_found': len(df[df['emails'] != '']),
            'sales_ready': len(sales_ready),
            'high_confidence': len(df[df['sce_confidence'] == 'high']),
            'medium_confidence': len(df[df['sce_confidence'] == 'medium']),
        }
        
        report_path = os.path.join(self.output_dir, f"validation_report{suffix}.txt")
        with open(report_path, 'w') as f:
            f.write(f"Verified Customer Re-Validation Report\n")
            f.write(f"Generated: {datetime.utcnow().isoformat()}\n")
            f.write("=" * 50 + "\n\n")
            for k, v in summary.items():
                pct = 100 * v / max(1, summary['total_processed'])
                f.write(f"{k}: {v} ({pct:.1f}%)\n")
        
        logger.info(f"Report saved to {report_path}")
        return full_path


def run_validation(
    input_csv: str,
    output_dir: str = "outputs/crm",
    limit: Optional[int] = None,
    tag: str = "",
) -> str:
    """
    Run the full validation pipeline.

    Args:
        input_csv: Path to verified customers CSV
        output_dir: Output directory
        limit: Max leads to process
        tag: Output filename tag

    Returns:
        Path to output file
    """
    validator = VerifiedReValidator(output_dir=output_dir)
    
    df = validator.load_verified_list(input_csv)
    results = validator.validate_batch(df, limit=limit)
    output_path = validator.export_results(results, tag=tag)
    
    return output_path


if __name__ == "__main__":
    import sys
    
    # Default paths
    input_csv = "data/inputs/verified_customers.csv"
    output_dir = "outputs/crm"
    limit = None
    
    # Parse args
    if len(sys.argv) > 1:
        input_csv = sys.argv[1]
    if len(sys.argv) > 2:
        limit = int(sys.argv[2])
    
    run_validation(input_csv, output_dir, limit)
