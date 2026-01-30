"""
Pattern Analyzer - Heuristic Data Pattern Detection

Analyzes captured network data and HTML to detect patterns
WITHOUT using any LLM. Pure Python pattern matching.

Key Features:
- JSON structure analysis
- Repeating element detection (lists, tables, cards)
- Field name heuristics (company, email, phone, etc.)
- Pagination pattern detection
- API endpoint classification
"""

import json
import re
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from collections import Counter
from bs4 import BeautifulSoup
import hashlib


class PatternAnalyzer:
    """
    Analyze website data patterns using pure heuristics.
    No LLM required - uses pattern matching and statistics.
    """
    
    # Field name mappings (discovered field -> normalized field)
    FIELD_MAPPINGS = {
        # Company
        "company": "company",
        "company_name": "company",
        "companyname": "company",
        "name": "company",
        "organization": "company",
        "org": "company",
        "business": "company",
        "firm": "company",
        "manufacturer": "company",
        "supplier": "company",
        "vendor": "company",
        "title": "company",  # Often company name in directories
        
        # Email
        "email": "email",
        "e-mail": "email",
        "mail": "email",
        "emailaddress": "email",
        "email_address": "email",
        "contact_email": "email",
        
        # Phone
        "phone": "phone",
        "telephone": "phone",
        "tel": "phone",
        "mobile": "phone",
        "fax": "fax",
        "phonenumber": "phone",
        "phone_number": "phone",
        
        # Address
        "address": "address",
        "street": "address",
        "street_address": "address",
        "location": "address",
        
        # City
        "city": "city",
        "town": "city",
        "locality": "city",
        
        # Country
        "country": "country",
        "nation": "country",
        "country_name": "country",
        "countryname": "country",
        "countrycode": "country_code",
        "country_code": "country_code",
        
        # Region
        "state": "region",
        "province": "region",
        "region": "region",
        
        # Website
        "website": "website",
        "url": "website",
        "homepage": "website",
        "web": "website",
        "site": "website",
        "link": "website",
        
        # Description
        "description": "description",
        "desc": "description",
        "about": "description",
        "bio": "description",
        "profile": "description",
        "summary": "description",
        
        # Industry
        "industry": "industry",
        "sector": "industry",
        "category": "industry",
        "type": "industry",
        
        # Certification
        "certificate": "certification",
        "certification": "certification",
        "certified": "certification",
        "standard": "certification",
    }
    
    # Patterns for detecting email addresses
    EMAIL_PATTERN = re.compile(
        r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    )
    
    # Patterns for detecting phone numbers
    PHONE_PATTERN = re.compile(
        r'[\+]?[(]?[0-9]{1,4}[)]?[-\s\./0-9]{7,15}'
    )
    
    # Patterns for detecting URLs
    URL_PATTERN = re.compile(
        r'https?://(?:www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b(?:[-a-zA-Z0-9()@:%_\+.~#?&/=]*)'
    )
    
    def __init__(self):
        pass
    
    def analyze_json_structure(self, data: Any, path: str = "") -> Dict:
        """
        Recursively analyze JSON structure to find lead-relevant fields.
        Returns field mappings and sample values.
        """
        findings = {
            "fields": {},      # field_path -> normalized_name
            "samples": {},     # field_path -> sample_value
            "lists": [],       # paths to list data
            "depth": 0,
        }
        
        self._analyze_recursive(data, path, findings, depth=0)
        return findings
    
    def _analyze_recursive(self, data: Any, path: str, findings: Dict, depth: int):
        """Recursive helper for JSON analysis."""
        findings["depth"] = max(findings["depth"], depth)
        
        if depth > 10:  # Prevent infinite recursion
            return
        
        if isinstance(data, dict):
            for key, value in data.items():
                key_lower = key.lower().replace("-", "_").replace(" ", "_")
                new_path = f"{path}.{key}" if path else key
                
                # Check if key matches known field
                if key_lower in self.FIELD_MAPPINGS:
                    normalized = self.FIELD_MAPPINGS[key_lower]
                    findings["fields"][new_path] = normalized
                    
                    # Store sample value
                    if isinstance(value, str) and len(value) < 200:
                        findings["samples"][new_path] = value
                
                # Recurse
                self._analyze_recursive(value, new_path, findings, depth + 1)
                
        elif isinstance(data, list):
            if len(data) > 0:
                findings["lists"].append({
                    "path": path,
                    "count": len(data),
                    "item_type": type(data[0]).__name__,
                })
                # Analyze first item as template
                if len(data) > 0:
                    self._analyze_recursive(data[0], f"{path}[0]", findings, depth + 1)
    
    def detect_list_pattern(self, data: Any) -> Optional[Dict]:
        """
        Detect if data contains a list of similar items (likely directory entries).
        Returns pattern info if found.
        """
        # Direct list
        if isinstance(data, list) and len(data) >= 3:
            return self._analyze_list_items(data, "root")
        
        # Nested in common wrapper keys
        if isinstance(data, dict):
            for key in ["items", "results", "data", "records", "members", 
                       "companies", "entries", "list", "rows", "hits"]:
                if key in data and isinstance(data[key], list) and len(data[key]) >= 3:
                    return self._analyze_list_items(data[key], key)
        
        return None
    
    def _analyze_list_items(self, items: List, path: str) -> Dict:
        """Analyze a list of items to detect repeating pattern."""
        if not items or not isinstance(items[0], dict):
            return None
        
        # Collect all keys from first 10 items
        all_keys = Counter()
        for item in items[:10]:
            if isinstance(item, dict):
                all_keys.update(item.keys())
        
        # Find common keys (appear in >50% of items)
        threshold = len(items[:10]) * 0.5
        common_keys = [k for k, v in all_keys.items() if v >= threshold]
        
        # Map common keys to normalized fields
        field_mapping = {}
        for key in common_keys:
            key_lower = key.lower().replace("-", "_").replace(" ", "_")
            if key_lower in self.FIELD_MAPPINGS:
                field_mapping[key] = self.FIELD_MAPPINGS[key_lower]
        
        return {
            "path": path,
            "count": len(items),
            "common_keys": common_keys,
            "field_mapping": field_mapping,
            "sample_item": items[0] if items else None,
        }
    
    def extract_from_pattern(self, data: Any, pattern: Dict) -> List[Dict]:
        """
        Extract normalized lead records from data using detected pattern.
        """
        leads = []
        
        # Get the list
        if pattern["path"] == "root":
            items = data
        else:
            items = data.get(pattern["path"], [])
        
        for item in items:
            if not isinstance(item, dict):
                continue
            
            lead = {}
            
            # Extract mapped fields
            for orig_key, norm_key in pattern.get("field_mapping", {}).items():
                if orig_key in item:
                    value = item[orig_key]
                    if isinstance(value, str):
                        lead[norm_key] = value.strip()
                    elif value is not None:
                        lead[norm_key] = str(value)
            
            # Fallback: scan all string values for emails/phones
            if "email" not in lead:
                for value in item.values():
                    if isinstance(value, str):
                        email_match = self.EMAIL_PATTERN.search(value)
                        if email_match:
                            lead["email"] = email_match.group()
                            break
            
            if "phone" not in lead:
                for value in item.values():
                    if isinstance(value, str):
                        phone_match = self.PHONE_PATTERN.search(value)
                        if phone_match:
                            lead["phone"] = phone_match.group()
                            break
            
            if "website" not in lead:
                for value in item.values():
                    if isinstance(value, str):
                        url_match = self.URL_PATTERN.search(value)
                        if url_match:
                            lead["website"] = url_match.group()
                            break
            
            # Only add if we have at least a company name
            if lead.get("company"):
                leads.append(lead)
        
        return leads
    
    def analyze_html_for_patterns(self, html: str) -> Dict:
        """
        Analyze HTML to find repeating patterns (cards, table rows, list items).
        Returns selector patterns for data extraction.
        """
        soup = BeautifulSoup(html, "html.parser")
        
        patterns = {
            "tables": [],
            "lists": [],
            "cards": [],
            "links": [],
        }
        
        # Find tables with company-like data
        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            if len(rows) >= 3:
                # Check headers
                headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
                header_matches = sum(1 for h in headers if any(
                    kw in h for kw in ["company", "name", "email", "country", "phone"]
                ))
                
                if header_matches >= 2:
                    patterns["tables"].append({
                        "rows": len(rows),
                        "headers": headers,
                        "selector": self._get_selector(table),
                    })
        
        # Find repeating card patterns (divs with same class containing company info)
        class_counts = Counter()
        for div in soup.find_all(["div", "article", "li"]):
            classes = div.get("class", [])
            if classes:
                class_key = ".".join(sorted(classes))
                text = div.get_text(strip=True).lower()
                # Check if content looks like company data
                if any(kw in text for kw in ["company", "email", "@", "contact"]):
                    class_counts[class_key] += 1
        
        # Classes that appear 3+ times are likely card patterns
        for class_key, count in class_counts.items():
            if count >= 3:
                patterns["cards"].append({
                    "class": class_key,
                    "count": count,
                    "selector": f".{class_key.replace('.', '.')}",
                })
        
        # Find company profile links
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)
            if any(kw in href.lower() for kw in ["company", "profile", "member", "detail"]):
                patterns["links"].append({
                    "href": href,
                    "text": text[:50],
                })
        
        return patterns
    
    def _get_selector(self, element) -> str:
        """Generate a CSS selector for an element."""
        parts = []
        
        # Tag name
        parts.append(element.name)
        
        # ID
        if element.get("id"):
            parts.append(f"#{element['id']}")
            return "".join(parts)
        
        # Classes
        classes = element.get("class", [])
        if classes:
            parts.append("." + ".".join(classes[:2]))  # Limit classes
        
        return "".join(parts)
    
    def generate_extractor_code(self, pattern: Dict, source_url: str) -> str:
        """
        Generate Python extractor code based on detected pattern.
        This creates a ready-to-use collector module.
        """
        domain = re.sub(r"[^a-z0-9]+", "_", source_url.split("//")[-1].split("/")[0].lower())
        
        code = f'''"""
Auto-generated collector for {source_url}
Generated by PatternAnalyzer
"""

import json
import requests
from typing import List, Dict

class {domain.title().replace("_", "")}Collector:
    """Auto-generated collector based on detected API pattern."""
    
    API_URL = "{pattern.get('api_url', source_url)}"
    
    FIELD_MAPPING = {json.dumps(pattern.get('field_mapping', {}), indent=8)}
    
    def harvest(self) -> List[Dict]:
        """Harvest leads from discovered API."""
        leads = []
        
        try:
            resp = requests.get(self.API_URL, timeout=30)
            if resp.status_code != 200:
                return leads
            
            data = resp.json()
            
            # Extract from detected list path
            items = data
            list_path = "{pattern.get('path', 'root')}"
            if list_path != "root":
                items = data.get(list_path, [])
            
            for item in items:
                if not isinstance(item, dict):
                    continue
                
                lead = {{}}
                for orig_key, norm_key in self.FIELD_MAPPING.items():
                    if orig_key in item:
                        lead[norm_key] = str(item[orig_key]).strip()
                
                if lead.get("company"):
                    lead["source"] = self.API_URL
                    lead["source_type"] = "auto_discovered"
                    leads.append(lead)
        
        except Exception as e:
            print(f"Error harvesting: {{e}}")
        
        return leads


if __name__ == "__main__":
    collector = {domain.title().replace("_", "")}Collector()
    leads = collector.harvest()
    print(f"Harvested {{len(leads)}} leads")
    for lead in leads[:5]:
        print(f"  - {{lead.get('company', 'N/A')}}")
'''
        return code


# CLI usage
if __name__ == "__main__":
    import sys
    
    # Test with sample JSON
    sample_data = {
        "results": [
            {"company_name": "Test Corp", "email": "test@example.com", "country": "Egypt"},
            {"company_name": "Demo Inc", "email": "demo@example.com", "country": "Morocco"},
            {"company_name": "Sample Ltd", "email": "sample@example.com", "country": "Brazil"},
        ]
    }
    
    analyzer = PatternAnalyzer()
    
    # Analyze structure
    structure = analyzer.analyze_json_structure(sample_data)
    print("Structure Analysis:")
    print(f"  Fields: {structure['fields']}")
    print(f"  Lists: {structure['lists']}")
    
    # Detect pattern
    pattern = analyzer.detect_list_pattern(sample_data)
    print(f"\nDetected Pattern:")
    print(f"  Path: {pattern['path']}")
    print(f"  Count: {pattern['count']}")
    print(f"  Field Mapping: {pattern['field_mapping']}")
    
    # Extract leads
    leads = analyzer.extract_from_pattern(sample_data, pattern)
    print(f"\nExtracted {len(leads)} leads:")
    for lead in leads:
        print(f"  - {lead}")
