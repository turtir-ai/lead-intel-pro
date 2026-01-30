import difflib
import math
import ast
from urllib.parse import urlparse

from src.processors.entity_extractor import EntityExtractor
from src.utils.logger import get_logger

logger = get_logger(__name__)

# GPT Fix #3: Source priority for merge (higher = more trusted)
SOURCE_PRIORITY = {
    "oekotex": 100,        # Official certification directory
    "gots": 100,           # Official certification directory
    "directory": 90,       # Official association directory
    "better_cotton": 90,   # Certification directory
    "wrap": 90,            # Certification directory
    "known_manufacturer": 85,  # Known target list
    "precision_search": 80,    # Official site crawl
    "fair": 70,            # Trade fair exhibitor list
    "oem_customer": 60,    # OEM website extraction
    "brave_search": 50,    # Web search result
    "comtrade": 40,        # Trade data
}


class LeadDedupe:
    def __init__(self, similarity_threshold=0.92):
        self.similarity_threshold = similarity_threshold
        self.extractor = EntityExtractor()
        try:
            from rapidfuzz import fuzz  # type: ignore

            self._fuzz = fuzz
        except Exception:
            self._fuzz = None

    def dedupe(self, leads):
        if not leads:
            return [], []

        # GPT Fix #3: Enhanced dedupe with normalized_company + country key
        by_domain = {}
        by_norm_country = {}  # NEW: normalized_company + country grouping
        leftovers = []
        
        for lead in leads:
            domain = self._domain(lead.get("website"))
            norm_key = self._get_norm_country_key(lead)
            
            if domain:
                by_domain.setdefault(domain, []).append(lead)
            elif norm_key:
                by_norm_country.setdefault(norm_key, []).append(lead)
            else:
                leftovers.append(lead)

        merged = []
        audit = []

        # Phase 1: Merge by domain
        for domain, items in by_domain.items():
            kept = self._select_best_source(items)
            for item in items:
                if item is not kept:
                    audit.append(
                        {
                            "kept_company": kept.get("company", ""),
                            "merged_company": item.get("company", ""),
                            "reason": f"same_domain:{domain}",
                        }
                    )
                    kept = self._merge_records(kept, item)
            merged.append(kept)

        # Phase 2: Merge by normalized_company + country (NEW)
        for norm_key, items in by_norm_country.items():
            kept = self._select_best_source(items)
            for item in items:
                if item is not kept:
                    audit.append(
                        {
                            "kept_company": kept.get("company", ""),
                            "merged_company": item.get("company", ""),
                            "reason": f"norm_country:{norm_key}",
                        }
                    )
                    kept = self._merge_records(kept, item)
            merged.append(kept)

        # Phase 3: Fuzzy name matching for leftovers
        merged.extend(self._dedupe_by_name(leftovers, audit))
        return merged, audit

    def _get_norm_country_key(self, lead):
        """GPT Fix #3: Create normalized_company + country key for grouping."""
        norm = lead.get("normalized_company") or self.extractor.normalize_company(lead.get("company", ""))
        country = lead.get("country")
        # Handle NaN/float values
        if country is None or (isinstance(country, float) and (math.isnan(country) or str(country) == "nan")):
            country = ""
        country = str(country).strip().lower()
        if norm and country:
            return f"{norm}|{country}"
        return None

    def _select_best_source(self, items):
        """GPT Fix #3: Select the lead from the most trusted source."""
        def get_priority(lead):
            source_type = (lead.get("source_type") or "").lower()
            return SOURCE_PRIORITY.get(source_type, 0)
        
        return max(items, key=get_priority)

    def _dedupe_by_name(self, leads, audit):
        merged = []
        seen = set()
        for lead in leads:
            if id(lead) in seen:
                continue
            kept = lead
            seen.add(id(lead))
            for other in leads:
                if id(other) in seen:
                    continue
                if self._is_similar_name(lead.get("company", ""), other.get("company", "")):
                    audit.append(
                        {
                            "kept_company": kept.get("company", ""),
                            "merged_company": other.get("company", ""),
                            "reason": "name_similarity",
                        }
                    )
                    kept = self._merge_records(kept, other)
                    seen.add(id(other))
            merged.append(kept)
        return merged

    def _is_similar_name(self, a, b):
        norm_a = self.extractor.normalize_company(a)
        norm_b = self.extractor.normalize_company(b)
        if not norm_a or not norm_b:
            return False
        if norm_a == norm_b:
            return True
        if self._fuzz:
            return self._fuzz.ratio(norm_a, norm_b) >= int(self.similarity_threshold * 100)
        ratio = difflib.SequenceMatcher(None, norm_a, norm_b).ratio()
        return ratio >= self.similarity_threshold

    def _merge_records(self, kept, other):
        merged = dict(kept)
        for field in ["emails", "phones", "websites", "country_mentions"]:
            merged[field] = sorted(
                set(self._as_list(kept.get(field)) + self._as_list(other.get(field)))
            )
        merged["score"] = max(kept.get("score", 0), other.get("score", 0))
        merged["context"] = kept.get("context") or other.get("context")
        return merged

    def _as_list(self, value):
        if value is None:
            return []
        if isinstance(value, float) and math.isnan(value):
            return []
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            stripped = value.strip()
            if stripped.startswith("[") or stripped.startswith("("):
                try:
                    parsed = ast.literal_eval(stripped)
                    if isinstance(parsed, list):
                        return parsed
                except Exception:
                    pass
            if not stripped:
                return []
            return [stripped]
        return [value]

    def _domain(self, url):
        if not url or not isinstance(url, str):
            return ""
        parsed = urlparse(url)
        return parsed.netloc.lower() or url.lower()
