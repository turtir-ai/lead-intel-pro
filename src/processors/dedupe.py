import difflib
import math
import ast
from urllib.parse import urlparse

from src.processors.entity_extractor import EntityExtractor
from src.utils.logger import get_logger

logger = get_logger(__name__)


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

        by_domain = {}
        leftovers = []
        for lead in leads:
            domain = self._domain(lead.get("website"))
            if domain:
                by_domain.setdefault(domain, []).append(lead)
            else:
                leftovers.append(lead)

        merged = []
        audit = []

        for domain, items in by_domain.items():
            kept = items[0]
            for item in items[1:]:
                audit.append(
                    {
                        "kept_company": kept.get("company", ""),
                        "merged_company": item.get("company", ""),
                        "reason": f"same_domain:{domain}",
                    }
                )
                kept = self._merge_records(kept, item)
            merged.append(kept)

        merged.extend(self._dedupe_by_name(leftovers, audit))
        return merged, audit

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
