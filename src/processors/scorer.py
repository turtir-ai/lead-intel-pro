from pathlib import Path
import yaml

from src.utils.logger import get_logger
from src.processors.heuristic_scorer import HeuristicScorer
from src.processors.hs_mapper import HSMapper

logger = get_logger(__name__)


class Scorer:
    def __init__(self, targets_config, scoring_config, country_priority=None, products_config=None):
        self.targets = targets_config or {}
        self.scoring = scoring_config or {}
        self.weights = self.scoring.get("weights", {})
        self.fit_keywords = self.scoring.get(
            "fit_keywords",
            self.targets.get("product_keywords", []),
        )
        self.capacity_keywords = self.scoring.get(
            "capacity_keywords",
            ["plant", "factory", "capacity", "employees", "facility", "mills", "lines"],
        )
        self.country_priority = country_priority or {}
        self.max_priority = max(self.country_priority.values(), default=0)
        self.country_label_to_iso3 = self._build_country_map()
        
        # Load products config for HS code / product matching
        self.products_config = products_config or self._load_products_config()
        self.product_keywords = self._build_product_keywords()
        self.oem_keywords = self._build_oem_keywords()
        self.oem_keywords = self._build_oem_keywords()
        self.competitor_names = self._build_competitor_names()

        # Initialize Heuristic Brain
        self.heuristic_scorer = HeuristicScorer()
        self.hs_mapper = HSMapper()

    def _load_products_config(self):
        """Load products.yaml if available."""
        config_path = Path(__file__).parent.parent.parent / "config" / "products.yaml"
        if config_path.exists():
            try:
                with open(config_path, "r", encoding="utf-8") as f:
                    return yaml.safe_load(f)
            except Exception as e:
                logger.warning(f"Failed to load products config: {e}")
        return {}
    
    def _build_product_keywords(self):
        """Extract product keywords from HS codes and multi-language part_keywords."""
        keywords = []
        
        # From HS codes (all language variants)
        for hs in self.products_config.get("hs_codes", []):
            # Get keywords from all language fields
            for key in hs.keys():
                if key.startswith("keywords"):
                    keywords.extend(hs.get(key, []))
        
        # From part_keywords (multi-language)
        part_keywords = self.products_config.get("part_keywords", {})
        for lang, kws in part_keywords.items():
            if isinstance(kws, list):
                keywords.extend(kws)
        
        # From products list
        for product in self.products_config.get("products", []):
            name = product.get("name", "")
            if name:
                keywords.append(name)
            name_en = product.get("name_en", "")
            if name_en:
                keywords.append(name_en)
        
        return list(set([k for k in keywords if k]))
    
    def _build_oem_keywords(self):
        """Extract OEM manufacturer names and brand keywords."""
        keywords = []
        
        # From brand_keywords
        keywords.extend(self.products_config.get("brand_keywords", []))
        
        # From oem_manufacturers
        oems = self.products_config.get("oem_manufacturers", {})
        if isinstance(oems, dict):
            for oem_key, oem_data in oems.items():
                if isinstance(oem_data, dict):
                    keywords.append(oem_data.get("name", ""))
                    keywords.extend(oem_data.get("products", []))
        elif isinstance(oems, list):
            for oem in oems:
                if isinstance(oem, dict):
                    keywords.append(oem.get("name", ""))
                    keywords.extend(oem.get("products", []))
        
        return [k for k in keywords if k]
    
    def _build_competitor_names(self):
        """Extract competitor company names."""
        competitors = self.products_config.get("competitors", {})
        names = []
        
        if isinstance(competitors, dict):
            for key, data in competitors.items():
                if isinstance(data, dict):
                    name = data.get("name", "")
                    if name:
                        names.append(name)
                        # Also add short versions
                        names.append(key)  # e.g., "interspare", "xty_elinmac"
                    names.extend(data.get("aliases", []))
        
        return [n for n in names if n]

    def score_lead(self, lead):
        context_text = lead.get("context", "")
        if context_text is None or (isinstance(context_text, float)):
            context_text = ""
        context_text = str(context_text) if context_text else ""
        
        # Add company name and source to context for better matching
        full_text = f"{context_text} {lead.get('company', '')} {lead.get('source', '')}"
        
        # V5 UPGRADE: Use Heuristic Scorer for "Fit"
        # This replaces simpler keyword matching with Proximity + Negative Logic
        heuristic_res = self.heuristic_scorer.score_text(
            full_text, 
            title=lead.get('title', ''), 
            url=lead.get('url', '')
        )
        
        # Use raw score from heuristic (can be negative!) but cap for the fit component
        # Heuristic scorer gives ~10-100 points. limit to 40 for compatibility with weights
        fit_score = max(0, min(40, heuristic_res['raw_score']))
        
        # Append V5 evidence
        if heuristic_res.get('evidence'):
            prev_evidence = lead.get('evidence', '')
            lead['evidence'] = f"{prev_evidence} | {heuristic_res['evidence']}".strip(' | ')

        # fit_score = self._keyword_score(full_text, self.fit_keywords, max_score=40)
        capacity_score = self._keyword_score(full_text, self.capacity_keywords, max_score=20)
        import_score = self._import_priority_score(lead, full_text)
        reachability_score = self._reachability_score(lead)
        
        # NEW: Product fit bonus - HS code related products
        product_bonus = self._product_fit_score(full_text)
        
        # NEW: OEM equipment bonus - has Brückner, Monforts etc.
        oem_bonus = self._oem_equipment_score(full_text)
        
        # NEW: Competitor customer bonus - known to buy from Interspare/XTY
        competitor_bonus = self._competitor_customer_score(lead, full_text)

        # GPT Audit Fix: Calculate base score (0-100 scale)
        # Base components are already 0-100 (fit=40, capacity=20, import=20, reach=20)
        base_score = 0.0
        base_score += fit_score * self.weights.get("fit_weight", 0.4)
        base_score += capacity_score * self.weights.get("capacity_weight", 0.2)
        base_score += import_score * self.weights.get("import_priority_weight", 0.2)
        base_score += reachability_score * self.weights.get("reachability_weight", 0.2)
        
        # Normalize to 0-100 scale (base components max = 40*0.4 + 20*0.2*3 = 28)
        # Scale up to use full 0-100 range
        normalized_score = min(100, base_score * 2.5)  # 28 * 2.5 = 70 base, bonuses push higher
        
        # Add bonuses (can exceed 100 for hot leads)
        final_score = normalized_score + product_bonus + oem_bonus + competitor_bonus

        lead["fit_score"] = round(fit_score, 2)
        lead["capacity_score"] = round(capacity_score, 2)
        lead["import_score"] = round(import_score, 2)
        lead["reachability_score"] = round(reachability_score, 2)
        lead["product_fit_bonus"] = round(product_bonus, 2)
        lead["oem_bonus"] = round(oem_bonus, 2)
        lead["competitor_bonus"] = round(competitor_bonus, 2)
        lead["score"] = round(min(150, final_score), 2)  # Allow up to 150 for hot leads

        # HS mapping for CRM/export (based on product keywords)
        hs_map = self.hs_mapper.map_text(full_text)
        lead["hs_primary"] = hs_map.get("hs_primary", "")
        lead["hs_secondary"] = hs_map.get("hs_secondary", "")
        lead["hs_fallback"] = ",".join(hs_map.get("hs_fallback", []) or [])
        lead["hs_reason"] = hs_map.get("hs_reason", "")
        lead["hs_matched_keywords"] = ",".join(hs_map.get("hs_matched_keywords", []) or [])
        return lead

    def _product_fit_score(self, text):
        """Score based on product/HS code keyword matches."""
        if not self.product_keywords:
            return 0
        
        text_l = text.lower()
        hits = sum(1 for kw in self.product_keywords if kw.lower() in text_l)
        
        # Max 15 bonus points for product fit
        return min(15, hits * 3)
    
    def _oem_equipment_score(self, text):
        """Bonus for companies with OEM equipment (Brückner, Monforts etc.)."""
        if not self.oem_keywords:
            return 0
        
        text_l = text.lower()
        hits = sum(1 for kw in self.oem_keywords if kw.lower() in text_l)
        
        # Max 20 bonus points for OEM equipment match
        return min(20, hits * 5)
    
    def _competitor_customer_score(self, lead, text):
        """Major bonus for known competitor customers."""
        bonus = 0
        
        # Check if lead has competitor_reference field (from CompetitorCustomerIntel)
        if lead.get("competitor_reference"):
            bonus += 25  # Huge bonus for confirmed competitor customer
        
        # Check for competitor mentions in text
        if self.competitor_names:
            text_l = text.lower()
            for name in self.competitor_names:
                if name.lower() in text_l:
                    bonus += 10
                    break
        
        return min(35, bonus)

    def _keyword_score(self, text, keywords, max_score=40):
        if not text or isinstance(text, float):
            return 0
        text = str(text)
        hits = 0
        text_l = text.lower()
        for kw in keywords or []:
            if kw.lower() in text_l:
                hits += 1
        return min(max_score, hits * (max_score / max(1, len(keywords) or 1)))

    def _region_match(self, text):
        if not text or isinstance(text, float):
            return False
        text_l = str(text).lower()
        for _, data in self.targets.get("target_regions", {}).items():
            for label in data.get("labels", []):
                if label.lower() in text_l:
                    return True
        return False

    def _import_priority_score(self, lead, context_text):
        # Prefer country field, fallback to country mentions in context
        country_val = lead.get("country") or ""
        iso3 = ""
        if country_val:
            iso3 = self._country_to_iso3(country_val)
        if not iso3:
            iso3 = self._country_from_context(context_text)
        if iso3 and iso3 in self.country_priority and self.max_priority > 0:
            value = self.country_priority.get(iso3, 0)
            return min(20, (value / self.max_priority) * 20)
        return 20 if self._region_match(context_text) else 0

    def _reachability_score(self, lead):
        """
        Calculate reachability score with proper list/string parsing.
        FIXED: Handles CSV-serialized lists like '[]' or '["a@b.com"]'
        """
        score = 0
        
        # Helper to check if field has real data
        def has_real_data(field_value):
            if not field_value:
                return False
            if isinstance(field_value, list):
                return len(field_value) > 0
            # Handle string representations from CSV
            val_str = str(field_value).strip()
            if val_str.lower() in ('', '[]', 'nan', 'none', 'null', '{}'):
                return False
            # Check for actual list content
            if val_str.startswith('[') and val_str.endswith(']'):
                import ast
                try:
                    parsed = ast.literal_eval(val_str)
                    return isinstance(parsed, list) and len(parsed) > 0
                except (ValueError, SyntaxError):
                    return False
            return True
        
        if has_real_data(lead.get("emails")):
            score += 10
        if has_real_data(lead.get("phones")):
            score += 6
        if has_real_data(lead.get("websites")) or has_real_data(lead.get("website")):
            score += 3
        if lead.get("contact_page_found") or has_real_data(lead.get("contact_urls")):
            score += 1
        return min(20, score)

    def rank_leads(self, leads):
        return sorted(leads, key=lambda x: x.get("score", 0), reverse=True)

    def _build_country_map(self):
        mapping = {}
        for _, data in self.targets.get("target_regions", {}).items():
            for iso3, label in zip(data.get("countries", []), data.get("labels", [])):
                mapping[label.lower()] = iso3
            for iso3 in data.get("countries", []):
                mapping[iso3.lower()] = iso3
        return mapping

    def _country_to_iso3(self, country_val):
        if not country_val:
            return ""
        key = str(country_val).strip().lower()
        return self.country_label_to_iso3.get(key, "")

    def _country_from_context(self, text):
        text_l = (text or "").lower()
        for label, iso3 in self.country_label_to_iso3.items():
            if label in text_l:
                return iso3
        return ""
