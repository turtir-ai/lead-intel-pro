import re

from src.utils.logger import get_logger

logger = get_logger(__name__)


class EntityExtractor:
    def __init__(self):
        self.company_suffixes = [
            "gmbh",
            "ag",
            "kg",
            "kgaa",
            "bv",
            "b.v.",
            "nv",
            "ltd",
            "limited",
            "llc",
            "inc",
            "corp",
            "co.",
            "company",
            "group",
            "s.a.",
            "s.l.",
            "s.p.a.",
            "spa",
            "sarl",
            "srl",
            "oy",
            "oyj",
            "as",
            "ab",
        ]
        self.short_suffixes = {"as", "ab", "ag", "kg", "oy", "oyj", "nv"}
        self.industry_terms = [
            "textile",
            "spinning",
            "weaving",
            "knitting",
            "dyeing",
            "finishing",
            "mill",
            "mills",
            "fabrics",
            "garment",
            "denim",
        ]
        self.stop_phrases = {
            "textile machinery",
            "dyeing and finishing",
            "spinning weaving dyeing",
            "spinning, weaving, dyeing",
            "textile industry",
            "textile machinery parts",
        }
        self.stop_entities = {
            "interspare",
            "elinmac",
            "itma",
            "citme",
            "singapore",
            "germany",
            "china",
            "asia",
            "europe",
        }
        self.generic_terms = {
            "company",
            "companies",
            "unternehmen",
            "industry",
            "industrie",
            "textilindustrie",
            "textile industry",
            "textile machinery",
            "textile machines",
            "textilmaschinen",
            "maschinen",
            "machinery",
            "machines",
            "equipment",
            "stenter",
            "stenters",
            "finishing machines",
            "dyeing machines",
            "spare parts",
            "solutions",
            "products",
            "textilveredelungsanlagen",
            "neuanlagen",
            "mitarbeiter",
            "anlagen",
            "artos",
            "automatisierungstechnik",
            "lagerlogistik",
            "techniker",
            "stand",
            "messe",
            "expo",
            "exhibitor",
        }
        self.person_role_terms = {
            "moderator",
            "bandleader",
            "manager",
            "director",
            "geschäftsführer",
            "ceo",
            "founder",
            "owner",
            "chairman",
        }
        suffix_pattern = "|".join([re.escape(s) for s in self.company_suffixes])
        self.suffix_regex = re.compile(rf"\b(?:{suffix_pattern})\b", re.IGNORECASE)
        self.trigger_regex = re.compile(
            r"\b(?:bei|für|for|at|with|cliente|client|customer)\s+([A-Z][A-Za-z0-9&\-.]+(?:\s+[A-Z][A-Za-z0-9&\-.]+){0,3})"
        )

    def extract_companies(self, text, strict=False):
        if not text:
            return []
        companies = set()

        lines = [line.strip() for line in text.split("\n") if line.strip()]
        for line in lines:
            if len(line) < 4 or len(line) > 200:
                continue
            if line.lower() in self.stop_phrases:
                continue

            if self.suffix_regex.search(line):
                companies.update(self._extract_with_suffix(line))

            if not strict and any(term in line.lower() for term in self.industry_terms):
                if any(role in line.lower() for role in self.person_role_terms):
                    continue
                candidate = self._extract_capitalized_phrase(line)
                if candidate and self._is_valid_company(candidate):
                    companies.add(candidate)

            for match in self.trigger_regex.finditer(line):
                candidate = self._clean_name(match.group(1))
                if candidate and self._is_valid_company(candidate, allow_single=True):
                    companies.add(candidate)

        return sorted(companies)

    def extract_emails(self, text):
        if not text or (isinstance(text, float)):
            return []
        text = str(text)
        emails = set(
            re.findall(
                r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}",
                text,
                flags=re.IGNORECASE,
            )
        )
        # Handle common obfuscations like "name (at) domain (dot) com"
        normalized = text.lower()
        normalized = normalized.replace("[at]", "@").replace("(at)", "@").replace(" at ", "@")
        normalized = normalized.replace("[dot]", ".").replace("(dot)", ".").replace(" dot ", ".")
        emails.update(
            re.findall(
                r"[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}",
                normalized,
                flags=re.IGNORECASE,
            )
        )
        return sorted(emails)

    def extract_phones(self, text):
        if not text or (isinstance(text, float)):
            return []
        text = str(text)
        phones = set()
        for match in re.findall(r"\+?\d[\d\s\-()]{6,}\d", text):
            cleaned = re.sub(r"\s+", " ", match).strip()
            if len(cleaned) >= 7:
                phones.add(cleaned)
        return sorted(phones)

    def extract_websites(self, text):
        if not text or (isinstance(text, float)):
            return []
        text = str(text)
        urls = set()
        for match in re.findall(r"(https?://[^\s)\]\"'>]+)", text, flags=re.IGNORECASE):
            urls.add(match.rstrip(".,;"))
        for match in re.findall(r"\bwww\.[^\s)\]\"'>]+", text, flags=re.IGNORECASE):
            urls.add(f"http://{match.rstrip('.,;')}")
        return sorted(urls)

    def normalize_company(self, name):
        if not name or (isinstance(name, float)):
            return ""
        name = str(name)
        cleaned = re.sub(r"[\"'.,()]", " ", name)
        cleaned = re.sub(r"\s+", " ", cleaned).strip().lower()
        for suffix in self.company_suffixes:
            cleaned = re.sub(rf"\b{re.escape(suffix)}\b", "", cleaned).strip()
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        return cleaned

    def _extract_with_suffix(self, line):
        companies = set()
        parts = line.split()
        for idx, word in enumerate(parts):
            if self.suffix_regex.search(word):
                if word.islower() and word.strip(".").lower() in self.short_suffixes:
                    continue
                start = max(0, idx - 4)
                name_parts = parts[start : idx + 1]
                # Drop leading prepositions/noise
                while name_parts and name_parts[0].lower().strip("-") in {
                    "bei",
                    "für",
                    "for",
                    "at",
                    "with",
                    "the",
                    "a",
                    "an",
                }:
                    name_parts = name_parts[1:]
                if not any(w[:1].isupper() for w in name_parts if w):
                    continue
                candidate = " ".join(name_parts)
                candidate = self._clean_name(candidate)
                if self._is_valid_company(candidate):
                    companies.add(candidate)
        return companies

    def _extract_capitalized_phrase(self, line):
        match = re.search(
            r"\b([A-Z][A-Za-z0-9&\-.]+(?:\s+[A-Z][A-Za-z0-9&\-.]+){1,5})\b",
            line,
        )
        return self._clean_name(match.group(1)) if match else ""

    def _is_valid_company(self, candidate, allow_single=False):
        if not candidate or len(candidate) < 4:
            return False
        lowered = candidate.lower()
        if lowered in self.stop_phrases:
            return False
        if lowered in self.stop_entities:
            return False
        for ent in self.stop_entities:
            if ent in lowered:
                return False
        if lowered in self.generic_terms:
            return False
        if any(term in lowered for term in self.generic_terms):
            if len(candidate.split()) <= 2 and not self.suffix_regex.search(candidate):
                return False
        if not allow_single and len(candidate.split()) == 1 and len(candidate) < 6:
            return False
        if all(word.isupper() for word in candidate.split()) and len(candidate.split()) <= 2:
            return False
        return True

    def _clean_name(self, name):
        cleaned = " ".join(name.split()).strip()
        cleaned = re.sub(r"^[,\.\s&]+", "", cleaned)
        cleaned = cleaned.strip("-")
        return cleaned
