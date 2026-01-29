from datetime import datetime

import requests

from src.utils.cache import load_json_cache, save_json_cache
from src.utils.logger import get_logger
from src.utils.storage import save_text_cache
from src.utils.evidence import record_evidence

logger = get_logger(__name__)


ISO3_TO_ISO2 = {
    # South America
    "BRA": "BR",
    "ARG": "AR",
    "COL": "CO",
    "PER": "PE",
    "MEX": "MX",
    "CHL": "CL",
    "ECU": "EC",
    # North Africa
    "MAR": "MA",
    "DZA": "DZ",
    "TUN": "TN",
    "EGY": "EG",
    "LBY": "LY",
    # South Asia
    "PAK": "PK",
    "IND": "IN",
    "BGD": "BD",
    "LKA": "LK",
    # Turkey
    "TUR": "TR",
    # Europe
    "DEU": "DE",
    "NLD": "NL",
    "ITA": "IT",
    "ESP": "ES",
    "PRT": "PT",
    "FRA": "FR",
    "GBR": "GB",
    # Asia
    "CHN": "CN",
    "VNM": "VN",
    "IDN": "ID",
    "THA": "TH",
    "UZB": "UZ",
    # Other
    "USA": "US",
}


class GotsCertifiedSuppliers:
    def __init__(self, settings=None, evidence_path="outputs/evidence/evidence_log.csv"):
        settings = settings or {}
        self.base_url = settings.get(
            "base_url", "https://www.global-trace-base.org/website-api/v2/certified-suppliers"
        )
        self.timeout = settings.get("timeout", 30)
        self.page_size = int(settings.get("page_size", 100))
        self.max_records = int(settings.get("max_records", 500))
        self.evidence_path = evidence_path

    def harvest(self, target_iso3=None):
        leads = []
        countries = []
        for iso3 in target_iso3 or []:
            iso2 = ISO3_TO_ISO2.get(iso3.upper())
            if iso2:
                countries.append(iso2)

        if not countries:
            countries = [None]

        for country_code in countries:
            offset = 0
            total = None
            while offset < self.max_records and (total is None or offset < total):
                params = {"offset": offset, "limit": self.page_size}
                if country_code:
                    params["country"] = country_code
                cache_key = f"gots:{country_code}:{offset}:{self.page_size}"
                data = load_json_cache(cache_key)
                if data is None:
                    try:
                        resp = requests.get(
                            self.base_url,
                            params=params,
                            headers={"Accept": "application/json"},
                            timeout=self.timeout,
                        )
                        if resp.status_code != 200:
                            logger.error(f"GOTS API error {resp.status_code}: {resp.text[:200]}")
                            break
                        data = resp.json()
                        save_json_cache(cache_key, data)
                    except Exception as exc:
                        logger.error(f"GOTS API request failed: {exc}")
                        break

                total = data.get("total", total)
                items = data.get("items", []) or []
                if not items:
                    break

                for item in items:
                    name = item.get("company_name", "")
                    if not name:
                        continue
                    name_l = name.lower()
                    if "withdrawn" in name_l or "suspended" in name_l:
                        continue
                    country = item.get("country", "")
                    product_category = item.get("product_category", "")
                    brand_names = item.get("brand_names") or ""
                    snippet = f"{name} | {country} | {product_category}"
                    content_hash = save_text_cache(f"{self.base_url}?{country_code}:{name}", snippet)
                    record_evidence(
                        self.evidence_path,
                        {
                            "source_type": "directory",
                            "source_name": "GOTS",
                            "url": f"{self.base_url}?country={country_code or ''}",
                            "title": name,
                            "snippet": snippet[:400],
                            "content_hash": content_hash,
                            "fetched_at": datetime.utcnow().isoformat(timespec="seconds"),
                        },
                    )
                    context = (
                        f"GOTS certified supplier. Product category: {product_category}. "
                        f"Country: {country}. Brands: {brand_names}."
                    )
                    leads.append(
                        {
                            "company": name,
                            "country": country,
                            "product_category": product_category,
                            "brand_names": brand_names,
                            "source": self.base_url,
                            "source_type": "gots",
                            "source_name": "GOTS",
                            "context": context,
                        }
                    )

                offset += self.page_size

        return leads
