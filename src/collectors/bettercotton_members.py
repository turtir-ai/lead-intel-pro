from datetime import datetime
from urllib.parse import urljoin

import pandas as pd
from bs4 import BeautifulSoup

from src.utils.http_client import HttpClient
from src.utils.logger import get_logger
from src.utils.storage import save_text_cache
from src.utils.evidence import record_evidence

logger = get_logger(__name__)


class BetterCottonMembers:
    def __init__(self, settings=None, policies=None, evidence_path="outputs/evidence/evidence_log.csv"):
        self.client = HttpClient(settings=settings, policies=policies)
        self.evidence_path = evidence_path

    def harvest(
        self,
        base_url,
        max_pages=10,
        country_filter=None,
        include_categories=None,
        use_xlsx=False,
        member_list_url=None,
    ):
        leads = []
        country_filter = self._normalize_set(country_filter)
        include_categories = self._normalize_set(include_categories)

        if use_xlsx:
            xlsx_leads = self._harvest_xlsx(member_list_url or base_url, country_filter, include_categories)
            if xlsx_leads:
                return xlsx_leads

        page = 1
        while page <= max_pages:
            url = base_url
            if page > 1:
                url = f"{base_url}?sf_paged={page}"
            logger.info(f"Better Cotton: fetching page {page}")
            html = self.client.get(url)
            if not html:
                break

            soup = BeautifulSoup(html, "html.parser")
            cards = soup.select("div.card.card--member")
            if not cards:
                break

            for card in cards:
                name_el = card.find("h3", class_="card-title")
                name = name_el.get_text(strip=True) if name_el else ""
                if not name:
                    continue

                details = {}
                for h5 in card.find_all("h5"):
                    label = h5.get_text(strip=True).rstrip(":").lower()
                    val = ""
                    p = h5.find_next_sibling("p")
                    if p:
                        val = p.get_text(strip=True)
                    details[label] = val

                country = details.get("country", "")
                category = details.get("category", "")
                member_since = details.get("member since", "")

                if country_filter and country.strip().lower() not in country_filter:
                    continue
                if include_categories and category.strip().lower() not in include_categories:
                    continue

                snippet = card.get_text(" ", strip=True)
                content_hash = save_text_cache(f"{url}#{name}", snippet)
                record_evidence(
                    self.evidence_path,
                    {
                        "source_type": "directory",
                        "source_name": "BetterCotton",
                        "url": url,
                        "title": name,
                        "snippet": snippet[:400],
                        "content_hash": content_hash,
                        "fetched_at": datetime.utcnow().isoformat(timespec="seconds"),
                    },
                )

                context = f"Better Cotton member. Category: {category}. Country: {country}. Member since: {member_since}."
                leads.append(
                    {
                        "company": name,
                        "country": country,
                        "category": category,
                        "member_since": member_since,
                        "source": url,
                        "source_type": "bettercotton",
                        "source_name": "BetterCotton",
                        "context": context,
                    }
                )

            page += 1

        return leads

    def _normalize_set(self, values):
        return {str(v).strip().lower() for v in (values or []) if str(v).strip()}

    def _find_col(self, columns, keywords):
        for key in keywords:
            for col_lower, col in columns.items():
                if key in col_lower:
                    return col
        return None

    def _harvest_xlsx(self, list_url, country_filter, include_categories):
        html = self.client.get(list_url)
        if not html:
            return []

        soup = BeautifulSoup(html, "html.parser")
        xlsx_href = ""
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if ".xlsx" in href.lower():
                xlsx_href = href
                break
        if not xlsx_href:
            return []

        xlsx_url = urljoin(list_url, xlsx_href)
        dest_path = "data/inputs/bettercotton_member_list.xlsx"
        if not self.client.download(xlsx_url, dest_path):
            return []

        try:
            df = pd.read_excel(dest_path, header=None)
        except Exception:
            return []

        header_row = None
        for idx, row in df.iterrows():
            values = [str(v).strip().lower() for v in row.tolist() if str(v).strip() and str(v).lower() != "nan"]
            if "member" in values and "country" in values:
                header_row = idx
                break
        if header_row is None:
            return []

        headers = [str(v).strip() for v in df.iloc[header_row].tolist()]
        data = df.iloc[header_row + 1 :].copy()
        data.columns = headers

        columns = {str(col).strip().lower(): col for col in data.columns}
        name_col = self._find_col(columns, ["member name", "company", "organisation", "organization", "member"])
        country_col = self._find_col(columns, ["country"])
        category_col = self._find_col(columns, ["category", "membership"])
        member_since_col = self._find_col(columns, ["member since", "since", "join"])
        website_col = self._find_col(columns, ["website", "web", "url", "homepage"])

        if not name_col:
            return []

        leads = []
        for _, row in data.iterrows():
            name = str(row.get(name_col, "")).strip()
            if not name or name.lower() == "nan":
                continue
            country = str(row.get(country_col, "")).strip() if country_col else ""
            category = str(row.get(category_col, "")).strip() if category_col else ""
            member_since = str(row.get(member_since_col, "")).strip() if member_since_col else ""
            website = str(row.get(website_col, "")).strip() if website_col else ""

            if country_filter and country.lower() not in country_filter:
                continue
            if include_categories and category.lower() not in include_categories:
                continue

            snippet = f"{name} | {country} | {category} | {member_since}".strip()
            content_hash = save_text_cache(f"{xlsx_url}#{name}", snippet)
            record_evidence(
                self.evidence_path,
                {
                    "source_type": "directory",
                    "source_name": "BetterCotton",
                    "url": xlsx_url,
                    "title": name,
                    "snippet": snippet[:400],
                    "content_hash": content_hash,
                    "fetched_at": datetime.utcnow().isoformat(timespec="seconds"),
                },
            )

            context = f"Better Cotton member. Category: {category}. Country: {country}. Member since: {member_since}."
            leads.append(
                {
                    "company": name,
                    "country": country,
                    "category": category,
                    "member_since": member_since,
                    "website": website,
                    "source": xlsx_url,
                    "source_type": "bettercotton",
                    "source_name": "BetterCotton",
                    "context": context,
                }
            )

        return leads
