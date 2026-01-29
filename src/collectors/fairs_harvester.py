import re
from datetime import datetime

from bs4 import BeautifulSoup

from src.utils.http_client import HttpClient
from src.utils.logger import get_logger
from src.utils.storage import save_text_cache
from src.utils.evidence import record_evidence

logger = get_logger(__name__)

class FairsHarvester:
    def __init__(self, settings=None, policies=None, evidence_path="outputs/evidence/evidence_log.csv"):
        self.client = HttpClient(settings=settings, policies=policies)
        self.evidence_path = evidence_path

    def _extract_items(self, soup, selectors):
        if selectors and selectors.get("item"):
            return soup.select(selectors["item"])
        return soup.find_all(["div", "tr", "li"])

    def _extract_name(self, item, selectors):
        if selectors and selectors.get("name"):
            node = item.select_one(selectors["name"])
            if node:
                return node.get_text(separator=" ", strip=True)
        text = item.get_text(separator=" ", strip=True)
        match = re.search(r"\b[A-Z][A-Za-z0-9&\-\.\s]{2,}\b", text)
        return match.group(0).strip() if match else ""

    def _extract_website(self, item, selectors):
        if selectors and selectors.get("website"):
            node = item.select_one(selectors["website"])
            if node and node.get("href"):
                return node.get("href")
        link = item.find("a", href=True)
        return link.get("href") if link else ""

    def harvest_sources(self, sources_config, targets_config=None):
        leads = []
        targets_config = targets_config or {}
        industry_keywords = sources_config.get(
            "industry_keywords",
            ["textile", "machinery", "dyeing", "finishing", "spinning", "weaving"],
        )

        for src in sources_config.get("fairs", []):
            if not src.get("enabled", True):
                continue
            leads.extend(self.harvest_fair(src, industry_keywords))

        for src in sources_config.get("directories", []):
            if not src.get("enabled", True):
                continue
            leads.extend(self.harvest_fair(src, industry_keywords))

        return leads

    def harvest_fair(self, src_config, industry_keywords):
        fair_url = src_config.get("url")
        if not fair_url:
            return []
        logger.info(f"Harvesting directory: {fair_url}")
        if fair_url.lower().endswith(".pdf") or src_config.get("type") == "pdf":
            filename = fair_url.split("/")[-1] or "directory.pdf"
            dest_path = f"data/inputs/auto_{filename}"
            downloaded = self.client.download(fair_url, dest_path)
            if downloaded:
                record_evidence(
                    self.evidence_path,
                    {
                        "source_type": "pdf",
                        "source_name": src_config.get("name", ""),
                        "url": fair_url,
                        "title": filename,
                        "snippet": f"Downloaded PDF to {dest_path}",
                        "content_hash": "",
                        "fetched_at": datetime.utcnow().isoformat(timespec="seconds"),
                    },
                )
            return []

        html = self.client.get(fair_url)
        if not html:
            return []

        soup = BeautifulSoup(html, "html.parser")
        title = soup.title.get_text(strip=True) if soup.title else ""
        text = soup.get_text(separator="\n", strip=True)
        content_hash = save_text_cache(fair_url, text)
        record_evidence(
            self.evidence_path,
            {
                "source_type": "directory",
                "source_name": src_config.get("name", ""),
                "url": fair_url,
                "title": title,
                "snippet": text[:400].replace("\n", " ").strip(),
                "content_hash": content_hash,
                "fetched_at": datetime.utcnow().isoformat(timespec="seconds"),
            },
        )

        leads = []
        selectors = src_config.get("selectors", {})
        skip_industry_filter = bool(src_config.get("skip_industry_filter", False))
        for item in self._extract_items(soup, selectors):
            text = item.get_text(separator=" ", strip=True)
            if not skip_industry_filter and not any(kw in text.lower() for kw in industry_keywords):
                continue
            company_name = self._extract_name(item, selectors)
            if company_name and len(company_name) > 3:
                leads.append(
                    {
                        "company": company_name,
                        "source": fair_url,
                        "context": text[:240],
                        "website": self._extract_website(item, selectors),
                        "source_type": src_config.get("type", "directory"),
                        "source_name": src_config.get("name", ""),
                    }
                )

        return leads
