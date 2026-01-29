from datetime import datetime

from src.collectors.discovery.brave_search import BraveSearchClient
from src.processors.entity_extractor import EntityExtractor
from src.utils.evidence import record_evidence
from src.utils.http_client import HttpClient
from src.utils.logger import get_logger
from src.utils.storage import save_text_cache

logger = get_logger(__name__)


class CompetitorWebSearch:
    def __init__(self, api_key, settings=None, policies=None, evidence_path="outputs/evidence/evidence_log.csv"):
        self.search = BraveSearchClient(api_key, settings=settings or {})
        self.client = HttpClient(settings=settings, policies=policies)
        self.extractor = EntityExtractor()
        self.evidence_path = evidence_path

    def harvest(self, competitors, search_cfg):
        leads = []
        if not search_cfg.get("enabled", True):
            return leads

        max_results = int(search_cfg.get("max_results", 5))
        queries = search_cfg.get("queries", [])
        disallow_domains = set(search_cfg.get("disallow_domains", []))
        exclude_company_keywords = [k.lower() for k in search_cfg.get("exclude_company_keywords", [])]
        trigger_keywords = [k.lower() for k in search_cfg.get("trigger_keywords", [])]
        strong_keywords = [k.lower() for k in search_cfg.get("strong_keywords", [])]
        exclude_context = [k.lower() for k in search_cfg.get("exclude_context_keywords", [])]
        for comp in competitors:
            name = comp.get("name") or ""
            if not name:
                continue
            if comp.get("search_enabled") is False:
                continue
            for template in queries:
                query = template.format(name=name)
                logger.info(f"Brave competitor search: {query}")
                results = self.search.search(query, count=max_results)
                for res in results:
                    url = res.get("url")
                    if not url:
                        continue
                    if self._is_disallowed(url, disallow_domains):
                        continue
                    snippet = res.get("description", "")
                    title = res.get("title", "")

                    # Fetch page to get stronger evidence
                    html = self.client.get(url)
                    text = ""
                    if html:
                        text = html[:20000]
                    content = f"{title}\n{snippet}\n{text}"
                    companies = self.extractor.extract_companies(content, strict=True)
                    if not companies:
                        continue

                    content_hash = save_text_cache(url, content)
                    record_evidence(
                        self.evidence_path,
                        {
                            "source_type": "competitor_search",
                            "source_name": name,
                            "url": url,
                            "title": title,
                            "snippet": snippet[:400],
                            "content_hash": content_hash,
                            "fetched_at": datetime.utcnow().isoformat(timespec="seconds"),
                        },
                    )

                    snippet_lower = (snippet or "").lower()
                    title_lower = (title or "").lower()
                    context_lower = content.lower()
                    if exclude_context and any(k in context_lower for k in exclude_context):
                        continue
                    if trigger_keywords and not any(t in context_lower for t in trigger_keywords):
                        continue
                    if strong_keywords and not any(k in snippet_lower or k in title_lower for k in strong_keywords):
                        continue

                    for company in companies:
                        if not self._is_plausible(company, exclude_company_keywords):
                            continue
                        leads.append(
                            {
                                "company": company,
                                "source": url,
                                "context": content[:2000],
                                "snippet": snippet[:400],
                                "source_type": "competitor_search",
                                "source_name": name,
                                "competitor": name,
                            }
                        )

        return leads

    def _is_disallowed(self, url, disallow_domains):
        if not disallow_domains:
            return False
        url_l = url.lower()
        return any(bad in url_l for bad in disallow_domains)

    def _is_plausible(self, name, exclude_keywords):
        name_l = name.lower().strip()
        if not name_l:
            return False
        if any(k in name_l for k in exclude_keywords):
            return False
        if len(name_l.split()) == 1 and len(name_l) < 5:
            return False
        return True
