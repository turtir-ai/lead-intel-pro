
import logging
import json
import trafilatura
import extruct
from w3lib.html import get_base_url
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class HTMLExtractor:
    """
    V5 Extractor: Extracts 'Smart Evidence' from HTML.
    - JSON-LD / OpenGraph (Structured Data)
    - Clean Main Text (Trafilatura)
    """

    def extract(self, html: str, url: str) -> Dict[str, Any]:
        """
        Extracts structured data and main text from HTML.
        """
        data = {
            "text": "",
            "metadata": {},
            "contacts": [],
            "socials": []
        }

        if not html:
            return data

        # 1. Main Text Extraction (Trafilatura)
        try:
            data["text"] = trafilatura.extract(html) or ""
        except Exception as e:
            logger.warning(f"Trafilatura failed: {e}")

        # 2. Structured Metadata (Extruct)
        try:
            base_url = get_base_url(html, url)
            metadata = extruct.extract(html, base_url=base_url, uniform=True)
            data["metadata"] = metadata
            
            # Parse key fields from JSON-LD
            if metadata.get("json-ld"):
                for item in metadata["json-ld"]:
                    # Find Organization / LocalBusiness
                    if item.get("@type") in ["Organization", "LocalBusiness", "Corporation"]:
                         self._parse_org(item, data)
                         
        except Exception as e:
            logger.warning(f"Extruct failed: {e}")

        return data

    def _parse_org(self, item: Dict, data: Dict):
        """Parse Organization object from JSON-LD."""
        # Email
        if item.get("email"):
            emails = item["email"]
            if isinstance(emails, str): emails = [emails]
            data["contacts"].extend(emails)
        
        # Phone
        if item.get("telephone"):
            phones = item["telephone"]
            if isinstance(phones, str): phones = [phones]
            data["contacts"].extend(phones)

        # Socials (sameAs)
        if item.get("sameAs"):
            same_as = item["sameAs"]
            if isinstance(same_as, str): same_as = [same_as]
            data["socials"].extend(same_as)

if __name__ == "__main__":
    # Test
    html_sample = """
    <html>
    <head>
    <script type="application/ld+json">
    {
      "@context": "https://schema.org",
      "@type": "Organization",
      "url": "http://www.example.com",
      "name": "Unlimited Ball Bearings Corp.",
      "contactPoint": {
        "@type": "ContactPoint",
        "telephone": "+1-401-555-1212",
        "contactType": "Customer service"
      },
      "sameAs": ["http://www.facebook.com/your-profile"]
    }
    </script>
    </head>
    <body>
    <h1>Welcome to our Factory</h1>
    <p>We produce high quality stenter machines.</p>
    </body>
    </html>
    """
    extractor = HTMLExtractor()
    res = extractor.extract(html_sample, "http://www.example.com")
    print(json.dumps(res, indent=2))
