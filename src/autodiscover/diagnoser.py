"""
Site Diagnoser - Playwright Network/Console/DOM Capture

Captures all network traffic, console logs, and DOM snapshots
for automatic pattern analysis. No LLM required.

Key Features:
- Network interception (XHR/Fetch → JSON APIs)
- Console log capture
- HAR recording
- DOM snapshot
- Screenshot
- Trace file for debugging
"""

import asyncio
import hashlib
import json
import os
import re
import time
from pathlib import Path
from typing import Optional, Dict, List, Any
from urllib.parse import urlparse, urljoin
from datetime import datetime

# Try async playwright first, fall back to sync
try:
    from playwright.async_api import async_playwright, Page, Response
    ASYNC_PLAYWRIGHT = True
except ImportError:
    from playwright.sync_api import sync_playwright
    ASYNC_PLAYWRIGHT = False


class SiteDiagnoser:
    """
    Diagnose a website by capturing network traffic, console logs, 
    and DOM structure. Used to automatically discover API endpoints
    and data patterns.
    """
    
    # Keywords that indicate valuable B2B lead data
    LEAD_KEYWORDS = [
        # Company info
        "company", "organization", "business", "firm", "enterprise",
        "manufacturer", "supplier", "vendor", "producer", "factory",
        # Contact info
        "email", "phone", "tel", "fax", "contact", "address",
        # Location
        "country", "city", "region", "location", "address",
        # Industry
        "textile", "fabric", "cotton", "yarn", "weaving", "dyeing",
        "spinning", "garment", "apparel", "fashion",
        # Certification
        "certificate", "certified", "oeko-tex", "gots", "bluesign",
        # Commerce
        "export", "import", "trade", "member", "directory",
    ]
    
    # URL patterns that indicate API endpoints
    API_PATTERNS = [
        r"/api/",
        r"/v[0-9]+/",
        r"/graphql",
        r"/rest/",
        r"/json",
        r"\.json$",
        r"/search",
        r"/list",
        r"/query",
        r"/data",
        r"/members",
        r"/companies",
        r"/directory",
    ]
    
    def __init__(self, output_dir: str = "data/diagnostics"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def _safe_domain(self, url: str) -> str:
        """Extract domain and make it filesystem-safe."""
        domain = urlparse(url).netloc.lower()
        return re.sub(r"[^a-z0-9.\-]+", "_", domain)
    
    def _score_json_data(self, data: Any, url: str = "") -> Dict:
        """
        Score JSON data for B2B lead relevance.
        Returns score and matched keywords.
        """
        data_str = str(data).lower()
        data_len = len(data_str)
        
        # Skip tiny or huge data (likely not useful)
        if data_len < 50 or data_len > 1_000_000:
            return {"score": 0, "keywords": [], "is_list": False}
        
        matched_keywords = []
        for keyword in self.LEAD_KEYWORDS:
            if keyword in data_str:
                matched_keywords.append(keyword)
        
        # Check if URL looks like an API
        api_bonus = 0
        for pattern in self.API_PATTERNS:
            if re.search(pattern, url.lower()):
                api_bonus = 2
                break
        
        # Check if data is a list (likely directory data)
        is_list = False
        if isinstance(data, list) and len(data) > 3:
            is_list = True
        elif isinstance(data, dict):
            # Check for common list patterns
            for key in ["items", "results", "data", "records", "members", "companies"]:
                if key in data and isinstance(data.get(key), list):
                    is_list = True
                    break
        
        list_bonus = 3 if is_list else 0
        
        return {
            "score": len(matched_keywords) + api_bonus + list_bonus,
            "keywords": matched_keywords,
            "is_list": is_list,
            "api_match": api_bonus > 0,
        }
    
    async def diagnose_async(self, url: str, scroll_count: int = 3) -> Dict:
        """
        Async version: Diagnose a URL by capturing all network traffic,
        console logs, and DOM structure.
        """
        domain = self._safe_domain(url)
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        session_dir = self.output_dir / domain / timestamp
        session_dir.mkdir(parents=True, exist_ok=True)
        
        # Collectors
        console_logs = []
        network_requests = []
        network_responses = []
        valuable_json = []
        
        result = {
            "url": url,
            "domain": domain,
            "timestamp": timestamp,
            "session_dir": str(session_dir),
            "success": False,
            "error": None,
            "apis_discovered": [],
            "valuable_data": [],
            "stats": {},
        }
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--disable-blink-features=AutomationControlled"]
            )
            
            context = await browser.new_context(
                viewport={"width": 1400, "height": 900},
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                # Record HAR for detailed network analysis
                record_har_path=str(session_dir / "network.har"),
                record_har_content="omit",  # Don't embed body (saves space)
            )
            
            # Start tracing for debugging
            await context.tracing.start(
                screenshots=True,
                snapshots=True,
                sources=False
            )
            
            page = await context.new_page()
            
            # Console log handler
            def on_console(msg):
                console_logs.append({
                    "type": msg.type,
                    "text": msg.text[:500],  # Truncate
                    "timestamp": datetime.utcnow().isoformat(),
                })
            page.on("console", on_console)
            
            # Request handler
            def on_request(request):
                network_requests.append({
                    "url": request.url[:500],
                    "method": request.method,
                    "resource_type": request.resource_type,
                })
            page.on("request", on_request)
            
            # Response handler - this is where we capture API data
            async def on_response(response: Response):
                content_type = response.headers.get("content-type", "")
                
                # Only process JSON responses
                if "application/json" not in content_type:
                    return
                
                resp_info = {
                    "url": response.url[:500],
                    "status": response.status,
                    "content_type": content_type,
                }
                network_responses.append(resp_info)
                
                # Try to parse and score JSON
                try:
                    json_data = await response.json()
                    score_info = self._score_json_data(json_data, response.url)
                    
                    if score_info["score"] >= 3:  # Threshold for valuable data
                        valuable_json.append({
                            "url": response.url,
                            "score": score_info["score"],
                            "keywords": score_info["keywords"],
                            "is_list": score_info["is_list"],
                            "data_preview": str(json_data)[:1000],
                            "full_data": json_data,
                        })
                except Exception:
                    pass
            
            page.on("response", lambda r: asyncio.create_task(on_response(r)))
            
            try:
                # Navigate to page
                response = await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                
                # Wait for dynamic content
                await page.wait_for_timeout(2000)
                
                # Scroll to trigger lazy loading
                for i in range(scroll_count):
                    await page.evaluate("window.scrollBy(0, 500)")
                    await page.wait_for_timeout(1000)
                
                # Wait for XHR requests to complete
                await page.wait_for_timeout(2000)
                
                # Capture DOM
                html = await page.content()
                with open(session_dir / "page.html", "w", encoding="utf-8") as f:
                    f.write(html)
                
                # Screenshot
                await page.screenshot(
                    path=str(session_dir / "screenshot.png"),
                    full_page=True
                )
                
                # Meta info
                title = await page.title()
                final_url = page.url
                
                result["success"] = True
                result["title"] = title
                result["final_url"] = final_url
                result["status_code"] = response.status if response else None
                
            except Exception as e:
                result["error"] = str(e)
            
            finally:
                # Save console logs
                with open(session_dir / "console.jsonl", "w", encoding="utf-8") as f:
                    for log in console_logs:
                        f.write(json.dumps(log, ensure_ascii=False) + "\n")
                
                # Save network requests
                with open(session_dir / "requests.jsonl", "w", encoding="utf-8") as f:
                    for req in network_requests:
                        f.write(json.dumps(req, ensure_ascii=False) + "\n")
                
                # Save network responses
                with open(session_dir / "responses.jsonl", "w", encoding="utf-8") as f:
                    for resp in network_responses:
                        f.write(json.dumps(resp, ensure_ascii=False) + "\n")
                
                # Save valuable JSON (most important!)
                if valuable_json:
                    # Sort by score
                    valuable_json.sort(key=lambda x: x["score"], reverse=True)
                    
                    # Save summary (without full data)
                    summary = [{
                        "url": v["url"],
                        "score": v["score"],
                        "keywords": v["keywords"],
                        "is_list": v["is_list"],
                        "data_preview": v["data_preview"],
                    } for v in valuable_json]
                    
                    with open(session_dir / "valuable_apis.json", "w", encoding="utf-8") as f:
                        json.dump(summary, f, indent=2, ensure_ascii=False)
                    
                    # Save full data separately (can be large)
                    with open(session_dir / "api_data.json", "w", encoding="utf-8") as f:
                        json.dump([{
                            "url": v["url"],
                            "data": v["full_data"]
                        } for v in valuable_json], f, indent=2, ensure_ascii=False)
                
                # Stop tracing
                await context.tracing.stop(path=str(session_dir / "trace.zip"))
                
                await context.close()
                await browser.close()
        
        # Build result
        result["apis_discovered"] = [v["url"] for v in valuable_json]
        result["valuable_data"] = [{
            "url": v["url"],
            "score": v["score"],
            "keywords": v["keywords"],
            "is_list": v["is_list"],
        } for v in valuable_json]
        
        result["stats"] = {
            "total_requests": len(network_requests),
            "json_responses": len(network_responses),
            "valuable_apis": len(valuable_json),
            "console_logs": len(console_logs),
        }
        
        # Save final result
        with open(session_dir / "diagnosis.json", "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        
        return result
    
    def diagnose(self, url: str, scroll_count: int = 3) -> Dict:
        """
        Sync wrapper for diagnose_async.
        """
        return asyncio.run(self.diagnose_async(url, scroll_count))


# CLI usage
if __name__ == "__main__":
    import sys
    
    url = sys.argv[1] if len(sys.argv) > 1 else "https://www.oeko-tex.com/en/buying-guide"
    
    diagnoser = SiteDiagnoser()
    result = diagnoser.diagnose(url)
    
    print("\n" + "="*60)
    print(f"Diagnosis Complete: {url}")
    print("="*60)
    print(f"Success: {result['success']}")
    print(f"APIs Discovered: {len(result['apis_discovered'])}")
    print(f"Stats: {result['stats']}")
    
    if result['valuable_data']:
        print("\nValuable APIs Found:")
        for api in result['valuable_data'][:5]:
            print(f"  - Score {api['score']}: {api['url'][:80]}...")
            print(f"    Keywords: {', '.join(api['keywords'][:5])}")
    
    print(f"\nFull output saved to: {result['session_dir']}")
