
import asyncio
import json
import os
import hashlib
from pathlib import Path
from urllib.parse import urlparse
from playwright.async_api import async_playwright
from typing import Dict, List, Optional
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def _safe_int(val, default=0):
    try:
        return int(val)
    except Exception:
        return default


def _safe_domain(url: str) -> str:
    try:
        netloc = urlparse(url).netloc.lower()
        return netloc.replace(":", "_")
    except Exception:
        return "unknown"


async def capture_json(response, storage, max_payload_kb: int = 512):
    """Capture JSON responses from network traffic."""
    try:
        content_type = response.headers.get("content-type", "")
        if "application/json" in content_type:
            # Avoid huge files or unrelated small pings
            if _safe_int(response.headers.get("content-length", 0)) < 50: 
                return
            
            try:
                data = await response.json()
                if isinstance(data, (list, dict)) and len(str(data)) > 100:  # Valid and substantial data
                    # Keep very large payloads from blowing up memory
                    if len(str(data)) > (max_payload_kb * 1024):
                        data = str(data)[: max_payload_kb * 1024]
                    storage.append({
                        "url": response.url,
                        "data": data,
                        "method": response.request.method,
                        "status": response.status
                    })
            except Exception:
                pass # Ignore parsing errors on non-json bodies
    except Exception:
        pass

async def probe_page(url: str, output_dir: str = "data/probe") -> Dict:
    """
    Probes a URL using Playwright to capture dynamic content, hidden APIs, and screenshots.
    
    Args:
        url: The target URL to probe.
        output_dir: Directory to save screenshots and JSON logs.
        
    Returns:
        Dictionary containing extracted content, screenshot path, and captured API data.
    """
    base_dir = Path(output_dir)
    domain_dir = base_dir / _safe_domain(url)
    domain_dir.mkdir(parents=True, exist_ok=True)
    page_content = ""
    screenshot_path = None
    network_data = []
    console_logs = []
    har_path = None
    dom_path = None
    
    async with async_playwright() as p:
        # Launch browser with options optimized for probing
        browser = await p.chromium.launch(
            headless=True,
            args=['--disable-dev-shm-usage', '--no-sandbox']
        )
        
        # Use a realistic user agent to avoid basic bot detection
        url_hash = hashlib.md5(url.encode()).hexdigest()
        har_path = str(domain_dir / f"{url_hash}.har")
        har_content = os.environ.get("PROBE_HAR_CONTENT", "omit")
        context = await browser.new_context(
            record_har_path=har_path,
            record_har_content=har_content,
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        # Hook into network responses
        page.on("response", lambda res: asyncio.create_task(capture_json(res, network_data)))
        page.on("console", lambda msg: console_logs.append({
            "type": msg.type,
            "text": msg.text,
            "location": msg.location
        }))

        try:
            logger.info(f"Probing URL: {url}")
            await page.goto(url, timeout=45000, wait_until="domcontentloaded")
            
            # Wait for dynamic content to load (heuristic wait)
            await page.wait_for_timeout(5000) 
            
            # Scroll down to trigger lazy loading
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(2000)

            # Capture Screenshot (Evidence)
            screenshot_path = str(domain_dir / f"{url_hash}.png")
            await page.screenshot(path=screenshot_path, full_page=True)
            logger.info(f"Screenshot saved to: {screenshot_path}")

            # Capture DOM Content
            page_content = await page.content()
            dom_path = str(domain_dir / f"{url_hash}_dom.html")
            with open(dom_path, "w", encoding="utf-8", errors="ignore") as f:
                f.write(page_content)
            
        except Exception as e:
            logger.error(f"Error probing {url}: {str(e)}")
            page_content = ""
            screenshot_path = None
        finally:
            try:
                await context.close()
            except Exception:
                pass
            await browser.close()

        # Save captured JSON data if found
        if network_data:
            json_log_path = str(domain_dir / f"{url_hash}_api.json")
            with open(json_log_path, "w", encoding='utf-8') as f:
                json.dump(network_data, f, indent=2, ensure_ascii=False)
            logger.info(f"Captured {len(network_data)} JSON responses to: {json_log_path}")
        else:
            json_log_path = None

        # Save console logs
        console_log_path = None
        if console_logs:
            console_log_path = str(domain_dir / f"{url_hash}_console.json")
            with open(console_log_path, "w", encoding="utf-8") as f:
                json.dump(console_logs, f, indent=2, ensure_ascii=False)

        return {
            "url": url,
            "content": page_content,
            "content_length": len(page_content),
            "screenshot_path": screenshot_path,
            "dom_path": dom_path,
            "har_path": har_path if har_path and os.path.exists(har_path) else None,
            "console_log_path": console_log_path,
            "api_responses_count": len(network_data),
            "api_data": network_data,
            "api_log_path": json_log_path
        }

if __name__ == "__main__":
    # Simple test execution
    test_url = "https://www.brueckner-textile.com/en/news/"
    # Run the async loop
    result = asyncio.run(probe_page(test_url))
    print(f"Probe complete. Captured {result['api_responses_count']} API responses.")
