import csv
import hashlib
import os
import time
from datetime import datetime
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import requests

from src.utils.logger import get_logger

logger = get_logger(__name__)


class HttpClient:
    def __init__(self, settings=None, policies=None, cache_dir="data/raw"):
        settings = settings or {}
        policies = policies or {}

        crawler_cfg = settings.get("crawler", {})
        user_agent = crawler_cfg.get("user_agent") or policies.get("user_agent")
        self.headers = {"User-Agent": user_agent} if user_agent else {}
        self.timeout = crawler_cfg.get("timeout", 30)
        self.max_retries = crawler_cfg.get("max_retries", 2)

        self.respect_robots = bool(policies.get("respect_robots", True))
        self.max_rps_per_domain = float(policies.get("max_rps_per_domain", 0.2))
        rate_limit_delay = float(crawler_cfg.get("rate_limit_delay", 0) or 0)
        self.min_delay = max(rate_limit_delay, (1.0 / self.max_rps_per_domain) if self.max_rps_per_domain > 0 else 0)
        self.cache_raw_html = bool(policies.get("cache_raw_html", True))

        self.cache_dir = cache_dir
        self.html_cache_dir = os.path.join(cache_dir, "html")
        os.makedirs(self.html_cache_dir, exist_ok=True)

        self.domain_last_request = {}
        self.robots_cache = {}
        self.session = requests.Session()

        self.url_log_path = os.path.join(cache_dir, "url_log.csv")

    def _rate_limit(self, domain):
        if self.min_delay <= 0:
            return
        last = self.domain_last_request.get(domain)
        if last is None:
            return
        elapsed = time.time() - last
        if elapsed < self.min_delay:
            time.sleep(self.min_delay - elapsed)

    def _url_hash(self, url):
        return hashlib.sha256(url.encode("utf-8")).hexdigest()

    def _cache_path(self, url):
        return os.path.join(self.html_cache_dir, f"{self._url_hash(url)}.html")

    def _log_fetch(self, url, status, from_cache, content_hash=None):
        os.makedirs(self.cache_dir, exist_ok=True)
        exists = os.path.exists(self.url_log_path)
        with open(self.url_log_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "fetched_at",
                    "url",
                    "status",
                    "from_cache",
                    "content_hash",
                ],
            )
            if not exists:
                writer.writeheader()
            writer.writerow(
                {
                    "fetched_at": datetime.utcnow().isoformat(timespec="seconds"),
                    "url": url,
                    "status": status,
                    "from_cache": from_cache,
                    "content_hash": content_hash or "",
                }
            )

    def _get_robot_parser(self, url):
        parsed = urlparse(url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        if base in self.robots_cache:
            return self.robots_cache[base]
        rp = RobotFileParser()
        robots_url = f"{base}/robots.txt"
        try:
            rp.set_url(robots_url)
            rp.read()
        except Exception:
            rp = None
        self.robots_cache[base] = rp
        return rp

    def _can_fetch(self, url):
        if not self.respect_robots:
            return True
        rp = self._get_robot_parser(url)
        if rp is None:
            return True
        user_agent = self.headers.get("User-Agent", "*")
        try:
            return rp.can_fetch(user_agent, url)
        except Exception:
            return True

    def get(self, url, params=None, force=False, allow_binary=False):
        parsed = urlparse(url)
        domain = parsed.netloc
        cache_path = self._cache_path(url)

        if self.cache_raw_html and not force and os.path.exists(cache_path):
            try:
                with open(cache_path, "r", encoding="utf-8", errors="ignore") as f:
                    html = f.read()
                self._log_fetch(url, "cached", True, self._url_hash(url))
                return html
            except Exception:
                pass

        if not self._can_fetch(url):
            logger.warning(f"Blocked by robots.txt: {url}")
            self._log_fetch(url, "robots_blocked", False)
            return None

        self._rate_limit(domain)
        logger.info(f"Fetching URL: {url}")
        last_error = None
        for _ in range(self.max_retries + 1):
            try:
                response = self.session.get(
                    url, headers=self.headers, params=params, timeout=self.timeout
                )
                response.raise_for_status()
                content_type = response.headers.get("Content-Type", "")
                if (not allow_binary) and ("application/pdf" in content_type or content_type.startswith("image/")):
                    self._log_fetch(url, response.status_code, False, self._url_hash(url))
                    return None
                html = response.text
                self.domain_last_request[domain] = time.time()
                if self.cache_raw_html:
                    with open(cache_path, "w", encoding="utf-8", errors="ignore") as f:
                        f.write(html)
                self._log_fetch(url, response.status_code, False, self._url_hash(url))
                return html
            except Exception as e:
                last_error = e
                time.sleep(1.0)

        logger.error(f"Error fetching {url}: {last_error}")
        self._log_fetch(url, "error", False)
        return None

    def download(self, url, dest_path):
        parsed = urlparse(url)
        domain = parsed.netloc
        if not self._can_fetch(url):
            logger.warning(f"Blocked by robots.txt: {url}")
            self._log_fetch(url, "robots_blocked", False)
            return False
        self._rate_limit(domain)
        try:
            response = self.session.get(url, headers=self.headers, timeout=self.timeout)
            response.raise_for_status()
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            with open(dest_path, "wb") as f:
                f.write(response.content)
            self.domain_last_request[domain] = time.time()
            self._log_fetch(url, response.status_code, False, self._url_hash(url))
            return True
        except Exception as e:
            logger.error(f"Error downloading {url}: {e}")
            self._log_fetch(url, "error", False)
            return False
