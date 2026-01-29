import hashlib
import os


def _url_hash(url):
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def save_text_cache(source_id, text, cache_dir="data/raw/text"):
    os.makedirs(cache_dir, exist_ok=True)
    content_hash = _url_hash(source_id)
    path = os.path.join(cache_dir, f"{content_hash}.txt")
    with open(path, "w", encoding="utf-8", errors="ignore") as f:
        f.write(text or "")
    return content_hash
