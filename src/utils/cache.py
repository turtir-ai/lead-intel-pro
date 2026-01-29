import hashlib
import json
import os


def _key_hash(key):
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def load_json_cache(key, cache_dir="data/raw/json"):
    path = os.path.join(cache_dir, f"{_key_hash(key)}.json")
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def save_json_cache(key, data, cache_dir="data/raw/json"):
    os.makedirs(cache_dir, exist_ok=True)
    path = os.path.join(cache_dir, f"{_key_hash(key)}.json")
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f)
    except Exception:
        return None
    return path
