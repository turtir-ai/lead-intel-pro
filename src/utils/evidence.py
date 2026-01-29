import csv
import os


def record_evidence(path, payload):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    exists = os.path.exists(path)
    fieldnames = [
        "source_type",
        "source_name",
        "url",
        "title",
        "snippet",
        "content_hash",
        "fetched_at",
    ]
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not exists:
            writer.writeheader()
        writer.writerow({k: payload.get(k, "") for k in fieldnames})
