
import sys
import os
import json
import logging
import pandas as pd
import requests
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("MeiliIndexer")

MEILI_URL = os.environ.get("MEILI_URL", "http://localhost:7700")
MEILI_KEY = os.environ.get("MEILI_MASTER_KEY", "masterKey123")

def index_leads():
    """Index master leads into Meilisearch."""
    
    # Check connection
    try:
        requests.get(f"{MEILI_URL}/health")
    except Exception:
        logger.error("❌ Meilisearch is not running. Start it with 'docker-compose up -d'")
        return

    # Load Data
    master_path = Path("outputs/crm/targets_master.csv")
    if not master_path.exists():
        master_path = Path("data/processed/leads_master.csv")
    if not master_path.exists():
        logger.error("No targets_master.csv or leads_master.csv found to index.")
        return

    df = pd.read_csv(master_path, on_bad_lines='skip')
    df = df.fillna("") # Meili doesn't like NaNs
    
    # Convert to standard dictionary list
    documents = df.to_dict(orient="records")
    
    # Create Index
    index_uid = "leads"
    headers = {"Authorization": f"Bearer {MEILI_KEY}", "Content-Type": "application/json"}
    
    # 1. Update Settings (filterable/searchable attributes)
    settings = {
        "searchableAttributes": [
            "company", "evidence", "evidence_snippet", "evidence_signals", "country", "keywords"
        ],
        "filterableAttributes": [
            "country", "score", "v5_status", "is_customer", "evidence_confidence"
        ],
        "sortableAttributes": [
            "score"
        ]
    }
    
    logger.info("⚙️ Updating Index Settings...")
    requests.post(f"{MEILI_URL}/indexes/{index_uid}/settings", headers=headers, json=settings)
    
    # 2. Add Documents (Batching 1000s)
    batch_size = 1000
    for i in range(0, len(documents), batch_size):
        batch = documents[i:i+batch_size]
        logger.info(f"🚀 Indexing batch {i} to {i+len(batch)}...")
        resp = requests.post(f"{MEILI_URL}/indexes/{index_uid}/documents", headers=headers, json=batch)
        if resp.status_code >= 400:
            logger.error(f"Error indexing batch: {resp.text}")
            
    logger.info(f"✅ Successfully indexed {len(documents)} leads to Meilisearch.")
    logger.info(f"🔎 Dashboard: {MEILI_URL}")

if __name__ == "__main__":
    index_leads()
