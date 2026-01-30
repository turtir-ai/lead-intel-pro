#!/usr/bin/env python3
"""Debug script to test heuristic scorer on existing leads"""

import pandas as pd
from src.processors.heuristic_scorer import HeuristicScorer
from pathlib import Path

def main():
    # Load sample leads
    df = pd.read_csv('data/processed/leads_master.csv')
    scorer = HeuristicScorer(Path('config'))
    
    print(f"Loaded {len(df)} leads")
    print(f"Columns: {list(df.columns[:10])}...")
    print()
    
    # Score first 10 leads
    for i, row in df.head(10).iterrows():
        lead = row.to_dict()
        
        # Build text from context field
        context = str(lead.get('context', '') or '')
        if context == 'nan':
            context = ''
            
        text = context
        title = str(lead.get('company', '') or '')
        if title == 'nan':
            title = ''
        
        country = str(lead.get('country', '') or '')
        if country == 'nan':
            country = ''
            
        metadata = {
            'company_name': title,
            'country': country,
            'source': str(lead.get('source', '') or ''),
        }
        
        result = scorer.calculate_score(text, title, metadata)
        
        print(f"[{i+1}] {title[:45]}")
        print(f"    Country: {country}")
        print(f"    Context: {context[:100]}...")
        print(f"    Score: {result.score} | Lead: {result.is_lead} | Conf: {result.confidence}")
        print(f"    Evidence: {result.evidence[:5]}")
        print(f"    HS Codes: {result.matched_hs_codes}")
        print(f"    Warnings: {result.warnings}")
        print()

if __name__ == "__main__":
    main()
