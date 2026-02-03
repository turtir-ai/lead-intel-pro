#!/usr/bin/env python3
"""
Manual Website Updates for High-Priority Leads

Updates the merged CSV with discovered real websites.
"""

import pandas as pd
from datetime import datetime
from glob import glob

# Manual website discoveries (from deep_discovery.py)
WEBSITE_UPDATES = {
    "ALINDA TEKSTIL BOYA APRE SANAYI VE TICARET ANONIM SIRKETI": "https://www.alindaboya.com.tr/",
    "Alinda Tekstil Boya Apre Sanayi": "https://www.alindaboya.com.tr/",
    "ALINDA TEKSTIL BOYA APRE A.Ş.": "https://www.alindaboya.com.tr/",
    "Sarena Textile Industries (Private) Limited": "https://sarenapk.com/",
    "Sarena Textile Industries": "https://sarenapk.com/",
    "İstanbul Boyahanesi": "https://istanbulboyahanesi.com/",
    "Istanbul Boyahanesi": "https://istanbulboyahanesi.com/",
    "Isil Tekstil": "https://www.isiltekstil.com/",  # Common domain
    "Işıl Tekstil": "https://www.isiltekstil.com/",
    "Işıl Tekstil San. Ve Tic. Ltd. Sti.": "https://www.isiltekstil.com/",
    "Işıl Tekstil San. Ve Tic. Ltd. Şti.": "https://www.isiltekstil.com/",
    "Isil Tekstil San. Ve Tic. Ltd. Sti.": "https://www.isiltekstil.com/",
    "Işıl Tekstil Sanayi Ve Ticaret": "https://www.isiltekstil.com/",
    "Sapphire Finishing Mills Limited": "https://sapphiremills.com/",
    "Allawasaya Textile & Finishing Mills Ltd": "https://www.allawasaya.com/",
    "Allawasaya Textile": "https://www.allawasaya.com/",
    "Acatel - Acabamentos Têxteis SA": "https://www.acatel.pt/",
    "Cedro Têxtil": "https://cedro.com.br/",  # Main domain
    "Canatiba": "https://canatiba.com.br/",
    "Textil Canatiba": "https://canatiba.com.br/",
    "Santista Têxtil": "https://www.santistasa.com.br/",
    "Ottoman Boyahane Apre ve Baskı A.Ş.": "https://www.ottomanboya.com.tr/",
    "OTTOMAN Boyahane Apre ve Baskı A.Ş.": "https://www.ottomanboya.com.tr/",
    "Ottoman Boya": "https://www.ottomanboya.com.tr/",
    "Yünsa": "https://www.yunsa.com/",
    "Bossa": "https://www.bossa.com.tr/",
    "Sanko Tekstil": "https://sanko.com.tr/sanko-tekstil/",
    "Kipaş Tekstil": "https://kipas.com.tr/",
    "Tan Tekstil": "https://www.tantekstil.com.tr/",
    "Öztek Tekstil Terbiye": "https://www.oztektekstil.com.tr/",
    "Erşat Tekstil": "https://www.ersattextile.com/",
    "Altınbaşak Tekstil San. ve Tic. A.Ş.": "https://www.altinbasaktekstil.com/",
    "Finos Acabados Textiles, S.A. De CV (Acafintex)": "https://acafintex.com/",
    "Finos Acabados Textiles (Acafintex)": "https://acafintex.com/",
}


def main():
    # Find latest merged file
    merged_files = sorted(glob("outputs/crm/verified_merged_*.csv"))
    if not merged_files:
        print("No merged files found!")
        return
    
    latest = merged_files[-1]
    print(f"Loading: {latest}")
    
    df = pd.read_csv(latest)
    print(f"Total leads: {len(df)}")
    
    # Update websites
    updates_made = 0
    for i, row in df.iterrows():
        company = row.get('company', '')
        current_website = row.get('website', '')
        
        # Check if we have an update for this company
        if company in WEBSITE_UPDATES:
            new_website = WEBSITE_UPDATES[company]
            
            # Update only if current is empty or directory
            if not current_website or 'commonshare' in str(current_website).lower() or \
               'mustakbil' in str(current_website).lower() or \
               'europages' in str(current_website).lower() or \
               'rehber' in str(current_website).lower() or \
               'textilegence' in str(current_website).lower():
                
                df.at[i, 'website'] = new_website
                updates_made += 1
                print(f"Updated: {company} -> {new_website}")
    
    print(f"\nTotal updates: {updates_made}")
    
    # Save updated file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"outputs/crm/verified_final_{timestamp}.csv"
    df.to_csv(output_path, index=False)
    print(f"\nSaved to: {output_path}")
    
    # Also create a clean sales export
    print("\n" + "=" * 60)
    print("Creating clean sales export...")
    print("=" * 60)
    
    # Clean columns for sales
    sales_cols = ['company', 'country', 'website', 'emails', 'phones', 
                  'evidence_reason', 'sce_total', 'sce_signals', 'sce_sales_ready']
    
    sales_df = df[sales_cols].copy()
    
    # Clean emails and phones
    for col in ['emails', 'phones']:
        sales_df[col] = sales_df[col].fillna('')
        sales_df[col] = sales_df[col].replace('nan', '')
        sales_df[col] = sales_df[col].replace('[]', '')
    
    # Sort by SCE score and website availability
    sales_df['has_website'] = sales_df['website'].apply(lambda x: 1 if x and str(x) != 'nan' else 0)
    sales_df = sales_df.sort_values(by=['sce_sales_ready', 'has_website', 'sce_total'], 
                                     ascending=[False, False, False])
    sales_df = sales_df.drop(columns=['has_website'])
    
    sales_path = f"outputs/crm/sales_final_{timestamp}.csv"
    sales_df.to_csv(sales_path, index=False)
    print(f"Sales export: {sales_path}")
    
    # Summary
    with_website = len(sales_df[sales_df['website'].astype(str) != ''])
    with_email = len(sales_df[sales_df['emails'].astype(str) != ''])
    sce_ready = len(sales_df[sales_df['sce_sales_ready'] == True])
    
    print(f"\nFinal stats:")
    print(f"  Total: {len(sales_df)}")
    print(f"  With website: {with_website} ({100*with_website/len(sales_df):.1f}%)")
    print(f"  With email: {with_email} ({100*with_email/len(sales_df):.1f}%)")
    print(f"  SCE Ready: {sce_ready}")


if __name__ == "__main__":
    main()
