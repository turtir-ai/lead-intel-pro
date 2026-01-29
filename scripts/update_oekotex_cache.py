#!/usr/bin/env python3
"""Update OEKO-TEX profiles cache with sample data for testing."""
import json
from pathlib import Path

# Sample profiles for testing
sample_data = {
    "Egypt": [
        {"company": "Abo Kamar Weaving Company", "location": "Egypt\n31951 EL MEHALLA EL KOBRA", "profile_url": "https://services.oeko-tex.com/newoekotex/portal/for-new-website/customer_profile/16019~1vl1Gi~v_gJd_SJnmwwgdqVOiCsqiOB1gg/", "country": "Egypt"},
        {"company": "AKTAN MISR TEXTILE & DYING S.A.E.", "location": "Egypt\n31911 El Mehalla El Kubra", "profile_url": "https://services.oeko-tex.com/newoekotex/portal/for-new-website/customer_profile/6015~1vl1Gi~gfeqz7agajurVehc3NJ9NGZSQn8/", "country": "Egypt"},
        {"company": "Alexandria Spinning & Weaving Co.", "location": "Egypt\nGovernorate Menofia", "profile_url": "https://services.oeko-tex.com/newoekotex/portal/for-new-website/customer_profile/8014~1vl1Gi~8hbWDJU4_2g_LpMkWbnzHYeXoRg/", "country": "Egypt"},
        {"company": "Delta Textile Egypt", "location": "Egypt\n11765 Cairo", "profile_url": "https://services.oeko-tex.com/newoekotex/portal/for-new-website/customer_profile/15184~1vl1Gm~Uw4i3pLVwQo1hFTfi2t3M-1J6CE/", "country": "Egypt"},
        {"company": "Giza Spinning and Weaving Co.", "location": "Egypt\n12875 Kerdasa", "profile_url": "https://services.oeko-tex.com/newoekotex/portal/for-new-website/customer_profile/2499~1vl1Gq~ZaaL1wU-DrF01SMcLsMTouRu_To/", "country": "Egypt"},
    ],
    "Morocco": [
        {"company": "ABF SARL", "location": "Morocco\n24000 El Jadida", "profile_url": "https://services.oeko-tex.com/newoekotex/portal/for-new-website/customer_profile/11008~1vl1NB~q-ij3UG_uTRzLbu0skCFYfIS7WI/", "country": "Morocco"},
        {"company": "Coats Maroc", "location": "Morocco\nAin Sebaa, Casablanca", "profile_url": "https://services.oeko-tex.com/newoekotex/portal/for-new-website/customer_profile/10174~1vl1ND~U6ZawTKozbuBoU8MsMaC_-j4R-g/", "country": "Morocco"},
        {"company": "RICHBOND", "location": "Morocco\n20630 Casablanca", "profile_url": "https://services.oeko-tex.com/newoekotex/portal/for-new-website/customer_profile/38319~1vl1NH~7l1426diRcGaYLf5jrN34dpw3J4/", "country": "Morocco"},
    ],
    "Tunisia": [
        {"company": "ACTIVE KNITTING", "location": "Tunisia\n5076 Bembla - Monastir", "profile_url": "https://services.oeko-tex.com/newoekotex/portal/for-new-website/customer_profile/49130~1vl186~knn0bMZmy-ZLnZpmSRdAHPEaYrA/", "country": "Tunisia"},
    ],
    "Algeria": [],
    "Brazil": [
        {"company": "Adatex S.A. Industrial e Comercial", "location": "Brazil\n12322-440 Jacarei", "profile_url": "https://services.oeko-tex.com/newoekotex/portal/for-new-website/customer_profile/16022~1vl1Bi~upLh3QPDXl7ZhfxRdY3IrTh97dA/", "country": "Brazil"},
        {"company": "Buddemeyer S/A", "location": "Brazil\n89280-901 Santa Catarina", "profile_url": "https://services.oeko-tex.com/newoekotex/portal/for-new-website/customer_profile/13106~1vl1Bi~Jiv2Pm6XAEkxLau1OvUJbSdDKos/", "country": "Brazil"},
    ],
    "Argentina": [],
    "Colombia": [],
    "Peru": [],
}

# Load existing and merge
cache_path = Path("data/raw/json/oekotex_profiles.json")
if cache_path.exists():
    with open(cache_path, "r") as f:
        existing = json.load(f)
    # Merge - keep existing Egypt data if it has more
    for country, profiles in existing.items():
        if len(profiles) > len(sample_data.get(country, [])):
            sample_data[country] = profiles
            
# Save
cache_path.parent.mkdir(parents=True, exist_ok=True)
with open(cache_path, "w") as f:
    json.dump(sample_data, f, indent=2, ensure_ascii=False)

print("OEKO-TEX profiles cache updated:")
for country, profiles in sample_data.items():
    print(f"  {country}: {len(profiles)} profiles")
