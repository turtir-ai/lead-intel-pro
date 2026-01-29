# 🎯 LeadIntel Pro — AI-Powered B2B Lead Discovery for Industrial Spare Parts

> **Transform trade data and web intelligence into qualified sales prospects.**

[![Python 3.10+](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Brave Search API](https://img.shields.io/badge/API-Brave%20Search-orange.svg)](https://brave.com/search/api/)
[![UN Comtrade](https://img.shields.io/badge/Data-UN%20Comtrade-blue.svg)](https://comtradeplus.un.org/)

---

## 🚀 What Is This?

**LeadIntel Pro** is a comprehensive B2B lead generation and market intelligence platform designed for industrial spare parts manufacturers targeting the textile finishing machinery sector.

This system was built to solve a real business problem: **How can a plastic injection company that manufactures stenter machine spare parts (clips, guide rails, bushings, spindle nuts) identify and reach actual customers who operate Brückner, Monforts, Krantz, Artos, and Santex finishing machines worldwide?**

### The Challenge

Traditional lead generation approaches fall short for niche industrial B2B:
- ❌ Generic business directories list thousands of "textile companies" — but most are spinners or garment makers, not finishing mills
- ❌ Trade fair exhibitor lists include irrelevant categories
- ❌ Manual research is time-consuming and incomplete

### The Solution

LeadIntel Pro combines **multiple data intelligence sources** to find **precision-qualified leads**:

| Data Source | Intelligence Extracted |
|-------------|----------------------|
| 🔍 **Brave Search API** | Real-time web search for OEM customer references, trade news, industry articles |
| 📊 **UN Comtrade API** | Import/export data by HS code to identify active importers of textile machinery parts |
| 🏭 **OEM Reference Pages** | Scrape Brückner, Monforts news/references to find their actual customers |
| 📋 **Trade Fair Exhibitors** | ITMA, Techtextil, Texprocess, regional textile fairs |
| 🌿 **Certification Directories** | GOTS, Better Cotton members with finishing operations |
| 📄 **PDF Exhibitor Lists** | Extract company data from trade fair PDF catalogs |

---

## 🏢 Business Context

This project was developed for **internal market research** at a plastic injection molding company that manufactures replacement parts for textile stenter (tenter/ramöz) machines.

### Products We Manufacture

| Product (German) | English | Application |
|-----------------|---------|-------------|
| Gleitstein | Guide Block | Chain guidance system |
| Gleitleiste | Guide Rail | Frame sliding mechanism |
| Kluppen | Clips/Clamps | Fabric edge gripping |
| Öffner Segment | Opener Segment | Clip opening mechanism |
| Buchse | Bushing | Bearing components |
| Spindel Mutter | Spindle Nut | Width adjustment |
| Nadelleiste | Needle Bar | Pin-frame systems |
| Kettenführung | Chain Guide | Chain path control |

### Target Market

We target **textile finishing mills** that operate stenter machines from:
- 🇩🇪 **Brückner** (Power-Frame, SUPRA)
- 🇩🇪 **Monforts** (Montex, Monfortex)
- 🇩🇪 **Krantz** (various models)
- 🇩🇪 **Artos** (Pin stenters)
- 🇨🇭 **Santex** (various models)

### Geographic Focus

Priority markets identified through Comtrade analysis:

| Priority | Region | Key Countries |
|----------|--------|---------------|
| 🥇 High | South America | Brazil, Argentina, Peru, Mexico, Colombia |
| 🥇 High | North Africa | Egypt, Morocco, Tunisia, Algeria |
| 🥈 Medium | South Asia | Pakistan, India, Bangladesh, Sri Lanka |
| 🥈 Medium | Middle East | Turkey |

---

## 🔧 Technical Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        LeadIntel Pro Pipeline                       │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   ┌──────────────┐    ┌──────────────┐    ┌──────────────┐        │
│   │  Brave API   │    │ OEM Websites │    │  Comtrade    │        │
│   │  (Discovery) │    │  (Scraping)  │    │  (Trade DB)  │        │
│   └──────┬───────┘    └──────┬───────┘    └──────┬───────┘        │
│          │                   │                   │                 │
│          ▼                   ▼                   ▼                 │
│   ┌─────────────────────────────────────────────────────┐         │
│   │              COLLECTORS LAYER                        │         │
│   │  • competitor_harvester.py (OEM references)         │         │
│   │  • regional_collector.py (geo-targeted search)      │         │
│   │  • auto_discover.py (fair/directory discovery)      │         │
│   │  • exhibitor_list.py (PDF extraction)               │         │
│   │  • gots_directory.py, bettercotton_members.py       │         │
│   └─────────────────────────┬───────────────────────────┘         │
│                             │                                      │
│                             ▼                                      │
│   ┌─────────────────────────────────────────────────────┐         │
│   │              PROCESSORS LAYER                        │         │
│   │  • enricher.py (contact extraction)                 │         │
│   │  • dedupe.py (entity resolution)                    │         │
│   │  • scorer.py (lead qualification)                   │         │
│   │  • customer_qualifier.py (precision filtering)      │         │
│   │  • exporter.py (CRM output generation)              │         │
│   └─────────────────────────┬───────────────────────────┘         │
│                             │                                      │
│                             ▼                                      │
│   ┌─────────────────────────────────────────────────────┐         │
│   │                    OUTPUTS                           │         │
│   │  • targets_master.csv (all qualified leads)         │         │
│   │  • qualified_customers.csv (precision filtered)     │         │
│   │  • top100.csv (highest scored)                      │         │
│   │  • linkedin_xray_queries.csv (search strings)       │         │
│   └─────────────────────────────────────────────────────┘         │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 📦 Installation

### Prerequisites

- Python 3.10 or higher
- Brave Search API key ([Get one here](https://brave.com/search/api/))
- UN Comtrade API key (optional, for trade ranking)

### Setup

```bash
# Clone the repository
git clone https://github.com/turtir-ai/lead-intel-pro.git
cd lead-intel-pro

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Configure environment variables
cp .env.example .env
# Edit .env with your API keys
```

### Environment Variables

Create a `.env` file in the project root:

```env
BRAVE_API_KEY=your_brave_api_key_here
COMTRADE_API_KEY=your_comtrade_key_here  # Optional
```

---

## ⚙️ Configuration

All configuration is managed through YAML files in the `config/` directory:

### `config/targets.yaml`
Define target markets, priority countries, and known manufacturers:

```yaml
priority_regions:
  south_america:
    priority: 1
    countries:
      - name: Brazil
        known_manufacturers:
          - GRUPO MALWEE
          - Cedro Têxtil
          - Vicunha Têxtil
```

### `config/products.yaml`
Define your product catalog with HS codes and multilingual keywords:

```yaml
products:
  - id: BRK-001
    name_de: Gleitstein
    name_en: Guide Block
    hs_codes: ["845190", "392690"]
    keywords:
      en: ["guide block", "stenter chain guide"]
      de: ["Gleitstein", "Kettenführung"]
      tr: ["kızak taşı", "zincir kılavuzu"]
```

### `config/competitors.yaml`
Configure OEM reference pages to scrape for customer leads:

```yaml
competitors:
  - name: Brückner
    url: https://www.brueckner-textile.com
    reference_pages:
      - /en/news/
      - /en/references/
```

### `config/scoring.yaml`
Customize lead scoring weights:

```yaml
weights:
  fit_score: 0.35
  capacity_score: 0.25
  import_score: 0.20
  reachability_score: 0.20
```

---

## 🚀 Usage

### Quick Start — Full Pipeline

```bash
# Run complete pipeline
python run_pipeline.py

# Or use app.py for individual stages
python app.py all
```

### Individual Stages

```bash
# 1. Discover new sources (fairs, directories)
python app.py discover

# 2. Harvest leads from all sources
python app.py harvest

# 3. Enrich with contact information
python app.py enrich

# 4. Deduplicate leads
python app.py dedupe

# 5. Score and rank leads
python app.py score

# 6. Export CRM-ready outputs
python app.py export
```

### Advanced: Regional Collection

```bash
# Collect leads from priority regions
python -c "from src.collectors.regional_collector import RegionalCollector; rc = RegionalCollector(); rc.collect_south_america(); rc.collect_north_africa()"
```

---

## 📊 Output Files

After running the pipeline, find your results in `outputs/crm/`:

| File | Description |
|------|-------------|
| `targets_master.csv` | All collected leads with scores |
| `qualified_customers.csv` | Precision-filtered real customers |
| `top100.csv` | Top 100 highest-scored leads |
| `linkedin_xray_queries.csv` | Ready-to-use LinkedIn search strings |

### Sample Output

```
✅ Pipeline completed successfully!

📊 Results Summary:
   Total leads collected: 1,215
   Qualified customers: 164
   
   By Source:
   • known_manufacturer: 80
   • oem_customer: 50
   • precision_search: 19
   • brave_search: 15
   
   Top Countries:
   • Brazil: 24
   • Turkey: 14
   • Egypt: 14
   • Pakistan: 13
```

---

## 🎯 Lead Qualification Logic

Not all textile companies are potential customers. The system uses multi-layer qualification:

### ✅ High Confidence Sources
| Source Type | Confidence | Reason |
|-------------|------------|--------|
| `known_manufacturer` | 100% | Pre-verified stenter operators |
| `oem_customer` | 95% | Mentioned in OEM news/references |
| `precision_search` | 90% | Found via product-specific search |

### ⚠️ Requires Filtering
| Source Type | Confidence | Filtering Applied |
|-------------|------------|-------------------|
| `brave_search` | 60% | Keyword qualification |
| `gots` | 40% | Must have finishing operations |
| `fair` | 50% | Category filtering |

### Qualification Keywords

**Qualifying (increases score):**
- Machine types: `stenter`, `tenter`, `ramöz`, `montex`, `power-frame`
- Operations: `finishing`, `dyeing`, `heat setting`, `coating`
- OEM brands: `Brückner`, `Monforts`, `Krantz`, `Artos`, `Santex`

**Disqualifying (decreases score):**
- `spinning only`, `garment manufacturer`, `trading company`

---

## 🔒 Data Privacy & Ethics

This tool is designed with ethical considerations:

- ✅ **Robots.txt Compliance**: All scrapers respect robots.txt directives
- ✅ **Rate Limiting**: Configurable delays between requests (default: 2-5 seconds)
- ✅ **No Personal Data Scraping**: Focuses on company-level information
- ✅ **API Terms Compliance**: Uses official APIs within their terms of service
- ✅ **Evidence Logging**: All data sources are logged with URL and timestamp

---

## 📁 Project Structure

```
lead_intel_v2/
├── app.py                 # Main CLI application
├── run_pipeline.py        # Full pipeline runner
├── requirements.txt       # Python dependencies
├── config/
│   ├── targets.yaml       # Target markets & known manufacturers
│   ├── products.yaml      # Product catalog with HS codes
│   ├── competitors.yaml   # OEM reference pages
│   ├── sources.yaml       # Fairs & directories
│   ├── scoring.yaml       # Lead scoring weights
│   └── policies.yaml      # Crawler policies
├── src/
│   ├── collectors/        # Data collection modules
│   │   ├── competitor_harvester.py
│   │   ├── regional_collector.py
│   │   ├── auto_discover.py
│   │   ├── exhibitor_list.py
│   │   └── ...
│   ├── processors/        # Data processing modules
│   │   ├── enricher.py
│   │   ├── dedupe.py
│   │   ├── scorer.py
│   │   ├── customer_qualifier.py
│   │   └── exporter.py
│   ├── utils/             # Utility modules
│   │   ├── http_client.py
│   │   ├── cache.py
│   │   └── logger.py
│   └── ui/
│       └── app_streamlit.py  # Optional web UI
├── skills/                # Modular skill documentation
└── outputs/               # Generated outputs (gitignored)
```

---

## 🛠️ Technologies Used

| Category | Technologies |
|----------|-------------|
| **Language** | Python 3.10+ |
| **Data Processing** | Pandas, NumPy |
| **Web Scraping** | BeautifulSoup4, Requests, lxml |
| **APIs** | Brave Search API, UN Comtrade API |
| **PDF Processing** | pdfplumber, PyMuPDF |
| **CLI** | Click, Rich |
| **Caching** | Local file cache with hash-based deduplication |

---

## 📈 Performance Metrics

From actual pipeline runs:

| Metric | Value |
|--------|-------|
| Total leads collected | 1,215+ |
| Qualified customers | 164 (14.9%) |
| Countries covered | 25+ |
| OEM references found | 50+ |
| Known manufacturers | 80+ |
| Processing time | ~5 minutes |

---

## 🤝 Contributing

This is a private project for internal business use, but the architecture and approach may be useful for similar B2B lead generation needs in other industries.

If you're building something similar, key learnings:
1. **Precision over volume**: Better to have 100 qualified leads than 10,000 unqualified ones
2. **Multi-source triangulation**: Combine trade data, web search, and OEM references
3. **Industry-specific keywords**: Generic searches produce noise; use domain expertise
4. **Evidence trail**: Always log where each lead came from for validation

---

## 📄 License

MIT License — See [LICENSE](LICENSE) for details.

---

## 👤 Author

**Turtir AI**  
📧 turtirhey@gmail.com  
🔗 [GitHub](https://github.com/turtir-ai)

---

## 🙏 Acknowledgments

- [Brave Search API](https://brave.com/search/api/) for real-time web intelligence
- [UN Comtrade](https://comtradeplus.un.org/) for international trade data
- The textile machinery industry for being complex enough to make this project interesting

---

<p align="center">
  <strong>Built with ❤️ for precision B2B lead generation</strong>
</p>
