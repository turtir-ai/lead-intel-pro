# AutoDiscover Engine - Otonom Web İstihbarat Sistemi

## 🎯 Amaç
LLM olmadan, tamamen Python heuristics + Playwright network sniffing ile:
- Yeni B2B lead kaynakları bulmak (Brave Search API)
- Sitelerin API'lerini otomatik keşfetmek
- Pattern matching ile veri çıkarmak
- Otomatik Python collector modülleri üretmek

## 📦 Modüller

### 1. BraveDiscoverer (`discoverer.py`)
Brave Search API ile yeni potansiyel kaynaklar bulur.

```python
from src.autodiscover.discoverer import BraveDiscoverer

discoverer = BraveDiscoverer()  # BRAVE_API_KEY env var gerekir
sources = discoverer.discover_sources(
    countries=["Egypt", "Morocco", "Brazil"],
    max_queries=20
)
```

**Özellikler:**
- Pre-tanımlı tekstil sorgular şablonu
- URL scoring (directory, member list, association vb.)
- Sonuç caching (7 gün)
- discovered_sources.yaml'a kayıt

### 2. SiteDiagnoser (`diagnoser.py`)
Playwright ile site analizi yapar.

```python
from src.autodiscover.diagnoser import SiteDiagnoser

diagnoser = SiteDiagnoser()
result = diagnoser.diagnose("https://example.com/directory")
```

**Yakalar:**
- Tüm network trafiği (XHR/Fetch → JSON API'ler)
- Console logları
- HAR dosyası
- DOM snapshot
- Screenshot
- Playwright trace

**Çıktı dizini:** `data/diagnostics/<domain>/<timestamp>/`

### 3. PatternAnalyzer (`analyzer.py`)
JSON ve HTML yapılarını analiz eder.

```python
from src.autodiscover.analyzer import PatternAnalyzer

analyzer = PatternAnalyzer()

# JSON analizi
pattern = analyzer.detect_list_pattern(json_data)
leads = analyzer.extract_from_pattern(json_data, pattern)

# HTML analizi
patterns = analyzer.analyze_html_for_patterns(html)
```

**Özellikler:**
- Field name mapping (company_name → company, etc.)
- Email/phone/URL regex extraction
- Repeating pattern detection (cards, tables)
- Liste path detection (items, results, data, etc.)

### 4. AdapterGenerator (`adapter_generator.py`)
Python collector modülleri üretir.

```python
from src.autodiscover.adapter_generator import AdapterGenerator

generator = AdapterGenerator()
adapter_path = generator.generate_api_adapter(
    source_url="https://example.com",
    api_url="https://api.example.com/companies",
    pattern=detected_pattern
)
```

**Çıktı:** `src/collectors/auto/<domain>_collector.py`

### 5. AutoDiscoverEngine (`engine.py`)
Tüm modülleri orkestra eder.

```python
from src.autodiscover.engine import AutoDiscoverEngine

engine = AutoDiscoverEngine()

# Full auto mode
summary = engine.run_auto(
    countries=["Egypt", "Morocco"],
    max_discoveries=10,
    max_diagnoses=5
)

# Tek URL işle
result = engine.process_url("https://example.com/directory")

# Status
print(engine.status())
```

## 🚀 CLI Kullanımı

```bash
# Yeni kaynaklar keşfet
python -m src.autodiscover.engine discover --countries Egypt,Morocco --max 20

# Tek siteyi diagnose et
python -m src.autodiscover.engine diagnose --url https://example.com

# Tek siteyi tam işle (diagnose + analyze + generate)
python -m src.autodiscover.engine process --url https://example.com

# Full auto mode
python -m src.autodiscover.engine run --countries Egypt,Morocco

# Status göster
python -m src.autodiscover.engine status
```

## 🔧 Konfigürasyon

### Brave API Key
```bash
export BRAVE_API_KEY="your-api-key"
```

### Lead Keywords (diagnoser.py içinde)
```python
LEAD_KEYWORDS = [
    "company", "manufacturer", "supplier",
    "email", "phone", "contact",
    "textile", "fabric", "cotton",
    "certificate", "oeko-tex", "gots",
]
```

### Field Mappings (analyzer.py içinde)
```python
FIELD_MAPPINGS = {
    "company_name": "company",
    "email_address": "email",
    "country_code": "country",
    # ...
}
```

## 📊 Pipeline Entegrasyonu

AutoDiscover tarafından üretilen collector'lar `src/collectors/auto/` altına kaydedilir.
Bunları aktifleştirmek için:

1. `config/auto_adapters.yaml` dosyasını kontrol et
2. `enabled: true` yap
3. `config/sources.yaml`'a ekle
4. `python app.py harvest` çalıştır

## 🏗️ Mimari

```
┌─────────────────┐
│  Brave Search   │ → Yeni URL'ler bul
└────────┬────────┘
         ↓
┌─────────────────┐
│  SiteDiagnoser  │ → Network traffic yakala
└────────┬────────┘
         ↓
┌─────────────────┐
│ PatternAnalyzer │ → JSON/HTML pattern bul
└────────┬────────┘
         ↓
┌─────────────────┐
│AdapterGenerator │ → Python collector üret
└────────┬────────┘
         ↓
┌─────────────────┐
│  lead_intel_v2  │ → Pipeline'a entegre
└─────────────────┘
```

## ⚠️ Limitasyonlar

1. **LLM Yok:** Semantic anlama yok, sadece keyword matching
2. **JS-Heavy Siteler:** Playwright ile çözülür ama bazı SPA'lar sorunlu olabilir
3. **Anti-Bot:** Rate limiting ve blocking algılanır, bypass denenmez
4. **Manual Review:** Üretilen adapter'lar manuel onay gerektirebilir

## 🔮 Gelecek Geliştirmeler

1. **Pagination Detection:** Otomatik sayfa çevirme
2. **Login/Auth:** Cookie-based session desteği
3. **PDF Processing:** PDF içinden veri çıkarma
4. **Scheduler:** Periyodik otomatik keşif
5. **Quality Scoring:** Üretilen adapter kalite puanı
