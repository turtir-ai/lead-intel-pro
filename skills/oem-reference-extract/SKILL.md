# Skill: oem-reference-extract

## Goal
Extract REAL customer company names from OEM manufacturer (Brückner, Monforts, Santex) reference pages and news articles.

## Problem Solved
Current OEM customer search extracts:
- Article headlines instead of company names
- Partial sentences as "company names"
- Technology descriptions instead of customers

We need **precision entity extraction** from OEM sources.

## Inputs
- OEM reference/news pages (HTML content)
- `config/oem_sources.yaml` - OEM website patterns

## Outputs
- Verified customer leads with:
  - `company`: Real company name (cleaned)
  - `country`: Extracted from context
  - `oem_brand`: Which OEM they bought from
  - `equipment_type`: What they installed (stenter, Montex, etc.)
  - `evidence_url`: Source page
  - `evidence_snippet`: Relevant quote
  - `confidence`: high/medium/low

## Extraction Patterns

### Pattern 1: Direct mention with location
```
"[Company Name] in [Country] has installed..."
"[Company Name] from [Country] ordered..."
"[Company Name], located in [City], [Country]..."
```

### Pattern 2: Reference/case study header
```
"Customer: [Company Name]"
"Project: [Company Name] - [Location]"
"Reference: [Company Name]"
```

### Pattern 3: News article body
```
"...delivered to [Company Name] in [Country]"
"...installed at [Company Name]'s facility"
"[Company Name] has commissioned..."
```

### Pattern 4: Quote attribution
```
'"....", says [Person], [Title] at [Company Name]'
```

## Validation Rules
1. Company name must be 2-8 words
2. Must NOT be an OEM name (Brückner, Monforts, etc.)
3. Should NOT contain process words (finishing, dyeing, etc.) alone
4. Prefer names with company suffix (GmbH, Ltd, SA, etc.)
5. Cross-reference with existing known_manufacturers list

## Implementation Files
- `src/collectors/oem_reference_extractor.py` - Main extraction logic
- `config/oem_sources.yaml` - OEM website configurations

## Verification Checklist
- [ ] Each extracted company validated against patterns
- [ ] Country extracted or inferred
- [ ] Evidence snippet stored
- [ ] No OEM names in customer list
- [ ] No article headlines as company names
