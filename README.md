# FDA-4: Comparative Financial Analysis Pipeline

## 📋 Overview

This is a professional **3-stage data extraction pipeline** for the FDA-4 assignment (Comparative Financial Analysis across Industry Peers).

**Data Source:** SEC EDGAR Company Facts API (100% real data, no faking!)
- API Documentation: https://www.sec.gov/edgar/sec-api-documentation
- Endpoint: `https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json`

---

## 📁 Directory Structure

```
fda4_pipeline/
├── run_pipeline.py              # Main pipeline runner
├── 01_fetch_raw_data.py         # Stage 1: Fetch raw JSON from SEC
├── 02_aggregate_raw_data.py     # Stage 2: Aggregate into raw CSV
├── 03_process_and_enhance.py    # Stage 3: Clean & calculate ratios
├── README.md                    # This file
└── data/
    ├── raw_json/                # Stage 1 output
    │   ├── AAPL_raw.json
    │   ├── MSFT_raw.json
    │   ├── _fetch_manifest.json
    │   └── ...
    ├── raw_csv/                 # Stage 2 output
    │   ├── raw_financial_data.csv
    │   └── _xbrl_concept_reference.csv
    └── processed/               # Stage 3 output (FINAL)
        ├── final_peer_comparison.csv
        ├── peer_comparison_latest_year.csv
        └── data_quality_report.json
```

---

## 🚀 Quick Start

### 1. Setup Environment

```bash
cd ~/fda4_pipeline
python3 -m venv venv
source venv/bin/activate
pip install pandas numpy requests
```

### 2. ⚠️ IMPORTANT: Edit Your Email

Open `01_fetch_raw_data.py` and change:
```python
USER_AGENT = "YourName your.email@iiita.ac.in"
```

### 3. Run Complete Pipeline

```bash
python run_pipeline.py
```

Or run stages individually:
```bash
python 01_fetch_raw_data.py    # Stage 1 only
python 02_aggregate_raw_data.py # Stage 2 only
python 03_process_and_enhance.py # Stage 3 only
```

---

## 📊 Pipeline Stages

### Stage 1: Raw Data Fetcher
- Calls SEC EDGAR API for each of 30 companies
- Saves raw JSON responses as `{TICKER}_raw.json`
- Zero-pads CIK numbers automatically (e.g., 320193 → 0000320193)
- No processing - pure data acquisition

### Stage 2: Data Aggregator
- Reads all raw JSON files
- Extracts XBRL concepts (Assets, Revenue, Net Income, etc.)
- Creates `raw_financial_data.csv` with raw values
- NO ratio calculations yet

### Stage 3: Data Processor
- Performs data exploration & quality analysis
- Handles missing values (merges alternative XBRL concepts)
- Calculates 12 financial ratios
- Creates final `final_peer_comparison.csv`

---

## 📈 Features Extracted

### Raw Financial Data (Stage 2)
| Category | Fields |
|----------|--------|
| Balance Sheet | Total Assets, Current Assets, Liabilities, Equity, Inventory |
| Income Statement | Revenue, COGS, Gross Profit, Operating Income, Net Income |
| Cash Flow | Operating, Investing, Financing Cash Flows |
| Per Share | EPS Basic, EPS Diluted, Shares Outstanding |

### Calculated Ratios (Stage 3)
| Category | Ratios |
|----------|--------|
| Profitability | ROE, ROA, Gross Margin, Net Margin, Operating Margin |
| Liquidity | Current Ratio, Quick Ratio |
| Leverage | Debt-to-Equity, Debt-to-Assets, Interest Coverage |
| Efficiency | Asset Turnover, Equity Turnover |

---

## 🏢 30 Peer Companies (Technology Sector)

| # | Ticker | Company | SIC |
|---|--------|---------|-----|
| 1 | AAPL | Apple Inc. | 3571 |
| 2 | MSFT | Microsoft Corporation | 7372 |
| 3 | GOOGL | Alphabet Inc. | 7370 |
| 4 | AMZN | Amazon.com Inc. | 5961 |
| 5 | META | Meta Platforms Inc. | 7370 |
| 6 | NVDA | NVIDIA Corporation | 3674 |
| 7 | TSLA | Tesla Inc. | 3711 |
| 8 | ADBE | Adobe Inc. | 7372 |
| 9 | CRM | Salesforce Inc. | 7372 |
| 10 | NFLX | Netflix Inc. | 7841 |
| 11 | ORCL | Oracle Corporation | 7372 |
| 12 | INTC | Intel Corporation | 3674 |
| 13 | AMD | Advanced Micro Devices | 3674 |
| 14 | CSCO | Cisco Systems Inc. | 3576 |
| 15 | IBM | IBM Corporation | 7370 |
| 16 | QCOM | Qualcomm Inc. | 3674 |
| 17 | NOW | ServiceNow Inc. | 7372 |
| 18 | INTU | Intuit Inc. | 7372 |
| 19 | PLTR | Palantir Technologies | 7372 |
| 20 | SNOW | Snowflake Inc. | 7372 |
| 21 | CRWD | CrowdStrike Holdings | 7372 |
| 22 | PANW | Palo Alto Networks | 7372 |
| 23 | WDAY | Workday Inc. | 7372 |
| 24 | UBER | Uber Technologies | 4121 |
| 25 | ABNB | Airbnb Inc. | 7011 |
| 26 | PYPL | PayPal Holdings | 7389 |
| 27 | SQ | Block Inc. | 7389 |
| 28 | SHOP | Shopify Inc. | 7372 |
| 29 | ZM | Zoom Video | 7372 |
| 30 | TWLO | Twilio Inc. | 7372 |

---

## ⏱️ Estimated Time

| Stage | Duration |
|-------|----------|
| Stage 1 (Fetching) | ~5-7 minutes |
| Stage 2 (Aggregating) | ~10 seconds |
| Stage 3 (Processing) | ~5 seconds |
| **Total** | **~8 minutes** |

---

## 📚 References

- SEC EDGAR API Documentation: https://www.sec.gov/edgar/sec-api-documentation
- XBRL US-GAAP Taxonomy: https://xbrl.us/xbrl-taxonomy/
- Company Facts API: https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json

---

## 👨‍💻 Author

FDA-4 Assignment - Big Data Analytics Course
IIIT Allahabad, Jan-July 2026
