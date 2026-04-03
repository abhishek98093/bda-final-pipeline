# FDA-4: Comparative Financial Analysis Pipeline

## 📋 Overview

This is a professional **3-stage data extraction pipeline** for the FDA-4 assignment (Comparative Financial Analysis across Industry Peers).

**Data Source:** SEC EDGAR Company Facts API (100% real data!)
- API Documentation: https://www.sec.gov/edgar/sec-api-documentation
- Endpoint: `https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json`

---

## 📁 Directory Structure

```
fda4_pipeline/
├── run_pipeline.py              # Main pipeline runner
├── 01_fetch_raw_data.py         # Stage 1: Fetch raw JSON from SEC
├── 02_json_to_company_csv.py    # Stage 2: Extract JSON → Company CSVs
├── 03_combine_and_process.py    # Stage 3: Combine, Clean & Calculate Ratios
├── xbrl_tags.py                 # XBRL tag mapping (all companies)
├── README.md                    # This file
│
├── data/
│   ├── raw_json/                # Stage 1 output
│   │   ├── AAPL_raw.json
│   │   ├── MSFT_raw.json
│   │   ├── _fetch_manifest.json
│   │   └── ... (30 JSON files)
│   │
│   ├── raw_csv/                 # Stage 3 intermediate
│   │   └── raw_combined_data.csv
│   │
│   └── processed/               # Stage 3 output (FINAL)
│       ├── final_peer_comparison.csv      ← MAIN DELIVERABLE
│       ├── peer_comparison_5year.csv      ← 2020-2024 data
│       ├── peer_comparison_latest_year.csv
│       └── data_quality_report.json
│
└── company_csv/                 # Stage 2 output
    ├── AAPL.csv
    ├── MSFT.csv
    └── ... (30 CSV files)
```

---

## 🚀 Quick Start

### 1. Setup Environment

```bash
cd ~/fda4_project/fda4_pipeline
python3 -m venv venv
source venv/bin/activate
pip install pandas numpy requests
```

### 2. ⚠️ IMPORTANT: Edit Your Email (Stage 1 only)

Open `01_fetch_raw_data.py` and change:
```python
USER_AGENT = "YourName your.email@iiita.ac.in"
```

### 3. Run Complete Pipeline

```bash
# Run all stages
python run_pipeline.py

# Or skip Stage 1 if you already have JSON files
python run_pipeline.py --skip-fetch
```

### 4. Run Stages Individually

```bash
python 01_fetch_raw_data.py        # Stage 1: Fetch from SEC API
python 02_json_to_company_csv.py   # Stage 2: JSON → Company CSVs
python 03_combine_and_process.py   # Stage 3: Combine & Process
```

---

## 📊 Pipeline Stages

### Stage 1: Raw Data Fetcher (`01_fetch_raw_data.py`)
- Calls SEC EDGAR API for each of 30 companies
- Saves raw JSON responses as `data/raw_json/{TICKER}_raw.json`
- Zero-pads CIK numbers automatically (e.g., 320193 → 0000320193)
- No processing - pure data acquisition
- **Duration:** ~5-7 minutes

### Stage 2: JSON to Company CSV (`02_json_to_company_csv.py`)
- Reads each raw JSON file
- Uses `xbrl_tags.py` mapping to extract features (first match wins)
- Creates individual CSV per company: `company_csv/{TICKER}.csv`
- Extracts 6 years of data (2020-2025)
- **Duration:** ~2-3 seconds

### Stage 3: Combine & Process (`03_combine_and_process.py`)
- Combines all 30 company CSVs into one dataset
- Performs data quality analysis
- Handles missing values
- Calculates 12 financial ratios
- Creates final `data/processed/final_peer_comparison.csv`
- **Duration:** ~2-3 seconds

---

## 📈 Features Extracted

### Direct Features (18 fields)

| Category | Fields |
|----------|--------|
| **Income Statement** | revenue, net_income, gross_profit, operating_income, cost_of_revenue, interest_expense, eps_basic, eps_diluted |
| **Balance Sheet** | total_assets, total_liabilities, stockholders_equity, current_assets, current_liabilities, cash_and_equivalents, short_term_investments, accounts_receivable, long_term_debt, inventory |

### Calculated Ratios (12 fields)

| Category | Ratios | Formula |
|----------|--------|---------|
| **Profitability** | ROE | net_income / stockholders_equity |
| | ROA | net_income / total_assets |
| | Gross Margin | gross_profit / revenue |
| | Net Margin | net_income / revenue |
| | Operating Margin | operating_income / revenue |
| **Liquidity** | Current Ratio | current_assets / current_liabilities |
| | Quick Ratio | (current_assets - inventory) / current_liabilities |
| **Leverage** | Debt-to-Equity | total_liabilities / stockholders_equity |
| | Debt-to-Assets | total_liabilities / total_assets |
| | Interest Coverage | operating_income / interest_expense |
| **Efficiency** | Asset Turnover | revenue / total_assets |
| | Equity Turnover | revenue / stockholders_equity |

### Metadata (3 fields)
- ticker, company_name, fiscal_year

**Total: 33 columns in final CSV**

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

## 📊 Final Output

### Main Deliverable
```
data/processed/final_peer_comparison.csv
```
- **Rows:** ~173 (30 companies × 6 years, minus missing years)
- **Columns:** 33 (3 metadata + 18 direct + 12 ratios)
- **Years:** 2020, 2021, 2022, 2023, 2024, 2025

### Additional Files
| File | Description |
|------|-------------|
| `peer_comparison_5year.csv` | 2020-2024 data only |
| `peer_comparison_latest_year.csv` | 2025 data only |
| `data_quality_report.json` | Data quality metrics |

---


## 🔧 XBRL Tag Mapping

The `xbrl_tags.py` file contains a comprehensive mapping of XBRL tags for each financial field. Different companies use different tag names for the same concept:

| Field | Example Tags |
|-------|--------------|
| revenue | Revenues, RevenueFromContractWithCustomerExcludingAssessedTax, SalesRevenueNet |
| net_income | NetIncomeLoss, ProfitLoss |
| total_assets | Assets |
| stockholders_equity | StockholdersEquity, StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest |

The pipeline uses **first match wins** - it tries each tag in order until one returns data.

---

## 📚 References

- SEC EDGAR API Documentation: https://www.sec.gov/edgar/sec-api-documentation
- XBRL US-GAAP Taxonomy: https://xbrl.us/xbrl-taxonomy/
- Company Facts API: `https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json`

---

## 👨‍💻 Author
Ankit	IIT2023120
Aditya Pankaj Sharma	IIT2023162
Abhishek kumar	IIT2023261
**FDA-4 Assignment** - Comparative Financial Analysis across Industry Peers  
**Course:** Big Data Analytics (Jan-July 2026)  
**Institute:** IIIT Allahabad

---

## 📝 Usage Examples

```bash
# Full pipeline run
python run_pipeline.py

# Skip fetching (if JSONs exist)
python run_pipeline.py --skip-fetch

# Run specific stage
python run_pipeline.py --stage 2
python run_pipeline.py --stage 3

# Check final output
head data/processed/final_peer_comparison.csv
wc -l data/processed/final_peer_comparison.csv
```
