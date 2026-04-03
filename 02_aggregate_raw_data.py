#!/usr/bin/env python3
"""
================================================================================
STAGE 2: DATA AGGREGATOR (FIXED)
================================================================================
FDA-4: Comparative Financial Analysis Pipeline

Input: data/raw_json/{TICKER}_raw.json
Output: data/raw_csv/raw_financial_data.csv
================================================================================
"""

import json
import os
import pandas as pd
import numpy as np
from datetime import datetime
import logging
from glob import glob

# ==============================================================================
# CONFIGURATION
# ==============================================================================

RAW_JSON_DIR = "data/raw_json"
RAW_CSV_DIR = "data/raw_csv"
YEARS_TO_EXTRACT = [2024, 2023, 2022, 2021, 2020]

# XBRL Concepts to extract
XBRL_CONCEPTS = {
    'Assets': {'output_field': 'total_assets', 'statement': 'Balance Sheet'},
    'AssetsCurrent': {'output_field': 'current_assets', 'statement': 'Balance Sheet'},
    'Liabilities': {'output_field': 'total_liabilities', 'statement': 'Balance Sheet'},
    'LiabilitiesCurrent': {'output_field': 'current_liabilities', 'statement': 'Balance Sheet'},
    'StockholdersEquity': {'output_field': 'stockholders_equity', 'statement': 'Balance Sheet'},
    'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest': {
        'output_field': 'stockholders_equity_alt', 'statement': 'Balance Sheet'
    },
    'InventoryNet': {'output_field': 'inventory', 'statement': 'Balance Sheet'},
    'LongTermDebt': {'output_field': 'long_term_debt', 'statement': 'Balance Sheet'},
    'LongTermDebtNoncurrent': {'output_field': 'long_term_debt_alt', 'statement': 'Balance Sheet'},
    'CashAndCashEquivalentsAtCarryingValue': {'output_field': 'cash_and_equivalents', 'statement': 'Balance Sheet'},
    'Revenues': {'output_field': 'revenue', 'statement': 'Income Statement'},
    'RevenueFromContractWithCustomerExcludingAssessedTax': {'output_field': 'revenue_alt1', 'statement': 'Income Statement'},
    'SalesRevenueNet': {'output_field': 'revenue_alt2', 'statement': 'Income Statement'},
    'CostOfRevenue': {'output_field': 'cost_of_revenue', 'statement': 'Income Statement'},
    'CostOfGoodsAndServicesSold': {'output_field': 'cogs', 'statement': 'Income Statement'},
    'GrossProfit': {'output_field': 'gross_profit', 'statement': 'Income Statement'},
    'OperatingIncomeLoss': {'output_field': 'operating_income', 'statement': 'Income Statement'},
    'NetIncomeLoss': {'output_field': 'net_income', 'statement': 'Income Statement'},
    'InterestExpense': {'output_field': 'interest_expense', 'statement': 'Income Statement'},
    'EarningsPerShareBasic': {'output_field': 'eps_basic', 'statement': 'Income Statement'},
    'EarningsPerShareDiluted': {'output_field': 'eps_diluted', 'statement': 'Income Statement'},
    'NetCashProvidedByUsedInOperatingActivities': {'output_field': 'operating_cash_flow', 'statement': 'Cash Flow'},
    'CommonStockSharesOutstanding': {'output_field': 'shares_outstanding', 'statement': 'Equity'},
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


# ==============================================================================
# HELPER FUNCTION FOR JSON SERIALIZATION (FIX)
# ==============================================================================

def convert_to_serializable(obj):
    """Convert numpy types to Python native types for JSON serialization."""
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    elif pd.isna(obj):
        return None
    return obj


def make_json_serializable(data):
    """Recursively convert all numpy types in a dict/list to native Python types."""
    if isinstance(data, dict):
        return {k: make_json_serializable(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [make_json_serializable(item) for item in data]
    else:
        return convert_to_serializable(data)


# ==============================================================================
# DATA EXTRACTION FUNCTIONS
# ==============================================================================

def load_raw_json(filepath):
    """Load raw JSON file from Stage 1."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data.get('sec_data', data)
    except Exception as e:
        logger.error(f"Error loading {filepath}: {e}")
        return None


def extract_concept_values(sec_data, concept, years, taxonomy='us-gaap'):
    """Extract values for a specific XBRL concept across multiple years."""
    results = {year: None for year in years}
    
    try:
        facts = sec_data.get('facts', {})
        if taxonomy not in facts:
            return results
        if concept not in facts[taxonomy]:
            return results
        
        concept_data = facts[taxonomy][concept]
        units = concept_data.get('units', {})
        
        if 'USD' in units:
            data_points = units['USD']
        elif 'pure' in units:
            data_points = units['pure']
        elif 'shares' in units:
            data_points = units['shares']
        elif units:
            first_unit = list(units.keys())[0]
            data_points = units[first_unit]
        else:
            return results
        
        annual_data = [dp for dp in data_points if dp.get('form') == '10-K']
        
        for dp in annual_data:
            fy = dp.get('fy')
            if fy in years and results[fy] is None:
                results[fy] = dp.get('val')
        
        return results
    except Exception as e:
        logger.debug(f"Error extracting {concept}: {e}")
        return results


def process_company_json(filepath, years):
    """Process a single company's raw JSON file."""
    filename = os.path.basename(filepath)
    ticker = filename.replace('_raw.json', '')
    
    logger.info(f"  Processing {ticker}...")
    
    sec_data = load_raw_json(filepath)
    if not sec_data:
        return []
    
    entity_name = sec_data.get('entityName', ticker)
    cik = sec_data.get('cik', 'N/A')
    
    extracted_data = {}
    for concept, config in XBRL_CONCEPTS.items():
        output_field = config['output_field']
        values = extract_concept_values(sec_data, concept, years)
        extracted_data[output_field] = values
    
    records = []
    for year in years:
        record = {
            'ticker': ticker,
            'company_name': entity_name,
            'cik': str(cik).zfill(10),
            'fiscal_year': year,
            'extraction_timestamp': datetime.now().isoformat()
        }
        
        for field, year_values in extracted_data.items():
            record[field] = year_values.get(year)
        
        has_data = any(
            record.get(config['output_field']) is not None 
            for config in XBRL_CONCEPTS.values()
        )
        
        if has_data:
            records.append(record)
    
    return records


# ==============================================================================
# MAIN
# ==============================================================================

def main():
    print("=" * 80)
    print("STAGE 2: DATA AGGREGATOR")
    print("=" * 80)
    print(f"\n📂 Input: {RAW_JSON_DIR}")
    print(f"📁 Output: {RAW_CSV_DIR}")
    print(f"📅 Years: {YEARS_TO_EXTRACT}")
    
    json_files = glob(os.path.join(RAW_JSON_DIR, "*_raw.json"))
    
    if not json_files:
        logger.error(f"No raw JSON files found. Run Stage 1 first!")
        return None
    
    print(f"\n📄 Found {len(json_files)} raw JSON files\n")
    
    all_records = []
    processing_log = []
    
    for filepath in sorted(json_files):
        filename = os.path.basename(filepath)
        if filename.startswith('_'):
            continue
        
        records = process_company_json(filepath, YEARS_TO_EXTRACT)
        
        if records:
            all_records.extend(records)
            processing_log.append({
                'file': filename,
                'records_extracted': len(records),
                'status': 'success'
            })
            logger.info(f"    ✅ Extracted {len(records)} records")
        else:
            processing_log.append({
                'file': filename,
                'records_extracted': 0,
                'status': 'no_data'
            })
            logger.warning(f"    ⚠️ No data extracted")
    
    # Create DataFrame
    df = pd.DataFrame(all_records)
    df = df.sort_values(['ticker', 'fiscal_year'], ascending=[True, False])
    
    os.makedirs(RAW_CSV_DIR, exist_ok=True)
    
    # Save CSV
    output_file = os.path.join(RAW_CSV_DIR, 'raw_financial_data.csv')
    df.to_csv(output_file, index=False)
    
    # Save log (with fix for numpy types)
    log_file = os.path.join(RAW_CSV_DIR, '_aggregation_log.json')
    log_data = make_json_serializable({
        'timestamp': datetime.now().isoformat(),
        'input_files': len(json_files),
        'total_records': len(all_records),
        'unique_companies': int(df['ticker'].nunique()) if len(df) > 0 else 0,
        'years_covered': [int(y) for y in df['fiscal_year'].unique()] if len(df) > 0 else [],
        'columns': list(df.columns),
        'processing_log': processing_log
    })
    
    with open(log_file, 'w') as f:
        json.dump(log_data, f, indent=2)
    
    # Print summary
    print("\n" + "=" * 80)
    print("✅ STAGE 2 COMPLETE")
    print("=" * 80)
    print(f"\n📊 Total records: {len(df)}")
    print(f"🏢 Companies: {df['ticker'].nunique() if len(df) > 0 else 0}")
    print(f"📅 Years: {sorted(df['fiscal_year'].unique().tolist(), reverse=True) if len(df) > 0 else []}")
    print(f"\n📁 Output: {output_file}")
    
    print(f"\n📋 Preview:")
    print("-" * 80)
    preview_cols = ['ticker', 'fiscal_year', 'total_assets', 'revenue', 'net_income']
    preview_cols = [c for c in preview_cols if c in df.columns]
    print(df[preview_cols].head(10).to_string(index=False))
    
    print("\n➡️  Next: Run Stage 3 (03_process_and_enhance.py)")
    
    return df


if __name__ == "__main__":
    df = main()
