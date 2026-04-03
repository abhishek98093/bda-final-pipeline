#!/usr/bin/env python3
"""
================================================================================
ABNB (Airbnb Inc.) - JSON to CSV Extraction
================================================================================
FDA-4: Comparative Financial Analysis Pipeline
Specific mapping for ABNB using the priority fallback system.
================================================================================
"""

import json
import pandas as pd
import os

# ==============================================================================
# CONFIGURATION
# ==============================================================================

TICKER = "ABNB"
COMPANY_NAME = "Airbnb Inc."

INPUT_FILE = f"../data/raw_json/{TICKER}_raw.json"
OUTPUT_DIR = "../company_csv"
OUTPUT_FILE = f"{OUTPUT_DIR}/{TICKER}.csv"

YEARS_TO_EXTRACT =[2025,2024,2023,2022,2021,2020]
# ==============================================================================
# UNIVERSAL TAG MAPPING (Updated for ABNB Specifics)
# ==============================================================================

TAG_PRIORITY_MAP = {
    # Income Statement
    'revenue': ['RevenueFromContractWithCustomerExcludingAssessedTax', 'Revenues'],
    'net_income': ['NetIncomeLoss', 'NetIncomeLossAvailableToCommonStockholdersBasic'],
    'gross_profit': ['GrossProfit'], # Often derived in logic below if ✗
    'operating_income': ['OperatingIncomeLoss'],
    'cost_of_revenue': ['CostOfRevenue'],
    'interest_expense': ['InterestIncomeExpenseNonoperatingNet', 'InterestPaidNet'],
    'eps_basic': ['IncomeLossFromContinuingOperationsPerBasicShare', 'EarningsPerShareBasic'],
    'eps_diluted': ['IncomeLossFromContinuingOperationsPerDilutedShare', 'EarningsPerShareDiluted'],
    
    # Balance Sheet
    'total_assets': ['Assets'],
    'total_liabilities': ['Liabilities'],
    'stockholders_equity': ['StockholdersEquity'],
    'current_assets': ['AssetsCurrent'],
    'current_liabilities': ['LiabilitiesCurrent'],
    'cash_and_equivalents': ['CashAndCashEquivalentsAtCarryingValue'],
    'short_term_investments': ['ShortTermInvestments', 'DebtSecuritiesAvailableForSaleExcludingAccruedInterestCurrent'],
    'accounts_receivable': ['AccountsReceivableGrossCurrent', 'AccountsReceivableNetCurrent'],
    'long_term_debt': ['LongTermDebtNoncurrent', 'LongTermDebt'],
    'inventory': ['InventoryNet'] # ABNB typically has 0 or None
}

# ==============================================================================
# FUNCTIONS
# ==============================================================================

def load_json(filepath):
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
        if 'sec_data' in data: data = data['sec_data']
        return data.get('facts', {}).get('us-gaap', {})
    except Exception as e:
        print(f"❌ Error loading JSON: {e}")
        return {}

def extract_best_value(facts, tag_list, fiscal_year):
    for tag in tag_list:
        if tag not in facts: continue
        units = facts[tag].get('units', {})
        data_points = units.get('USD') or units.get('USD/shares') or units.get('pure') or []
        for dp in data_points:
            if dp.get('form') == '10-K' and dp.get('fy') == fiscal_year:
                return dp.get('val')
    return None

def safe_divide(a, b):
    if a is None or b is None or b == 0: return None
    return round(a / b, 4)

def main():
    print("=" * 60)
    print(f"🚀 {TICKER} - Full Feature Extraction")
    print("=" * 60)
    
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Error: {INPUT_FILE} not found!")
        return
    
    facts = load_json(INPUT_FILE)
    records = []

    for year in YEARS_TO_EXTRACT:
        print(f"📅 Processing Year {year}...")
        record = {'ticker': TICKER, 'company_name': COMPANY_NAME, 'fiscal_year': year}
        
        # 1. Extract raw values using the priority map
        for field, tags in TAG_PRIORITY_MAP.items():
            record[field] = extract_best_value(facts, tags, year)

        # 2. ABNB SPECIFIC FALLBACKS
        # Derive Gross Profit if missing: Revenue - CostOfRevenue
        if record['gross_profit'] is None and record['revenue'] and record['cost_of_revenue']:
            record['gross_profit'] = record['revenue'] - record['cost_of_revenue']
        
        # Derive Revenue if missing: Gross Profit + CostOfRevenue
        if record['revenue'] is None and record['gross_profit'] and record['cost_of_revenue']:
            record['revenue'] = record['gross_profit'] + record['cost_of_revenue']

        # 3. Unit Conversion & Scaling
        billions_scale = ['revenue', 'net_income', 'gross_profit', 'operating_income', 
                          'total_assets', 'total_liabilities', 'stockholders_equity', 'cost_of_revenue']
        
        for field in record:
            val = record[field]
            if val is None or field in ['ticker', 'company_name', 'fiscal_year']: continue
            
            if 'eps_' in field:
                record[field] = round(val, 2)
            elif field in billions_scale:
                record[field] = round(val / 1e9, 3) # To Billions
            else:
                record[field] = round(val / 1e6, 2) # To Millions

        if record.get('total_assets') or record.get('revenue'):
            records.append(record)
    
    df = pd.DataFrame(records)
    
    # 4. Calculate Ratios
    print(f"📐 Calculating ratios...")
    if not df.empty:
        df['roe'] = df.apply(lambda r: safe_divide(r['net_income'], r['stockholders_equity']), axis=1)
        df['roa'] = df.apply(lambda r: safe_divide(r['net_income'], r['total_assets']), axis=1)
        df['gross_margin'] = df.apply(lambda r: safe_divide(r['gross_profit'], r['revenue']), axis=1)
        df['net_margin'] = df.apply(lambda r: safe_divide(r['net_income'], r['revenue']), axis=1)
        df['operating_margin'] = df.apply(lambda r: safe_divide(r['operating_income'], r['revenue']), axis=1)
        df['current_ratio'] = df.apply(lambda r: safe_divide(r['current_assets'], r['current_liabilities']), axis=1)
        df['debt_to_equity'] = df.apply(lambda r: safe_divide(r['total_liabilities'], r['stockholders_equity']), axis=1)

    # 5. DEFINE FULL COLUMN LIST
    all_cols = [
        'ticker', 'company_name', 'fiscal_year', 
        'revenue', 'net_income', 'total_assets', 'total_liabilities', 
        'stockholders_equity', 'current_assets', 'current_liabilities', 
        'cash_and_equivalents', 'short_term_investments', 'accounts_receivable', 
        'gross_profit', 'operating_income', 'cost_of_revenue', 'interest_expense', 
        'eps_basic', 'eps_diluted', 'long_term_debt', 'inventory',
        'roe', 'roa', 'gross_margin', 'net_margin', 'operating_margin', 'current_ratio', 'debt_to_equity'
    ]
    
    for c in all_cols:
        if c not in df.columns:
            df[c] = None

    df = df[all_cols].sort_values('fiscal_year', ascending=False)
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)
    
    print("=" * 60)
    print(f"✅ SAVED ALL FEATURES FOR ABNB: {OUTPUT_FILE}")
    print(df[['fiscal_year', 'revenue', 'net_income', 'roe', 'roa']].to_string(index=False))
    print("=" * 60)

if __name__ == "__main__":
    main()
