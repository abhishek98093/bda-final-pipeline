#!/usr/bin/env python3
"""
================================================================================
STAGE 2: JSON TO COMPANY CSV EXTRACTOR
================================================================================
FDA-4: Comparative Financial Analysis Pipeline
IIIT Allahabad - Big Data Analytics Course

This script:
1. Reads each raw JSON file from data/raw_json/
2. Extracts features using XBRL tag mapping (first match wins)
3. Creates individual CSV for each company (6 years: 2020-2025)
4. Saves to company_csv/{TICKER}.csv

Input: data/raw_json/{TICKER}_raw.json
Output: company_csv/{TICKER}.csv (one per company)

Usage:
    python 02_json_to_company_csv.py
================================================================================
"""

import json
import os
import pandas as pd
from datetime import datetime
from glob import glob

# Import XBRL tag mapping
from xbrl_tags import FEATURE_MAP

# ==============================================================================
# CONFIGURATION
# ==============================================================================

RAW_JSON_DIR = "data/raw_json"
COMPANY_CSV_DIR = "company_csv"

# 6 Years: 2020 to 2025
YEARS_TO_EXTRACT = [2025, 2024, 2023, 2022, 2021, 2020]

# Features to extract (21 direct features)
DIRECT_FEATURES = [
    # Income Statement
    'revenue', 'net_income', 'gross_profit', 'operating_income',
    'cost_of_revenue', 'interest_expense', 'eps_basic', 'eps_diluted',
    # Balance Sheet
    'total_assets', 'total_liabilities', 'stockholders_equity',
    'current_assets', 'current_liabilities', 'cash_and_equivalents',
    'short_term_investments', 'accounts_receivable', 'long_term_debt', 'inventory',
]


# ==============================================================================
# EXTRACTION FUNCTIONS
# ==============================================================================

def load_json(filepath):
    """Load raw JSON file."""
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data.get('sec_data', data)


def extract_value(facts, tag_list, fiscal_year, taxonomy='us-gaap'):
    """
    Extract value using first matching XBRL tag.
    
    Args:
        facts: The 'facts' section from SEC JSON
        tag_list: List of possible XBRL tag names (first match wins)
        fiscal_year: Year to extract (e.g., 2024)
        taxonomy: Usually 'us-gaap'
    
    Returns:
        Value if found, None otherwise
    """
    if taxonomy not in facts:
        return None
    
    for tag_name in tag_list:
        try:
            if tag_name not in facts[taxonomy]:
                continue
            
            units = facts[taxonomy][tag_name].get('units', {})
            
            # Try different unit types in order of preference
            data_points = None
            for unit_type in ['USD', 'USD/shares', 'pure', 'shares']:
                if unit_type in units:
                    data_points = units[unit_type]
                    break
            
            # Fallback to first available unit
            if not data_points and units:
                data_points = list(units.values())[0]
            
            if not data_points:
                continue
            
            # Find value for the specific fiscal year from 10-K filing
            for dp in data_points:
                if dp.get('form') == '10-K' and dp.get('fy') == fiscal_year:
                    return dp.get('val')
        except:
            continue
    
    return None


def extract_company_data(json_filepath, years):
    """
    Extract all financial data for one company across multiple years.
    
    Args:
        json_filepath: Path to the raw JSON file
        years: List of fiscal years to extract
    
    Returns:
        DataFrame with extracted data
    """
    # Get ticker from filename
    filename = os.path.basename(json_filepath)
    ticker = filename.replace('_raw.json', '')
    
    # Load JSON
    sec_data = load_json(json_filepath)
    facts = sec_data.get('facts', {})
    
    # Get company name
    company_name = sec_data.get('entityName', ticker)
    
    records = []
    
    for year in years:
        record = {
            'ticker': ticker,
            'company_name': company_name,
            'fiscal_year': year,
        }
        
        # Extract each direct feature using FEATURE_MAP
        for feature in DIRECT_FEATURES:
            if feature in FEATURE_MAP:
                tag_list = FEATURE_MAP[feature]
                value = extract_value(facts, tag_list, year)
                record[feature] = value
        
        # =============================================
        # DERIVED VALUES - Calculate if direct tag missing
        # =============================================
        
        # Revenue = Gross Profit + Cost of Revenue (if revenue not found)
        if record.get('revenue') is None:
            if record.get('gross_profit') is not None and record.get('cost_of_revenue') is not None:
                record['revenue'] = record['gross_profit'] + record['cost_of_revenue']
        
        # Gross Profit = Revenue - Cost of Revenue (if gross_profit not found)
        if record.get('gross_profit') is None:
            if record.get('revenue') is not None and record.get('cost_of_revenue') is not None:
                record['gross_profit'] = record['revenue'] - record['cost_of_revenue']
        
        # Only add record if we have at least some core data
        has_data = (
            record.get('total_assets') is not None or 
            record.get('revenue') is not None or 
            record.get('net_income') is not None
        )
        
        if has_data:
            records.append(record)
    
    return pd.DataFrame(records)


def process_all_companies():
    """Process all JSON files and create individual company CSVs."""
    
    print("=" * 70)
    print("STAGE 2: JSON TO COMPANY CSV EXTRACTOR")
    print("=" * 70)
    print(f"\n📂 Input: {RAW_JSON_DIR}")
    print(f"📁 Output: {COMPANY_CSV_DIR}")
    print(f"📅 Years: {YEARS_TO_EXTRACT}")
    print(f"📊 Features to extract: {len(DIRECT_FEATURES)}")
    print()
    
    # Find all JSON files
    json_files = sorted(glob(os.path.join(RAW_JSON_DIR, "*_raw.json")))
    
    if not json_files:
        print(f"❌ No JSON files found in {RAW_JSON_DIR}")
        print("   Run Stage 1 first!")
        return None
    
    print(f"📄 Found {len(json_files)} JSON files\n")
    
    # Create output directory
    os.makedirs(COMPANY_CSV_DIR, exist_ok=True)
    
    # Process each company
    success_count = 0
    failed = []
    extraction_log = []
    
    for i, json_file in enumerate(json_files, 1):
        filename = os.path.basename(json_file)
        
        # Skip manifest file
        if filename.startswith('_'):
            continue
        
        ticker = filename.replace('_raw.json', '')
        
        print(f"[{i:2d}/{len(json_files)}] {ticker}")
        
        try:
            # Extract data
            df = extract_company_data(json_file, YEARS_TO_EXTRACT)
            
            if len(df) > 0:
                # Save to CSV
                output_file = os.path.join(COMPANY_CSV_DIR, f"{ticker}.csv")
                df.to_csv(output_file, index=False)
                
                # Stats
                years_found = len(df)
                revenue_found = df['revenue'].notna().sum()
                assets_found = df['total_assets'].notna().sum()
                
                print(f"       ✅ {years_found} years | Revenue: {revenue_found}/{years_found} | Assets: {assets_found}/{years_found}")
                
                success_count += 1
                extraction_log.append({
                    'ticker': ticker,
                    'status': 'SUCCESS',
                    'years': years_found,
                    'revenue_coverage': int(revenue_found),
                    'assets_coverage': int(assets_found),
                    'file': output_file
                })
            else:
                print(f"       ⚠️ No data extracted")
                failed.append(ticker)
                extraction_log.append({
                    'ticker': ticker,
                    'status': 'NO_DATA',
                    'years': 0
                })
        
        except Exception as e:
            print(f"       ❌ Error: {e}")
            failed.append(ticker)
            extraction_log.append({
                'ticker': ticker,
                'status': f'ERROR: {str(e)}',
                'years': 0
            })
    
    # Save extraction log
    log_file = os.path.join(COMPANY_CSV_DIR, '_extraction_log.json')
    with open(log_file, 'w') as f:
        json.dump({
            'timestamp': datetime.now().isoformat(),
            'total_companies': len(json_files) - 1,  # Exclude manifest
            'successful': success_count,
            'failed': len(failed),
            'years_extracted': YEARS_TO_EXTRACT,
            'features_extracted': DIRECT_FEATURES,
            'log': extraction_log
        }, f, indent=2)
    
    # Summary
    print("\n" + "=" * 70)
    print("✅ STAGE 2 COMPLETE")
    print("=" * 70)
    print(f"\n📊 Summary:")
    print(f"   • Companies processed: {success_count}/{len(json_files) - 1}")
    print(f"   • Individual CSVs created: {success_count}")
    
    if failed:
        print(f"\n❌ Failed ({len(failed)}): {', '.join(failed)}")
    
    print(f"\n📁 Output: {COMPANY_CSV_DIR}/")
    print(f"📄 Log: {log_file}")
    print("\n➡️  Next: Run Stage 3 (03_combine_and_process.py)")
    
    return extraction_log


if __name__ == "__main__":
    process_all_companies()
