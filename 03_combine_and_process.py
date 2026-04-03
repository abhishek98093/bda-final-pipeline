#!/usr/bin/env python3
"""
================================================================================
STAGE 3: COMBINE, CLEAN & CALCULATE RATIOS (STANDARDIZED UNITS)
================================================================================
FDA-4: Comparative Financial Analysis Pipeline

Units:
- Billions (USD): revenue, assets, liabilities, equity, etc.
- Millions (USD): interest_expense
- Per Share (USD): eps_basic, eps_diluted  
- Decimal: all ratios (0.250000 = 25%)

All values rounded to 6 decimal places.
Null values are written as "NA" in CSV output.
================================================================================
"""

import pandas as pd
import numpy as np
import json
import os
from datetime import datetime
from glob import glob
import warnings
warnings.filterwarnings('ignore')

# ==============================================================================
# CONFIGURATION
# ==============================================================================

COMPANY_CSV_DIR = "company_csv"
RAW_CSV_DIR = "data/raw_csv"
PROCESSED_DIR = "data/processed"

DECIMAL_PLACES = 6

# Fields and their target units
BILLION_FIELDS = [
    'revenue', 'net_income', 'gross_profit', 'operating_income', 'cost_of_revenue',
    'total_assets', 'total_liabilities', 'stockholders_equity',
    'current_assets', 'current_liabilities', 'cash_and_equivalents',
    'short_term_investments', 'accounts_receivable', 'long_term_debt', 'inventory'
]

MILLION_FIELDS = ['interest_expense']

PER_SHARE_FIELDS = ['eps_basic', 'eps_diluted']

RATIO_FIELDS = [
    'roe', 'roa', 'gross_margin', 'net_margin', 'operating_margin',
    'current_ratio', 'quick_ratio', 'debt_to_equity', 'debt_to_assets',
    'interest_coverage', 'asset_turnover', 'equity_turnover'
]

DIRECT_FEATURES = BILLION_FIELDS + MILLION_FIELDS + PER_SHARE_FIELDS


# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

def make_serializable(obj):
    """Convert numpy types to native Python for JSON."""
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, dict):
        return {k: make_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [make_serializable(i) for i in obj]
    elif pd.isna(obj):
        return None
    return obj


def safe_divide(numerator, denominator):
    """Safe division - returns NaN for invalid operations."""
    if pd.isna(numerator) or pd.isna(denominator) or denominator == 0:
        return np.nan
    return numerator / denominator


def replace_nan_with_na(df):
    """Replace NaN/None values with 'NA' string for CSV output."""
    df = df.copy()
    # Replace NaN with 'NA' for all columns
    df = df.replace({np.nan: 'NA', None: 'NA'})
    return df


# ==============================================================================
# STEP 1: COMBINE CSVs
# ==============================================================================

def combine_company_csvs():
    print("\n" + "=" * 70)
    print("📦 STEP 1: COMBINING COMPANY CSVs")
    print("=" * 70)
    
    csv_files = sorted(glob(os.path.join(COMPANY_CSV_DIR, "*.csv")))
    csv_files = [f for f in csv_files if not os.path.basename(f).startswith('_')]
    
    if not csv_files:
        print(f"❌ No CSV files found in {COMPANY_CSV_DIR}")
        return None
    
    print(f"\n📄 Found {len(csv_files)} company CSV files")
    
    all_dfs = []
    for csv_file in csv_files:
        ticker = os.path.basename(csv_file).replace('.csv', '')
        try:
            df = pd.read_csv(csv_file)
            all_dfs.append(df)
            print(f"   ✓ {ticker}: {len(df)} rows")
        except Exception as e:
            print(f"   ✗ {ticker}: Error - {e}")
    
    df_combined = pd.concat(all_dfs, ignore_index=True)
    df_combined = df_combined.sort_values(['ticker', 'fiscal_year'], ascending=[True, False])
    
    os.makedirs(RAW_CSV_DIR, exist_ok=True)
    df_combined.to_csv(os.path.join(RAW_CSV_DIR, 'raw_combined_data.csv'), index=False, na_rep='NA')
    
    print(f"\n📊 Combined: {len(df_combined)} rows, {df_combined['ticker'].nunique()} companies")
    
    return df_combined


# ==============================================================================
# STEP 2: DATA QUALITY ANALYSIS
# ==============================================================================

def explore_data_quality(df):
    print("\n" + "=" * 70)
    print("🔍 STEP 2: DATA QUALITY ANALYSIS")
    print("=" * 70)
    
    quality_report = {
        'basic_info': {
            'rows': int(len(df)),
            'companies': int(df['ticker'].nunique()),
            'years': [int(y) for y in sorted(df['fiscal_year'].unique(), reverse=True)]
        },
        'missing_values': []
    }
    
    print(f"\n📋 Dataset: {len(df)} rows, {df['ticker'].nunique()} companies")
    print(f"   Years: {quality_report['basic_info']['years']}")
    
    print(f"\n⚠️  Data Availability:")
    for col in DIRECT_FEATURES:
        if col in df.columns:
            avail_pct = (1 - df[col].isnull().mean()) * 100
            quality_report['missing_values'].append({
                'feature': col, 
                'available_pct': round(avail_pct, 1)
            })
            status = "✓" if avail_pct >= 80 else "⚠" if avail_pct >= 50 else "✗"
            bar = "█" * int(avail_pct // 5) + "░" * (20 - int(avail_pct // 5))
            print(f"   {status} {col:25s} {bar} {avail_pct:5.1f}%")
    
    return quality_report


# ==============================================================================
# STEP 3: DATA CLEANING & MISSING VALUE HANDLING
# ==============================================================================

def clean_data(df):
    print("\n" + "=" * 70)
    print("🧹 STEP 3: DATA CLEANING & MISSING VALUE HANDLING")
    print("=" * 70)
    
    df_clean = df.copy()
    cleaning_log = []
    
    # 1. Derive revenue if missing: revenue = gross_profit + cost_of_revenue
    if 'revenue' in df_clean.columns:
        before = int(df_clean['revenue'].isnull().sum())
        mask = df_clean['revenue'].isnull()
        if 'gross_profit' in df_clean.columns and 'cost_of_revenue' in df_clean.columns:
            can_derive = mask & df_clean['gross_profit'].notna() & df_clean['cost_of_revenue'].notna()
            df_clean.loc[can_derive, 'revenue'] = (
                df_clean.loc[can_derive, 'gross_profit'] + 
                df_clean.loc[can_derive, 'cost_of_revenue']
            )
        after = int(df_clean['revenue'].isnull().sum())
        if before - after > 0:
            print(f"   ✓ revenue: derived {before - after} values (gross_profit + cost_of_revenue)")
            cleaning_log.append(f"revenue: derived {before - after}")
    
    # 2. Derive gross_profit if missing: gross_profit = revenue - cost_of_revenue
    if 'gross_profit' in df_clean.columns:
        before = int(df_clean['gross_profit'].isnull().sum())
        mask = df_clean['gross_profit'].isnull()
        if 'revenue' in df_clean.columns and 'cost_of_revenue' in df_clean.columns:
            can_derive = mask & df_clean['revenue'].notna() & df_clean['cost_of_revenue'].notna()
            df_clean.loc[can_derive, 'gross_profit'] = (
                df_clean.loc[can_derive, 'revenue'] - 
                df_clean.loc[can_derive, 'cost_of_revenue']
            )
        after = int(df_clean['gross_profit'].isnull().sum())
        if before - after > 0:
            print(f"   ✓ gross_profit: derived {before - after} values (revenue - cost_of_revenue)")
            cleaning_log.append(f"gross_profit: derived {before - after}")
    
    # 3. Derive total_liabilities if missing: total_liabilities = total_assets - stockholders_equity
    if 'total_liabilities' in df_clean.columns:
        before = int(df_clean['total_liabilities'].isnull().sum())
        mask = df_clean['total_liabilities'].isnull()
        if 'total_assets' in df_clean.columns and 'stockholders_equity' in df_clean.columns:
            can_derive = mask & df_clean['total_assets'].notna() & df_clean['stockholders_equity'].notna()
            df_clean.loc[can_derive, 'total_liabilities'] = (
                df_clean.loc[can_derive, 'total_assets'] - 
                df_clean.loc[can_derive, 'stockholders_equity']
            )
        after = int(df_clean['total_liabilities'].isnull().sum())
        if before - after > 0:
            print(f"   ✓ total_liabilities: derived {before - after} values (total_assets - stockholders_equity)")
            cleaning_log.append(f"total_liabilities: derived {before - after}")
    
    # 4. Fill inventory with 0 (many tech companies have no inventory)
    if 'inventory' in df_clean.columns:
        null_count = int(df_clean['inventory'].isnull().sum())
        df_clean['inventory'] = df_clean['inventory'].fillna(0)
        if null_count > 0:
            print(f"   ✓ inventory: filled {null_count} missing with 0 (tech/SaaS typical)")
            cleaning_log.append(f"inventory: filled {null_count} with 0")
    
    # 5. Fill long_term_debt with 0 if missing (some companies are debt-free)
    if 'long_term_debt' in df_clean.columns:
        null_count = int(df_clean['long_term_debt'].isnull().sum())
        df_clean['long_term_debt'] = df_clean['long_term_debt'].fillna(0)
        if null_count > 0:
            print(f"   ✓ long_term_debt: filled {null_count} missing with 0")
            cleaning_log.append(f"long_term_debt: filled {null_count} with 0")
    
    # 6. Fill short_term_investments with 0 if missing
    if 'short_term_investments' in df_clean.columns:
        null_count = int(df_clean['short_term_investments'].isnull().sum())
        df_clean['short_term_investments'] = df_clean['short_term_investments'].fillna(0)
        if null_count > 0:
            print(f"   ✓ short_term_investments: filled {null_count} missing with 0")
            cleaning_log.append(f"short_term_investments: filled {null_count} with 0")
    
    # 7. Fill interest_expense with 0 if missing (some companies have no debt)
    if 'interest_expense' in df_clean.columns:
        null_count = int(df_clean['interest_expense'].isnull().sum())
        df_clean['interest_expense'] = df_clean['interest_expense'].fillna(0)
        if null_count > 0:
            print(f"   ✓ interest_expense: filled {null_count} missing with 0")
            cleaning_log.append(f"interest_expense: filled {null_count} with 0")
    
    # 8. Fill current_assets if missing and can derive from current_liabilities + working capital
    # (This is optional and company-specific, so we skip complex derivation)
    
    print(f"\n   📊 After cleaning: {len(df_clean)} rows")
    
    return df_clean, cleaning_log


# ==============================================================================
# STEP 4: CALCULATE FINANCIAL RATIOS (using RAW values before unit conversion)
# ==============================================================================

def calculate_ratios(df):
    print("\n" + "=" * 70)
    print("📐 STEP 4: CALCULATING FINANCIAL RATIOS (from raw values)")
    print("=" * 70)
    
    df_ratios = df.copy()
    
    # Profitability Ratios
    print("\n   Profitability Ratios:")
    
    # ROE = Net Income / Stockholders Equity
    if all(c in df_ratios.columns for c in ['net_income', 'stockholders_equity']):
        df_ratios['roe'] = df_ratios.apply(
            lambda r: safe_divide(r['net_income'], r['stockholders_equity']), axis=1
        )
        valid = df_ratios['roe'].notna().sum()
        print(f"   ✓ roe = net_income / stockholders_equity ({valid}/{len(df_ratios)})")
    
    # ROA = Net Income / Total Assets
    if all(c in df_ratios.columns for c in ['net_income', 'total_assets']):
        df_ratios['roa'] = df_ratios.apply(
            lambda r: safe_divide(r['net_income'], r['total_assets']), axis=1
        )
        valid = df_ratios['roa'].notna().sum()
        print(f"   ✓ roa = net_income / total_assets ({valid}/{len(df_ratios)})")
    
    # Gross Margin = Gross Profit / Revenue
    if all(c in df_ratios.columns for c in ['gross_profit', 'revenue']):
        df_ratios['gross_margin'] = df_ratios.apply(
            lambda r: safe_divide(r['gross_profit'], r['revenue']), axis=1
        )
        valid = df_ratios['gross_margin'].notna().sum()
        print(f"   ✓ gross_margin = gross_profit / revenue ({valid}/{len(df_ratios)})")
    
    # Net Margin = Net Income / Revenue
    if all(c in df_ratios.columns for c in ['net_income', 'revenue']):
        df_ratios['net_margin'] = df_ratios.apply(
            lambda r: safe_divide(r['net_income'], r['revenue']), axis=1
        )
        valid = df_ratios['net_margin'].notna().sum()
        print(f"   ✓ net_margin = net_income / revenue ({valid}/{len(df_ratios)})")
    
    # Operating Margin = Operating Income / Revenue
    if all(c in df_ratios.columns for c in ['operating_income', 'revenue']):
        df_ratios['operating_margin'] = df_ratios.apply(
            lambda r: safe_divide(r['operating_income'], r['revenue']), axis=1
        )
        valid = df_ratios['operating_margin'].notna().sum()
        print(f"   ✓ operating_margin = operating_income / revenue ({valid}/{len(df_ratios)})")
    
    # Liquidity Ratios
    print("\n   Liquidity Ratios:")
    
    # Current Ratio = Current Assets / Current Liabilities
    if all(c in df_ratios.columns for c in ['current_assets', 'current_liabilities']):
        df_ratios['current_ratio'] = df_ratios.apply(
            lambda r: safe_divide(r['current_assets'], r['current_liabilities']), axis=1
        )
        valid = df_ratios['current_ratio'].notna().sum()
        print(f"   ✓ current_ratio = current_assets / current_liabilities ({valid}/{len(df_ratios)})")
    
    # Quick Ratio = (Current Assets - Inventory) / Current Liabilities
    if all(c in df_ratios.columns for c in ['current_assets', 'inventory', 'current_liabilities']):
        df_ratios['quick_ratio'] = df_ratios.apply(
            lambda r: safe_divide(
                r['current_assets'] - (r['inventory'] if pd.notna(r['inventory']) else 0),
                r['current_liabilities']
            ), axis=1
        )
        valid = df_ratios['quick_ratio'].notna().sum()
        print(f"   ✓ quick_ratio = (current_assets - inventory) / current_liabilities ({valid}/{len(df_ratios)})")
    
    # Leverage Ratios
    print("\n   Leverage Ratios:")
    
    # Debt to Equity = Total Liabilities / Stockholders Equity
    if all(c in df_ratios.columns for c in ['total_liabilities', 'stockholders_equity']):
        df_ratios['debt_to_equity'] = df_ratios.apply(
            lambda r: safe_divide(r['total_liabilities'], r['stockholders_equity']), axis=1
        )
        valid = df_ratios['debt_to_equity'].notna().sum()
        print(f"   ✓ debt_to_equity = total_liabilities / stockholders_equity ({valid}/{len(df_ratios)})")
    
    # Debt to Assets = Total Liabilities / Total Assets
    if all(c in df_ratios.columns for c in ['total_liabilities', 'total_assets']):
        df_ratios['debt_to_assets'] = df_ratios.apply(
            lambda r: safe_divide(r['total_liabilities'], r['total_assets']), axis=1
        )
        valid = df_ratios['debt_to_assets'].notna().sum()
        print(f"   ✓ debt_to_assets = total_liabilities / total_assets ({valid}/{len(df_ratios)})")
    
    # Interest Coverage = Operating Income / |Interest Expense|
    if all(c in df_ratios.columns for c in ['operating_income', 'interest_expense']):
        df_ratios['interest_coverage'] = df_ratios.apply(
            lambda r: safe_divide(r['operating_income'], abs(r['interest_expense']))
            if pd.notna(r['interest_expense']) and r['interest_expense'] != 0 else np.nan,
            axis=1
        )
        valid = df_ratios['interest_coverage'].notna().sum()
        print(f"   ✓ interest_coverage = operating_income / |interest_expense| ({valid}/{len(df_ratios)})")
    
    # Efficiency Ratios
    print("\n   Efficiency Ratios:")
    
    # Asset Turnover = Revenue / Total Assets
    if all(c in df_ratios.columns for c in ['revenue', 'total_assets']):
        df_ratios['asset_turnover'] = df_ratios.apply(
            lambda r: safe_divide(r['revenue'], r['total_assets']), axis=1
        )
        valid = df_ratios['asset_turnover'].notna().sum()
        print(f"   ✓ asset_turnover = revenue / total_assets ({valid}/{len(df_ratios)})")
    
    # Equity Turnover = Revenue / Stockholders Equity
    if all(c in df_ratios.columns for c in ['revenue', 'stockholders_equity']):
        df_ratios['equity_turnover'] = df_ratios.apply(
            lambda r: safe_divide(r['revenue'], r['stockholders_equity']), axis=1
        )
        valid = df_ratios['equity_turnover'].notna().sum()
        print(f"   ✓ equity_turnover = revenue / stockholders_equity ({valid}/{len(df_ratios)})")
    
    return df_ratios


# ==============================================================================
# STEP 5: CONVERT UNITS & STANDARDIZE
# ==============================================================================

def standardize_units(df):
    print("\n" + "=" * 70)
    print("📏 STEP 5: CONVERTING UNITS & STANDARDIZING")
    print("=" * 70)
    
    df_std = df.copy()
    
    # Convert to Billions (divide by 1,000,000,000)
    print("\n   Converting to Billions (÷ 1e9):")
    for col in BILLION_FIELDS:
        if col in df_std.columns:
            new_col = f"{col}_billions"
            # Round to 6 decimal places, keep NaN as NaN
            df_std[new_col] = df_std[col].apply(
                lambda x: round(x / 1e9, DECIMAL_PLACES) if pd.notna(x) else np.nan
            )
            df_std = df_std.drop(columns=[col])
            print(f"   ✓ {col} → {new_col}")
    
    # Convert to Millions (divide by 1,000,000)
    print("\n   Converting to Millions (÷ 1e6):")
    for col in MILLION_FIELDS:
        if col in df_std.columns:
            new_col = f"{col}_millions"
            df_std[new_col] = df_std[col].apply(
                lambda x: round(x / 1e6, DECIMAL_PLACES) if pd.notna(x) else np.nan
            )
            df_std = df_std.drop(columns=[col])
            print(f"   ✓ {col} → {new_col}")
    
    # Per Share (keep as USD, just rename)
    print("\n   Per Share values (USD):")
    for col in PER_SHARE_FIELDS:
        if col in df_std.columns:
            new_col = f"{col}_usd"
            df_std[new_col] = df_std[col].apply(
                lambda x: round(x, DECIMAL_PLACES) if pd.notna(x) else np.nan
            )
            df_std = df_std.drop(columns=[col])
            print(f"   ✓ {col} → {new_col}")
    
    # Round all ratios to 6 decimal places
    print("\n   Ratios (decimal, 6 places):")
    for col in RATIO_FIELDS:
        if col in df_std.columns:
            df_std[col] = df_std[col].apply(
                lambda x: round(x, DECIMAL_PLACES) if pd.notna(x) else np.nan
            )
            print(f"   ✓ {col}")
    
    return df_std


# ==============================================================================
# STEP 6: GENERATE FINAL DATASET
# ==============================================================================

def generate_final_dataset(df):
    print("\n" + "=" * 70)
    print("📁 STEP 6: GENERATING FINAL DATASET")
    print("=" * 70)
    
    # Column ordering
    metadata_cols = ['ticker', 'company_name', 'fiscal_year']
    
    income_cols = [
        'revenue_billions', 'cost_of_revenue_billions', 'gross_profit_billions',
        'operating_income_billions', 'net_income_billions',
        'interest_expense_millions', 'eps_basic_usd', 'eps_diluted_usd'
    ]
    
    balance_cols = [
        'total_assets_billions', 'current_assets_billions',
        'total_liabilities_billions', 'current_liabilities_billions',
        'stockholders_equity_billions', 'cash_and_equivalents_billions',
        'short_term_investments_billions', 'accounts_receivable_billions',
        'long_term_debt_billions', 'inventory_billions'
    ]
    
    ratio_cols = [
        'roe', 'roa', 'gross_margin', 'net_margin', 'operating_margin',
        'current_ratio', 'quick_ratio',
        'debt_to_equity', 'debt_to_assets', 'interest_coverage',
        'asset_turnover', 'equity_turnover'
    ]
    
    # Build final column list (only existing columns)
    all_cols = metadata_cols.copy()
    all_cols += [c for c in income_cols if c in df.columns]
    all_cols += [c for c in balance_cols if c in df.columns]
    all_cols += [c for c in ratio_cols if c in df.columns]
    
    df_final = df[all_cols].copy()
    df_final = df_final.sort_values(['ticker', 'fiscal_year'], ascending=[True, False])
    df_final = df_final.reset_index(drop=True)
    
    # Replace NaN with 'NA' for CSV output
    df_final = replace_nan_with_na(df_final)
    
    print(f"\n📊 Final Dataset Summary:")
    print(f"   • Rows: {len(df_final)}")
    print(f"   • Columns: {len(df_final.columns)}")
    print(f"   • Companies: {df_final['ticker'].nunique()}")
    print(f"   • Years: {sorted([int(y) for y in df_final['fiscal_year'].unique()], reverse=True)}")
    print(f"   • Null values replaced with: 'NA'")
    
    # Column count by category
    income_count = len([c for c in income_cols if c in df_final.columns])
    balance_count = len([c for c in balance_cols if c in df_final.columns])
    ratio_count = len([c for c in ratio_cols if c in df_final.columns])
    
    print(f"\n📋 Column Breakdown:")
    print(f"   • Metadata: {len(metadata_cols)}")
    print(f"   • Income Statement: {income_count}")
    print(f"   • Balance Sheet: {balance_count}")
    print(f"   • Financial Ratios: {ratio_count}")
    print(f"   • Total: {len(df_final.columns)}")
    
    return df_final


# ==============================================================================
# MAIN
# ==============================================================================

def main():
    print("=" * 70)
    print("STAGE 3: COMBINE, CLEAN, CALCULATE & STANDARDIZE UNITS")
    print("=" * 70)
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📏 Decimal places: {DECIMAL_PLACES}")
    print(f"📝 Null values will be written as 'NA' in CSV")
    
    # Step 1: Combine
    df_combined = combine_company_csvs()
    if df_combined is None:
        return None
    
    # Step 2: Quality Analysis
    quality_report = explore_data_quality(df_combined)
    
    # Step 3: Clean Data
    df_clean, cleaning_log = clean_data(df_combined)
    
    # Step 4: Calculate Ratios (using RAW values)
    df_ratios = calculate_ratios(df_clean)
    
    # Step 5: Convert Units
    df_standardized = standardize_units(df_ratios)
    
    # Step 6: Generate Final
    df_final = generate_final_dataset(df_standardized)
    
    # Save outputs
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    
    # Main output - with NA for nulls
    output_file = os.path.join(PROCESSED_DIR, 'final_peer_comparison.csv')
    df_final.to_csv(output_file, index=False, na_rep='NA')
    
    # Latest year
    # Need to handle 'NA' values when filtering
    df_final_numeric = df_final.replace('NA', np.nan)
    df_final_numeric['fiscal_year'] = pd.to_numeric(df_final_numeric['fiscal_year'], errors='coerce')
    latest_year = int(df_final_numeric['fiscal_year'].max())
    df_latest = df_final[df_final['fiscal_year'] == latest_year]
    df_latest.to_csv(os.path.join(PROCESSED_DIR, 'peer_comparison_latest_year.csv'), index=False, na_rep='NA')
    
    # 5-year data
    df_5year = df_final[df_final['fiscal_year'].isin([2024, 2023, 2022, 2021, 2020])]
    df_5year.to_csv(os.path.join(PROCESSED_DIR, 'peer_comparison_5year.csv'), index=False, na_rep='NA')
    
    # Quality report
    quality_report['processing_timestamp'] = datetime.now().isoformat()
    quality_report['cleaning_log'] = cleaning_log
    quality_report['null_representation'] = 'NA'
    quality_report['final_shape'] = {
        'rows': int(len(df_final)),
        'columns': int(len(df_final.columns)),
        'companies': int(df_final['ticker'].nunique())
    }
    quality_report['units_reference'] = {
        'billions_usd': [f"{c}_billions" for c in BILLION_FIELDS],
        'millions_usd': [f"{c}_millions" for c in MILLION_FIELDS],
        'per_share_usd': [f"{c}_usd" for c in PER_SHARE_FIELDS],
        'ratios_decimal': RATIO_FIELDS
    }
    quality_report['decimal_places'] = DECIMAL_PLACES
    
    quality_report = make_serializable(quality_report)
    
    with open(os.path.join(PROCESSED_DIR, 'data_quality_report.json'), 'w') as f:
        json.dump(quality_report, f, indent=2)
    
    # Final Summary
    print("\n" + "=" * 70)
    print("✅ STAGE 3 COMPLETE")
    print("=" * 70)
    
    print(f"\n📁 Output Files:")
    print(f"   • {output_file} ({len(df_final)} rows)")
    print(f"   • peer_comparison_latest_year.csv ({len(df_latest)} rows)")
    print(f"   • peer_comparison_5year.csv ({len(df_5year)} rows)")
    print(f"   • data_quality_report.json")
    
    print(f"\n📏 UNITS REFERENCE:")
    print(f"   ┌────────────────────────────────────────────────────────┐")
    print(f"   │ Column Suffix      │ Unit              │ Example      │")
    print(f"   ├────────────────────────────────────────────────────────┤")
    print(f"   │ *_billions         │ USD Billions      │ 383.285000   │")
    print(f"   │ *_millions         │ USD Millions      │ 150.500000   │")
    print(f"   │ *_usd              │ USD per share     │ 6.420000     │")
    print(f"   │ ratios (no suffix) │ Decimal (×100=%)  │ 0.250000=25% │")
    print(f"   └────────────────────────────────────────────────────────┘")
    
    print(f"\n📝 NULL HANDLING:")
    print(f"   • All missing/null values are written as 'NA' in CSV files")
    print(f"   • This allows clear distinction between zero and missing data")
    
    print(f"\n📋 Sample Data Preview (first 5 rows):")
    preview_cols = ['ticker', 'fiscal_year', 'revenue_billions', 'net_income_billions', 'roe', 'gross_margin']
    preview_cols = [c for c in preview_cols if c in df_final.columns]
    print(df_final[preview_cols].head(10).to_string(index=False))
    
    return df_final


if __name__ == "__main__":
    df = main()
