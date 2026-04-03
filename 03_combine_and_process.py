#!/usr/bin/env python3
"""
================================================================================
STAGE 3: COMBINE, CLEAN & CALCULATE RATIOS (FIXED)
================================================================================
FDA-4: Comparative Financial Analysis Pipeline
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

DIRECT_FEATURES = [
    'revenue', 'net_income', 'gross_profit', 'operating_income',
    'cost_of_revenue', 'interest_expense', 'eps_basic', 'eps_diluted',
    'total_assets', 'total_liabilities', 'stockholders_equity',
    'current_assets', 'current_liabilities', 'cash_and_equivalents',
    'short_term_investments', 'accounts_receivable', 'long_term_debt', 'inventory',
]

DERIVED_RATIOS = {
    'roe': ('net_income', 'stockholders_equity'),
    'roa': ('net_income', 'total_assets'),
    'gross_margin': ('gross_profit', 'revenue'),
    'net_margin': ('net_income', 'revenue'),
    'operating_margin': ('operating_income', 'revenue'),
    'current_ratio': ('current_assets', 'current_liabilities'),
    'debt_to_equity': ('total_liabilities', 'stockholders_equity'),
}


# ==============================================================================
# FIX: JSON SERIALIZATION HELPER
# ==============================================================================

def make_serializable(obj):
    """Convert numpy types to native Python types for JSON."""
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
    """Safe division handling None, NaN, and zero."""
    if pd.isna(numerator) or pd.isna(denominator) or denominator == 0:
        return np.nan
    return numerator / denominator


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
    raw_output = os.path.join(RAW_CSV_DIR, 'raw_combined_data.csv')
    df_combined.to_csv(raw_output, index=False)
    
    print(f"\n📊 Combined: {len(df_combined)} rows, {df_combined['ticker'].nunique()} companies")
    print(f"📁 Saved: {raw_output}")
    
    return df_combined


# ==============================================================================
# STEP 2: DATA QUALITY
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
        }
    }
    
    print(f"\n📋 Basic Info: {len(df)} rows, {df['ticker'].nunique()} companies")
    print(f"   Years: {quality_report['basic_info']['years']}")
    
    # Missing values
    print(f"\n⚠️  Missing Value Analysis:")
    missing_stats = []
    for col in DIRECT_FEATURES:
        if col in df.columns:
            avail_pct = (1 - df[col].isnull().mean()) * 100
            missing_stats.append({'feature': col, 'available_pct': round(avail_pct, 1)})
    
    missing_stats.sort(key=lambda x: x['available_pct'])
    for stat in missing_stats:
        status = "✓" if stat['available_pct'] >= 80 else "⚠" if stat['available_pct'] >= 50 else "✗"
        bar = "█" * int(stat['available_pct'] // 5) + "░" * (20 - int(stat['available_pct'] // 5))
        print(f"   {status} {stat['feature']:25s} {bar} {stat['available_pct']:5.1f}% available")
    
    quality_report['missing_values'] = missing_stats
    
    # Coverage
    coverage = df.groupby('ticker')['fiscal_year'].count()
    quality_report['coverage'] = {
        'full_6_years': int((coverage == 6).sum()),
        'partial': int(((coverage >= 3) & (coverage < 6)).sum()),
        'low': int((coverage < 3).sum())
    }
    
    print(f"\n🏢 Company Coverage: {quality_report['coverage']}")
    
    return quality_report


# ==============================================================================
# STEP 3: CLEAN DATA
# ==============================================================================

def clean_data(df):
    print("\n" + "=" * 70)
    print("🧹 STEP 3: DATA CLEANING")
    print("=" * 70)
    
    df_clean = df.copy()
    cleaning_log = []
    
    # Derive revenue if missing
    if 'revenue' in df_clean.columns:
        before = df_clean['revenue'].isnull().sum()
        mask = df_clean['revenue'].isnull()
        if 'gross_profit' in df_clean.columns and 'cost_of_revenue' in df_clean.columns:
            df_clean.loc[mask, 'revenue'] = df_clean.loc[mask, 'gross_profit'] + df_clean.loc[mask, 'cost_of_revenue']
        after = df_clean['revenue'].isnull().sum()
        if before - after > 0:
            print(f"   ✓ Revenue: derived {before - after} values")
            cleaning_log.append(f"Revenue: derived {before - after}")
    
    # Fill inventory with 0
    if 'inventory' in df_clean.columns:
        null_count = int(df_clean['inventory'].isnull().sum())
        df_clean['inventory'] = df_clean['inventory'].fillna(0)
        if null_count > 0:
            print(f"   ✓ Inventory: filled {null_count} NaN with 0")
            cleaning_log.append(f"Inventory: filled {null_count}")
    
    print(f"\n   Rows: {len(df_clean)}, Cleaning actions: {len(cleaning_log)}")
    
    return df_clean, cleaning_log


# ==============================================================================
# STEP 4: CALCULATE RATIOS
# ==============================================================================

def calculate_ratios(df):
    print("\n" + "=" * 70)
    print("📐 STEP 4: CALCULATING FINANCIAL RATIOS")
    print("=" * 70)
    
    df_ratios = df.copy()
    
    for ratio_name, (num, denom) in DERIVED_RATIOS.items():
        if num in df_ratios.columns and denom in df_ratios.columns:
            df_ratios[ratio_name] = df_ratios.apply(
                lambda row: safe_divide(row[num], row[denom]), axis=1
            ).round(4)
            valid = df_ratios[ratio_name].notna().sum()
            print(f"   ✓ {ratio_name}: {valid}/{len(df_ratios)} valid")
    
    # Additional ratios
    if all(c in df_ratios.columns for c in ['current_assets', 'inventory', 'current_liabilities']):
        df_ratios['quick_ratio'] = df_ratios.apply(
            lambda r: safe_divide(r['current_assets'] - (r['inventory'] or 0), r['current_liabilities']), axis=1
        ).round(4)
        print(f"   ✓ quick_ratio: {df_ratios['quick_ratio'].notna().sum()}/{len(df_ratios)} valid")
    
    if all(c in df_ratios.columns for c in ['total_liabilities', 'total_assets']):
        df_ratios['debt_to_assets'] = df_ratios.apply(
            lambda r: safe_divide(r['total_liabilities'], r['total_assets']), axis=1
        ).round(4)
    
    if all(c in df_ratios.columns for c in ['operating_income', 'interest_expense']):
        df_ratios['interest_coverage'] = df_ratios.apply(
            lambda r: safe_divide(r['operating_income'], abs(r['interest_expense']))
            if pd.notna(r['interest_expense']) and r['interest_expense'] != 0 else np.nan, axis=1
        ).round(4)
    
    if all(c in df_ratios.columns for c in ['revenue', 'total_assets']):
        df_ratios['asset_turnover'] = df_ratios.apply(
            lambda r: safe_divide(r['revenue'], r['total_assets']), axis=1
        ).round(4)
    
    if all(c in df_ratios.columns for c in ['revenue', 'stockholders_equity']):
        df_ratios['equity_turnover'] = df_ratios.apply(
            lambda r: safe_divide(r['revenue'], r['stockholders_equity']), axis=1
        ).round(4)
    
    return df_ratios


# ==============================================================================
# STEP 5: GENERATE FINAL
# ==============================================================================

def generate_final_dataset(df):
    print("\n" + "=" * 70)
    print("📁 STEP 5: GENERATING FINAL DATASET")
    print("=" * 70)
    
    metadata_cols = ['ticker', 'company_name', 'fiscal_year']
    income_cols = ['revenue', 'cost_of_revenue', 'gross_profit', 'operating_income', 'net_income', 'interest_expense', 'eps_basic', 'eps_diluted']
    balance_cols = ['total_assets', 'current_assets', 'total_liabilities', 'current_liabilities', 'stockholders_equity', 'cash_and_equivalents', 'short_term_investments', 'accounts_receivable', 'long_term_debt', 'inventory']
    ratio_cols = ['roe', 'roa', 'gross_margin', 'net_margin', 'operating_margin', 'current_ratio', 'quick_ratio', 'debt_to_equity', 'debt_to_assets', 'interest_coverage', 'asset_turnover', 'equity_turnover']
    
    all_cols = metadata_cols + [c for c in income_cols if c in df.columns] + [c for c in balance_cols if c in df.columns] + [c for c in ratio_cols if c in df.columns]
    
    df_final = df[all_cols].copy()
    df_final = df_final.sort_values(['ticker', 'fiscal_year'], ascending=[True, False]).reset_index(drop=True)
    
    print(f"\n📊 Final: {len(df_final)} rows × {len(df_final.columns)} columns")
    print(f"   Companies: {df_final['ticker'].nunique()}")
    
    return df_final


# ==============================================================================
# MAIN
# ==============================================================================

def main():
    print("=" * 70)
    print("STAGE 3: COMBINE, CLEAN & CALCULATE RATIOS")
    print("=" * 70)
    
    df_combined = combine_company_csvs()
    if df_combined is None:
        return None
    
    quality_report = explore_data_quality(df_combined)
    df_clean, cleaning_log = clean_data(df_combined)
    df_ratios = calculate_ratios(df_clean)
    df_final = generate_final_dataset(df_ratios)
    
    # Save outputs
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    
    output_file = os.path.join(PROCESSED_DIR, 'final_peer_comparison.csv')
    df_final.to_csv(output_file, index=False)
    
    latest_year = int(df_final['fiscal_year'].max())
    df_latest = df_final[df_final['fiscal_year'] == latest_year]
    df_latest.to_csv(os.path.join(PROCESSED_DIR, 'peer_comparison_latest_year.csv'), index=False)
    
    df_5year = df_final[df_final['fiscal_year'].isin([2024, 2023, 2022, 2021, 2020])]
    df_5year.to_csv(os.path.join(PROCESSED_DIR, 'peer_comparison_5year.csv'), index=False)
    
    # Save report with fix for numpy types
    quality_report['processing_timestamp'] = datetime.now().isoformat()
    quality_report['cleaning_log'] = cleaning_log
    quality_report['final_shape'] = {
        'rows': int(len(df_final)),
        'columns': int(len(df_final.columns)),
        'companies': int(df_final['ticker'].nunique())
    }
    
    # FIX: Convert all numpy types before JSON dump
    quality_report = make_serializable(quality_report)
    
    with open(os.path.join(PROCESSED_DIR, 'data_quality_report.json'), 'w') as f:
        json.dump(quality_report, f, indent=2)
    
    # Summary
    print("\n" + "=" * 70)
    print("✅ STAGE 3 COMPLETE")
    print("=" * 70)
    print(f"\n📁 Output: {output_file}")
    print(f"   • final_peer_comparison.csv ({len(df_final)} rows)")
    print(f"   • peer_comparison_5year.csv ({len(df_5year)} rows)")
    print(f"   • peer_comparison_latest_year.csv ({len(df_latest)} rows)")
    
    print(f"\n📋 Preview:")
    preview = df_final[['ticker', 'fiscal_year', 'revenue', 'net_income', 'roe', 'gross_margin']].head(10)
    print(preview.to_string(index=False))
    
    return df_final


if __name__ == "__main__":
    df = main()
