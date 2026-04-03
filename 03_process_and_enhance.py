#!/usr/bin/env python3
"""
================================================================================
STAGE 3: DATA PROCESSOR & ENHANCER
================================================================================
FDA-4: Comparative Financial Analysis Pipeline
IIIT Allahabad - Big Data Analytics Course

This script performs:
1. Data Exploration & Quality Analysis
2. Missing Value Handling
3. Data Cleaning & Validation
4. Feature Engineering (Financial Ratio Calculations)
5. Final Dataset Creation

Input: data/raw_csv/raw_financial_data.csv
Output: data/processed/final_peer_comparison.csv

Usage:
    python 03_process_and_enhance.py
================================================================================
"""

import pandas as pd
import numpy as np
import json
import os
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# ==============================================================================
# CONFIGURATION
# ==============================================================================

RAW_CSV_DIR = "data/raw_csv"
PROCESSED_DIR = "data/processed"
INPUT_FILE = os.path.join(RAW_CSV_DIR, "raw_financial_data.csv")

# Financial ratio definitions
RATIO_FORMULAS = {
    'roe': ('net_income', 'stockholders_equity', 'Profitability'),
    'roa': ('net_income', 'total_assets', 'Profitability'),
    'gross_margin': ('gross_profit', 'revenue', 'Profitability'),
    'net_margin': ('net_income', 'revenue', 'Profitability'),
    'operating_margin': ('operating_income', 'revenue', 'Profitability'),
    'current_ratio': ('current_assets', 'current_liabilities', 'Liquidity'),
    'debt_to_equity': ('total_liabilities', 'stockholders_equity', 'Leverage'),
    'debt_to_assets': ('total_liabilities', 'total_assets', 'Leverage'),
    'asset_turnover': ('revenue', 'total_assets', 'Efficiency'),
    'equity_turnover': ('revenue', 'stockholders_equity', 'Efficiency'),
}


def safe_divide(num, denom):
    """Safe division handling None, NaN, and zero."""
    if pd.isna(num) or pd.isna(denom) or denom == 0:
        return np.nan
    return num / denom


def explore_data(df):
    """Perform data exploration and quality analysis."""
    print("\n" + "=" * 70)
    print("📊 DATA EXPLORATION & QUALITY ANALYSIS")
    print("=" * 70)
    
    report = {'basic_info': {}, 'missing': {}, 'stats': {}}
    
    # Basic info
    print(f"\n📋 Basic Information:")
    print(f"   • Total rows: {len(df)}")
    print(f"   • Total columns: {len(df.columns)}")
    print(f"   • Unique companies: {df['ticker'].nunique()}")
    print(f"   • Fiscal years: {sorted(df['fiscal_year'].unique(), reverse=True)}")
    
    report['basic_info'] = {
        'rows': len(df),
        'columns': len(df.columns),
        'companies': df['ticker'].nunique(),
        'years': list(df['fiscal_year'].unique())
    }
    
    # Missing value analysis
    print(f"\n⚠️  Missing Value Analysis:")
    missing = df.isnull().sum()
    missing_pct = (missing / len(df) * 100).round(1)
    missing_df = pd.DataFrame({'count': missing, 'percent': missing_pct})
    missing_df = missing_df[missing_df['count'] > 0].sort_values('percent', ascending=False)
    
    if len(missing_df) > 0:
        print(missing_df.head(15).to_string())
    else:
        print("   No missing values!")
    
    report['missing'] = missing_df.to_dict()
    
    # Company coverage
    print(f"\n🏢 Company Coverage:")
    coverage = df.groupby('ticker')['fiscal_year'].count()
    full_coverage = (coverage == coverage.max()).sum()
    print(f"   • Companies with full 5-year data: {full_coverage}/{df['ticker'].nunique()}")
    
    return report


def clean_data(df):
    """Clean and prepare data."""
    print("\n" + "=" * 70)
    print("🧹 DATA CLEANING")
    print("=" * 70)
    
    df_clean = df.copy()
    
    # 1. Merge alternative columns
    print("\n1️⃣ Merging alternative XBRL concepts...")
    
    # Revenue alternatives
    if 'revenue' in df_clean.columns:
        before = df_clean['revenue'].isnull().sum()
        for alt in ['revenue_alt1', 'revenue_alt2']:
            if alt in df_clean.columns:
                df_clean['revenue'] = df_clean['revenue'].fillna(df_clean[alt])
        after = df_clean['revenue'].isnull().sum()
        print(f"   ✓ Revenue: filled {before - after} missing values")
    
    # Stockholders equity alternative
    if 'stockholders_equity' in df_clean.columns and 'stockholders_equity_alt' in df_clean.columns:
        before = df_clean['stockholders_equity'].isnull().sum()
        df_clean['stockholders_equity'] = df_clean['stockholders_equity'].fillna(
            df_clean['stockholders_equity_alt']
        )
        after = df_clean['stockholders_equity'].isnull().sum()
        print(f"   ✓ Stockholders Equity: filled {before - after} missing values")
    
    # Long-term debt alternative
    if 'long_term_debt' in df_clean.columns and 'long_term_debt_alt' in df_clean.columns:
        before = df_clean['long_term_debt'].isnull().sum()
        df_clean['long_term_debt'] = df_clean['long_term_debt'].fillna(
            df_clean['long_term_debt_alt']
        )
        after = df_clean['long_term_debt'].isnull().sum()
        print(f"   ✓ Long-term Debt: filled {before - after} missing values")
    
    # COGS alternative
    if 'cost_of_revenue' in df_clean.columns and 'cogs' in df_clean.columns:
        before = df_clean['cost_of_revenue'].isnull().sum()
        df_clean['cost_of_revenue'] = df_clean['cost_of_revenue'].fillna(df_clean['cogs'])
        after = df_clean['cost_of_revenue'].isnull().sum()
        print(f"   ✓ Cost of Revenue: filled {before - after} missing values")
    
    # 2. Derive gross_profit if missing
    print("\n2️⃣ Deriving calculated fields...")
    
    if 'gross_profit' in df_clean.columns:
        before = df_clean['gross_profit'].isnull().sum()
        mask = df_clean['gross_profit'].isnull()
        if 'revenue' in df_clean.columns and 'cost_of_revenue' in df_clean.columns:
            df_clean.loc[mask, 'gross_profit'] = (
                df_clean.loc[mask, 'revenue'] - df_clean.loc[mask, 'cost_of_revenue']
            )
        after = df_clean['gross_profit'].isnull().sum()
        print(f"   ✓ Gross Profit: derived {before - after} values")
    
    # 3. Handle inventory for tech companies (many have zero)
    if 'inventory' in df_clean.columns:
        df_clean['inventory'] = df_clean['inventory'].fillna(0)
        print(f"   ✓ Inventory: filled NaN with 0 (typical for tech)")
    
    # 4. Drop temporary alternative columns
    print("\n3️⃣ Dropping temporary columns...")
    alt_cols = [c for c in df_clean.columns if c.endswith('_alt') or c.endswith('_alt1') or c.endswith('_alt2')]
    if alt_cols:
        df_clean = df_clean.drop(columns=alt_cols)
        print(f"   ✓ Dropped {len(alt_cols)} alternative columns")
    
    if 'cogs' in df_clean.columns:
        df_clean = df_clean.drop(columns=['cogs'])
        print(f"   ✓ Dropped 'cogs' (merged into cost_of_revenue)")
    
    return df_clean


def calculate_ratios(df):
    """Calculate all financial ratios."""
    print("\n" + "=" * 70)
    print("📐 CALCULATING FINANCIAL RATIOS")
    print("=" * 70)
    
    df_ratios = df.copy()
    
    for ratio_name, (numerator, denominator, category) in RATIO_FORMULAS.items():
        if numerator in df_ratios.columns and denominator in df_ratios.columns:
            df_ratios[ratio_name] = df_ratios.apply(
                lambda row: safe_divide(row[numerator], row[denominator]),
                axis=1
            ).round(4)
            
            valid_count = df_ratios[ratio_name].notna().sum()
            print(f"   ✓ {ratio_name} ({category}): {valid_count}/{len(df_ratios)} valid values")
        else:
            print(f"   ⚠ {ratio_name}: missing required columns")
    
    # Special: Quick Ratio = (Current Assets - Inventory) / Current Liabilities
    if all(c in df_ratios.columns for c in ['current_assets', 'inventory', 'current_liabilities']):
        df_ratios['quick_ratio'] = df_ratios.apply(
            lambda row: safe_divide(
                row['current_assets'] - (row['inventory'] if pd.notna(row['inventory']) else 0),
                row['current_liabilities']
            ),
            axis=1
        ).round(4)
        valid_count = df_ratios['quick_ratio'].notna().sum()
        print(f"   ✓ quick_ratio (Liquidity): {valid_count}/{len(df_ratios)} valid values")
    
    # Special: Interest Coverage = Operating Income / Interest Expense
    if all(c in df_ratios.columns for c in ['operating_income', 'interest_expense']):
        # Only calculate where interest_expense > 0
        df_ratios['interest_coverage'] = df_ratios.apply(
            lambda row: safe_divide(row['operating_income'], row['interest_expense'])
            if pd.notna(row['interest_expense']) and row['interest_expense'] > 0 else np.nan,
            axis=1
        ).round(4)
        valid_count = df_ratios['interest_coverage'].notna().sum()
        print(f"   ✓ interest_coverage (Leverage): {valid_count}/{len(df_ratios)} valid values")
    
    return df_ratios


def generate_final_dataset(df):
    """Generate the final cleaned and enhanced dataset."""
    print("\n" + "=" * 70)
    print("📁 GENERATING FINAL DATASET")
    print("=" * 70)
    
    # Define final column order
    metadata_cols = ['ticker', 'company_name', 'cik', 'fiscal_year']
    
    absolute_cols = [
        'revenue', 'cost_of_revenue', 'gross_profit', 'operating_income', 'net_income',
        'total_assets', 'current_assets', 'total_liabilities', 'current_liabilities',
        'stockholders_equity', 'inventory', 'long_term_debt', 'cash_and_equivalents',
        'operating_cash_flow', 'eps_basic', 'shares_outstanding'
    ]
    
    ratio_cols = [
        'roe', 'roa', 'gross_margin', 'net_margin', 'operating_margin',
        'current_ratio', 'quick_ratio',
        'debt_to_equity', 'debt_to_assets', 'interest_coverage',
        'asset_turnover', 'equity_turnover'
    ]
    
    # Filter to existing columns
    final_cols = metadata_cols.copy()
    final_cols += [c for c in absolute_cols if c in df.columns]
    final_cols += [c for c in ratio_cols if c in df.columns]
    
    df_final = df[final_cols].copy()
    
    # Sort
    df_final = df_final.sort_values(['ticker', 'fiscal_year'], ascending=[True, False])
    
    print(f"\n📊 Final Dataset Summary:")
    print(f"   • Rows: {len(df_final)}")
    print(f"   • Columns: {len(df_final.columns)}")
    print(f"   • Companies: {df_final['ticker'].nunique()}")
    print(f"   • Years: {sorted(df_final['fiscal_year'].unique(), reverse=True)}")
    
    # Calculate completeness
    ratio_completeness = df_final[ratio_cols].notna().mean() * 100
    print(f"\n📈 Ratio Completeness:")
    for col in ratio_cols:
        if col in df_final.columns:
            pct = df_final[col].notna().mean() * 100
            print(f"   • {col}: {pct:.1f}%")
    
    return df_final


def main():
    """Main pipeline execution."""
    print("=" * 70)
    print("STAGE 3: DATA PROCESSOR & ENHANCER")
    print("FDA-4: Comparative Financial Analysis Pipeline")
    print("=" * 70)
    print(f"\n📂 Input: {INPUT_FILE}")
    print(f"📁 Output: {PROCESSED_DIR}/")
    
    # Check input file
    if not os.path.exists(INPUT_FILE):
        print(f"\n❌ Error: Input file not found!")
        print(f"   Please run Stage 2 (02_aggregate_raw_data.py) first.")
        return None
    
    # Load raw data
    print(f"\n📥 Loading raw data...")
    df = pd.read_csv(INPUT_FILE)
    print(f"   Loaded {len(df)} rows, {len(df.columns)} columns")
    
    # Step 1: Data Exploration
    quality_report = explore_data(df)
    
    # Step 2: Data Cleaning
    df_clean = clean_data(df)
    
    # Step 3: Calculate Ratios
    df_ratios = calculate_ratios(df_clean)
    
    # Step 4: Generate Final Dataset
    df_final = generate_final_dataset(df_ratios)
    
    # Save outputs
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    
    # Main output
    output_file = os.path.join(PROCESSED_DIR, 'final_peer_comparison.csv')
    df_final.to_csv(output_file, index=False)
    
    # Latest year only (for simple analysis)
    latest_year = df_final['fiscal_year'].max()
    df_latest = df_final[df_final['fiscal_year'] == latest_year]
    latest_file = os.path.join(PROCESSED_DIR, 'peer_comparison_latest_year.csv')
    df_latest.to_csv(latest_file, index=False)
    
    # Quality report
    report_file = os.path.join(PROCESSED_DIR, 'data_quality_report.json')
    quality_report['processing_timestamp'] = datetime.now().isoformat()
    quality_report['final_shape'] = {'rows': len(df_final), 'columns': len(df_final.columns)}
    with open(report_file, 'w') as f:
        json.dump(quality_report, f, indent=2, default=str)
    
    # Print final summary
    print("\n" + "=" * 70)
    print("✅ STAGE 3 COMPLETE")
    print("=" * 70)
    print(f"\n📁 Output Files:")
    print(f"   • {output_file} ({len(df_final)} rows)")
    print(f"   • {latest_file} ({len(df_latest)} rows)")
    print(f"   • {report_file}")
    
    print(f"\n📋 Final Dataset Preview:")
    print("-" * 70)
    preview_cols = ['ticker', 'fiscal_year', 'revenue', 'roe', 'roa', 'current_ratio', 'debt_to_equity']
    preview_cols = [c for c in preview_cols if c in df_final.columns]
    print(df_final[preview_cols].head(10).to_string(index=False))
    
    print("\n" + "=" * 70)
    print("🎉 PIPELINE COMPLETE!")
    print("=" * 70)
    
    return df_final


if __name__ == "__main__":
    df = main()
