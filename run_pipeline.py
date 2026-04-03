#!/usr/bin/env python3
"""
================================================================================
FDA-4: COMPLETE DATA EXTRACTION PIPELINE
================================================================================
IIIT Allahabad - Big Data Analytics Course

This is the main pipeline runner that executes all three stages:
  Stage 1: Fetch raw data from SEC EDGAR API → JSON files
  Stage 2: Extract JSON to individual company CSVs
  Stage 3: Combine, clean & calculate ratios → final CSV

Usage:
    python run_pipeline.py           # Run all stages
    python run_pipeline.py --stage 1 # Run only Stage 1 (fetch data)
    python run_pipeline.py --stage 2 # Run only Stage 2 (JSON to CSV)
    python run_pipeline.py --stage 3 # Run only Stage 3 (combine & process)
    python run_pipeline.py --skip-fetch  # Skip Stage 1, run 2 and 3

Output Directory Structure:
    data/
    ├── raw_json/                    # Stage 1: Raw API responses
    │   ├── AAPL_raw.json
    │   └── ...
    company_csv/                     # Stage 2: Individual company CSVs
    │   ├── AAPL.csv
    │   └── ...
    data/
    ├── raw_csv/                     # Stage 3: Combined raw data
    │   └── raw_combined_data.csv
    └── processed/                   # Stage 3: Final processed data
        ├── final_peer_comparison.csv
        ├── peer_comparison_latest_year.csv
        └── peer_comparison_5year.csv
================================================================================
"""

import subprocess
import sys
import os
import argparse
from datetime import datetime


def run_stage(stage_num, script_name, description):
    """Run a pipeline stage."""
    print(f"\n{'=' * 70}")
    print(f"🚀 STAGE {stage_num}: {description}")
    print(f"{'=' * 70}")
    
    script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), script_name)
    
    if not os.path.exists(script_path):
        print(f"❌ Error: Script not found: {script_path}")
        return False
    
    try:
        result = subprocess.run(
            [sys.executable, script_path],
            check=True,
            cwd=os.path.dirname(os.path.abspath(__file__))
        )
        return result.returncode == 0
    except subprocess.CalledProcessError as e:
        print(f"❌ Stage {stage_num} failed with error code {e.returncode}")
        return False
    except Exception as e:
        print(f"❌ Error running Stage {stage_num}: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description='FDA-4 Data Extraction Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python run_pipeline.py             # Run all stages
    python run_pipeline.py --stage 1   # Fetch raw data only
    python run_pipeline.py --stage 2   # Extract JSON to CSV only
    python run_pipeline.py --stage 3   # Combine & process only
    python run_pipeline.py --skip-fetch # Skip fetch, run stages 2 & 3
        """
    )
    
    parser.add_argument(
        '--stage', '-s',
        type=int,
        choices=[1, 2, 3],
        help='Run only specific stage (1, 2, or 3)'
    )
    
    parser.add_argument(
        '--skip-fetch',
        action='store_true',
        help='Skip Stage 1 (data fetch), useful if you already have JSON files'
    )
    
    args = parser.parse_args()
    
    # Header
    print("=" * 70)
    print("🏛️  FDA-4: COMPARATIVE FINANCIAL ANALYSIS PIPELINE")
    print("   IIIT Allahabad - Big Data Analytics Course")
    print("=" * 70)
    print(f"\n📅 Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📂 Working directory: {os.getcwd()}")
    
    # Pipeline stages
    stages = [
        (1, "01_fetch_raw_data.py", "Fetch raw data from SEC EDGAR API"),
        (2, "02_json_to_company_csv.py", "Extract JSON to individual company CSVs"),
        (3, "03_combine_and_process.py", "Combine, clean & calculate ratios"),
    ]
    
    # Determine which stages to run
    if args.stage:
        stages_to_run = [s for s in stages if s[0] == args.stage]
    elif args.skip_fetch:
        stages_to_run = [s for s in stages if s[0] >= 2]
    else:
        stages_to_run = stages
    
    print(f"\n📋 Stages to execute:")
    for num, script, desc in stages_to_run:
        print(f"   Stage {num}: {desc}")
    
    # Execute stages
    start_time = datetime.now()
    results = []
    
    for stage_num, script_name, description in stages_to_run:
        success = run_stage(stage_num, script_name, description)
        results.append((stage_num, success))
        
        if not success:
            print(f"\n❌ Pipeline stopped at Stage {stage_num}")
            break
    
    # Summary
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print("\n" + "=" * 70)
    print("📊 PIPELINE EXECUTION SUMMARY")
    print("=" * 70)
    
    for stage_num, success in results:
        status = "✅ Success" if success else "❌ Failed"
        print(f"   Stage {stage_num}: {status}")
    
    print(f"\n⏱️  Total duration: {duration:.1f} seconds")
    print(f"📅 Completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    all_success = all(success for _, success in results)
    
    if all_success:
        print("\n" + "=" * 70)
        print("🎉 PIPELINE COMPLETED SUCCESSFULLY!")
        print("=" * 70)
        print("\n📁 Output files:")
        print("   • data/raw_json/*.json          (Raw API responses)")
        print("   • company_csv/*.csv             (Individual company data)")
        print("   • data/raw_csv/*.csv            (Combined raw data)")
        print("   • data/processed/*.csv          (Final processed data)")
        print("\n📊 Main deliverables:")
        print("   → data/processed/final_peer_comparison.csv (all 6 years)")
        print("   → data/processed/peer_comparison_5year.csv (2020-2024)")
        print("   → data/processed/peer_comparison_latest_year.csv")
    else:
        print("\n❌ Pipeline completed with errors. Check output above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
