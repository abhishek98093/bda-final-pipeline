#!/usr/bin/env python3
"""
================================================================================
STAGE 1: RAW DATA FETCHER
================================================================================
FDA-4: Comparative Financial Analysis Pipeline
IIIT Allahabad - Big Data Analytics Course

This script fetches RAW data from SEC EDGAR API and saves as individual JSON files.
No processing - just pure data acquisition.

API Source: https://www.sec.gov/edgar/sec-api-documentation
Endpoint: https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json

Output: data/raw_json/{TICKER}_raw.json (one file per company)

Usage:
    python 01_fetch_raw_data.py
================================================================================
"""

import requests
import json
import os
import time
from datetime import datetime
import logging

# ==============================================================================
# CONFIGURATION
# ==============================================================================

# ⚠️ IMPORTANT: Change this to YOUR name and email!
# SEC requires a valid User-Agent for API access
USER_AGENT = "Abhishek_IIITA iit2023261@iiita.ac.in"

# Output directory for raw JSON files
RAW_DATA_DIR = "data/raw_json"

# SEC EDGAR API Base URL
SEC_API_BASE = "https://data.sec.gov/api/xbrl/companyfacts"

# Rate limiting (SEC allows max 10 requests/second)
REQUEST_DELAY = 0.15  # seconds between requests

# 30 Peer Companies - Technology Sector
# Source: SEC EDGAR Company Search https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany
PEER_COMPANIES = [
    {"ticker": "AAPL", "cik": "320193", "name": "Apple Inc.", "sic": "3571"},
    {"ticker": "MSFT", "cik": "789019", "name": "Microsoft Corporation", "sic": "7372"},
    {"ticker": "GOOGL", "cik": "1652044", "name": "Alphabet Inc.", "sic": "7370"},
    {"ticker": "AMZN", "cik": "1018724", "name": "Amazon.com Inc.", "sic": "5961"},
    {"ticker": "META", "cik": "1326801", "name": "Meta Platforms Inc.", "sic": "7370"},
    {"ticker": "NVDA", "cik": "1045810", "name": "NVIDIA Corporation", "sic": "3674"},
    {"ticker": "TSLA", "cik": "1318605", "name": "Tesla Inc.", "sic": "3711"},
    {"ticker": "ADBE", "cik": "796343", "name": "Adobe Inc.", "sic": "7372"},
    {"ticker": "CRM", "cik": "1108524", "name": "Salesforce Inc.", "sic": "7372"},
    {"ticker": "NFLX", "cik": "1065280", "name": "Netflix Inc.", "sic": "7841"},
    {"ticker": "ORCL", "cik": "1341439", "name": "Oracle Corporation", "sic": "7372"},
    {"ticker": "INTC", "cik": "50863", "name": "Intel Corporation", "sic": "3674"},
    {"ticker": "AMD", "cik": "2488", "name": "Advanced Micro Devices", "sic": "3674"},
    {"ticker": "CSCO", "cik": "858877", "name": "Cisco Systems Inc.", "sic": "3576"},
    {"ticker": "IBM", "cik": "51143", "name": "IBM Corporation", "sic": "7370"},
    {"ticker": "QCOM", "cik": "804328", "name": "Qualcomm Inc.", "sic": "3674"},
    {"ticker": "NOW", "cik": "1373715", "name": "ServiceNow Inc.", "sic": "7372"},
    {"ticker": "INTU", "cik": "896878", "name": "Intuit Inc.", "sic": "7372"},
    {"ticker": "PLTR", "cik": "1321655", "name": "Palantir Technologies", "sic": "7372"},
    {"ticker": "SNOW", "cik": "1640147", "name": "Snowflake Inc.", "sic": "7372"},
    {"ticker": "CRWD", "cik": "1535527", "name": "CrowdStrike Holdings", "sic": "7372"},
    {"ticker": "PANW", "cik": "1327567", "name": "Palo Alto Networks", "sic": "7372"},
    {"ticker": "WDAY", "cik": "1327811", "name": "Workday Inc.", "sic": "7372"},
    {"ticker": "UBER", "cik": "1543151", "name": "Uber Technologies", "sic": "4121"},
    {"ticker": "ABNB", "cik": "1559720", "name": "Airbnb Inc.", "sic": "7011"},
    {"ticker": "PYPL", "cik": "1633917", "name": "PayPal Holdings", "sic": "7389"},
    {"ticker": "SQ", "cik": "1512673", "name": "Block Inc.", "sic": "7389"},
    {"ticker": "SHOP", "cik": "1594805", "name": "Shopify Inc.", "sic": "7372"},
    {"ticker": "ZM", "cik": "1585521", "name": "Zoom Video", "sic": "7372"},
    {"ticker": "TWLO", "cik": "1447669", "name": "Twilio Inc.", "sic": "7372"},
]

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


# ==============================================================================
# API FUNCTIONS
# ==============================================================================

def create_session():
    """Create a requests session with proper headers."""
    session = requests.Session()
    session.headers.update({
        'User-Agent': USER_AGENT,
        'Accept-Encoding': 'gzip, deflate',
        'Accept': 'application/json',
        'Host': 'data.sec.gov'
    })
    return session


def fetch_company_facts(session, cik):
    """
    Fetch raw company facts from SEC EDGAR API.
    
    API Documentation: https://www.sec.gov/edgar/sec-api-documentation
    
    Args:
        session: requests.Session object
        cik: Company CIK (will be zero-padded)
    
    Returns:
        dict: Raw JSON response from SEC API
    """
    # Zero-pad CIK to 10 digits (SEC requirement)
    cik_padded = str(cik).zfill(10)
    
    url = f"{SEC_API_BASE}/CIK{cik_padded}.json"
    
    logger.info(f"    Fetching: {url}")
    
    try:
        response = session.get(url, timeout=30)
        
        # Log response headers for debugging
        logger.debug(f"    Response Status: {response.status_code}")
        logger.debug(f"    Content-Length: {response.headers.get('Content-Length', 'N/A')}")
        
        response.raise_for_status()
        
        return response.json()
    
    except requests.exceptions.HTTPError as e:
        if response.status_code == 404:
            logger.warning(f"    Company not found in SEC database")
        else:
            logger.error(f"    HTTP Error {response.status_code}: {e}")
        return None
    
    except requests.exceptions.Timeout:
        logger.error(f"    Request timed out after 30 seconds")
        return None
    
    except requests.exceptions.RequestException as e:
        logger.error(f"    Request failed: {e}")
        return None
    
    except json.JSONDecodeError as e:
        logger.error(f"    Invalid JSON response: {e}")
        return None


def save_raw_json(data, ticker, output_dir):
    """
    Save raw API response as JSON file.
    
    Args:
        data: Raw JSON data from API
        ticker: Company ticker symbol
        output_dir: Directory to save file
    
    Returns:
        str: Path to saved file
    """
    os.makedirs(output_dir, exist_ok=True)
    
    filepath = os.path.join(output_dir, f"{ticker}_raw.json")
    
    # Add metadata wrapper
    output_data = {
        "_metadata": {
            "ticker": ticker,
            "fetch_timestamp": datetime.now().isoformat(),
            "source": "SEC EDGAR Company Facts API",
            "api_endpoint": f"{SEC_API_BASE}/CIK{{cik}}.json",
            "documentation": "https://www.sec.gov/edgar/sec-api-documentation"
        },
        "sec_data": data
    }
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    return filepath


# ==============================================================================
# MAIN EXECUTION
# ==============================================================================

def main():
    """Main function to fetch and save raw data for all companies."""
    
    print("=" * 80)
    print("STAGE 1: RAW DATA FETCHER")
    print("FDA-4: Comparative Financial Analysis Pipeline")
    print("=" * 80)
    print()
    print(f"📡 API Source: SEC EDGAR Company Facts API")
    print(f"🔗 Endpoint: {SEC_API_BASE}/CIK{{cik}}.json")
    print(f"📚 Documentation: https://www.sec.gov/edgar/sec-api-documentation")
    print(f"📁 Output Directory: {RAW_DATA_DIR}")
    print(f"🏢 Companies to fetch: {len(PEER_COMPANIES)}")
    print()
    
    # Create session
    session = create_session()
    
    # Track results
    successful = []
    failed = []
    
    start_time = datetime.now()
    
    for i, company in enumerate(PEER_COMPANIES, 1):
        ticker = company['ticker']
        cik = company['cik']
        name = company['name']
        
        print(f"[{i:2d}/{len(PEER_COMPANIES)}] {name} ({ticker})")
        print(f"    CIK: {cik} → Padded: {str(cik).zfill(10)}")
        
        # Fetch raw data
        raw_data = fetch_company_facts(session, cik)
        
        if raw_data:
            # Save to file
            filepath = save_raw_json(raw_data, ticker, RAW_DATA_DIR)
            file_size = os.path.getsize(filepath) / 1024  # KB
            
            logger.info(f"    ✅ Saved: {filepath} ({file_size:.1f} KB)")
            successful.append({
                'ticker': ticker,
                'cik': cik,
                'name': name,
                'file': filepath,
                'size_kb': round(file_size, 1)
            })
        else:
            logger.warning(f"    ❌ Failed to fetch data")
            failed.append({'ticker': ticker, 'cik': cik, 'name': name})
        
        # Rate limiting
        time.sleep(REQUEST_DELAY)
        print()
    
    # Save fetch manifest
    manifest = {
        "fetch_timestamp": datetime.now().isoformat(),
        "total_companies": len(PEER_COMPANIES),
        "successful": len(successful),
        "failed": len(failed),
        "duration_seconds": (datetime.now() - start_time).total_seconds(),
        "successful_companies": successful,
        "failed_companies": failed
    }
    
    manifest_path = os.path.join(RAW_DATA_DIR, "_fetch_manifest.json")
    with open(manifest_path, 'w') as f:
        json.dump(manifest, f, indent=2)
    
    # Print summary
    print("=" * 80)
    print("STAGE 1 COMPLETE: RAW DATA FETCHING")
    print("=" * 80)
    print(f"\n✅ Successfully fetched: {len(successful)}/{len(PEER_COMPANIES)} companies")
    
    if failed:
        print(f"❌ Failed: {len(failed)} companies")
        for f in failed:
            print(f"   • {f['ticker']} ({f['name']})")
    
    total_size = sum(c['size_kb'] for c in successful)
    print(f"\n📁 Total data downloaded: {total_size:.1f} KB")
    print(f"📄 Manifest saved: {manifest_path}")
    print(f"⏱️  Duration: {(datetime.now() - start_time).total_seconds():.1f} seconds")
    print()
    print("➡️  Next step: Run Stage 2 (02_aggregate_raw_data.py)")
    
    return successful, failed


if __name__ == "__main__":
    successful, failed = main()
