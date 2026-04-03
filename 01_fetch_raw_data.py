#!/usr/bin/env python3
"""
================================================================================
STAGE 1: RAW DATA FETCHER (CORRECTED)
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

# 30 Peer Companies - Technology Sector (VERIFIED CIKs)
# Source: SEC EDGAR Database - Verified January 2026
PEER_COMPANIES = [
    {"ticker": "AAPL", "cik": "0000320193", "name": "Apple Inc.", "sic": "3571"},
    {"ticker": "MSFT", "cik": "0000789019", "name": "Microsoft Corporation", "sic": "7372"},
    {"ticker": "GOOGL", "cik": "0001652044", "name": "Alphabet Inc.", "sic": "7370"},
    {"ticker": "AMZN", "cik": "0001018724", "name": "Amazon.com Inc.", "sic": "5961"},
    {"ticker": "META", "cik": "0001326801", "name": "Meta Platforms Inc.", "sic": "7370"},
    {"ticker": "NVDA", "cik": "0001045810", "name": "NVIDIA Corporation", "sic": "3674"},
    {"ticker": "TSLA", "cik": "0001318605", "name": "Tesla Inc.", "sic": "3711"},
    {"ticker": "ADBE", "cik": "0000796343", "name": "Adobe Inc.", "sic": "7372"},
    {"ticker": "CRM", "cik": "0001108524", "name": "Salesforce Inc.", "sic": "7372"},
    {"ticker": "NFLX", "cik": "0001065280", "name": "Netflix Inc.", "sic": "7841"},
    {"ticker": "ORCL", "cik": "0001341439", "name": "Oracle Corporation", "sic": "7372"},
    {"ticker": "INTC", "cik": "0000050863", "name": "Intel Corporation", "sic": "3674"},
    {"ticker": "AMD", "cik": "0000002488", "name": "Advanced Micro Devices", "sic": "3674"},
    {"ticker": "CSCO", "cik": "0000858877", "name": "Cisco Systems Inc.", "sic": "3576"},
    {"ticker": "IBM", "cik": "0000051143", "name": "IBM Corporation", "sic": "7370"},
    {"ticker": "QCOM", "cik": "0000804328", "name": "Qualcomm Inc.", "sic": "3674"},
    {"ticker": "NOW", "cik": "0001373715", "name": "ServiceNow Inc.", "sic": "7372"},
    {"ticker": "INTU", "cik": "0000896878", "name": "Intuit Inc.", "sic": "7372"},
    {"ticker": "PLTR", "cik": "0001321655", "name": "Palantir Technologies", "sic": "7372"},
    {"ticker": "SNOW", "cik": "0001640147", "name": "Snowflake Inc.", "sic": "7372"},
    {"ticker": "CRWD", "cik": "0001535527", "name": "CrowdStrike Holdings", "sic": "7372"},
    {"ticker": "PANW", "cik": "0001327567", "name": "Palo Alto Networks", "sic": "7372"},
    {"ticker": "WDAY", "cik": "0001327811", "name": "Workday Inc.", "sic": "7372"},
    {"ticker": "UBER", "cik": "0001543151", "name": "Uber Technologies", "sic": "4121"},
    {"ticker": "ABNB", "cik": "0001559720", "name": "Airbnb Inc.", "sic": "7011"},
    {"ticker": "PYPL", "cik": "0001633917", "name": "PayPal Holdings", "sic": "7389"},
    {"ticker": "SQ", "cik": "0001512673", "name": "Block Inc.", "sic": "7389"},
    {"ticker": "SHOP", "cik": "0001594805", "name": "Shopify Inc.", "sic": "7372"},
    {"ticker": "ZM", "cik": "0001585521", "name": "Zoom Video", "sic": "7372"},
    {"ticker": "TWLO", "cik": "0001447669", "name": "Twilio Inc.", "sic": "7372"},
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


def verify_company_exists(session, cik):
    """Verify if company exists in SEC database by checking CIK lookup."""
    cik_padded = str(cik).zfill(10)
    # SEC CIK lookup URL
    lookup_url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik_padded}&output=atom"
    
    try:
        response = session.get(lookup_url, timeout=10)
        return response.status_code == 200
    except:
        return False


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
        
        if response.status_code == 200:
            logger.debug(f"    Content-Length: {response.headers.get('Content-Length', 'N/A')}")
        
        response.raise_for_status()
        
        data = response.json()
        
        # Verify we got actual data (not just an error message)
        if 'facts' not in data:
            logger.warning(f"    Response missing 'facts' section - possibly no data available")
            return None
            
        # Check if there's any US-GAAP data
        if 'us-gaap' not in data.get('facts', {}):
            logger.warning(f"    No US-GAAP data found for this company")
            return None
            
        return data
    
    except requests.exceptions.HTTPError as e:
        if response.status_code == 404:
            logger.warning(f"    Company not found in SEC database (404)")
        elif response.status_code == 403:
            logger.warning(f"    Access forbidden (403) - Check User-Agent header")
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
            "documentation": "https://www.sec.gov/edgar/sec-api-documentation",
            "data_quality": {
                "has_facts_section": "facts" in data,
                "has_us_gaap": "us-gaap" in data.get('facts', {}),
                "us_gaap_tags_count": len(data.get('facts', {}).get('us-gaap', {}))
            }
        },
        "sec_data": data
    }
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    return filepath


def test_single_company(ticker, cik):
    """Test function to verify data fetching for a single company."""
    print(f"\n🧪 Testing data fetch for {ticker} (CIK: {cik})...")
    
    session = create_session()
    data = fetch_company_facts(session, cik)
    
    if data:
        us_gaap_count = len(data.get('facts', {}).get('us-gaap', {}))
        print(f"   ✅ Success! Found {us_gaap_count} US-GAAP tags")
        
        # Show sample tags
        sample_tags = list(data.get('facts', {}).get('us-gaap', {}).keys())[:5]
        print(f"   Sample tags: {', '.join(sample_tags)}")
        return True
    else:
        print(f"   ❌ Failed to fetch data for {ticker}")
        return False


# ==============================================================================
# MAIN EXECUTION
# ==============================================================================

def main():
    """Main function to fetch and save raw data for all companies."""
    
    print("=" * 80)
    print("STAGE 1: RAW DATA FETCHER (CORRECTED)")
    print("FDA-4: Comparative Financial Analysis Pipeline")
    print("=" * 80)
    print()
    print(f"📡 API Source: SEC EDGAR Company Facts API")
    print(f"🔗 Endpoint: {SEC_API_BASE}/CIK{{cik}}.json")
    print(f"📚 Documentation: https://www.sec.gov/edgar/sec-api-documentation")
    print(f"📁 Output Directory: {RAW_DATA_DIR}")
    print(f"🏢 Companies to fetch: {len(PEER_COMPANIES)}")
    print(f"👤 User-Agent: {USER_AGENT}")
    print()
    
    # Optional: Test first company to verify connection
    print("🔍 Testing API connection with first company...")
    test_company = PEER_COMPANIES[0]
    if not test_single_company(test_company['ticker'], test_company['cik']):
        print("\n⚠️  API test failed! Possible issues:")
        print("   1. Check your internet connection")
        print("   2. Verify User-Agent string is correct")
        print("   3. SEC API might be rate limiting (wait a few minutes)")
        print("\n   Continuing anyway...")
    print()
    
    # Create session
    session = create_session()
    
    # Track results
    successful = []
    failed = []
    empty_data = []
    
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
            # Check if data has actual content
            us_gaap_data = raw_data.get('facts', {}).get('us-gaap', {})
            
            if len(us_gaap_data) == 0:
                logger.warning(f"    ⚠️  Empty US-GAAP data (0 tags)")
                empty_data.append({'ticker': ticker, 'cik': cik, 'name': name})
            else:
                # Save to file
                filepath = save_raw_json(raw_data, ticker, RAW_DATA_DIR)
                file_size = os.path.getsize(filepath) / 1024  # KB
                
                logger.info(f"    ✅ Saved: {filepath} ({file_size:.1f} KB, {len(us_gaap_data)} tags)")
                successful.append({
                    'ticker': ticker,
                    'cik': cik,
                    'name': name,
                    'file': filepath,
                    'size_kb': round(file_size, 1),
                    'us_gaap_tags': len(us_gaap_data)
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
        "empty_data": len(empty_data),
        "duration_seconds": (datetime.now() - start_time).total_seconds(),
        "successful_companies": successful,
        "failed_companies": failed,
        "empty_data_companies": empty_data,
        "api_info": {
            "base_url": SEC_API_BASE,
            "user_agent": USER_AGENT,
            "rate_limit_delay": REQUEST_DELAY
        }
    }
    
    manifest_path = os.path.join(RAW_DATA_DIR, "_fetch_manifest.json")
    with open(manifest_path, 'w') as f:
        json.dump(manifest, f, indent=2)
    
    # Print summary
    print("=" * 80)
    print("STAGE 1 COMPLETE: RAW DATA FETCHING")
    print("=" * 80)
    print(f"\n✅ Successfully fetched: {len(successful)}/{len(PEER_COMPANIES)} companies")
    
    if empty_data:
        print(f"⚠️  Empty data (no US-GAAP): {len(empty_data)} companies")
        for e in empty_data:
            print(f"   • {e['ticker']} ({e['name']}) - No financial data available")
    
    if failed:
        print(f"❌ Failed: {len(failed)} companies")
        for f in failed:
            print(f"   • {f['ticker']} ({f['name']})")
    
    total_size = sum(c['size_kb'] for c in successful)
    total_tags = sum(c['us_gaap_tags'] for c in successful)
    print(f"\n📊 Statistics:")
    print(f"   • Total data downloaded: {total_size:.1f} KB")
    print(f"   • Total US-GAAP tags: {total_tags}")
    print(f"   • Average tags per company: {total_tags/len(successful):.0f}" if successful else "   • No data")
    print(f"\n📄 Manifest saved: {manifest_path}")
    print(f"⏱️  Duration: {(datetime.now() - start_time).total_seconds():.1f} seconds")
    print()
    
    if len(successful) < len(PEER_COMPANIES) * 0.7:
        print("⚠️  WARNING: Low success rate!")
        print("   Possible issues:")
        print("   1. SEC API rate limiting - wait and retry")
        print("   2. Some CIKs might be incorrect")
        print("   3. Some companies might not have XBRL filings")
    
    print("➡️  Next step: Run Stage 2 (02_json_to_company_csv.py)")
    
    return successful, failed, empty_data


if __name__ == "__main__":
    successful, failed, empty_data = main()
