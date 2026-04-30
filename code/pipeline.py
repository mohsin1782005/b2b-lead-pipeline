# pipeline.py
# Phase 4: Master orchestrator — combines all 3 phases into one clean pipeline

import time
import pandas as pd
from datetime import datetime

from sheets_client import get_sheet, write_headers, append_dataframe
from data_fetcher import fetch_businesses
from email_scraper import scrape_emails_from_dataframe

# ─────────────────────────────────────────────
#  CONFIGURATION — edit these before each run
# ─────────────────────────────────────────────
CONFIG = {
    "city": "Madison",               # Target city
    "business_type": "shop",         # OSM tag: shop, office, amenity, etc.
    "sheet_name": "B2B Lead Pipeline",
    "max_records": 500,               # Limit scraping to first N records
}

HEADERS = [
    "Business Name",
    "Phone",
    "Website",
    "Email",
    "Address",
    "Source Query"
]


def print_banner():
    """Prints a startup banner for the pipeline."""
    print("\n" + "=" * 55)
    print("   🚀 B2B Lead Mining Pipeline")
    print(f"   City         : {CONFIG['city']}")
    print(f"   Business Type: {CONFIG['business_type']}")
    print(f"   Sheet        : {CONFIG['sheet_name']}")
    print(f"   Max Records  : {CONFIG['max_records']}")
    print(f"   Started at   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55 + "\n")


def print_summary(df: pd.DataFrame, start_time: float):
    """Prints a final summary report after pipeline completes."""
    elapsed = round(time.time() - start_time, 2)
    total = len(df)
    with_website = df[df["Website"] != ""].shape[0]
    with_email = df[df["Email"] != ""].shape[0]
    with_phone = df[df["Phone"] != ""].shape[0]

    print("\n" + "=" * 55)
    print("   ✅ Pipeline Complete — Summary Report")
    print("=" * 55)
    print(f"   Total Businesses Fetched : {total}")
    print(f"   With Website             : {with_website}")
    print(f"   With Email               : {with_email}")
    print(f"   With Phone               : {with_phone}")
    print(f"   Total Time               : {elapsed}s")
    print(f"   Completed at             : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55 + "\n")


def run_pipeline():
    """
    Master function that orchestrates all 4 phases:
    1. Connect to Google Sheets
    2. Fetch businesses from Overpass API
    3. Scrape emails from business websites
    4. Write clean data to Google Sheet
    """
    start_time = time.time()
    print_banner()

    # ── Phase 1: Connect to Google Sheet ──────────────────
    print("📋 Phase 1: Connecting to Google Sheet...")
    try:
        sheet = get_sheet(CONFIG["sheet_name"])
        write_headers(sheet, HEADERS)
        print("✅ Google Sheet connected.\n")
    except Exception as e:
        print(f"❌ Failed to connect to Google Sheet: {e}")
        print("   Check your service_account.json and sheet name.")
        return

    # ── Phase 2: Fetch Business Data ──────────────────────
    print("📡 Phase 2: Fetching business data from OpenStreetMap...")
    try:
        df = fetch_businesses(
            city=CONFIG["city"],
            business_type=CONFIG["business_type"]
        )
    except Exception as e:
        print(f"❌ Failed to fetch business data: {e}")
        return

    if df.empty:
        print("❌ No data fetched. Try a different city or business type.")
        return

    # Limit records to avoid long scraping times
    if len(df) > CONFIG["max_records"]:
        print(f"⚙️  Limiting to first {CONFIG['max_records']} records for scraping.")
        df = df.head(CONFIG["max_records"]).copy()

    print(f"✅ {len(df)} businesses ready for scraping.\n")

    # ── Phase 3: Scrape Emails ─────────────────────────────
    print("📧 Phase 3: Scraping emails from business websites...")
    try:
        df = scrape_emails_from_dataframe(df)
    except Exception as e:
        print(f"❌ Email scraping failed: {e}")
        print("   Continuing with available data...")

    # ── Phase 4: Write to Google Sheet ────────────────────
    print("\n📊 Phase 4: Writing data to Google Sheet...")
    try:
        append_dataframe(sheet, df)
        print("✅ Data written to Google Sheet.")
    except Exception as e:
        print(f"❌ Failed to write to Google Sheet: {e}")
        return

    # ── Final Summary ──────────────────────────────────────
    print_summary(df, start_time)


# ── Entry point ───────────────────────────────────────────
if __name__ == "__main__":
    run_pipeline()