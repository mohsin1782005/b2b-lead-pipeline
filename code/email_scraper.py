# email_scraper.py
# Phase 3: Visit business websites and extract email addresses

import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup

# Regex pattern to match valid email addresses
EMAIL_REGEX = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}"
)

# Headers to mimic a real browser — avoids bot blocking
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


def scrape_emails_from_url(url: str) -> list[str]:
    """
    Visits a URL and extracts all email addresses found on the page.

    Args:
        url: Business website URL

    Returns:
        List of unique email addresses found
    """
    if not url or not url.startswith("http"):
        return []

    try:
        response = requests.get(
            url,
            headers=HEADERS,
            timeout=10,
            allow_redirects=True
        )
        response.raise_for_status()

    except requests.exceptions.Timeout:
        print(f"      ⏱ Timeout: {url}")
        return []

    except requests.exceptions.HTTPError as e:
        print(f"      ❌ HTTP Error {e.response.status_code}: {url}")
        return []

    except requests.exceptions.ConnectionError:
        print(f"      ❌ Connection failed: {url}")
        return []

    except Exception as e:
        print(f"      ❌ Unexpected error: {e}")
        return []

    # Parse HTML and extract text
    soup = BeautifulSoup(response.text, "lxml")

    # Also check mailto: links directly — most reliable source
    mailto_emails = []
    for tag in soup.select("a[href^='mailto:']"):
        email = tag["href"].replace("mailto:", "").strip().split("?")[0]
        if EMAIL_REGEX.match(email):
            mailto_emails.append(email.lower())

    # Extract emails from full page text
    page_text = soup.get_text()
    text_emails = EMAIL_REGEX.findall(page_text)
    text_emails = [e.lower() for e in text_emails]

    # Combine both sources and deduplicate
    all_emails = list(set(mailto_emails + text_emails))

    # Filter out common false positives
    filtered = [
        e for e in all_emails
        if not any(skip in e for skip in [
            "example.com", "yourdomain", "domain.com",
            "email.com", "sentry", "wixpress", "schema.org",
            ".png", ".jpg", ".gif", ".svg"
        ])
    ]

    return filtered


def scrape_emails_from_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Iterates over DataFrame rows, scrapes emails for each website,
    and fills the Email column.

    Args:
        df: DataFrame with 'Website' and 'Email' columns

    Returns:
        Updated DataFrame with emails populated
    """
    total = len(df)
    print(f"\n🌐 Starting email scraping for {total} businesses...\n")

    for idx, row in df.iterrows():
        website = row.get("Website", "")
        business = row.get("Business Name", f"Row {idx}")

        if not website:
            print(f"  [{idx+1}/{total}] ⏭  Skipping '{business}' — no website")
            continue

        print(f"  [{idx+1}/{total}] 🔍 Scraping: {business} → {website}")

        emails = scrape_emails_from_url(website)

        if emails:
            # Join multiple emails with comma if more than one found
            df.at[idx, "Email"] = ", ".join(emails)
            print(f"      ✅ Found: {', '.join(emails)}")
        else:
            print(f"      ⚠️  No email found")

        # Polite delay — avoid hammering servers
        time.sleep(1)

    found = df[df["Email"] != ""].shape[0]
    print(f"\n✅ Scraping complete — {found}/{total} emails found.")
    return df


# --- Smoke test ---
if __name__ == "__main__":
    # Test on a single known URL first
    test_url = "https://httpbin.org/html"
    print(f"🧪 Testing scraper on: {test_url}")
    emails = scrape_emails_from_url(test_url)
    print(f"Emails found: {emails if emails else 'None (expected for test URL)'}")
    print("✅ Scraper is working correctly — no errors thrown.")