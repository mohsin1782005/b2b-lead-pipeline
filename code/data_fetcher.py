# data_fetcher.py
# Phase 2: Fetch business data using OSMPythonTools (handles Overpass internally)

import pandas as pd
import time
from OSMPythonTools.overpass import Overpass, overpassQueryBuilder
from OSMPythonTools.nominatim import Nominatim


def fetch_businesses(city: str, business_type: str) -> pd.DataFrame:
    """
    Fetches businesses from OpenStreetMap for a given city and type.

    Args:
        city: Target city name e.g. "Madison"
        business_type: OSM tag value e.g. "shop", "office"

    Returns:
        pandas DataFrame with business details
    """
    print(f"🔍 Fetching '{business_type}' businesses in '{city}'...")

    try:
        # Step 1: Resolve city name to OSM area ID
        nominatim = Nominatim()
        area = nominatim.query(f"{city}, USA")
        area_id = area.areaId()
        print(f"✅ City resolved — OSM Area ID: {area_id}")

        # Step 2: Build and run Overpass query
        time.sleep(2)  # be polite to OSM servers
        overpass = Overpass()

        query = overpassQueryBuilder(
            area=area_id,
            elementType=["node", "way"],
            selector=f'"{business_type}"',
            out="body"
        )

        result = overpass.query(query, timeout=60)

    except Exception as e:
        print(f"❌ Error fetching data: {e}")
        return pd.DataFrame()

    elements = result.elements()

    if not elements:
        print("⚠️  No results found. Try a different city or business type.")
        return pd.DataFrame()

    print(f"✅ {len(elements)} raw results fetched.")

    records = []
    for el in elements:
        name = el.tag("name")

        # Skip entries with no business name
        if not name:
            continue

        records.append({
            "Business Name": name,
            "Phone": el.tag("phone") or el.tag("contact:phone") or "",
            "Website": el.tag("website") or el.tag("contact:website") or "",
            "Address": build_address(el),
            "Email": "",           # populated in Phase 3
            "Source Query": f"{business_type} in {city}"
        })

    df = pd.DataFrame(records)

    # Remove duplicates by business name
    df.drop_duplicates(subset=["Business Name"], inplace=True)
    df.reset_index(drop=True, inplace=True)

    print(f"✅ {len(df)} unique businesses after cleaning.")
    return df


def build_address(el) -> str:
    """Constructs readable address from OSM element tags."""
    parts = [
        el.tag("addr:housenumber") or "",
        el.tag("addr:street") or "",
        el.tag("addr:city") or "",
        el.tag("addr:state") or "",
        el.tag("addr:postcode") or "",
    ]
    return ", ".join(part for part in parts if part)


# --- End-to-end test: API → Google Sheet ---
if __name__ == "__main__":
    from sheets_client import get_sheet, write_headers, append_dataframe

    SHEET_NAME = "B2B Lead Pipeline"
    HEADERS = [
        "Business Name",
        "Phone",
        "Website",
        "Email",
        "Address",
        "Source Query"
    ]

    df = fetch_businesses(city="Madison", business_type="shop")

    if not df.empty:
        sheet = get_sheet(SHEET_NAME)
        write_headers(sheet, HEADERS)
        append_dataframe(sheet, df)
        print("🚀 Pipeline Phase 2 complete — check your Google Sheet!")