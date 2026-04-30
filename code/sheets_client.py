# sheets_client.py
# Phase 1: Google Sheets connection and header initialization

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

# Required OAuth scopes for gspread
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def get_sheet(sheet_name: str, worksheet_index: int = 0):
    """
    Authenticates via service account and returns a gspread Worksheet object.

    Args:
        sheet_name: Exact name of the Google Sheet
        worksheet_index: Tab index (0 = first tab)

    Returns:
        gspread.Worksheet object
    """
    creds = Credentials.from_service_account_file(
        "service_account.json", scopes=SCOPES
    )
    client = gspread.authorize(creds)
    sheet = client.open(sheet_name).get_worksheet(worksheet_index)
    return sheet


def write_headers(sheet, headers: list) -> None:
    """
    Writes header row to row 1 only if not already present.
    Prevents overwriting existing data on re-runs.

    Args:
        sheet: gspread Worksheet object
        headers: list of column header strings
    """
    existing = sheet.row_values(1)
    if existing != headers:
        sheet.update([headers], "A1")
        print(f"✅ Headers written: {headers}")
    else:
        print("ℹ️  Headers already present — skipping.")


def append_dataframe(sheet, df: pd.DataFrame) -> None:
    """
    Appends all rows from a DataFrame to the Google Sheet.
    Skips the header row — assumes headers already written.

    Args:
        sheet: gspread Worksheet object
        df: pandas DataFrame with matching column order
    """
    if df.empty:
        print("⚠️  DataFrame is empty — nothing to append.")
        return

    # Convert NaN to empty string for clean Sheets output
    df = df.fillna("")
    rows = df.values.tolist()

    sheet.append_rows(rows, value_input_option="RAW")
    print(f"✅ {len(rows)} rows appended to Google Sheet.")


# --- Smoke test ---
if __name__ == "__main__":
    SHEET_NAME = "B2B Lead Pipeline"
    HEADERS = [
        "Business Name",
        "Phone",
        "Website",
        "Email",
        "Address",
        "Source Query"
    ]

    sheet = get_sheet(SHEET_NAME)
    write_headers(sheet, HEADERS)
    print(f"✅ Connected to: '{sheet.title}' | Total Rows: {sheet.row_count}")