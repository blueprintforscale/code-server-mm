#!/usr/bin/env python3
"""
Google Sheets OAuth2 — One-time auth.

Usage:
  python3 sheets_auth.py

Opens a browser for Google login. Saves refresh token to tokens/sheets.json.
Only needs to be run once.
"""

import json
import os
import sys

from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CREDENTIALS_FILE = os.path.join(
    os.path.dirname(SCRIPT_DIR), "client-intelligence", "gmail_credentials.json"
)
TOKENS_DIR = os.path.join(SCRIPT_DIR, "tokens")


def main():
    print("Authorizing Google Sheets read-only access...")
    print("A browser window will open. Log in and grant access.\n")

    os.makedirs(TOKENS_DIR, exist_ok=True)

    flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
    creds = flow.run_local_server(port=0)

    token_file = os.path.join(TOKENS_DIR, "sheets.json")
    with open(token_file, "w") as f:
        f.write(creds.to_json())

    print(f"\nToken saved to {token_file}")
    print("You can now run pull_sheet_data.py")


if __name__ == "__main__":
    main()
