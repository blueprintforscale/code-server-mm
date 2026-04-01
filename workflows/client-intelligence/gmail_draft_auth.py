#!/usr/bin/env python3
"""
Gmail OAuth2 — Re-authorize with compose scope for creating drafts.

Usage:
  python3 gmail_draft_auth.py

Opens a browser for Google login. Saves token with compose scope.
Only needs to be run once.
"""

import json
import os
import sys

from google_auth_oauthlib.flow import InstalledAppFlow

# Need compose scope to create drafts
SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.compose",
]
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CREDENTIALS_FILE = os.path.join(SCRIPT_DIR, "gmail_credentials.json")
TOKENS_DIR = os.path.join(SCRIPT_DIR, "gmail_tokens")


def main():
    email = "info@blueprintforscale.com"
    print(f"Re-authorizing Gmail for: {email}")
    print("This will add draft creation permissions.")
    print("A browser window will open. Log in with that account and grant access.\n")

    os.makedirs(TOKENS_DIR, exist_ok=True)

    flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
    creds = flow.run_local_server(port=0)

    token_file = os.path.join(TOKENS_DIR, f"{email}.json")
    with open(token_file, "w") as f:
        f.write(creds.to_json())

    print(f"\nToken saved to {token_file}")
    print("You can now create email drafts.")


if __name__ == "__main__":
    main()
