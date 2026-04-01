#!/usr/bin/env python3
"""
Gmail OAuth2 — One-time auth for each inbox.

Usage:
  python3 gmail_auth.py info@blueprintforscale.com
  python3 gmail_auth.py susie@blueprintforscale.com
  python3 gmail_auth.py martin@blueprintforscale.com
  python3 gmail_auth.py josh@blueprintforscale.com
  python3 gmail_auth.py kiana@blueprintforscale.com

Opens a browser for Google login. Saves refresh token to gmail_tokens/<email>.json.
Only needs to be run once per inbox.
"""

import json
import os
import random
import sys

from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
CREDENTIALS_FILE = os.path.join(os.path.dirname(__file__), "gmail_credentials.json")
TOKENS_DIR = os.path.join(os.path.dirname(__file__), "gmail_tokens")


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 gmail_auth.py <email@blueprintforscale.com>")
        sys.exit(1)

    email = sys.argv[1].strip().lower()
    print(f"Authorizing Gmail access for: {email}")
    print("A browser window will open. Log in with that account and grant access.\n")

    os.makedirs(TOKENS_DIR, exist_ok=True)

    flow = InstalledAppFlow.from_client_secrets_file(
        CREDENTIALS_FILE,
        scopes=SCOPES,
    )

    # Run local server for OAuth callback (random port to avoid conflicts)
    port = random.randint(8100, 8199)
    creds = flow.run_local_server(
        port=port,
        prompt="consent",
        login_hint=email,
    )

    token_path = os.path.join(TOKENS_DIR, f"{email}.json")
    token_data = {
        "token": creds.token,
        "refresh_token": creds.refresh_token,
        "token_uri": creds.token_uri,
        "client_id": creds.client_id,
        "client_secret": creds.client_secret,
        "scopes": list(creds.scopes),
        "email": email,
    }

    with open(token_path, "w") as f:
        json.dump(token_data, f, indent=2)

    print(f"\nToken saved to {token_path}")
    print(f"Gmail ETL can now pull emails from {email}")


if __name__ == "__main__":
    main()
