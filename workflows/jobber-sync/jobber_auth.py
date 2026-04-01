#!/usr/bin/env python3
"""
Jobber OAuth2 re-authorization for a single client.

Usage:
  python3 jobber_auth.py --client 1338532896

1. Opens a browser to Jobber's authorization page
2. You log in and approve
3. Jobber redirects to localhost with an auth code
4. Script exchanges the code for tokens and saves them to the DB
"""

import argparse
import json
import os
import sys
import urllib.request
import urllib.parse
from http.server import HTTPServer, BaseHTTPRequestHandler
import webbrowser
import psycopg2

CLIENT_ID = os.environ.get("JOBBER_CLIENT_ID", "e46bcd1e-04a1-4770-bd86-88cf4abd9f35")
CLIENT_SECRET = os.environ.get("JOBBER_CLIENT_SECRET", "204ba95f7ec0f79a236112e4e475c1d22e8910689cd085573eade9d9009c2f62")
REDIRECT_URI = "http://localhost:9876/callback"
TOKEN_URL = "https://api.getjobber.com/api/oauth/token"
AUTH_URL = "https://api.getjobber.com/api/oauth/authorize"

DB_CONFIG = {
    "dbname": "blueprint",
    "user": "blueprint",
    "host": "localhost",
    "port": 5432,
}

auth_code = None

class CallbackHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        global auth_code
        from urllib.parse import urlparse, parse_qs
        query = parse_qs(urlparse(self.path).query)
        if 'code' in query:
            auth_code = query['code'][0]
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(b'<h2>Authorization successful!</h2><p>You can close this tab and return to the terminal.</p>')
        else:
            self.send_response(400)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(b'<h2>Error</h2><p>No authorization code received.</p>')

    def log_message(self, format, *args):
        pass  # Suppress server logs


def main():
    global auth_code

    parser = argparse.ArgumentParser(description="Re-authorize Jobber OAuth for a client")
    parser.add_argument("--client", type=int, required=True, help="customer_id to re-authorize")
    args = parser.parse_args()

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("SELECT name FROM clients WHERE customer_id = %s", (args.client,))
    row = cur.fetchone()
    if not row:
        print(f"No client found with customer_id {args.client}")
        sys.exit(1)

    print(f"\nRe-authorizing Jobber for: {row[0]}")
    print(f"A browser window will open. Log in to the Jobber account and approve.\n")

    # Build auth URL
    params = urllib.parse.urlencode({
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "response_type": "code",
        "scope": "read",
    })
    url = f"{AUTH_URL}?{params}"

    # Start local server to catch the callback
    server = HTTPServer(("localhost", 9876), CallbackHandler)
    webbrowser.open(url)

    print("Waiting for authorization callback...")
    while auth_code is None:
        server.handle_request()

    print(f"Got authorization code: {auth_code[:20]}...")

    # Exchange code for tokens
    data = urllib.parse.urlencode({
        "grant_type": "authorization_code",
        "code": auth_code,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "redirect_uri": REDIRECT_URI,
    }).encode()

    req = urllib.request.Request(TOKEN_URL, data=data, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")

    try:
        resp = urllib.request.urlopen(req)
        tokens = json.loads(resp.read())
    except Exception as e:
        print(f"ERROR exchanging code for tokens: {e}")
        sys.exit(1)

    access_token = tokens["access_token"]
    refresh_token = tokens["refresh_token"]

    # Save to DB
    cur.execute("""
        UPDATE clients SET
            jobber_access_token = %s,
            jobber_refresh_token = %s,
            jobber_token_expires_at = NOW() + INTERVAL '60 minutes'
        WHERE customer_id = %s
    """, (access_token, refresh_token, args.client))
    conn.commit()
    conn.close()

    print(f"\nTokens saved successfully!")
    print(f"Access token: {access_token[:20]}...")
    print(f"Refresh token: {refresh_token[:20]}...")
    print(f"\nYou can now run the Jobber sync:")
    print(f"  python3 pull_jobber_data.py --client {args.client}")


if __name__ == "__main__":
    main()
