#!/usr/bin/env python3
"""Re-authorize Google Calendar with read+write scope."""
import json
import os

CREDS_FILE = os.path.join(os.path.dirname(__file__), 'gmail_credentials.json')
TOKEN_FILE = os.path.join(os.path.dirname(__file__), 'calendar_tokens/info@blueprintforscale.com.json')
SCOPES = ['https://www.googleapis.com/auth/calendar']

with open(CREDS_FILE) as f:
    creds_data = json.load(f)

client_config = creds_data.get('installed', creds_data.get('web', {}))
client_id = client_config['client_id']
client_secret = client_config['client_secret']
redirect_uri = client_config.get('redirect_uris', ['urn:ietf:wg:oauth:2.0:oob'])[0]

# Build auth URL
import urllib.parse
auth_url = 'https://accounts.google.com/o/oauth2/auth?' + urllib.parse.urlencode({
    'client_id': client_id,
    'redirect_uri': redirect_uri,
    'scope': ' '.join(SCOPES),
    'response_type': 'code',
    'access_type': 'offline',
    'prompt': 'consent',
})

print()
print('Open this URL in your browser and sign in with info@blueprintforscale.com:')
print()
print(auth_url)
print()
code = input('Paste the authorization code here: ').strip()

# Exchange code for token
import urllib.request
token_url = 'https://oauth2.googleapis.com/token'
data = urllib.parse.urlencode({
    'code': code,
    'client_id': client_id,
    'client_secret': client_secret,
    'redirect_uri': redirect_uri,
    'grant_type': 'authorization_code',
}).encode()
req = urllib.request.Request(token_url, data=data, method='POST')
resp = urllib.request.urlopen(req)
token_data = json.loads(resp.read())

# Save token
token_data['scopes'] = SCOPES
token_data['client_id'] = client_id
token_data['client_secret'] = client_secret
with open(TOKEN_FILE, 'w') as f:
    json.dump(token_data, f, indent=2)

print()
print('Token saved with calendar read+write scope!')
print('Scopes:', SCOPES)
