#!/usr/bin/env python3
"""One-off: pull Jobber requests for Nathan Brown and store in jobber_requests."""
import json
import sys
sys.path.insert(0, '/Users/bp/projects/workflows/jobber-sync')
import pull_jobber_data as etl
import psycopg2

CUSTOMER_ID = 1916645644

conn = psycopg2.connect("host=localhost dbname=blueprint user=blueprint")
cur = conn.cursor()

cur.execute("SELECT jobber_access_token FROM clients WHERE customer_id = %s", [CUSTOMER_ID])
token = cur.fetchone()[0]

REQUESTS_QUERY = """
query GetRequests($first: Int!, $after: String) {
  requests(first: $first, after: $after) {
    nodes { id title createdAt client { id name } }
    totalCount
    pageInfo { hasNextPage endCursor }
  }
}
"""

all_requests = etl.fetch_all_pages(token, REQUESTS_QUERY, "requests")
print(f"Fetched {len(all_requests)} requests")

upserted = 0
for r in all_requests:
    client_node = r.get("client") or {}
    try:
        cur.execute("SAVEPOINT sp")
        cur.execute("""
            INSERT INTO jobber_requests (jobber_request_id, customer_id, jobber_customer_id, title, created_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (jobber_request_id, customer_id) DO UPDATE SET
                title = EXCLUDED.title, updated_at = NOW()
        """, [r["id"], CUSTOMER_ID, client_node.get("id"), r.get("title"), r.get("createdAt")])
        cur.execute("RELEASE SAVEPOINT sp")
        upserted += 1
    except Exception as e:
        cur.execute("ROLLBACK TO SAVEPOINT sp")
        print(f"Error: {e}")

conn.commit()
print(f"Upserted {upserted} requests")

# Check how many match CallRail leads
cur.execute("""
    SELECT COUNT(*) as total,
      COUNT(CASE WHEN jr.jobber_customer_id IN (
        SELECT jobber_customer_id FROM jobber_customers WHERE callrail_id IS NOT NULL AND customer_id = %s
      ) THEN 1 END) as matched
    FROM jobber_requests jr WHERE jr.customer_id = %s
""", [CUSTOMER_ID, CUSTOMER_ID])
total, matched = cur.fetchone()
print(f"Total requests: {total}, matched to CallRail: {matched}")

conn.close()
