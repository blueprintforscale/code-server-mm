#!/usr/bin/env python3
"""Sync ads_manager column from Google Ads MCC labels (Martin/Luke/Nima)."""
import psycopg2
from google.ads.googleads.client import GoogleAdsClient

MCC_ID = '2985235474'
LABEL_MAP = {
    'customers/2985235474/labels/22095762246': 'Martin',
    'customers/2985235474/labels/22095762447': 'Luke',
    'customers/2985235474/labels/22107491573': 'Nima',
}

def main():
    ga_client = GoogleAdsClient.load_from_storage('/Users/bp/projects/.mcp-servers/google_ads_mcp/google-ads.yaml')
    ga_service = ga_client.get_service('GoogleAdsService')

    query = '''
      SELECT customer_client.id, customer_client.descriptive_name, customer_client.applied_labels
      FROM customer_client WHERE customer_client.status = 'ENABLED'
    '''
    response = ga_service.search(customer_id=MCC_ID, query=query)

    label_assignments = {}
    for row in response:
        cc = row.customer_client
        labels = [LABEL_MAP.get(l) for l in cc.applied_labels if l in LABEL_MAP] if cc.applied_labels else []
        if labels:
            label_assignments[cc.id] = labels[0]

    conn = psycopg2.connect(dbname='blueprint', user='blueprint', host='localhost')
    cur = conn.cursor()
    updated = 0
    for customer_id, manager in label_assignments.items():
        cur.execute(
            "UPDATE clients SET ads_manager = %s WHERE customer_id = %s AND (ads_manager IS DISTINCT FROM %s)",
            (manager, customer_id, manager)
        )
        if cur.rowcount > 0:
            print(f"Updated {customer_id}: ads_manager → {manager}")
            updated += 1
    conn.commit()
    cur.close()
    conn.close()
    print(f"Done. {updated} client(s) updated.")

if __name__ == '__main__':
    main()
