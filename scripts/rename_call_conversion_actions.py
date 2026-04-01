#!/usr/bin/env python3
"""
One-off script: Rename 'Qualified Lead [AI]' to 'Qualified Call [AI]' across all active accounts.
"""
import sys
sys.path.insert(0, "/Users/bp/projects/.mcp-servers/google_ads_mcp/.venv/lib/python3.12/site-packages")

from google.ads.googleads.client import GoogleAdsClient
from google.protobuf import field_mask_pb2

YAML_PATH = "/Users/bp/projects/.mcp-servers/google_ads_mcp/google-ads.yaml"

CUSTOMERS = [
    ("5934796127", "CitruSolution NWC"),
    ("3718816142", "Logotherapy"),
    ("9699974772", "Golden State Pure Maintenance"),
    ("6336922860", "NoMoldWNC"),
    ("7123434733", "Pure Air Alabama"),
    ("6577791459", "Pure Maintenance Utah"),
    ("2584059484", "Pure Maintenance Wisconsin"),
    ("7441590915", "Pure Air Pros"),
    ("5281179937", "Pure Maintenance of Georgia"),
    ("6213328850", "Pure Maintenance of Oregon"),
    ("7217247125", "The Black Mold Guy"),
    ("9050376294", "NY Mold Solutions"),
    ("5109709947", "Pure Maintenance of Central Illinois"),
    ("5232371427", "Pure Maintenance of Indy"),
    ("3093864117", "Pacific Pure Maintenance"),
    ("4108933448", "Pure Maintenance of Pueblo"),
    ("7572700309", "PureAire Technology"),
    ("5011436807", "Lone Star Pure Maintenance"),
    ("1141718331", "Mountain Pure Mold Pro"),
    ("9108630791", "Pure Mold Masters"),
    ("8354910845", "Pure Air Nevada"),
    ("2987081978", "Pure Maintenance of Palm Beach"),
    ("1338532896", "Mold Cure"),
    ("1916645644", "Pure Restore"),
    ("9159518133", "Pure Maintenance of Kansas"),
    ("1714816135", "Pure Maintenance of East Texas"),
    ("9746041093", "PureAir Restored"),
    ("1737058570", "Pure Maintenance Ohio"),
    ("9668537931", "Pure Maintenance UK"),
    ("3703996852", "Mold Solutions SoCal"),
    ("9005635774", "Pure Maintenance of Southern California"),
    ("5317214982", "San Diego"),
    ("2669465729", "EnviroPure Services"),
]

def main():
    client = GoogleAdsClient.load_from_storage(YAML_PATH)

    for customer_id, name in CUSTOMERS:
        service = client.get_service("GoogleAdsService")
        query = """
            SELECT conversion_action.resource_name, conversion_action.name
            FROM conversion_action
            WHERE conversion_action.name = 'Qualified Lead [AI]'
              AND conversion_action.status = 'ENABLED'
        """
        resource_name = None
        try:
            results = service.search(customer_id=customer_id, query=query)
            for row in results:
                resource_name = row.conversion_action.resource_name
                break
        except Exception as e:
            print(f"  ERROR searching {customer_id} ({name}): {e}")
            continue

        if not resource_name:
            print(f"  SKIP {customer_id} ({name}) — no 'Qualified Lead [AI]' found")
            continue

        # Rename it
        try:
            ca_service = client.get_service("ConversionActionService")
            ca_operation = client.get_type("ConversionActionOperation")
            ca = ca_operation.update
            ca.resource_name = resource_name
            ca.name = "Qualified Call [AI]"

            ca_operation.update_mask.CopyFrom(field_mask_pb2.FieldMask(paths=["name"]))

            response = ca_service.mutate_conversion_actions(
                customer_id=customer_id,
                operations=[ca_operation],
            )
            print(f"  OK {customer_id} ({name}) — renamed to 'Qualified Call [AI]'")
        except Exception as e:
            print(f"  ERROR renaming {customer_id} ({name}): {e}")

if __name__ == "__main__":
    main()
