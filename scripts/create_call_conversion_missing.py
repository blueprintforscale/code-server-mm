#!/usr/bin/env python3
"""Create 'Qualified Call [AI]' for accounts that were missing 'Qualified Lead [AI]'."""
import sys
sys.path.insert(0, "/Users/bp/projects/.mcp-servers/google_ads_mcp/.venv/lib/python3.12/site-packages")
from google.ads.googleads.client import GoogleAdsClient

YAML_PATH = "/Users/bp/projects/.mcp-servers/google_ads_mcp/google-ads.yaml"

MISSING = [
    ("5934796127", "CitruSolution NWC"),
    ("3718816142", "Logotherapy"),
    ("9668537931", "Pure Maintenance UK"),
]

def main():
    client = GoogleAdsClient.load_from_storage(YAML_PATH)
    for customer_id, name in MISSING:
        # Check if it already exists
        service = client.get_service("GoogleAdsService")
        query = """
            SELECT conversion_action.resource_name
            FROM conversion_action
            WHERE conversion_action.name = 'Qualified Call [AI]'
              AND conversion_action.status = 'ENABLED'
        """
        try:
            exists = any(True for _ in service.search(customer_id=customer_id, query=query))
            if exists:
                print(f"  SKIP {customer_id} ({name}) — already exists")
                continue
        except Exception as e:
            print(f"  ERROR checking {customer_id} ({name}): {e}")
            continue

        try:
            ca_service = client.get_service("ConversionActionService")
            ca_op = client.get_type("ConversionActionOperation")
            ca = ca_op.create
            ca.name = "Qualified Call [AI]"
            ca.type_ = client.enums.ConversionActionTypeEnum.UPLOAD_CLICKS
            ca.category = client.enums.ConversionActionCategoryEnum.QUALIFIED_LEAD
            ca.status = client.enums.ConversionActionStatusEnum.ENABLED
            ca.primary_for_goal = False
            ca.value_settings.default_value = 1.0
            ca.value_settings.always_use_default_value = True

            response = ca_service.mutate_conversion_actions(customer_id=customer_id, operations=[ca_op])
            print(f"  OK {customer_id} ({name}) — created: {response.results[0].resource_name}")
        except Exception as e:
            print(f"  ERROR creating {customer_id} ({name}): {e}")

if __name__ == "__main__":
    main()
