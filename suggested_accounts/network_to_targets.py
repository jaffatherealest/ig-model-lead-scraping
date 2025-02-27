import requests
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Airtable configuration
AIRTABLE_API_KEY = os.getenv('AIRTABLE_API_KEY')
BASE_ID = os.getenv('AIRTABLE_BASE_ID')
NETWORK_TABLE = os.getenv('AIRTABLE_NETWORK_TABLE')
TARGETS_TABLE = os.getenv('AIRTABLE_TARGETS_TABLE')

AIRTABLE_API_URL = f"https://api.airtable.com/v0/{BASE_ID}"

AIRTABLE_HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
    "Content-Type": "application/json"
}

def fetch_qualified_network_accounts(offset=None, all_records=None):
    """
    Fetch network accounts that meet certain criteria:
    - Has follower count
    - Matches your target demographic
    - Hasn't been converted to target yet
    """
    if all_records is None:
        all_records = []
        
    url = f"{AIRTABLE_API_URL}/{NETWORK_TABLE}"
    
    # # Customize this formula based on your criteria
    # formula = "AND(" + \
    #          "{follower_count} > 1000," + \
    #          "{follower_count} < 100000," + \
    #          "{converted_to_target} != 1," + \ 
    #          "{is_business} = 1" + \
    #          ")"

    ### add filtered view from airtable instead of formula in the code - decide on filter options ###
    
    params = {}
    if offset:
        params['offset'] = offset

    try:
        response = requests.get(url, headers=AIRTABLE_HEADERS, params=params)
        response.raise_for_status()
        data = response.json()
        
        all_records.extend(data.get('records', []))
        
        if 'offset' in data:
            return fetch_qualified_network_accounts(data['offset'], all_records)
        return all_records
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching qualified accounts: {e}")
        return all_records

def create_target_record(account_data):
    """Create a new record in the targets table"""
    url = f"{AIRTABLE_API_URL}/{TARGETS_TABLE}"
    
    payload = {
        "records": [{
            "fields": {
                "username": account_data.get('fields', {}).get('username'),
                "Processed": False,
                "Source": "Network Conversion"
            }
        }]
    }
    
    try:
        response = requests.post(url, headers=AIRTABLE_HEADERS, json=payload)
        response.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error creating target record: {e}")
        return False

def mark_as_converted(record_id):
    """Mark network record as converted to target"""
    url = f"{AIRTABLE_API_URL}/{NETWORK_TABLE}/{record_id}"
    
    payload = {
        "fields": {
            "converted_to_target": True
        }
    }
    
    try:
        response = requests.patch(url, headers=AIRTABLE_HEADERS, json=payload)
        response.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error marking as converted: {e}")
        return False

def convert_network_to_targets():
    qualified_accounts = fetch_qualified_network_accounts()
    
    for account in qualified_accounts:
        username = account.get('fields', {}).get('username')
        record_id = account.get('id')
        
        if username:
            print(f"\nProcessing qualified account: {username}")
            
            if create_target_record(account):
                print(f"Created target record for: {username}")
                
                if mark_as_converted(record_id):
                    print(f"Marked {username} as converted to target")
            
            print(f"Completed processing {username}")

if __name__ == "__main__":
    convert_network_to_targets()