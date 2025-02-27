import os
from dotenv import load_dotenv
from instagram import get_user_info
from airtable import update_business_network_gender
import requests

# Get Airtable credentials from .env
AIRTABLE_API_KEY = os.getenv('AIRTABLE_API_KEY')
AIRTABLE_BASE_ID = os.getenv('AIRTABLE_BASE_ID')
AIRTABLE_LOCATIONS_TABLE = os.getenv('AIRTABLE_LOCATIONS_TABLE')
AIRTABLE_LOCATION_POSTS_TABLE = os.getenv('AIRTABLE_LOCATION_POSTS_TABLE')
AIRTABLE_FIRE_LOCATIONS_VIEW = os.getenv('AIRTABLE_FIRE_LOCATIONS_VIEW')
AIRTABLE_BUSINESS_TARGETS_TABLE = os.getenv('AIRTABLE_BUSINESS_TARGETS_TABLE')
AIRTABLE_BUSINESS_NETWORK_TABLE = os.getenv('AIRTABLE_BUSINESS_NETWORK_TABLE')
AIRTABLE_BUSINESS_NETWORK_FEMALE_VIEW = os.getenv('AIRTABLE_BUSINESS_NETWORK_FEMALE_VIEW')

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

def fetch_female_business_accounts(offset=None, all_records=None):
    """
    Function to fetch female accounts from Business Network table using a filtered view
    """
    url = f'https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_BUSINESS_NETWORK_TABLE}?view={os.getenv("AIRTABLE_BUSINESS_NETWORK_FEMALE_VIEW")}'
    headers = {
        'Authorization': f'Bearer {AIRTABLE_API_KEY}',
    }

    if all_records is None:
        all_records = []
        
    query_params = {}
    if offset:
        query_params['offset'] = offset

    response = requests.get(url, headers=headers, params=query_params)
    response.raise_for_status()
    data = response.json()
    
    all_records.extend(data.get('records', []))
    
    if 'offset' in data:
        return fetch_female_business_accounts(data['offset'], all_records)
    else:
        return all_records

def update_account_info(record_id, update_data):
    """
    Function to update business account info in Business Network table
    """
    url = f'https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_BUSINESS_NETWORK_TABLE}/{record_id}'
    headers = {
        'Authorization': f'Bearer {AIRTABLE_API_KEY}',
        'Content-Type': 'application/json'
    }
    
    payload = {
        "fields": update_data
    }
    
    try:
        response = requests.patch(url, headers=headers, json=payload)
        response.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error updating account info: {e}")
        return False

def process_female_business_info():
    """
    Function to:
    1. Fetch female accounts from Business Network table
    2. Get additional Instagram info for each account
    3. Update Airtable with the new info
    """
    
    # Get female accounts that need info fetched
    accounts = fetch_female_business_accounts()
    if not accounts:
        print("No female business accounts found needing info fetch")
        return
        
    print(f"Found {len(accounts)} female business accounts to process")
    
    for account in accounts:
        if account.get('Follower Count'):
            continue # skip already scraped accounts
        record_id = account.get('id')
        username = account.get('fields', {}).get('Username')
        
        if not username:
            print("No username found for account, skipping")
            continue
            
        print(f"\nProcessing info for {username}")
        
        # Get Instagram user info
        user_info = get_user_info(username)
        if not user_info or 'data' not in user_info:
            print(f"Could not get user info for {username}")
            continue
        
        # Extract relevant info
        info = user_info['data']
        update_data = {
            "Bio": info.get('biography'),
            "Bio Link": info.get('external_url'),
            "Follower Count": info.get('follower_count'),
            "Following Count": info.get('following_count'),
        }
        
        # Update Airtable
        if update_account_info(record_id, update_data):
            print(f"Updated info for {username}")
        else:
            print(f"Failed to update info for {username}")

if __name__ == "__main__":
    process_female_business_info()
