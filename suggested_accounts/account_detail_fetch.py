import requests
import time
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Airtable configuration
AIRTABLE_API_KEY = os.getenv('AIRTABLE_API_KEY')
BASE_ID = os.getenv('AIRTABLE_BASE_ID')
NETWORK_TABLE = os.getenv('AIRTABLE_NETWORK_TABLE')

AIRTABLE_API_URL = f"https://api.airtable.com/v0/{BASE_ID}"

AIRTABLE_HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
    "Content-Type": "application/json"
}

def get_account_details(username_or_id):
    url = "https://instagram-scraper-api2.p.rapidapi.com/v1/info"
    
    querystring = {"username_or_id_or_url": username_or_id}
    
    headers = {
        "x-rapidapi-key": os.getenv('RAPIDAPI_KEY'),
        "x-rapidapi-host": os.getenv('RAPIDAPI_HOST')
    }
    
    try:
        response = requests.get(url, headers=headers, params=querystring)
        response.raise_for_status()
        
        data = response.json()
        if 'data' in data:
            account_data = data['data']
            return {
                'follower_count': account_data.get('follower_count'),
                'following_count': account_data.get('following_count'),
                'media_count': account_data.get('media_count'),
                'bio': account_data.get('biography'),
                'bio_link': account_data.get('external_url'),
                'email': account_data.get('public_email'),
                'phone_number': account_data.get('contact_phone_number')
            }
        return None
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching account details: {e}")
        return None

def fetch_unprocessed_network_accounts(offset=None, all_records=None):
    """Fetch network accounts that haven't had their details processed yet"""
    if all_records is None:
        all_records = []
        
    url = f"{AIRTABLE_API_URL}/{NETWORK_TABLE}"
    
    params = {}
    if offset:
        params['offset'] = offset

    try:
        response = requests.get(url, headers=AIRTABLE_HEADERS, params=params)
        response.raise_for_status()
        data = response.json()
        
        all_records.extend(data.get('records', []))
        
        if 'offset' in data:
            return fetch_unprocessed_network_accounts(data['offset'], all_records)
        return all_records
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching network accounts: {e}")
        return all_records

def update_account_details(record_id, details):
    """Update network record with account details"""
    url = f"{AIRTABLE_API_URL}/{NETWORK_TABLE}/{record_id}"
    
    payload = {
        "fields": {
            "follower_count": details.get('follower_count'),
            "following_count": details.get('following_count'),
            "media_count": details.get('media_count'),
            "bio": details.get('biography'),
            "bio_link": details.get('external_url'),
            "email": details.get('public_email'),
            'phone_number': details.get('contact_phone_number'),
            "details_fetched": True
        }
    }
    
    try:
        response = requests.patch(url, headers=AIRTABLE_HEADERS, json=payload)
        response.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error updating account details: {e}")
        return False

def process_network_accounts():
    unprocessed_records = fetch_unprocessed_network_accounts()
    
    for record in unprocessed_records:
        username = record.get('fields', {}).get('username')
        pk_id = record.get('fields', {}).get('pk_id')
        record_id = record.get('id')
        
        if username or pk_id:
            print(f"\nProcessing account: {username or pk_id}")
            details = get_account_details(username or pk_id)
            
            if details:
                if update_account_details(record_id, details):
                    print(f"Updated details for: {username or pk_id}")
                
                # Add delay to avoid rate limits
                time.sleep(1)
            
            print(f"Completed processing {username or pk_id}")

if __name__ == "__main__":
    process_network_accounts()