import requests
import time
import json
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Airtable configuration
AIRTABLE_API_KEY = os.getenv('AIRTABLE_API_KEY')
BASE_ID = os.getenv('AIRTABLE_BASE_ID')
SOURCE_TABLE = os.getenv('AIRTABLE_TARGETS_TABLE')
RESULTS_TABLE = os.getenv('AIRTABLE_NETWORK_TABLE')

AIRTABLE_API_URL = f"https://api.airtable.com/v0/{BASE_ID}"

# Airtable API headers
AIRTABLE_HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
    "Content-Type": "application/json"
}

def get_similar_accounts(username):
    # Remove any @ symbol if present
    username = username.strip('@').strip()
    
    url = "https://instagram-scraper-api2.p.rapidapi.com/v1/similar_accounts"
    
    querystring = {"username_or_id_or_url": username}
    
    headers = {
        "x-rapidapi-key": os.getenv('RAPIDAPI_KEY'),
        "x-rapidapi-host": os.getenv('RAPIDAPI_HOST')
    }
    
    try:
        response = requests.get(url, headers=headers, params=querystring)
        
        if response.status_code == 404:
            print(f"\nNo similar accounts found for @{username}")
            print("This might happen if:")
            print("- The account is private")
            print("- The account has very few followers")
            print("- The account is too new")
            print("\nTry another username with more followers or a public account.")
            return None
        elif response.status_code != 200:
            print(f"API Response Status Code: {response.status_code}")
            print(f"API Response Headers: {response.headers}")
            print(f"API Response Body: {response.text}")
            return None
            
        response.raise_for_status()
        
        data = response.json()
        similar_accounts = []
        
        if 'data' in data and 'items' in data['data']:
            for item in data['data']['items']:
                account_info = {
                    'username': item['username'],
                    'full_name': item['full_name'],
                    'pk_id': item['id'],
                    'private': item['is_private'],
                    'verified': item['is_verified'],
                    'pfp_url': item['profile_pic_url'],
                    'profile_url': f"https://instagram.com/{item['username']}"
                }
                similar_accounts.append(account_info)
            
            if not similar_accounts:
                print(f"\nNo similar accounts found for @{username}")
                return None
                
        return similar_accounts
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        if hasattr(e, 'response'):
            print(f"Response Status Code: {e.response.status_code}")
            print(f"Response Headers: {e.response.headers}")
            print(f"Response Body: {e.response.text}")
        return None

def fetch_unprocessed_targets(offset=None, all_records=None):
    """
    Function to fetch unprocessed target accounts from targets table
    filterByFormula = Processed = 0
    """

    url = f'https://api.airtable.com/v0/{BASE_ID}/{SOURCE_TABLE}'
    headers = {
        'Authorization': f'Bearer {AIRTABLE_API_KEY}',
    }

    if all_records is None:
        all_records = []
        
    query_params = {}
    if offset:
        query_params['offset'] = offset

    response = requests.get(url, headers=headers, params=query_params)
    response.raise_for_status()  # Raises a HTTPError if the status is 4XX, 5XX
    data = response.json()
    
    all_records.extend(data.get('records', []))
    
    # Airtable API will include an 'offset' if there are more records to fetch
    if 'offset' in data:
        return fetch_unprocessed_targets(data['offset'], all_records)
    else:
        return all_records

def fetch_existing_network(username, offset=None, all_records=None):
    """
    Function to fetch all records from network table
    """

    url = f'https://api.airtable.com/v0/{BASE_ID}/{RESULTS_TABLE}'
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
        return fetch_existing_network(username, data['offset'], all_records)
    else:
        # Check if username exists in any of the records
        return any(record.get('fields', {}).get('username') == username for record in all_records)

def create_result_record(account_data, source_username, record_id):
    """Create a new record in the results table"""
    url = f"{AIRTABLE_API_URL}/{RESULTS_TABLE}"
    
    payload = {
        "records": [{
            "fields": {
                "username": source_username,
                "full_name": account_data['full_name'],
                "pk_id": account_data['pk_id'],
                "private": account_data['private'],
                "verified": account_data['verified'],
                "pfp_url": account_data['pfp_url'],
                "profile_url": account_data['profile_url'],
                "Targets": [record_id]
            }
        }]
    }
    
    try:
        response = requests.post(url, headers=AIRTABLE_HEADERS, json=payload)
        response.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error creating record in Airtable: {e}")
        return False

def mark_as_processed(record_id):
    """Mark a source record as processed"""
    url = f"{AIRTABLE_API_URL}/{SOURCE_TABLE}/{record_id}"
    
    payload = {
        "fields": {
            "Processed": True
        }
    }
    
    try:
        response = requests.patch(url, headers=AIRTABLE_HEADERS, json=payload)
        response.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error marking record as processed: {e}")
        return False

def process_airtable_accounts():
    unprocessed_records = fetch_unprocessed_targets()
    
    for record in unprocessed_records:
        username = record.get('fields', {}).get('username')
        record_id = record.get('id')
        
        if username:
            print(f"\nProcessing username: {username}")
            similar_accounts = get_similar_accounts(username)
            
            if similar_accounts:
                for account in similar_accounts:
                    if not fetch_existing_network(account['username']):
                        if create_result_record(account, username, record_id):
                            print(f"Added similar account: {account['username']}")
                
                # Mark source record as processed
                if mark_as_processed(record_id):
                    print(f"Marked {username} as processed")
                
                # Add small delay to avoid rate limits
                time.sleep(1)
            
            print(f"Completed processing {username}")

if __name__ == "__main__":
    process_airtable_accounts()