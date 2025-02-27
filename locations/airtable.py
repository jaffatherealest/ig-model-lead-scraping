import requests
from dotenv import load_dotenv
import os
import time  # Add this import at the top

# Load environment variables
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

# Get Airtable credentials from .env
AIRTABLE_API_KEY = os.getenv('AIRTABLE_API_KEY')
AIRTABLE_BASE_ID = os.getenv('AIRTABLE_BASE_ID')
AIRTABLE_LOCATIONS_TABLE = os.getenv('AIRTABLE_LOCATIONS_TABLE')
AIRTABLE_LOCATION_POSTS_TABLE = os.getenv('AIRTABLE_LOCATION_POSTS_TABLE')
AIRTABLE_FIRE_LOCATIONS_VIEW = os.getenv('AIRTABLE_FIRE_LOCATIONS_VIEW')
AIRTABLE_BUSINESS_TARGETS_TABLE = os.getenv('AIRTABLE_BUSINESS_TARGETS_TABLE')
AIRTABLE_BUSINESS_NETWORK_TABLE = os.getenv('AIRTABLE_BUSINESS_NETWORK_TABLE')

def fetch_existing_locations(offset=None, all_records=None):
    """
    Function to fetch all records from locations table
    """
    
    url = f'https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_LOCATIONS_TABLE}?view={AIRTABLE_FIRE_LOCATIONS_VIEW}' #NOTE: using 🔥 location view
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
        return fetch_existing_locations(data['offset'], all_records)
    else:
        return all_records

def create_location_records(records):
    """
    Function to create new location records in Airtable
    Handles batches of 10 records at a time (Airtable limit)
    """
    
    url = f'https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_LOCATIONS_TABLE}'
    headers = {
        'Authorization': f'Bearer {AIRTABLE_API_KEY}',
        'Content-Type': 'application/json'
    }
    
    # Split records into batches of 10
    batch_size = 10
    total_created = 0
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        payload = {
            "records": batch
        }
        
        try:
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()
            total_created += len(batch)
            print(f"Successfully created batch of {len(batch)} records")
        except requests.exceptions.RequestException as e:
            print(f"Error creating location records batch: {e}")
            if hasattr(e.response, 'text'):
                print(f"Response text: {e.response.text}")
            return False
    
    print(f"Total records created: {total_created}")
    return True

def fetch_existing_location_posts(offset=None, all_records=None):
    """
    Function to fetch all records from location posts table
    """
    
    url = f'https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_LOCATION_POSTS_TABLE}'
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
        return fetch_existing_location_posts(data['offset'], all_records)
    else:
        return all_records

def create_location_post_records(records):
    """
    Function to create new location post records in Airtable
    Handles batches of 10 records at a time (Airtable limit)
    """
    
    url = f'https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_LOCATION_POSTS_TABLE}'
    headers = {
        'Authorization': f'Bearer {AIRTABLE_API_KEY}',
        'Content-Type': 'application/json'
    }
    
    # Split records into batches of 10
    batch_size = 10
    total_created = 0
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        payload = {
            "records": batch
        }
        
        try:
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()
            total_created += len(batch)
            print(f"Successfully created batch of {len(batch)} post records")
        except requests.exceptions.RequestException as e:
            print(f"Error creating location post records batch: {e}")
            if hasattr(e.response, 'text'):
                print(f"Response text: {e.response.text}")
            return False
    
    print(f"Total post records created: {total_created}")
    return True

def fetch_location_posts_without_gender(offset=None, all_records=None):
    """
    Function to fetch posts that haven't been gender checked
    """
    
    url = f'https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_LOCATION_POSTS_TABLE}'
    headers = {
        'Authorization': f'Bearer {AIRTABLE_API_KEY}',
    }

    if all_records is None:
        all_records = []
        
    query_params = {
        'filterByFormula': '{Gender Checked} != TRUE()'
    }
    if offset:
        query_params['offset'] = offset

    response = requests.get(url, headers=headers, params=query_params)
    response.raise_for_status()
    data = response.json()
    
    all_records.extend(data.get('records', []))
    
    if 'offset' in data:
        return fetch_location_posts_without_gender(data['offset'], all_records)
    else:
        return all_records

def update_post_gender(record_id, update_data):
    """
    Function to update gender information for a post in Location Posts table
    """
    
    url = f'https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_LOCATION_POSTS_TABLE}/{record_id}'
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
        print(f"Error updating post gender: {e}")
        return False

def fetch_business_targets(offset=None, all_records=None):
    """
    Function to fetch all business target records from Airtable
    """
    
    url = f'https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_BUSINESS_TARGETS_TABLE}'
    headers = {
        'Authorization': f'Bearer {AIRTABLE_API_KEY}',
    }

    if all_records is None:
        all_records = []
        
    query_params = {
        'filterByFormula': '{Network Scraped} != TRUE()'  # Only get unscraped targets
    }
    if offset:
        query_params['offset'] = offset

    response = requests.get(url, headers=headers, params=query_params)
    response.raise_for_status()
    data = response.json()
    
    all_records.extend(data.get('records', []))
    
    if 'offset' in data:
        return fetch_business_targets(data['offset'], all_records)
    else:
        return all_records

def create_business_network_records(records):
    """
    Function to create new business network records in Airtable
    Handles batches of 10 records at a time
    """
    
    url = f'https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_BUSINESS_NETWORK_TABLE}'
    headers = {
        'Authorization': f'Bearer {AIRTABLE_API_KEY}',
        'Content-Type': 'application/json'
    }
    
    batch_size = 10
    total_created = 0
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        payload = {
            "records": batch
        }
        
        try:
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()
            total_created += len(batch)
            print(f"Successfully created batch of {len(batch)} network records")
        except requests.exceptions.RequestException as e:
            print(f"Error creating network records batch: {e}")
            if hasattr(e.response, 'text'):
                print(f"Response text: {e.response.text}")
            return False
    
    print(f"Total network records created: {total_created}")
    return True

def update_target_as_scraped(record_id):
    """
    Function to mark a target as scraped
    """
    url = f'https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_BUSINESS_TARGETS_TABLE}/{record_id}'
    headers = {
        'Authorization': f'Bearer {AIRTABLE_API_KEY}',
        'Content-Type': 'application/json'
    }
    
    payload = {
        "fields": {
            "Network Scraped": True
        }
    }
    
    try:
        response = requests.patch(url, headers=headers, json=payload)
        response.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error updating target as scraped: {e}")
        return False

def fetch_existing_business_network_accounts(offset=None, all_records=None):
    """
    Function to fetch all existing network accounts from Airtable
    Includes rate limiting (5 requests per second max)
    """
    
    url = f'https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_BUSINESS_NETWORK_TABLE}'
    headers = {
        'Authorization': f'Bearer {AIRTABLE_API_KEY}',
    }

    if all_records is None:
        all_records = []
        
    query_params = {}
    if offset:
        query_params['offset'] = offset

    try:
        response = requests.get(url, headers=headers, params=query_params)
        response.raise_for_status()
        data = response.json()
        
        all_records.extend(data.get('records', []))
        
        if 'offset' in data:
            print(f"Fetched {len(data.get('records', []))} records, waiting before next request...")
            time.sleep(0.25)  # Wait 250ms between requests (4 requests per second to be safe)
            return fetch_existing_business_network_accounts(data['offset'], all_records)
        else:
            return all_records
            
    except requests.exceptions.RequestException as e:
        print(f"Error fetching records, retrying in 2 seconds: {e}")
        time.sleep(2)  # Wait longer on error
        return fetch_existing_business_network_accounts(offset, all_records)
    
def update_business_network_gender(record_id, update_data):
    """
    Function to update gender information for a account in Business Network table
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
        print(f"Error updating post gender: {e}")
        return False

def fetch_business_network_without_gender(offset=None, all_records=None):
    """
    Function to fetch business network accounts that haven't been gender checked
    """
    
    url = f'https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_BUSINESS_NETWORK_TABLE}'
    headers = {
        'Authorization': f'Bearer {AIRTABLE_API_KEY}',
    }

    if all_records is None:
        all_records = []
        
    query_params = {
        'filterByFormula': '{Gender Checked} != TRUE()'
    }
    if offset:
        query_params['offset'] = offset

    response = requests.get(url, headers=headers, params=query_params)
    response.raise_for_status()
    data = response.json()
    
    all_records.extend(data.get('records', []))
    
    if 'offset' in data:
        return fetch_business_network_without_gender(data['offset'], all_records)
    else:
        return all_records

def update_target_pagination_token(record_id, pagination_token):
    """
    Function to update the Last Pagination Token field for a target
    """
    url = f'https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_BUSINESS_TARGETS_TABLE}/{record_id}'
    headers = {
        'Authorization': f'Bearer {AIRTABLE_API_KEY}',
        'Content-Type': 'application/json'
    }
    
    payload = {
        "fields": {
            "Last Pagination Token": pagination_token
        }
    }
    
    try:
        response = requests.patch(url, headers=headers, json=payload)
        response.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error updating pagination token: {e}")
        return False

if __name__ == "__main__":
    locations = fetch_existing_locations()
    for location in locations:
        loc_name = location.get('fields', {}).get('Location Name')
        print(loc_name)