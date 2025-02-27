import os
import requests
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

RAPIDAPI_KEY = os.getenv('RAPIDAPI_KEY')
RAPIDAPI_HOST = os.getenv('RAPIDAPI_HOST')

def get_location_ids(location_name):
    """
    Function to fetch location IDs from Instagram API for a given location name
    Returns raw API response data
    """
    
    url = "https://instagram-scraper-api2.p.rapidapi.com/v1/search_location"
    
    headers = {
        "x-rapidapi-key": os.getenv('RAPIDAPI_KEY'),
        "x-rapidapi-host": os.getenv('RAPIDAPI_HOST')
    }
    
    query_params = {
        "search_query": location_name
    }
    
    try:
        response = requests.get(url, headers=headers, params=query_params)
        response.raise_for_status()  # Raises a HTTPError if the status is 4XX, 5XX
        return response.json()
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching location data: {e}")
        return None

def get_location_posts(location_id, pagination_token=None):
    """
    Function to fetch posts from a specific location using Instagram API
    Returns raw API response data for analysis
    """
    
    url = "https://instagram-scraper-api2.p.rapidapi.com/v1/location_posts"
    
    headers = {
        "x-rapidapi-key": os.getenv('RAPIDAPI_KEY'),
        "x-rapidapi-host": os.getenv('RAPIDAPI_HOST')
    }
    
    query_params = {
        "location_id": location_id
    }
    
    if pagination_token:
        query_params["pagination_token"] = pagination_token
    
    try:
        response = requests.get(url, headers=headers, params=query_params)
        response.raise_for_status()
        return response.json()
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching location posts: {e}")
        return None

def get_followers(username, pagination_token=None):
    """
    Function to fetch followers from Instagram API
    Handles pagination
    """
    
    url = "https://instagram-scraper-api2.p.rapidapi.com/v1/followers"
    
    headers = {
        "x-rapidapi-key": os.getenv('RAPIDAPI_KEY'),
        "x-rapidapi-host": os.getenv('RAPIDAPI_HOST')
    }
    
    query_params = {
        "username_or_id_or_url": username
    }
    
    if pagination_token:
        query_params["pagination_token"] = pagination_token
    
    try:
        response = requests.get(url, headers=headers, params=query_params)
        response.raise_for_status()
        return response.json()
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching followers: {e}")
        return None

def get_user_info(username):
    """
    Function to fetch detailed user info from Instagram API
    """
    url = "https://instagram-scraper-api2.p.rapidapi.com/v1/info"
    
    headers = {
        "x-rapidapi-key": os.getenv('RAPIDAPI_KEY'),
        "x-rapidapi-host": os.getenv('RAPIDAPI_HOST')
    }
    
    query_params = {
        "username_or_id_or_url": username
    }
    
    try:
        response = requests.get(url, headers=headers, params=query_params)
        response.raise_for_status()
        return response.json()
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching user info: {e}")
        return None