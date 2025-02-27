import requests
import os
from dotenv import load_dotenv
from airtable import (
    fetch_location_posts_without_gender, 
    update_post_gender, 
    update_business_network_gender,
    fetch_business_network_without_gender
)

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

def get_gender_from_image(image_url):
    """
    Function to get gender prediction from profile picture URL using https://rapidapi.com/nyckel-nyckel-default/api/image-gender
    Currently not working!!
    """
    url = "https://image-gender.p.rapidapi.com/invoke"
    
    headers = {
        "x-rapidapi-key": os.getenv('RAPIDAPI_KEY'),
        "x-rapidapi-host": "image-gender.p.rapidapi.com",
        "Content-Type": "application/json"
    }
    
    payload = {
        "data": image_url
    }
    
    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error getting gender prediction: {e}")
        return None

def get_gender_from_image_picpurify(image_url):
    """
    Function to get gender prediction from profile picture URL using PicPurify API
    https://www.picpurify.com/api-services.html#single_image_api_doc
    """
    url = "https://www.picpurify.com/analyse/1.1"
    
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    
    payload = {
        'API_KEY': os.getenv('PICPURIFY_API_KEY'),
        'url_image': image_url,
        'task': 'face_gender_detection'
    }
    
    try:
        response = requests.post(url, data=payload, headers=headers)
        response.raise_for_status()
        result = response.json()
        
        if result.get('status') == 'success' and 'face_detection' in result:
            # Get first face result (assuming profile picture has one main face)
            faces = result['face_detection'].get('results', [])
            if faces:
                first_face = faces[0]
                gender_info = first_face.get('gender', {})
                return {
                    'labelName': gender_info.get('decision', '').capitalize(),  # 'male' or 'female'
                    'confidence': gender_info.get('confidence_score')
                }
            else:
                print("No faces detected in image")
                return None
        else:
            print(f"API error: {result.get('error', {}).get('errorMsg', 'Unknown error')}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"Error getting gender prediction from PicPurify: {e}")
        return None

def process_gender_labels():
    """
    Function to:
    1. Fetch posts without gender labels
    2. Get gender prediction for each profile picture
    3. Update Airtable with results
    """
    
    # Get posts that haven't been gender checked
    posts = fetch_business_network_without_gender()
    if not posts:
        print("No business network accounts found needing gender check")
        return
        
    print(f"Found {len(posts)} business network accounts needing gender check")
    
    for post in posts:
        record_id = post.get('id')
        pfp_url = post.get('fields', {}).get('Pfp Url')
        username = post.get('fields', {}).get('Username')
        
        if not pfp_url:
            print(f"No profile picture URL for {username}, skipping")
            continue
            
        print(f"\nProcessing gender for {username}")
        
        # Get gender prediction using PicPurify API
        gender_data = get_gender_from_image_picpurify(pfp_url)
            
        # Get gender from first (and likely only) face
        if gender_data and 'labelName' in gender_data:
                
            # Update Airtable with gender data
            update_data = {
                "Gender": gender_data.get('labelName'),
                "Gender Confidence": gender_data.get('confidence'),
                "Gender Checked": True
            }
                
            if update_business_network_gender(record_id, update_data):
                print(f"Updated gender for {username}")
            else:
                print(f"Failed to update gender for {username}")
        else:
            # No faces detected or API error
            update_data = {
                "Gender Checked": True,
                "No Face Detected": True
            }
            if update_business_network_gender(record_id, update_data):
                print(f"Marked {username} as checked - no faces detected")
            else:
                print(f"Failed to update no face detection status for {username}")

if __name__ == "__main__":
    process_gender_labels()