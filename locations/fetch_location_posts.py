from airtable import (
    fetch_existing_locations, 
    fetch_existing_location_posts, 
    create_location_post_records
)
from instagram import get_location_posts
from misc_functions import convert_taken_at_to_iso

def process_location_posts():
    """
    Function to:
    1. Fetch locations from Airtable
    2. Get posts for each location from Instagram API
    3. Compare with existing posts to avoid duplicates (by username)
    4. Save new posts to Airtable
    """

    # Get locations from Airtable
    locations = fetch_existing_locations()
    if not locations:
        print("No locations found in Airtable")
        return

    # Get ALL existing posts for global deduplication
    existing_posts = fetch_existing_location_posts()
    # Create a set of all usernames across all locations
    all_existing_usernames = set(post.get('fields', {}).get('Username') for post in existing_posts)
    print(f"Found {len(all_existing_usernames)} existing unique usernames in database")

    # Track all usernames seen across all locations in this run
    global_seen_usernames = set()

    # Process each location
    for location in locations:
        location_name = location.get('fields', {}).get('Location Name')
        print(location_name)
        
        # Get current post count and calculate remaining needed
        current_post_count = location.get('fields', {}).get('Total Posts Scraped For Location', 0) or 0
        if current_post_count >= 300:
            print(f'Skipping {location_name} as 300 posts already scraped.')
            continue
            
        posts_needed = 300 - current_post_count
        print(f"Need to scrape {posts_needed} more posts for {location_name}")
        
        location_record_id = location.get('id')
        location_id = location.get('fields', {}).get('Location Id')
        
        if not location_id:
            print('No location id, skipping record')
            continue
            
        print(f"\nProcessing posts for location: {location_name}")
        
        pagination_token = None
        posts_scraped_this_run = 0
        
        while True:
            # Check if we've reached our target
            if posts_scraped_this_run >= posts_needed:
                print(f"Reached target of {posts_needed} new posts")
                break
                
            # Get posts data from Instagram
            posts_data = get_location_posts(location_id, pagination_token)
            if not posts_data or 'data' not in posts_data:
                print(f"No posts data returned for {location_name}")
                break
            
            # Process new posts
            new_posts = []
            for post in posts_data['data'].get('items', []):
                # Check if we've reached our target
                if posts_scraped_this_run >= posts_needed:
                    break
                    
                user_info = post.get('user', {})
                username = user_info.get('username')
                
                # Skip if username exists in database or has been seen in this run
                if username in all_existing_usernames or username in global_seen_usernames:
                    print(f"Username {username} already exists in database or current run")
                    continue
                
                # Add username to global seen set
                global_seen_usernames.add(username)
                
                if post.get('caption'): 
                    caption_info = post.get('caption', {})
                
                # Create new post record
                new_post = {
                    "fields": {
                        "Post Id": post.get('id'),
                        "Username": username,
                        "Full Name": user_info.get('full_name'),
                        "Pfp Url": user_info.get('profile_pic_url'),
                        "Pk Id": user_info.get('id'),
                        "Locations": [location_record_id],
                        "Posted Date": convert_taken_at_to_iso(post.get('taken_at')),
                        "Post Caption": caption_info.get('text') if post.get('caption') else None
                    }
                }
                new_posts.append(new_post)
                posts_scraped_this_run += 1
            
            if new_posts:
                # Create records in Airtable
                create_location_post_records(new_posts)
                print(f"Added {len(new_posts)} new posts for {location_name}")
                print(f"Total posts scraped this run: {posts_scraped_this_run}")
            
            # Check if we've reached our target
            if posts_scraped_this_run >= posts_needed:
                print(f"Reached target of {posts_needed} new posts")
                break
            
            # Check for pagination token
            pagination_token = posts_data.get('pagination_token')
            if not pagination_token:
                print("No more pages to fetch")
                break
            
            print(f"Fetching next page with token: {pagination_token[:30]}...")

if __name__ == "__main__":
    process_location_posts()
