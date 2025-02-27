from airtable import (
    fetch_business_targets,
    create_business_network_records,
    update_target_as_scraped,
    fetch_existing_business_network_accounts,
    update_target_pagination_token
)
from instagram import get_followers

def process_business_network():
    """
    Function to:
    1. Fetch business targets from Airtable
    2. Get followers for each target using saved pagination token if exists
    3. Save followers to network table in batches
    4. Update pagination token after each request
    5. Mark target as scraped when complete
    """
    
    # Get targets that haven't been scraped
    targets = fetch_business_targets()
    if not targets:
        print("No targets found needing network scrape")
        return
        
    print(f"Found {len(targets)} targets to process")
    
    # Get ALL existing network accounts for global deduplication
    existing_accounts = fetch_existing_business_network_accounts()
    # Create a set of all usernames across all targets
    all_existing_usernames = set(account.get('fields', {}).get('Username') for account in existing_accounts)
    print(f"Found {len(all_existing_usernames)} existing unique accounts in database")
    
    # Track all usernames seen across all targets in this run
    global_seen_usernames = set()
    
    for target in targets:
        target_record_id = target.get('id')
        username = target.get('fields', {}).get('Username')
        # Get saved pagination token if exists
        pagination_token = target.get('fields', {}).get('Last Pagination Token')
        
        if not username:
            print("No username found for target, skipping")
            continue
            
        print(f"\nProcessing followers for {username}")
        if pagination_token:
            print(f"Continuing from previous pagination token: {pagination_token}")
        
        # Track followers for batch processing
        current_batch = []
        total_followers_added = 0
        batch_size = 100  # Process in larger batches for efficiency
        
        while True:
            # Get followers data
            followers_data = get_followers(username, pagination_token)
            if not followers_data or 'data' not in followers_data:
                print(f"No followers data returned for {username}")
                break
            
            # Save pagination token immediately after each request
            new_pagination_token = followers_data.get('pagination_token')
            if update_target_pagination_token(target_record_id, new_pagination_token):
                pagination_token = new_pagination_token
                if pagination_token:
                    print(f"Saved new pagination token: {pagination_token[:30]}...")
            else:
                print("Failed to save pagination token")
            
            # Process followers
            for follower in followers_data['data'].get('items', []):
                follower_username = follower.get('username')
                
                # Skip if username exists in database or has been seen in this run
                if follower_username in all_existing_usernames or follower_username in global_seen_usernames:
                    print(f"Username {follower_username} already exists in database or current run")
                    continue
                
                # Add username to global seen set
                global_seen_usernames.add(follower_username)
                
                follower_data = {
                    "fields": {
                        "Username": follower_username,
                        "Full Name": follower.get('full_name'),
                        "Profile Url": f"https://instagram.com/{follower_username}",
                        "Targets (Business)": [target_record_id],
                        "Pk Id": follower.get('id'),
                        "Is Private": follower.get('is_private'),
                        "Is Verified": follower.get('is_verified'),
                        "Pfp Url": follower.get('profile_pic_url')
                    }
                }
                current_batch.append(follower_data)
                
                # If batch is full, send to Airtable
                if len(current_batch) >= batch_size:
                    if create_business_network_records(current_batch):
                        total_followers_added += len(current_batch)
                        print(f"Added batch of {len(current_batch)} followers. Total for {username}: {total_followers_added}")
                        current_batch = []  # Clear the batch
                    else:
                        print("Error adding batch to Airtable, stopping process")
                        return
            
            # Check for pagination token
            if not pagination_token:
                print("No more pages to fetch")
                break
                
            print(f"Fetching next page...")
        
        # Send any remaining followers in the final batch
        if current_batch:
            if create_business_network_records(current_batch):
                total_followers_added += len(current_batch)
                print(f"Added final batch of {len(current_batch)} followers. Total for {username}: {total_followers_added}")
            else:
                print("Error adding final batch to Airtable")
                return
        
        # Clear pagination token and mark as scraped when done
        if update_target_pagination_token(target_record_id, None) and update_target_as_scraped(target_record_id):
            print(f"Marked {username} as scraped. Total followers added: {total_followers_added}")
        else:
            print(f"Error marking {username} as scraped")

if __name__ == "__main__":
    process_business_network()
