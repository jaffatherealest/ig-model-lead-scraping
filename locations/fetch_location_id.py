from airtable import fetch_existing_locations, create_location_records
from instagram import get_location_ids

def populate_location_data():
    """
    Function to:
    1. Get search term from user input
    2. Fetch location data from Instagram API
    3. Compare with existing locations in Airtable
    4. Save new unique locations to Airtable
    """
    
    # Get search term from user
    search_term = input("Enter location to search (e.g. 'London', 'Bali'): ")
    
    # Fetch data from Instagram API
    location_data = get_location_ids(search_term)
    if not location_data or 'data' not in location_data:
        print("No data returned from Instagram API")
        return
    
    # Fetch existing locations from Airtable
    existing_locations = fetch_existing_locations()
    existing_location_ids = [loc.get('fields', {}).get('Id') for loc in existing_locations]
    
    # Process new locations
    new_locations = []
    for location in location_data['data'].get('items', []):
        location_id = location.get('id')
        location_name = location.get('name')
        
        # Skip if location already exists
        if location_id in existing_location_ids:
            print(f"Location {location.get('name')} already exists in database")
            continue
            
        # Create new location record
        new_location = {
            "fields": {
                "Id": location_id,
                "Location Name": location_name,
                "Search Term": search_term
            }
        }
        new_locations.append(new_location)
    
    if new_locations:
        # Create records in Airtable
        create_location_records(new_locations)
        print(f"Added {len(new_locations)} new locations to database")
    else:
        print("No new locations to add")

if __name__ == "__main__":
    populate_location_data()