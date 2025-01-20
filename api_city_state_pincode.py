import requests
import pandas as pd
from pymongo import MongoClient

# MongoDB client setup
client = MongoClient('mongodb://localhost:27017/')
db = client["Alkem"]  # Replace with your MongoDB database name
collection = db["17k_city_state_pincode"] 

# Load data from the Excel file
data = pd.read_excel("C:\\Users\\Desk0012\\Downloads\\ALKEM_BATCH_5_FINAL.xlsx",sheet_name='Sheet2')

# Function to get location details
def get_location_details(record_id, address, api_key):
    # Define the endpoint
    endpoint = f"https://maps.googleapis.com/maps/api/geocode/json"
    
    # Set up parameters for the request
    params = {
        "address": address,
        "key": api_key
    }
    
    # Make the request
    response = requests.get(endpoint, params=params)
    
    # Parse the response
    if response.status_code == 200:
        data = response.json()
        if data['status'] == 'OK':
            results = data['results'][0]
            location = results['geometry']['location']
            
            # Extract latitude and longitude
            latitude = location['lat']
            longitude = location['lng']
            
            # Initialize empty values for other components
            city, state, pincode, area = None, None, None, None
            
            # Extract components
            for component in results['address_components']:
                if "locality" in component['types']:
                    city = component['long_name']
                elif "administrative_area_level_1" in component['types']:
                    state = component['long_name']
                elif "postal_code" in component['types']:
                    pincode = component['long_name']
                elif "sublocality" in component['types']:
                    area = component['long_name']
                    
            # Return a dictionary of extracted details
            return {
                "record_id": record_id,
                "address": address,
                "city": city,
                "state": state,
                "pincode": pincode,
                "area": area,
                "latitude": latitude,
                "longitude": longitude
            }
        else:
            print(f"Error: {data['status']}")
    else:
        print("Failed to connect to the API")
    
    return None

# API key for Google Geocoding
# give your api key here
api_key = ""

# Loop through each row in the data and fetch location details
for i, row in data.iterrows():
    #print(f"Processing Record ID: {row['Record ID']}")
    
    
    # Fetch location details
    details = get_location_details(row['Record ID'], row['Address'], api_key)
    
    # Insert details into MongoDB if successfully fetched
    if details:
        collection.insert_one(details)
        #print(f"Inserted: {details}")
    else:
        print(f"Failed to fetch details for Record ID: {row['Record ID']}")
    
    # Optional: Break after one iteration for testing
    # break  # Remove this line to process all records
