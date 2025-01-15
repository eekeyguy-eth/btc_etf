import requests
import json
import csv
from io import StringIO
from datetime import datetime, timedelta

def get_daily_eth_exported():
    # API endpoint
    url = "https://api.growthepie.xyz/v1/eim/eth_exported.json"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        daily_data = data['data']['chart']['total']['daily']['data']
        
        # Calculate timestamp for 1 year ago
        one_year_ago = datetime.now() - timedelta(days=365)
        one_year_ago_timestamp = int(one_year_ago.timestamp() * 1000)
        
        # Filter and format data for last year
        daily_eth = [
            {
                'date': datetime.fromtimestamp(entry[0]/1000).strftime('%Y-%m-%d'),
                'eth_exported': round(entry[2], 2),
                'usd_value': round(entry[1], 2)
            }
            for entry in daily_data
            if entry[0] >= one_year_ago_timestamp
        ]
        
        daily_eth.sort(key=lambda x: x['date'])
        return daily_eth
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None

def convert_to_csv(data):
    csv_file = StringIO()
    fieldnames = ['date', 'eth_exported', 'usd_value']
    
    csv_writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    csv_writer.writeheader()
    csv_writer.writerows(data)
    
    csv_data = csv_file.getvalue()
    csv_file.close()
    return csv_data

def upload_to_dune(csv_data):
    dune_upload_url = "https://api.dune.com/api/v1/table/upload/csv"
    
    payload = json.dumps({
        "data": csv_data,
        "description": "Daily ETH Exported on L1 Data",
        "table_name": "eth_exported_l1_daily",  # Your desired table name in Dune
        "is_private": False
    })
    
    headers = {
        'Content-Type': 'application/json',
        'X-DUNE-API-KEY': 'BbxP6Oq2RHQS8nJurQlMfXWsovZNIrro'  # Replace with your Dune API key
    }
    
    try:
        response = requests.post(dune_upload_url, headers=headers, data=payload)
        response.raise_for_status()
        print("Successfully uploaded to Dune!")
        print(response.json())
    except requests.exceptions.RequestException as e:
        print(f"Error uploading to Dune: {e}")
        print(f"Response: {response.text if 'response' in locals() else 'No response'}")

def main():
    # Fetch ETH export data
    daily_data = get_daily_eth_exported()
    
    if daily_data:
        # Convert to CSV
        csv_data = convert_to_csv(daily_data)
        
        # Save locally (optional)
        with open('eth_exported_data.csv', 'w') as f:
            f.write(csv_data)
        print("Data saved locally to 'eth_exported_data.csv'")
        
        # Upload to Dune
        upload_to_dune(csv_data)
    else:
        print("Failed to fetch data")

if __name__ == "__main__":
    main()
