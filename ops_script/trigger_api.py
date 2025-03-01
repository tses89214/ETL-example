import requests
import os

# Data Processor API configuration
api_endpoint = 'http://localhost:5000/process_sales_data'


def trigger_api(api_endpoint):
    try:
        # Trigger API
        response = requests.get(api_endpoint)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)

        print(f"API triggered successfully. Response: {response.text}")
    except Exception as e:
        print(f"Error triggering API: {e}")
        return False
    return True


if __name__ == "__main__":
    if trigger_api(api_endpoint):
        print("API triggered successfully")
    else:
        print("API trigger failed")
