"""
This script triggers the data processor API.
"""

import logging
import requests

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Data Processor API configuration
API_URL = 'http://localhost:5000/process_sales_data'


def trigger_api(api_url):
    """
    Triggers the data processor API.

    Args:
        api_url (str): The URL of the API endpoint.

    Returns:
        bool: True if the API was triggered successfully, False otherwise.
    """
    try:
        # Trigger API
        response = requests.get(api_url, timeout=5, verify=False)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)

        logging.info("API triggered successfully. Response: %s", response.text)
    except requests.exceptions.RequestException as exc:
        logging.error("Error triggering API: %s", exc)
        return False
    return True


if __name__ == "__main__":
    if trigger_api(API_URL):
        logging.info("API triggered successfully")
    else:
        logging.error("API trigger failed")
