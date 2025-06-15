import os
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pandas as pd
import json
from src.settings import data_dir, log_dir, raw_data, date
import logging
load_dotenv()

logging.basicConfig(
    filename=os.path.join(log_dir, 'fetch_exchange_rate.log'),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def fetch_exchange_rate():

    print(f'Date : {date}')
    url = f"https://api.currencyfreaks.com/v2.0/rates/latest?apikey={os.getenv('CURRENCY_FREAK_API_KEY')}"
    print(f"Sending GET request to {url}")
    logging.info(f"Sending GET request to {url}")

    try:
        response = requests.get(url)
        response.raise_for_status() # this line mean if not status code = 200 will error kub
        res = response.json()

        if "rates" in res: # check for rates response
            save_path = os.path.join(raw_data, f'exchange_rate_{date}.json')
            with open(save_path, 'w') as f:
                json.dump(res, f, indent=4)
            logging.info(f'Saved exchange rate data to {save_path}')
            print(f'Saved exchange rate data to {save_path}')
        else:
            logging.error("API responded but missing 'rates' key")
            print("API responded but missing 'rates' key")

    except requests.exceptions.RequestException as e:
        logging.error(f'API Requests Failed : {e}')
        print(f'API Requests Failed : {e}')


if __name__ == "__main__":
    fetch_exchange_rate()