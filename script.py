import csv
import os
import requests
from dotenv import load_dotenv
load_dotenv()


# "source pythonenv/bin/activate" -> to activate the virtual terminal

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
LIMIT = 1000

url = (
    "https://api.massive.com/v3/reference/tickers"
    f"?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}"
)

response = requests.get(url)
# print(response.json())

tickers = []

data = response.json()

for item in data["results"]:
    tickers.append(item)

while 'next_url' in data:
    print("requesting next page", data['next_url'])
    response = requests.get(data['next_url'] + f"&apiKey={POLYGON_API_KEY}")
    data = response.json()
    print(data)
    # for item in data['results']:
    #     tickers.append(item)

example_ticker = {
    "ticker": "GREK",
    "name": "Global X MSCI Greece ETF",
    "market": "stocks",
    "locale": "us",
    "primary_exchange": "ARCX",
    "type": "ETF",
    "active": True,
    "currency_name": "usd",
    "cik": "0001432353",
    "composite_figi": "BBG0029YL8G3",
    "share_class_figi": "BBG0029YL962",
    "last_updated_utc": "2025-11-13T07:05:58.580359036Z",
}

csv_headers = list(example_ticker.keys())
output_path = os.path.join(os.getcwd(), "tickers.csv")

with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=csv_headers)
    writer.writeheader()
    for ticker in tickers:
        writer.writerow({key: ticker.get(key) for key in csv_headers})

print(f"Wrote {len(tickers)} tickers to {output_path}")