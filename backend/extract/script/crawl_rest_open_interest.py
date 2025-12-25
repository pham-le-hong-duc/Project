import pandas as pd
import requests
import json

def  get_open_interest_data(url, params, name):
    r = requests.get(url, params=params)
    r.raise_for_status()
    data = r.json().get('data', [])
    d1=pd.to_datetime(pd.to_numeric(data[-1][0]),unit='ms').strftime("%Y_%m_%d")
    d2=pd.to_datetime(pd.to_numeric(data[0][0]),unit='ms').strftime("%Y_%m_%d")
    path=f'/mnt/d/learn/DE/Semina_project/backend/extract/data/raw/oi/crawl_{name}_from{d1}_to_{d2}.json'
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Extracted {len(data)} regions and exchanges.")
def full_open_interest_data(currency,period):
    api_url=[
        (
            "https://www.okx.com/api/v5/rubik/stat/contracts/open-interest-volume",
            {"ccy": currency, "period": period},
            'open-interest-volume'
        ),
        (
            "https://www.okx.com/api/v5/rubik/stat/contracts/long-short-account-ratio",
            {"ccy": currency, "period": period},
            'long-short-account-ratio'
        ),
        (
            "https://www.okx.com/api/v5/rubik/stat/taker-volume",
            {"ccy": currency, "instType": "SPOT", "period": period},
            'taker-volume'
        )
    ]
    for url,params,name in api_url:
        get_open_interest_data(url, params, name)
full_open_interest_data('BTC','1H')