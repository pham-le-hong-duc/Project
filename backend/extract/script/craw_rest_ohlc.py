import requests, json, os
import pandas as pd
from datetime import datetime, timedelta
import time
def crawl_his_ohlc(instrument_id, timeframe, limit,after=None):
    url="https://www.okx.com/api/v5/market/candles"
    params = {'instId': instrument_id, 'bar': timeframe, 'limit': limit}
    if after: params['after'] = after
    response = requests.get(url, params=params)
    data = response.json().get('data', [])
    return data

def crawl_his_ohlc_full(start_date, end_date):
    ohlc = []
    start_ts = int(start.timestamp() * 1000)
    init = crawl_his_ohlc('BTC-USDT-SWAP', '1H',300,None)
    oldest=int(init[-1][0])
    ohlc.extend(init)
    #cur_ts = int(oldest.timestamp() * 1000) - 1
    while oldest > start_ts:
            data = crawl_his_ohlc('BTC-USDT-SWAP', '1H',300,oldest)

            if not data:
                print('empty data')
                break
            #df = df[df.index >= start_date]
            ohlc.extend(data)
            oldest = int(data[-1][0])
            #cur_ts = int(oldest.timestamp() * 1000)

            print(f"get data to {pd.to_datetime(oldest,unit='ms').strftime('%Y-%m-%d')}")
            time.sleep(0.5) # condition avoid ratelimit
    d1=pd.to_datetime(pd.to_numeric(ohlc[-1][0]),unit='ms').strftime("%Y_%m_%d")
    d2=pd.to_datetime(pd.to_numeric(ohlc[0][0]),unit='ms').strftime("%Y_%m_%d")
    path=f'/mnt/d/learn/DE/Semina_project/backend/extract/data/raw/crawl_ohlc_from{d1}_to_{d2}.json'
    with open(path, "w", encoding="utf-8") as f:
        json.dump(ohlc, f, ensure_ascii=False, indent=2)
    print(f"Extracted {len(ohlc)} regions and exchanges.")
end=datetime.now()
start = end - timedelta(days=30)
crawl_his_ohlc_full(start,end)

