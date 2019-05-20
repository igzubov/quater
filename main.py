import time
import requests

from ccxt import binance, bitstamp, bitfinex, coinbasepro, bitmex, hitbtc2, kraken, bittrex, huobipro

# API KEYS ZONE
from datetime import datetime, timedelta

from dateutil import parser

BITMEX_API_KEY = 'f'
BITMEX_API_SECRET = ''

exchanges = {'binance': binance(), 'bitstamp': bitstamp(), 'bitfinex': bitfinex(), 'coinbase': coinbasepro(),
             'bitmex': bitmex(), 'hitbtc': hitbtc2(), 'kraken': kraken(), 'bittrex': bittrex(), 'huobi': huobipro()}
symbols = {'binance': 'BTC/USDC', 'bitstamp': 'BTC/USD', 'bitfinex': 'BTC/USD', 'coinbase': 'BTC/USD',
           'bitmex': 'BTC/USD', 'hitbtc': 'BTC/USDT', 'kraken': 'BTC/USD', 'bittrex': 'BTC/USD', 'huobi': 'BTC/USDT'}


# use external website to get bitstamp volume historical data
def get_bitstamp_vol():
    res = requests.get(
        'https://data.bitcoinity.org/export_data.csv?c=c&data_type=volume&exchange=bitstamp&r=hour&t=b&timespan=24h').text
    res = res.split('\n')[2:-1]
    data = []
    for line in res:
        candle = line.split(',')
        date = parser.parse(candle[0])
        timestamp = int(date.timestamp() * 1000)
        vol = float(candle[2])
        data.append([timestamp, 0, 0, 0, 0, vol])
    data = data[4:]
    return data


# changing hours to UTC
def fix_bitmex_vol(bitmex_ohlcv):
    bitmex_ohlcv = bitmex_ohlcv[1:]
    for candle in bitmex_ohlcv:
        old_date = datetime.fromtimestamp(candle[0]/1000)
        new_date = old_date - timedelta(hours=1)
        candle[0] = int(new_date.timestamp() * 1000)
    return bitmex_ohlcv


# get OHLCV for all exchanges for 20 last hours
def get_data(exchanges, symbols):
    to = datetime.now() - timedelta(hours=20)
    to = to.timestamp() * 1000
    ohlcv = {}
    for k, v in exchanges.items():
        ohlcv[k] = v.fetch_ohlcv(symbols[k], '1h', int(to))
        if k == 'bitstamp':
            last_candle = ohlcv[k][-1]
            ohlcv[k] = get_bitstamp_vol()
            ohlcv[k].append(last_candle)
        if k == 'bitmex':
            ohlcv[k] = fix_bitmex_vol(ohlcv[k])
        # print(k, len(ohlcv[k]), ohlcv[k])
    return ohlcv


# print OHLCV for last hour
def print_last_ohlcv(ohlcv):
    for k, v in ohlcv.items():
        print(k, 'OHLCV', ohlcv[k][-1])


# calculate sum
def calculate_sum(ohlcv):
    sum = [0] * 20
    for i in range(20):
        for val in ohlcv.values():
            sum[i] += val[i][5]
    return sum


ohlcv = get_data(exchanges, symbols)

print_last_ohlcv(ohlcv)

sums = calculate_sum(ohlcv)

sma = sum(sums) / len(sums)
print('SMA', sma)




# print(to)
# get_volumes(to)
# res = exch.fetch_ohlcv('BTC/USDC', '1h', int(to))
# print(res)
# bt = bitmex()
# print(time.time())

# print(bt.fetch_ohlcv('BTC/USD', '1h', int(to)))
