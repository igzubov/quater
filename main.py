import time
from random import randint

import requests

from ccxt import binance, bitstamp, bitfinex, coinbasepro, bitmex, hitbtc2, kraken, bittrex, huobipro

# API KEYS ZONE
from datetime import datetime, timedelta

from dateutil import parser

BITMEX_API_KEY = 'f'
BITMEX_API_SECRET = ''

# SL & TP LEVELS
SL_PERCENT = 5
TP_PERCENT = 3

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
    data = data[3:]
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
    to = datetime.now() - timedelta(hours=21)
    to = to.timestamp() * 1000
    ohlcv = {}
    for k, v in exchanges.items():
        ohlcv[k] = v.fetch_ohlcv(symbols[k], '1h', int(to))[:-1]
        if k == 'bitstamp':
            ohlcv[k] = get_bitstamp_vol()
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


def get_current_ohlc(exchanges):
    ohlcv = exchanges['bitmex'].fetch_ohlcv('BTC/USD', '1h', (datetime.now() - timedelta(hours=1)).timestamp() * 1000)[-1]
    return ohlcv[1], ohlcv[2], ohlcv[3], ohlcv[4]


def check_close_cond(exchanges, type, sl_level, tp_level):
    open, high, low, close = get_current_ohlc(exchanges)
    if type == 'long':
        if close >= tp_level:
            # print(str(datetime.now()), 'long hit tp')
            log('long hit tp')
            return True
        elif close <= sl_level:
            # print(str(datetime.now()), 'long hit sl')
            log('long hit sl')
            return True
    else:
        if close <= tp_level:
            # print(str(datetime.now()), 'short hit tp')
            log('short hit tp')
            return True
        elif close >= sl_level:
            # print(str(datetime.now()), 'short hit sl')
            log('short hit sl')
            return True
    return False


def log(data):
    with open('log.txt', 'a') as f:
        print(str(datetime.now()) + ' ' + str(data))
        f.write(str(datetime.now()) + ' ' + str(data) + '\n')


def main():
    while True:
        try:
            closed_order = False
            ohlcv = get_data(exchanges, symbols)
            # print_last_ohlcv(ohlcv)
            sums = calculate_sum(ohlcv)
            # print('Sum of volumes for last 2 hours: ', sums[-1], sums[-2])
            log('Sum of volumes for last 2 hours: ' + str(sums[-1]) + ' ' + str(sums[-2]))

            sma = sum(sums) / len(sums)
            # print('SMA', sma)
            log('SMA ' + str(sma))

            htfopen = [ohlcv['bitmex'][-1][1], ohlcv['bitmex'][-2][1]]
            htfhigh = [ohlcv['bitmex'][-1][2], ohlcv['bitmex'][-2][2]]
            htflow = [ohlcv['bitmex'][-1][3], ohlcv['bitmex'][-2][3]]
            htfclose = [ohlcv['bitmex'][-1][4], ohlcv['bitmex'][-2][4]]
            htfvolume_sum = [sums[-1], sums[-2]]
            htfx_sma = sma

            up_major = htfclose[0] > ((htfhigh[0] + htflow[0]) / 2)
            down_major = htfclose[0] < ((htfhigh[0] + htflow[0]) / 2)

            climactic_up = htfclose[0] if htfclose[0] > htfclose[1] and htfclose[0] > htfopen[0] and up_major and htfvolume_sum[0] > htfx_sma and htfvolume_sum[0] > htfvolume_sum[1] else 0
            climactic_down = htfclose[0] if htfclose[1] > htfclose[0] and htfclose[0] < htfopen[0] and down_major and htfvolume_sum[0] > htfx_sma and htfvolume_sum[0] > htfvolume_sum[1] else 0

            open, high, low, close = get_current_ohlc(exchanges)

            long_entry = close >= climactic_up and close >= climactic_down and htfvolume_sum[0] > htfx_sma and htfvolume_sum[0] > htfvolume_sum[1]
            short_entry = close <= climactic_up and close <= climactic_down and htfvolume_sum[0] > htfx_sma and htfvolume_sum[0] > htfvolume_sum[1]

            if long_entry or short_entry:
                entry_price = (open + high + low + close) / 4
                sl = (1 - 0.01 * SL_PERCENT) * entry_price if long_entry else (1 + 0.01 * SL_PERCENT)
                tp = (1 + 0.01 * TP_PERCENT) * entry_price if long_entry else (1 - 0.01 * TP_PERCENT)
                type = 'long' if long_entry else 'short'

                # print(str(datetime.now()), 'Entered ' + type + ' at' + str(entry_price))
                log('Entered ' + type + ' at' + str(entry_price))
                while not check_close_cond(exchanges, type, sl, tp):
                    time.sleep(5 * 60 + randint(0, 30))
                closed_order = True

            if not closed_order:
                time.sleep(5 * 60 + randint(0, 30))

        except Exception as e:
            log(e)


if __name__ == '__main__':
    main()
