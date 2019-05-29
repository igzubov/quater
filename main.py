import time
from random import randint
import requests
from ccxt import binance, bitstamp, bitfinex, coinbasepro, bitmex, hitbtc2, kraken, bittrex, huobipro
from datetime import datetime, timedelta
from dateutil import parser


# API KEYS ZONE
BITMEX_API_KEY = ''
BITMEX_API_SECRET = ''

# SL & TP LEVELS
SL_LEVEL = 200
TP_LEVEL = 500
SL2_LEVEL = 50
SL_OFFSET = 30

# TRADE SIZE
TRADE_SIZE = 500

exchanges = {'binance': binance(), 'bitstamp': bitstamp(), 'bitfinex': bitfinex(), 'coinbase': coinbasepro(),
             'bitmex': bitmex(), 'hitbtc': hitbtc2(), 'kraken': kraken(), 'bittrex': bittrex(), 'huobi': huobipro()}
symbols = {'binance': 'BTC/USDC', 'bitstamp': 'BTC/USD', 'bitfinex': 'BTC/USD', 'coinbase': 'BTC/USD',
           'bitmex': 'BTC/USD', 'hitbtc': 'BTC/USDT', 'kraken': 'BTC/USD', 'bittrex': 'BTC/USD', 'huobi': 'BTC/USDT'}

btmx = bitmex({'apiKey': BITMEX_API_KEY, 'secret': BITMEX_API_SECRET})

# uncomment for testnet
# if 'test' in btmx.urls:
#     btmx.urls['api'] = btmx.urls['test']  # ←----- switch the base URL to testnet
#     exchanges['bitmex'].urls['api'] = exchanges['bitmex'].urls['test']


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


def check_opposite_signal(long_entry, short_entry, type):
    if type == 'long' and short_entry:
        log('opposite signal short')
        return True
    elif type == 'short' and long_entry:
        log('opposite signal long')
        return True
    return False


def check_profit(type, set_price, curr_price):
    diff = curr_price - set_price
    if type == 'long' and diff >= 50:
        return True
        bitmex_sl2(set_price + 20, -1, -30)
    elif type == 'short' and diff <= -50:
        return True

    return False


def bitmex_close_pos():
    res = None
    params = {'symbol': 'XBTUSD', 'execInst': 'Close'}
    while not res:
        res = btmx.private_post_order(params)


def bitmex_remove_ord():
    res = None
    params = {'symbol': 'XBTUSD'}
    while not res:
        res = btmx.private_delete_order_all(params)


def bitmex_move_trail(price, order_qty, order_id):
    res = None
    params = {'orderID': order_id, 'orderQty': order_qty, 'stopPx': price, 'ordType': 'MarketIfTouched',
              'execInst': 'LastPrice'}
    while not res:
        res = btmx.private_put_order(params)


def bitmex_sl(stop_price, order_qty):
    res = None
    params = {'symbol': 'XBTUSD', 'orderQty': order_qty, 'ordType': 'Stop', 'stopPx': stop_price, 'execInst': 'LastPrice'}
    while not res:
        res = btmx.private_post_order(params)
    return res['orderID']


def bitmex_sl2(stop_price, order_qty, offset):
    res = None
    params = {'symbol': 'XBTUSD', 'orderQty': order_qty, 'ordType': 'Stop', 'pegPriceType': 'TrailingStopPeg', 'stopPx': stop_price, 'pegOffsetValue': offset, 'execInst': 'LastPrice'}
    while not res:
        res = btmx.private_post_order(params)
    return res['orderID']


def bitmex_tp(price, order_qty):
    res = None
    params = {'symbol': 'XBTUSD', 'orderQty': order_qty, 'stopPx': price, 'ordType': 'MarketIfTouched', 'execInst': 'LastPrice'}
    while not res:
        res = btmx.private_post_order(params)
    return res['orderID']


def bitmex_enter(price, order_qty):
    res = None
    params = {'symbol': 'XBTUSD', 'orderQty': order_qty, 'price': price, 'ordType': 'Limit'}
    while not res:
        res = btmx.private_post_order(params)
    return res['orderID']


def enter_position(long_entry, short_entry, open, high, low, close):
    entry_price = (open + high + low + close) / 4
    sl = entry_price - SL_LEVEL if long_entry else entry_price + SL_LEVEL
    tp = entry_price + TP_LEVEL if long_entry else entry_price - TP_LEVEL
    type = 'long' if long_entry else 'short'
    log('Entered ' + type + ' at ' + str(entry_price))

    entry_price = round(entry_price / 0.5) * 0.5
    sl = round(sl / 0.5) * 0.5
    tp = round(tp / 0.5) * 0.5

    order_qty = TRADE_SIZE if type == 'long' else -TRADE_SIZE
    bitmex_enter(entry_price, order_qty)
    # place stop loss
    bitmex_sl(sl, order_qty * -1)
    # place take profit
    bitmex_tp(tp, order_qty * -1)

    return type, tp, sl, entry_price


def log(data):
    with open('log.txt', 'a') as f:
        print(str(datetime.now()) + ' ' + str(data))
        f.write(str(datetime.now()) + ' ' + str(data) + '\n')


def main():
    type = ''
    set_price = 0
    tp = 0
    sl = 0
    entered = False
    while True:
        try:
            ohlcv = get_data(exchanges, symbols)
            # print_last_ohlcv(ohlcv)
            try:
                sums = calculate_sum(ohlcv)
            except IndexError:
                time.sleep(60)
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
            log('climactic up: ' + str(climactic_up) + ' climactic down: ' + str(climactic_down))

            open, high, low, close = get_current_ohlc(exchanges)

            long_entry = climactic_up > 0 and close > climactic_up and htfvolume_sum[0] > htfx_sma and htfvolume_sum[0] > htfvolume_sum[1]
            short_entry = climactic_down > 0 and close < climactic_down and htfvolume_sum[0] > htfx_sma and htfvolume_sum[0] > htfvolume_sum[1]
            log('long entry: ' + str(long_entry) + ' short entry: ' + str(short_entry))

            if not entered and (long_entry or short_entry):
                type, tp, sl, set_price = enter_position(long_entry, short_entry, open, high, low, close)
                entered = True

            if entered:
                dbg = check_profit(type, set_price, close)
                if dbg:
                    sl2 = set_price + SL_OFFSET if type == 'long' else set_price - SL_OFFSET
                    offset = -SL_OFFSET if type == 'long' else SL_OFFSET
                    order_qty = -TRADE_SIZE if type == 'long' else TRADE_SIZE
                    bitmex_sl2(sl2, order_qty, offset)
                dbg2 = check_close_cond(exchanges, type, sl, tp)
                if dbg2:
                    bitmex_close_pos()
                    bitmex_remove_ord()
                    entered = False
                elif check_opposite_signal(long_entry, short_entry, type):
                    bitmex_close_pos()
                    bitmex_remove_ord()
                    type, tp, sl, set_price = enter_position(long_entry, short_entry, open, high, low, close)

            time.sleep(1 * 60 + randint(0, 30))

        except Exception as e:
            time.sleep(5)
            log(e)


if __name__ == '__main__':
    main()
