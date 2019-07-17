import json
import time
from random import randint
from threading import Thread

import requests
from ccxt import binance, bitfinex, coinbasepro, bitmex, hitbtc2, kraken, bittrex, huobipro
from datetime import datetime, timedelta
from dateutil import parser

# API KEYS ZONE
BITMEX_API_KEY = ''
BITMEX_API_SECRET = ''

# SL & TP LEVELS
SL_LEVEL = 300
TP_LEVEL = 500
SL2_LEVEL = 70
SL_OFFSET = 70

# TRADE SIZE
TRADE_SIZE = 200

# PERCENTAGE VOL ABOVE AVG
pavg = 100

exchanges = {'binance': binance(), 'bitfinex': bitfinex(), 'coinbase': coinbasepro(),
             'bitmex': bitmex(), 'hitbtc': hitbtc2(), 'kraken': kraken(), 'bittrex': bittrex(), 'huobi': huobipro()}
symbols = {'binance': 'BTC/USDC', 'bitfinex': 'BTC/USD', 'coinbase': 'BTC/USD',
           'bitmex': 'BTC/USD', 'hitbtc': 'BTC/USDT', 'kraken': 'BTC/USD', 'bittrex': 'BTC/USD', 'huobi': 'BTC/USDT'}

btmx = bitmex({'apiKey': BITMEX_API_KEY, 'secret': BITMEX_API_SECRET})

new_signal = False
hour_closed = False

trade_time = datetime.now()


# uncomment for testnet
# if 'test' in btmx.urls:
#     btmx.urls['api'] = btmx.urls['test']  # ←----- switch the base URL to testnet
#     exchanges['bitmex'].urls['api'] = exchanges['bitmex'].urls['test']


def bitmex_virtual_sl(set_price, type):
    is_profit = False
    is_open = True
    sl_price = set_price - SL_OFFSET if type == 'long' else set_price + SL_OFFSET
    while not is_profit and not new_signal and is_open:
        try:
            time.sleep(2)
            curr_price = bitmex_last_price()
            is_profit = check_profit(type, set_price, curr_price)
            time.sleep(1)
            is_open = bitmex_check_position()
            print(set_price, curr_price, is_profit)
        except Exception as e:
            log(e)
            handle_timeout(e)
    log('Current position is in profit now')

    prev_diff = 0
    while is_open and not new_signal:
        try:
            time.sleep(2)
            curr_price = bitmex_last_price()

            diff = curr_price - set_price

            if type == 'long' and diff > prev_diff and diff >= SL_OFFSET:
                sl_price = curr_price - SL_OFFSET
                prev_diff = diff
                log('Moved price to ' + str(sl_price))
            elif type == 'short' and diff < prev_diff and diff <= SL_OFFSET:
                prev_diff = diff
                sl_price = curr_price + SL_OFFSET
                log('Moved price to ' + str(sl_price))

            if (type == 'long' and curr_price <= sl_price) or (type == 'short' and curr_price >= sl_price):
                log('Closing trailing stop..')
                # sleep till next hour
                global hour_closed
                hour_closed = True

                time.sleep(1)
                bitmex_close_pos()
                time.sleep(1)
                bitmex_remove_ord()

                # next_hour = 5 - datetime.now().minute
                # if next_hour < 0:
                # next_hour + 10
                time.sleep(600)
                hour_closed = False
                break

            is_open = bitmex_check_position()

        except Exception as e:
            log(e)
            handle_timeout(e)


def handle_timeout(e):
    str = 'timeout='
    pos = e.args[0].find(str)
    if pos > 0:
        timeout = int(e.args[0][pos + len(str):-1])
        time.sleep(timeout)
    else:
        time.sleep(2)


# changing hours to UTC
def fix_bitmex_vol(bitmex_ohlcv):
    bitmex_ohlcv = bitmex_ohlcv[1:]
    for candle in bitmex_ohlcv:
        old_date = datetime.fromtimestamp(candle[0] / 1000)
        new_date = old_date - timedelta(minutes=5)
        candle[0] = int(new_date.timestamp() * 1000)
    return bitmex_ohlcv


# get OHLCV for all exchanges for 20 last hours
def get_data(exchanges, symbols):
    to = datetime.now() - timedelta(minutes=105)
    to = to.timestamp() * 1000
    ohlcv = {}
    for k, v in exchanges.items():
        ohlcv[k] = v.fetch_ohlcv(symbols[k], '5m', int(to))[:-1]
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


def bitmex_last_price():
    res = None
    params = {'symbol': 'XBT'}
    while not res:
        try:
            res = btmx.public_get_instrument(params)
        except Exception as e:
            log(e)
            handle_timeout(e)
    return res[0]['lastPrice']


def get_current_ohlc(exchanges):
    ohlcv = \
        exchanges['coinbase'].fetch_ohlcv('BTC/USD', '5m', (datetime.now() - timedelta(minutes=5)).timestamp() * 1000)[-1]
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
    if type == 'long' and diff >= SL2_LEVEL:
        return True
    elif type == 'short' and diff <= -SL2_LEVEL:
        return True

    return False


def bitmex_close_pos():
    res = None
    params = {'symbol': 'XBTUSD', 'execInst': 'Close'}
    while not res:
        try:
            res = btmx.private_post_order(params)
        except Exception as e:
            log(e)
            handle_timeout(e)


def bitmex_remove_ord():
    if len(bitmex_get_orders()) == 0:
        return

    res = None
    params = {'symbol': 'XBTUSD'}
    while not res:
        try:
            res = btmx.private_delete_order_all(params)

        except Exception as e:
            log(e)
            handle_timeout(e)


def bitmex_sl(stop_price, order_qty):
    res = None
    params = {'symbol': 'XBTUSD', 'orderQty': order_qty, 'ordType': 'Stop', 'stopPx': stop_price,
              'execInst': 'LastPrice'}
    while not res:
        try:
            res = btmx.private_post_order(params)
        except Exception as e:
            log(e)
            handle_timeout(e)
    return res['orderID']


def bitmex_get_orders():
    filter = json.dumps({'open': 'true'})
    params = {'symbol': 'XBTUSD', 'filter': filter, 'count': 5}
    res = btmx.private_get_order(params)

    return res


def bitmex_check_position():
    filter = json.dumps({'symbol': 'XBTUSD'})
    params = {'filter': filter, 'count': 1}
    res = btmx.private_get_position(params)
    if res:
        return res[0]['isOpen']
    return False


def bitmex_tp(price, order_qty):
    res = None
    params = {'symbol': 'XBTUSD', 'orderQty': order_qty, 'stopPx': price, 'ordType': 'MarketIfTouched',
              'execInst': 'LastPrice'}
    while not res:
        try:
            res = btmx.private_post_order(params)
        except Exception as e:
            log(e)
            handle_timeout(e)
    return res['orderID']


def bitmex_enter(price, order_qty):
    global trade_time

    res = None
    params = {'symbol': 'XBTUSD', 'orderQty': order_qty, 'price':price, 'ordType': 'Limit', 'ExecInst': 'ParticipateDoNotInitiate'}
    while not res:
        try:
            res = btmx.private_post_order(params)
            trade_time = datetime.now()
        except Exception as e:
            log(e)
            handle_timeout(e)

    return res['orderID']


def enter_position(long_entry, short_entry, open, high, low, close):
    entry_price = bitmex_last_price()  # (open + high + low + close) / 4
    sl = entry_price - SL_LEVEL if long_entry else entry_price + SL_LEVEL
    tp = entry_price + TP_LEVEL if long_entry else entry_price - TP_LEVEL
    ep_longshort = entry_price - 30 if long_entry else entry_price + 30
    type = 'long' if long_entry else 'short'
    log('Entered ' + type + ' at ' + str(entry_price))

    entry_price = round(entry_price / 0.5) * 0.5
    sl = round(sl / 0.5) * 0.5
    tp = round(tp / 0.5) * 0.5

    order_qty = TRADE_SIZE if type == 'long' else -TRADE_SIZE
    bitmex_enter(ep_longshort, order_qty)
    time.sleep(1)
    # place stop loss
    bitmex_sl(sl, order_qty * -1)
    time.sleep(1)
    # place take profit
    bitmex_tp(tp, order_qty * -1)
    time.sleep(1)

    return type, tp, sl, entry_price


def no_open_limitOrder():
    orders = bitmex_get_orders()
    for order in orders:
        if order.get('ordType') == "Limit":
            return False

    return True


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
            if datetime.now() > trade_time + timedelta(minutes=30):
                bitmex_remove_ord()

            # sleep till next hour
            if hour_closed:
                # next_hour = 10 - datetime.now().minute
                time.sleep(600)

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

            htfopen = [ohlcv['coinbase'][-1][1], ohlcv['coinbase'][-2][1]]
            htfhigh = [ohlcv['coinbase'][-1][2], ohlcv['coinbase'][-2][2]]
            htflow = [ohlcv['coinbase'][-1][3], ohlcv['bitmex'][-2][3]]
            htfclose = [ohlcv['coinbase'][-1][4], ohlcv['coinbase'][-2][4]]
            htfvolume_sum = [sums[-1], sums[-2]]
            htfx_sma = sma

            range = htfhigh[0] - htflow[0]
            up_major = htfclose[0] >= (htflow[0] + (0.70 * range)) and htfopen[0] >= htflow[0] - 0.10 * range
            down_major = htfclose[0] <= (htfhigh[0] - (0.70 * range)) and htfopen[0] <= htfhigh[0] + 0.10 * range

            climactic_up = htfclose[0] if htfclose[0] > htfclose[1] and htfclose[0] > htfopen[0] and up_major and \
                                          htfvolume_sum[0] > htfx_sma and htfvolume_sum[0] > htfvolume_sum[1] else 0
            climactic_down = htfclose[0] if htfclose[1] > htfclose[0] and htfclose[0] < htfopen[0] and down_major and \
                                            htfvolume_sum[0] > htfx_sma and htfvolume_sum[0] > htfvolume_sum[1] else 0
            log('climactic up: ' + str(climactic_up) + ' climactic down: ' + str(climactic_down))

            open, high, low, close = get_current_ohlc(exchanges)

            percentage_above_avg = ((htfvolume_sum[0]-sma)/sma)*100

            above = percentage_above_avg > pavg

            long_entry = climactic_up > 0 and close > climactic_up and htfvolume_sum[0] > htfx_sma and htfvolume_sum[
                0] > htfvolume_sum[1] and above
            short_entry = climactic_down > 0 and close < climactic_down and htfvolume_sum[0] > htfx_sma and \
                          htfvolume_sum[0] > htfvolume_sum[1] and above
            log('long entry: ' + str(long_entry) + ' short entry: ' + str(short_entry))

            if not hour_closed and not bitmex_check_position() and no_open_limitOrder():
                time.sleep(1)
                bitmex_remove_ord()

                if long_entry or short_entry:
                    type, tp, sl, set_price = enter_position(long_entry, short_entry, open, high, low, close)
                    sl_thread = Thread(target=bitmex_virtual_sl, args=(set_price, type))
                    sl_thread.start()

            time.sleep(1)
            if not hour_closed and bitmex_check_position() and no_open_limitOrder():
                dbg = check_opposite_signal(long_entry, short_entry, type)
                if dbg:
                    global new_signal
                    new_signal = True
                    time.sleep(20)

                    bitmex_close_pos()
                    bitmex_remove_ord()
                    type, tp, sl, set_price = enter_position(long_entry, short_entry, open, high, low, close)
                    new_signal = False
                    sl_thread = Thread(target=bitmex_virtual_sl, args=(set_price, type))
                    sl_thread.start()

            # time.sleep(1 * 15 + randint(0, 15))

        except Exception as e:
            log(e)
            handle_timeout(e)


if __name__ == '__main__':
    start = time.time()
    main()
    print(time.time()-start)

    # cp = bitmex_last_price()
    # print(cp)
    # price = cp - 30
    # print(bitmex_enter(price, TRADE_SIZE))
    # enter_position(price, TRADE_SIZE)

    # print(bitmex_get_orders())
