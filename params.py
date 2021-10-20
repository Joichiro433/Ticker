import os

import pybotters

COLUMNS = [
    'timestamp',
    'open', 
    'high', 
    'low', 
    'close', 
    'buy_volume',
    'sell_volume',
    'buy_price_avg',
    'sell_price_avg',
    'buy_liq_qty',
    'sell_liq_qty',
    'oi_open',
    'oi_high',
    'oi_low',
    'oi_close',
    'orderbook']

SECRET_KET_PATH = 'secret_key.json'  # GCPのsecret_keyのpath
BAKET_NAME = 'trading_datas_storage'
SAVE_DIR = 'trading_datas'
os.makedirs(SAVE_DIR, exist_ok=True)

BYBIT = 'bybit'
FTX = 'ftx'
BITMEX = 'bitmex'
EXCHANGES = [BYBIT]
# EXCHANGES = [BYBIT, FTX, BITMEX]

# TODO: 複数の取引所に対応させる
STORES = {
    BYBIT: pybotters.BybitDataStore(),
    FTX: pybotters.FTXDataStore(),
    BITMEX: pybotters.BitMEXDataStore()}

URLS = {
    BYBIT: 'wss://stream.bybit.com/realtime',
    FTX: 'wss://ftx.com/ws/',
    BITMEX: 'wss://www.bitmex.com/realtime'}

SEND_JSONS = {
    BYBIT: {
        'op': 'subscribe', 
        'args': ['orderBookL2_25.BTCUSD', 'trade.BTCUSD', 'instrument_info.100ms.BTCUSD', 'liquidation.BTCUSD']},
    FTX: [
        {'op': 'subscribe', 'channel': 'trades', 'market': 'BTC/USD'},
        {'op': 'subscribe', 'channel': 'orderbook', 'market': 'BTC/USD'}],
    BITMEX: {
        'op': 'subscribe', 
        'args': ['orderBookL2_25:XBTUSD', 'trade:XBTUSD']}}

TRADE_IDS = {
    BYBIT: 'trade_id',
    FTX: 'time',
    BITMEX: 'trdMatchID'}

LIQUIDATION_IDS = {
    BYBIT: 'time'}

OI_IDS = {
    BYBIT: 'open_interest'}