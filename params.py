import os

COLUMNS = [
    'timestamp',
    'open', 
    'high', 
    'low', 
    'close', 
    'volume', 
    'buy_size_1', 
    'buy_size_2', 
    'buy_price_1', 
    'buy_price_2', 
    'sell_size_1', 
    'sell_size_2', 
    'sell_price_1', 
    'sell_price_2']


SECRET_KET_PATH = 'secret_key.json'
BAKET_NAME = 'trading_datas_storage'
SAVE_DIR = 'trading_datas'
os.makedirs(SAVE_DIR, exist_ok=True)

BYBIT = 'bybit'
FTX = 'ftx'
BITMEX = 'bitmex'