from typing import List, Dict
import os
from datetime import date, datetime
from copy import deepcopy
import asyncio

import pandas as pd

import pybotters
from utils.utils import extract_dict
from logger import Logger


COLUMNS = ['timestamp', 'close', 'buy_size_1', 'buy_size_2', 'buy_price_1', 'buy_price_2', 'sell_size_1', 'sell_size_2', 'sell_price_1', 'sell_price_2']
SAVE_DIR = 'orderbook'

logger = Logger()

class Ticker:
    def __init__(
            self,
            timestamp : datetime,
            close : float,
            orderbook : Dict[str, List[Dict[str, float]]]) -> None:
        self.timestamp : datetime = timestamp
        self.close : float = close
        self.buy_size_1 : float = orderbook['Buy'][0]['size']
        self.buy_size_2 : float = orderbook['Buy'][1]['size']
        self.buy_price_1 : float = orderbook['Buy'][0]['price']
        self.buy_price_2 : float = orderbook['Buy'][1]['price']
        self.sell_size_1 : float = orderbook['Sell'][0]['size']
        self.sell_size_2 : float = orderbook['Sell'][1]['size']
        self.sell_price_1 : float = orderbook['Sell'][0]['price']
        self.sell_price_2 : float = orderbook['Sell'][1]['price']


class ApiClient:
    def __init__(self) -> None:
        self.store_bybit : pybotters.BybitDataStore = pybotters.BybitDataStore()
        self.store_ftx : pybotters.FTXDataStore = pybotters.FTXDataStore()
        self.store_bitmex : pybotters.BitMEXDataStore = pybotters.BitMEXDataStore()
        self.df_bybit : pd.DataFrame = pd.DataFrame(columns=COLUMNS)
        self.df_ftx : pd.DataFrame = pd.DataFrame(columns=COLUMNS)
        self.df_bitmex : pd.DataFrame = pd.DataFrame(columns=COLUMNS)
        self.today : date = datetime.now().date()

    async def get_realtime_orderbook(self):
        async with pybotters.Client() as client:
            # bybit
            await client.ws_connect(
                url='wss://stream.bybit.com/realtime',
                send_json={
                    'op': 'subscribe',
                    'args': ['orderBookL2_25.BTCUSD', 'klineV2.1.BTCUSD']},
                hdlr_json=self.store_bybit.onmessage)
            # ftx
            await client.ws_connect(
                url='wss://ftx.com/ws/',
                send_json=[
                    {'op': 'subscribe', 'channel': 'ticker', 'market': 'BTC/USD'},
                    {'op': 'subscribe', 'channel': 'orderbook', 'market': 'BTC/USD'},
                ],
                hdlr_json=self.store_ftx.onmessage)
            # bitmex
            await client.ws_connect(
                url='wss://www.bitmex.com/realtime',
                send_json={
                    'op': 'subscribe',
                    'args': ['orderBookL2_25:XBTUSD', 'instrument:XBTUSD'],
                },
                hdlr_json=self.store_bitmex.onmessage)

            while not self._has_update():
                await self.store_bybit.wait()
                await self.store_ftx.wait()
                await self.store_bitmex.wait()

            # キリの良い時間まで待機
            while datetime.now().second % 5 != 0:
                await asyncio.sleep(0)

            while True:
                timestamp : datetime = datetime.now()
                orderbook_bybit : List[dict] = self.store_bybit.orderbook.find()
                kline_bybit : List[dict] = self.store_bybit.kline.find()
                orderbook_ftx : List[dict] = self.store_ftx.orderbook.find()
                kline_ftx : List[dict] = self.store_ftx.ticker.find()
                orderbook_bitmex : List[dict] = self.store_bitmex.orderbook.find()
                kline_bitmex : List[dict] = self.store_bitmex.instrument.find()

                ticker_bybit : Ticker = Ticker(
                    timestamp=timestamp,
                    close=self._parse_kline_to_close(kline_bybit, 'bybit'),
                    orderbook=self._parse_orderbook(orderbook_bybit))

                ticker_ftx : Ticker = Ticker(
                    timestamp=timestamp,
                    close=self._parse_kline_to_close(kline_ftx, 'ftx'),
                    orderbook=self._parse_orderbook(orderbook_ftx))

                ticker_bitmex : Ticker = Ticker(
                    timestamp=timestamp,
                    close=self._parse_kline_to_close(kline_bitmex, 'bitmex'),
                    orderbook=self._parse_orderbook(orderbook_bitmex))

                self._update_df(
                    now=timestamp.date(),
                    ticker_bybit=ticker_bybit,
                    ticker_ftx=ticker_ftx,
                    ticker_bitmex=ticker_bitmex)

                # logger.debug(self.df_bybit.tail(1))
                # logger.debug(self.df_ftx.tail(1))
                # logger.debug(self.df_bitmex.tail(1))

                await asyncio.sleep(self._cal_delay())  # 取得するorderbookの更新

    def save_ticker(self) -> None:
        os.makedirs(SAVE_DIR, exist_ok=True)
        self.df_bybit.to_pickle(os.path.join(SAVE_DIR, f'{self.today.strftime("%Y%m%d")}_bybit.pkl.bz2'), compression='bz2')
        self.df_ftx.to_pickle(os.path.join(SAVE_DIR, f'{self.today.strftime("%Y%m%d")}_ftx.pkl.bz2'), compression='bz2')
        self.df_bitmex.to_pickle(os.path.join(SAVE_DIR, f'{self.today.strftime("%Y%m%d")}_bitmex.pkl.bz2'), compression='bz2')
        logger.info('save ticker info')
        # df初期化
        self.df_bybit : pd.DataFrame = pd.DataFrame(columns=COLUMNS)
        self.df_ftx : pd.DataFrame = pd.DataFrame(columns=COLUMNS)
        self.df_bitmex : pd.DataFrame = pd.DataFrame(columns=COLUMNS)

    def _has_update(self) -> bool:
        update_flag_bybit : bool = all([len(self.store_bybit.orderbook), len(self.store_bybit.kline)])
        update_flag_ftx : bool = all([len(self.store_ftx.orderbook), len(self.store_ftx.ticker)])
        update_flag_bitmex : bool = all([self.store_bitmex.orderbook, self.store_bitmex.instrument])
        return update_flag_bybit and update_flag_ftx and update_flag_bitmex    

    def _parse_kline_to_close(
            self,
            kline: List[dict],
            cryptocurrency_exchange: str) -> float:
        if cryptocurrency_exchange == 'bybit':
            close : float = kline[-1]['close']
        elif cryptocurrency_exchange == 'ftx':
            close : float = kline[-1]['last']
        elif cryptocurrency_exchange == 'bitmex':
            close : float = kline[-1]['lastPrice']
        return close

    def _parse_orderbook(
            self, 
            orderbook: List[dict]) -> Dict[str, List[Dict[str, float]]]:
        result = {'Buy': [], 'Sell': []}
        wanted_keys = ['size', 'price']
        for dict_ in deepcopy(orderbook):
            side : str = dict_['side'].capitalize()  # 一文字目だけを大文字
            dict_ : dict = extract_dict(dict_, wanted_keys)
            result[side].append(dict_)
        result['Sell'].sort(key=lambda x: x['price'])
        result['Buy'].sort(key=lambda x: x['price'], reverse=True)
        return result

    def _cal_delay(self) -> float:
        millisec : float = float('0.' + str(datetime.now()).split('.')[-1])
        return 5 - millisec

    def _update_df(
            self,
            now: date,
            ticker_bybit: Ticker,
            ticker_ftx: Ticker,
            ticker_bitmex: Ticker) -> None:
        if now > self.today:  # 日付が変わった場合
            self.save_ticker()  # dfの保存 & 初期化
            self.today = now  # 日付更新
        # df更新
        self.df_bybit = self.df_bybit.append(ticker_bybit.__dict__, ignore_index=True)
        self.df_ftx = self.df_ftx.append(ticker_ftx.__dict__, ignore_index=True)
        self.df_bitmex = self.df_bitmex.append(ticker_bitmex.__dict__, ignore_index=True)