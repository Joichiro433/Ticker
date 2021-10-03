from typing import List, Dict, Optional, Tuple
import os
from datetime import date, datetime
from copy import deepcopy
import asyncio

import pandas as pd
import pybotters

from utils.utils import extract_dict
from logger import Logger


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
SAVE_DIR = 'trading_datas'

logger = Logger()


class TradingHistoryStorage:
    def __init__(self) -> None:
        self.bybits : List[float] = []
        self.size_bybit : float = 0
        self.ftxs : List[float] = []
        self.size_ftx : float = 0
        self.bitmexs : List[float] = []
        self.size_bitmex : float = 0


class Ohlcv:
    def __init__(
            self, 
            open: Optional[float] = None, 
            high: Optional[float] = None, 
            low: Optional[float] = None, 
            close: Optional[float] = None,
            volume: float = 0) -> None:
        self.open : Optional[float] = open
        self.high : Optional[float] = high
        self.low : Optional[float] = low
        self.close : Optional[float] = close
        self.volume : float = volume

    def __str__(self) -> str:
        return str(self.__dict__)


class Ticker:
    def __init__(
            self,
            timestamp : datetime,
            ohlcv : Ohlcv,
            orderbook : Dict[str, List[Dict[str, float]]]) -> None:
        self.timestamp : datetime = timestamp
        self.open : Optional[float] = ohlcv.open
        self.high : Optional[float] = ohlcv.high
        self.low : Optional[float] = ohlcv.low
        self.close : Optional[float] = ohlcv.close
        self.volume : float = ohlcv.volume
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
        self.trading_history_storage : TradingHistoryStorage = TradingHistoryStorage()
        self.warning_count : int = 0

    async def get_realtime_orderbook(self):
        async with pybotters.Client() as client:
            # bybit
            await client.ws_connect(
                url='wss://stream.bybit.com/realtime',
                send_json={
                    'op': 'subscribe',
                    'args': ['orderBookL2_25.BTCUSD', 'trade.BTCUSD']},
                hdlr_json=self.store_bybit.onmessage)
            # ftx
            await client.ws_connect(
                url='wss://ftx.com/ws/',
                send_json=[
                    {'op': 'subscribe', 'channel': 'trades', 'market': 'BTC/USD'},
                    {'op': 'subscribe', 'channel': 'orderbook', 'market': 'BTC/USD'},
                ],
                hdlr_json=self.store_ftx.onmessage)
            # bitmex
            await client.ws_connect(
                url='wss://www.bitmex.com/realtime',
                send_json={
                    'op': 'subscribe',
                    'args': ['orderBookL2_25:XBTUSD', 'trade:XBTUSD'],
                },
                hdlr_json=self.store_bitmex.onmessage)

            while not self._has_update():
                await self.store_bybit.wait()
                await self.store_ftx.wait()
                await self.store_bitmex.wait()

            # キリの良い時間まで待機
            while datetime.now().second % 5 != 0:
                await asyncio.sleep(0)

            asyncio.create_task(self._store_bybit_trading_history())
            asyncio.create_task(self._store_ftx_trading_history())
            asyncio.create_task(self._store_bitmex_trading_history())

            await asyncio.sleep(5)  # 約定情報を貯める

            while True:
                try:
                    timestamp : datetime = datetime.now()
                    ohlcv_bybit, ohlcv_ftx, ohlcv_bitmex = self._create_ohlcvs()
                    self.trading_history_storage : TradingHistoryStorage = TradingHistoryStorage()  # 約定履歴ストレージのリセット
                    orderbook_bybit : List[dict] = self.store_bybit.orderbook.find()
                    orderbook_ftx : List[dict] = self.store_ftx.orderbook.find()
                    orderbook_bitmex : List[dict] = self.store_bitmex.orderbook.find()

                    ticker_bybit : Ticker = Ticker(
                        timestamp=timestamp,
                        ohlcv=ohlcv_bybit,
                        orderbook=self._parse_orderbook(orderbook_bybit))

                    ticker_ftx : Ticker = Ticker(
                        timestamp=timestamp,
                        ohlcv=ohlcv_ftx,
                        orderbook=self._parse_orderbook(orderbook_ftx))

                    ticker_bitmex : Ticker = Ticker(
                        timestamp=timestamp,
                        ohlcv=ohlcv_bitmex,
                        orderbook=self._parse_orderbook(orderbook_bitmex))

                    self._update_df(
                        now=timestamp.date(),
                        ticker_bybit=ticker_bybit,
                        ticker_ftx=ticker_ftx,
                        ticker_bitmex=ticker_bitmex)

                    logger.debug(self.df_bybit.tail(1))
                    logger.debug(self.df_ftx.tail(1))
                    logger.debug(self.df_bitmex.tail(1))
                    self.warning_count = 0

                    await asyncio.sleep(self._cal_delay())  # 取得するorderbookの更新

                except Exception as e:
                    logger.warn(e)
                    self.warning_count += 1
                    if self.warning_count > 5:
                        raise Exception(e)
                    # キリの良い時間まで待機
                    while datetime.now().second % 5 != 0:
                        await asyncio.sleep(0)



    async def _store_bybit_trading_history(self) -> None:
        while True:
            self.trading_history_storage.bybits.append(self.store_bybit.trade.find()[-1]['price'])
            self.trading_history_storage.size_bybit += self.store_bybit.trade.find()[-1]['size']
            await self.store_bybit.wait()

    async def _store_ftx_trading_history(self) -> None:
        while True:
            self.trading_history_storage.ftxs.append(self.store_ftx.trades.find()[-1]['price'])
            self.trading_history_storage.size_ftx += self.store_ftx.trades.find()[-1]['size']
            await self.store_ftx.wait()

    async def _store_bitmex_trading_history(self) -> None:
        while True:
            self.trading_history_storage.bitmexs.append(self.store_bitmex.trade.find()[-1]['price'])
            self.trading_history_storage.size_bitmex += self.store_bitmex.trade.find()[-1]['size']
            await self.store_bitmex.wait()

    def _create_ohlcvs(self) -> Tuple[Ohlcv, Ohlcv, Ohlcv]:
        bybits : List[float] = self.trading_history_storage.bybits
        ftxs : List[float] = self.trading_history_storage.ftxs
        bitmexs : List[float] = self.trading_history_storage.bitmexs

        if len(bybits) != 0:
            ohlcv_bybit : Ohlcv = Ohlcv(
                open=bybits[0],
                high=max(bybits),
                low=min(bybits),
                close=bybits[-1],
                volume=self.trading_history_storage.size_bybit)
        else:
            ohlcv_bybit : Ohlcv = Ohlcv()  # 約定が一つも無かった場合
        
        if len(ftxs) != 0:
            ohlcv_ftx : Ohlcv = Ohlcv(
                open=ftxs[0],
                high=max(ftxs),
                low=min(ftxs),
                close=ftxs[-1],
                volume=self.trading_history_storage.size_ftx)
        else:
            ohlcv_ftx : Ohlcv = Ohlcv()  # 約定が一つも無かった場合

        if len(bitmexs) != 0:
            ohlcv_bitmex : Ohlcv = Ohlcv(
                open=bitmexs[0],
                high=max(bitmexs),
                low=min(bitmexs),
                close=bitmexs[-1],
                volume=self.trading_history_storage.size_bitmex)
        else:
            ohlcv_bitmex : Ohlcv = Ohlcv()  # 約定が一つも無かった場合

        return ohlcv_bybit, ohlcv_ftx, ohlcv_bitmex

    def save_ticker(self) -> None:
        os.makedirs(SAVE_DIR, exist_ok=True)
        self.df_bybit.to_pickle(os.path.join(SAVE_DIR, f'{self.today.strftime("%Y%m%d")}_bybit.pkl.bz2'), compression='bz2')
        self.df_ftx.to_pickle(os.path.join(SAVE_DIR, f'{self.today.strftime("%Y%m%d")}_ftx.pkl.bz2'), compression='bz2')
        self.df_bitmex.to_pickle(os.path.join(SAVE_DIR, f'{self.today.strftime("%Y%m%d")}_bitmex.pkl.bz2'), compression='bz2')
        logger.info(f'Save ticker data. DATE: {self.today}.')
        # df初期化
        self.df_bybit : pd.DataFrame = pd.DataFrame(columns=COLUMNS)
        self.df_ftx : pd.DataFrame = pd.DataFrame(columns=COLUMNS)
        self.df_bitmex : pd.DataFrame = pd.DataFrame(columns=COLUMNS)

    def _has_update(self) -> bool:
        update_flag_bybit : bool = all([len(self.store_bybit.orderbook), len(self.store_bybit.trade)])
        update_flag_ftx : bool = all([len(self.store_ftx.orderbook), len(self.store_ftx.trades)])
        update_flag_bitmex : bool = all([self.store_bitmex.orderbook, self.store_bitmex.trade])
        return update_flag_bybit and update_flag_ftx and update_flag_bitmex    

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