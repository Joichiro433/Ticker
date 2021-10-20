from typing import List, Dict, Optional, Tuple, Any, Union
import os
import sys
import json
from datetime import date, datetime
from copy import deepcopy
import asyncio

import pandas as pd
import pybotters
from pybotters.store import DataStoreManager
from google.cloud import storage
from google.oauth2.service_account import Credentials

import params
from utils.utils import extract_dict
from logger import Logger


logger = Logger()


class ContractPrice:
    """約定の情報をもつクラス

    Attributes
    ----------
    side : str = 'buy' | 'sell'
        約定のside
    price : float
        約定価格
    """
    def __init__(self, side: str, price: float) -> None:
        self.side : str = side
        self.price : float = price


class TradingHistoryStorage:
    """約定履歴を保持するクラス

    ClassAttributes
    ---------------
    trade_ids_dict : Dict[str, Any | None]
        取引所をkey, トレードのidをvalueとするdict。
        新たなトレードが行われたか判定するために使用
    liquidation_ids_dict : Dict[str, Any | None]
        取引所をkey, 清算のidをvalueとするdict。
        新たな清算が行われたか判定するために使用

    Attributes
    ----------
    contract_prices : Dict[str, List[float]]
        取引所をkey, 約定価格をvalueとするdict
    buy_volume_dict : Dict[str, float]
        取引所をkey, 買いの約定数をvalueとするdict
    sell_volume_dict : Dict[str, float]
        取引所をkey, 売りsの約定数をvalueとするdict
    buy_liquidation_qty_dict : Dict[str, float]
        取引所をkey, 買いの精算数をvalueとするdict
    sell_liquidation_qty_dict : Dict[str, float]
        取引所をkey, 売りの精算数をvalueとするdict
    OIids_dict : Dict[str, Any | None]
        取引所をkey, 未決済建玉のidをvalueとするdict。
        未決済建玉が更新されたか判定するために使用（※未決済建玉数をidをしているためインスタンス変数としている）
    OIs_dict : Dict[str, List[float]]
        取引所をkey, 未決済建玉数をvalueとするdict
    """
    trade_ids_dict : Dict[str, Optional[Any]] = {exchange: None for exchange in params.EXCHANGES}
    liquidation_ids_dict : Dict[str, Optional[Any]] = {exchange: None for exchange in params.EXCHANGES}
    def __init__(self) -> None:
        # trade
        self.contract_prices_dict : Dict[str, List[ContractPrice]] = {exchange: [] for exchange in params.EXCHANGES}
        self.buy_volume_dict : Dict[str, float] = {exchange: 0.0 for exchange in params.EXCHANGES}
        self.sell_volume_dict : Dict[str, float] = {exchange: 0.0 for exchange in params.EXCHANGES}
        # liquidation
        self.buy_liquidation_qty_dict : Dict[str, float] = {exchange: 0.0 for exchange in params.EXCHANGES}
        self.sell_liquidation_qty_dict : Dict[str, float] = {exchange: 0.0 for exchange in params.EXCHANGES}
        # OI
        self.OIids_dict : Dict[str, Optional[Any]] = {exchange: None for exchange in params.EXCHANGES}
        self.OIs_dict : Dict[str, List[float]] = {exchange: [] for exchange in params.EXCHANGES}


class Ohlcv:
    """ローソク足の情報をもつクラス

    Attributes
    ----------
    open : float | None
        始値
    high : float | None
        高値
    low : float | None
        安値
    close : float | None
        終値
    buy_volume : float
        買い出来高
    sell_volume : float
        売り出来高
    buy_price_avg : float | None
        買い値の平均値
    sell_price_avg : float | None
        売り値の平均値
    """
    def __init__(
            self, 
            cps: Optional[List[ContractPrice]] = None,
            buy_volume: float = 0.0,
            sell_volume: float = 0.0) -> None:
        self.open : Optional[float] = cps[0].price if cps is not None else None
        self.high : Optional[float] = max([cp.price for cp in cps]) if cps is not None else None
        self.low : Optional[float] = min([cp.price for cp in cps]) if cps is not None else None
        self.close : Optional[float] = cps[-1].price if cps is not None else None
        self.buy_volume : float = buy_volume
        self.sell_volume : float = sell_volume
        try:
            self.buy_price_avg : Optional[float] = sum([cp.price for cp in cps if cp.side == 'buy']) / buy_volume if cps is not None else None
        except ZeroDivisionError:
            self.buy_price_avg : Optional[float] = None
        try:
            self.sell_price_avg : Optional[float] = sum([cp.price for cp in cps if cp.side == 'sell']) / sell_volume if cps is not None else None
        except ZeroDivisionError:
            self.sell_price_avg : Optional[float] = None

    def __str__(self) -> str:
        return str(self.__dict__)


class LiquidationQty:
    """精算数の情報をもつクラス

    Attributes
    ----------
    buy_liq_qty : float
        買いの精算数
    sell_liq_qty : float
        売りの精算数
    """
    def __init__(self, buy_liq_qty: float, sell_liq_qty: float) -> None:
        self.buy_liq_qty : float = buy_liq_qty
        self.sell_liq_qty : float = sell_liq_qty


class OIohlc:
    """未決済建玉の情報をもつクラス

    Attributes
    ----------
    oi_open : float | None
        未決済建玉数の始値
    oi_high : float | None
        未決済建玉数の高値
    oi_low : float | None
        未決済建玉数の安値
    oi_close : float | None
        未決済建玉数の終値
    """
    def __init__(self, ois: Optional[List[float]] = None) -> None:
        self.oi_open : Optional[float] = ois[0] if ois is not None else None
        self.oi_high : Optional[float] = max(ois) if ois is not None else None
        self.oi_low : Optional[float] = min(ois) if ois is not None else None
        self.oi_close : Optional[float] = ois[-1] if ois is not None else None


class Ticker:
    """取引情報を保持するクラス。保存するdfの１レコードに相当

    Attributes
    ----------
    open : float | None
        始値
    high : float | None
        高値
    low : float | None
        安値
    close : float | None
        終値
    buy_volume : float
        買い出来高
    sell_volume : float
        売り出来高
    buy_price_avg : float | None
        買い値の平均値
    sell_price_avg : float | None
        売り値の平均値
    buy_liq_qty : float
        買いの精算数
    sell_liq_qty : float
        売りの精算数
    oi_open : float | None
        未決済建玉数の始値
    oi_high : float | None
        未決済建玉数の高値
    oi_low : float | None
        未決済建玉数の安値
    oi_close : float | None
        未決済建玉数の終値
    orderbook : Dict[str, List[Dict[str, float]]]
        板情報
    """
    def __init__(
            self,
            timestamp : datetime,
            ohlcv : Ohlcv,
            liquidation_qty : LiquidationQty,
            oi_ohlc : OIohlc,
            orderbook : Dict[str, List[Dict[str, float]]]) -> None:
        self.timestamp : datetime = timestamp
        self.open : Optional[float] = ohlcv.open
        self.high : Optional[float] = ohlcv.high
        self.low : Optional[float] = ohlcv.low
        self.close : Optional[float] = ohlcv.close
        self.buy_volume : float = ohlcv.buy_volume
        self.sell_volume : float = ohlcv.sell_volume
        self.buy_price_avg : Optional[float] = ohlcv.buy_price_avg
        self.sell_price_avg : Optional[float] = ohlcv.sell_price_avg
        self.buy_liq_qty : float = liquidation_qty.buy_liq_qty
        self.sell_liq_qty : float = liquidation_qty.sell_liq_qty
        self.oi_open : Optional[float] = oi_ohlc.oi_open
        self.oi_high : Optional[float] = oi_ohlc.oi_high
        self.oi_low : Optional[float] = oi_ohlc.oi_low
        self.oi_close : Optional[float] = oi_ohlc.oi_close
        self.orderbook : Dict[str, List[Dict[str, float]]] = orderbook


class ApiClient:
    """取引所のAPIを使用するクラス

    Attributes
    ----------
    store_dict : Dict[str, DataStoreManager]
        取引所をkey, 対応するDataStoreをvalueとするdict
    df_dict : Dict[str, pd.DataFrame]
        取引所をkey, 取引情報を保持するdfをvalueとするdict
    trading_history_storage : TradingHistoryStorage
        各取引所の約定履歴を保持するインスタンス
    warning_count : int
        エラーが生じた際にインクレメント、5を超えるとプログラムを異常終了

    Methods
    -------
    get_realtime_orderbook
        リアルタイムに取引情報を保存する。約定履歴は5秒ごとにohlcvにまとめ、板情報とともにdfに保存
    save_ticker -> None
        dfを日が変わるごとにSAVE_DIRに書き出し、初期化する
    """
    def __init__(self) -> None:
        self.store_dict : Dict[str, DataStoreManager] = {exchange: params.STORES[exchange] for exchange in params.EXCHANGES}
        self.df_dict : Dict[str, pd.DataFrame] = {exchange: pd.DataFrame(columns=params.COLUMNS) for exchange in params.EXCHANGES}
        self.today : date = datetime.now().date()
        self.trading_history_storage : TradingHistoryStorage = TradingHistoryStorage()
        self.warning_count : int = 0
        # GCS設定
        _cred : Credentials = Credentials.from_service_account_info(json.load(open(params.SECRET_KET_PATH)))
        self.gcs_client : storage.Client = storage.Client(credentials=_cred, project=_cred.project_id)
        self.gcs_bucket : storage.Bucket = self.gcs_client.get_bucket(params.BAKET_NAME)

    async def get_realtime_orderbook(self):
        """リアルタイムに取引情報を保存する。確約履歴は5秒ごとにohlcvにまとめ、板情報とともにdfに保存"""
        async with pybotters.Client() as client:
            for exchange in params.EXCHANGES:
                await client.ws_connect(
                    url=params.URLS[exchange],
                    send_json=params.SEND_JSONS[exchange],
                    hdlr_json=self.store_dict[exchange].onmessage)

            # 各取引所の情報が取得できるまで待機
            while not self._has_update():
                await asyncio.sleep(0)

            # キリの良い時間まで待機
            while datetime.now().second % 5 != 0:
                await asyncio.sleep(0)

            for exchange in params.EXCHANGES:
                asyncio.create_task(self._store_trading_history(exchange=exchange))

            await asyncio.sleep(5)  # 約定情報を貯める

            while True:
                try:
                    timestamp : datetime = datetime.now()
                    # subprocessで動いているトレード情報を収集
                    ohlcv_dict : Dict[str, Ohlcv] = self._create_ohlcvs()
                    liquidation_qty_dict : Dict[str, LiquidationQty] = self._create_liquidation_qty()
                    oi_ohlc_dict : Dict[str, OIohlc] = self._create_oi_ohlc()
                    self.trading_history_storage : TradingHistoryStorage = TradingHistoryStorage()  # 約定履歴ストレージのリセット
                    # 各取引所のticker情報を保存
                    ticker_dict : Dict[str, Ticker] = {}
                    for exchange in params.EXCHANGES:
                        orderbook : List[dict] = self.store_dict[exchange].orderbook.find()
                        ticker_dict[exchange] = Ticker(
                            timestamp=timestamp, 
                            ohlcv=ohlcv_dict[exchange],
                            liquidation_qty=liquidation_qty_dict[exchange],
                            oi_ohlc=oi_ohlc_dict[exchange],
                            orderbook=self._parse_orderbook(orderbook))

                    self._update_df(now=timestamp.date(), ticker_dict=ticker_dict)

                    # for exchange in params.EXCHANGES:
                    #     logger.debug(self.df_dict[exchange].tail(1))
                    self.warning_count = 0

                    # キリの良い時間まで待機
                    await asyncio.sleep(1)
                    while datetime.now().second % 5 != 0:
                        await asyncio.sleep(0)

                except Exception as e:
                    logger.warn(e)
                    self.warning_count += 1
                    if self.warning_count > 5:
                        raise Exception(e)
                    # キリの良い時間まで待機
                    while datetime.now().second % 5 != 0:
                        await asyncio.sleep(0)

    async def _store_trading_history(self, exchange: str):
        """約定履歴をリアルタイムに保存"""

        def inner_store_trade(trades: List[Dict]) -> None:
            """約定情報の更新時にその情報を保持する"""
            if trades == []:
                return None
            latest_trade : Dict = trades[-1]
            if latest_trade[params.TRADE_IDS[exchange]] != TradingHistoryStorage.trade_ids_dict[exchange]:
                # トレードidが更新されていれば情報を保持
                TradingHistoryStorage.trade_ids_dict[exchange] = latest_trade[params.TRADE_IDS[exchange]]
                if latest_trade['side'].lower() == 'buy':
                    self.trading_history_storage.contract_prices_dict[exchange].append(ContractPrice(side='buy', price=float(latest_trade['price'])))
                    self.trading_history_storage.buy_volume_dict[exchange] += float(latest_trade['size'])
                elif latest_trade['side'].lower() == 'sell':
                    self.trading_history_storage.contract_prices_dict[exchange].append(ContractPrice(side='sell', price=float(latest_trade['price'])))
                    self.trading_history_storage.sell_volume_dict[exchange] += float(latest_trade['size'])

        def inner_store_liquidation(liquidations: List[Dict]) -> None:
            """精算の更新時にその情報を保持する"""
            if liquidations == []:
                return None
            latest_liquidation : Dict = liquidations[-1]
            if latest_liquidation[params.LIQUIDATION_IDS[exchange]] != TradingHistoryStorage.liquidation_ids_dict[exchange]:
                # liquidation idが更新されていれば情報を保持
                TradingHistoryStorage.liquidation_ids_dict[exchange] = latest_liquidation[params.LIQUIDATION_IDS[exchange]]
                if latest_liquidation['side'].lower() == 'buy':
                    self.trading_history_storage.buy_liquidation_qty_dict[exchange] += float(latest_liquidation['qty'])
                if latest_liquidation['side'].lower() == 'sell':
                    self.trading_history_storage.sell_liquidation_qty_dict[exchange] += float(latest_liquidation['qty'])

        def inner_store_OI(OIs: List[Dict]) -> None:
            """未決済建玉の更新時にその情報を保持する"""
            if OIs == []:
                return None
            latest_OI : Dict = OIs[-1]
            if latest_OI[params.OI_IDS[exchange]] != self.trading_history_storage.OIids_dict[exchange]:
                # OI idが更新されていれば情報を保持
                self.trading_history_storage.OIids_dict[exchange] = latest_OI[params.OI_IDS[exchange]]
                self.trading_history_storage.OIs_dict[exchange].append(float(latest_OI[params.OI_IDS[exchange]]))
            
        while True:
            try:
                trades : List[Dict] = self.store_dict[exchange].trade.find()
                liquidations : List[Dict] = self.store_dict[exchange].liquidation.find()
                OIs : List[Dict] = self.store_dict[exchange].instrument.find()
            except AttributeError:  # FTX用
                trades : Dict = self.store_dict[exchange].trades.find()
            except Exception as e:
                logger.error(e)
                self.save_ticker()
                sys.exit(1)
            try:
                inner_store_trade(trades=trades)  # 約定情報の更新時にその情報を保持
                inner_store_liquidation(liquidations=liquidations)  # 精算の更新時にその情報を保持
                inner_store_OI(OIs=OIs)  # 未決済建玉の更新時にその情報を保持
                await self.store_dict[exchange].wait()
            except Exception as e:
                logger.error(e)
                self.save_ticker()
                sys.exit(1)

    def _create_ohlcvs(self) -> Dict[str, Ohlcv]:
        """貯められた各取引所の約定履歴をohlcvに変換する。１件も無い場合はプロパティがNoneのohlcvを作成

        Returns
        -------
        Dict[str, Ohlcv]
            取引所をkey, ohlcvをvalueとするdict
        """
        ohlcv_dict : Dict[str, Ohlcv] = {}
        for exchange in params.EXCHANGES:
            contract_prices : List[ContractPrice] = self.trading_history_storage.contract_prices_dict[exchange]
            buy_volume : float = self.trading_history_storage.buy_volume_dict[exchange]
            sell_volume : float = self.trading_history_storage.sell_volume_dict[exchange]
            if len(contract_prices) != 0:
                ohlcv : Ohlcv = Ohlcv(cps=contract_prices, buy_volume=buy_volume, sell_volume=sell_volume)
            else:
                ohlcv : Ohlcv = Ohlcv()  # 約定が一つも無かった場合
            ohlcv_dict[exchange] = ohlcv
        return ohlcv_dict

    def _create_liquidation_qty(self) -> Dict[str, LiquidationQty]:
        """貯められた各取引所の精算情報を、買い精算数、売り精算数をもつクラス: LiquidationQtyに変換する

        Returns
        -------
        Dict[str, LiquidationQty]
            取引所をkey, LiquidationQtyをvalueとするdict
        """
        liquidation_qty_dict : Dict[str, LiquidationQty] = {}
        for exchange in params.EXCHANGES:
            buy_liquidation_qty : float = self.trading_history_storage.buy_liquidation_qty_dict[exchange]
            sell_liquidation_qty : float = self.trading_history_storage.sell_liquidation_qty_dict[exchange]
            liquidation_qty : LiquidationQty = LiquidationQty(buy_liq_qty=buy_liquidation_qty, sell_liq_qty=sell_liquidation_qty)
            liquidation_qty_dict[exchange] = liquidation_qty
        return liquidation_qty_dict

    def _create_oi_ohlc(self) -> Dict[str, OIohlc]:
        """貯められた各取引所の未決済建玉を、未決済建玉数ohlcに変換する。１件も無い場合はプロパティがNoneの未決済建玉数ohlcを作成

        Returns
        -------
        Dict[str, OIohlc]
            取引所をkey, 未決済建玉数ohlcをvalueとするdict
        """
        oi_ohlc_dict : Dict[str, OIohlc] = {}
        for exchange in params.EXCHANGES:
            ois : List[float] = self.trading_history_storage.OIs_dict[exchange]
            if len(ois) != 0:
                oi_ohlc : OIohlc = OIohlc(ois=ois)
            else:
                oi_ohlc : OIohlc = OIohlc()
            oi_ohlc_dict[exchange] = oi_ohlc
        return oi_ohlc_dict

    def save_ticker(self) -> None:
        """dfを日が変わるごとにGCSに書き出し、初期化する"""

        def inner_upload_df_to_gcs(df: pd.DataFrame, exchange: str) -> None:
            """GCSにdfをアップロードする"""
            temp_filepath : str = os.path.join(params.SAVE_DIR, f'temp_{exchange}.pkl.bz2')
            df.to_pickle(temp_filepath, compression='bz2')  # ローカルにdfを一時書き出し
            blob : storage.Blob = self.gcs_bucket.blob(os.path.join(params.SAVE_DIR, f'{self.today.strftime("%Y%m%d")}_{exchange}.pkl.bz2'))
            blob.upload_from_filename(temp_filepath)  # GCSにdfをアップロード

        # GCSにdfをアップロード
        for exchange in params.EXCHANGES:
            inner_upload_df_to_gcs(df=self.df_dict[exchange], exchange=exchange)
        logger.info(f'Uploaded ticker datas to GCS. DATE: {self.today}')
        # GCSにlogをアップロード
        blob : storage.Blob = self.gcs_bucket.blob(logger.log_file_path)
        blob.upload_from_filename(logger.log_file_path)
        # df初期化
        self.df_dict : Dict[str, pd.DataFrame] = {exchange: pd.DataFrame(columns=params.COLUMNS) for exchange in params.EXCHANGES}

    def _has_update(self) -> bool:
        """各取引所の情報が取得できたか

        Returns
        -------
        bool
            全ての取引所の情報が取得できた場合True
        """

        def inner_check_element(arg, *args) -> bool:
            has_element : bool = True
            args = (arg, ) + args
            for arg in args:
                try:
                    has_element &= False if len(arg) == 0 else True
                except TypeError:
                    has_element &= False
            return has_element

        has_updated : bool = True
        for exchange in params.EXCHANGES:
            orderbook = self.store_dict[exchange].orderbook
            try:
                trade = self.store_dict[exchange].trade
            except AttributeError:
                trade = self.store_dict[exchange].trades
            has_updated &= inner_check_element(orderbook, trade)
            
        return has_updated

    def _parse_orderbook(self, orderbook: List[dict]) -> Dict[str, List[Dict[str, float]]]:
        """APIで取得した板情報をパース

        Parameters
        ----------
        orderbook : List[dict]
            APIで取得した板情報

        Returns
        -------
        Dict[str, List[Dict[str, float]]]
            パース後の板情報
        """
        result = {'Buy': [], 'Sell': []}
        wanted_keys = ['size', 'price']
        for dict_ in deepcopy(orderbook):
            side : str = dict_['side'].capitalize()  # 一文字目だけを大文字
            dict_ : dict = extract_dict(dict_, wanted_keys)
            result[side].append(dict_)
        result['Sell'].sort(key=lambda x: x['price'])
        result['Buy'].sort(key=lambda x: x['price'], reverse=True)
        return result

    def _update_df(self, now: date, ticker_dict: Dict[str, Ticker]) -> None:
        """dfにレコードを追加

        Parameters
        ----------
        now : date
            現在日付
        ticker_dict : Dict[str, Ticker]
            取引所をkey, 取引情報であるtickerをvalueとするdict
        """
        if now > self.today:  # 日付が変わった場合
            self.save_ticker()  # dfの保存 & 初期化
            self.today = now  # 日付更新
        # df更新
        for exchange in params.EXCHANGES:
            self.df_dict[exchange] = self.df_dict[exchange].append(ticker_dict[exchange].__dict__, ignore_index=True)