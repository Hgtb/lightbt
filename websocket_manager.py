from collections import OrderedDict

import logging
import pandas as pd
from binance.streams import ThreadedWebsocketManager
from threading import Lock
from typing import Optional, List, Union, Dict

from btlib.event import BarFinishedEvent
from btlib.event import EventTypeDrivenEngine
from btlib.time_utils import timestamp_to_time

rename_dict = {'t': 'timestamp', "s": "symbol", "i": "interval", "n": "count",
                   'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'q': 'volume', 'v': 'amount',
                   "Q": 'buy_volume', 'V': 'buy_amount'}
columns_to_convert = ['open', 'close', 'high', 'low', 'amount', 'volume', 'buy_amount', 'buy_volume']


class BarBuffer(object):

    __slots__ = ("symbols", "_data", "time", "update_time")

    def __init__(self, symbols: Union[str, List[str]]):
        self.symbols: List[str] = symbols if isinstance(symbols, list) else (symbols)

        self.time: Optional[int] = None
        self._data: Optional[Dict[str, Optional[dict]]] = None
        self.update_time: Optional[Dict[str, int]] = OrderedDict({symbol: 0 for symbol in symbols})

        self._init_buf(self.symbols)

    def _init_buf(self, symbols):
        self.time = None
        self._data = {symbol: None for symbol in symbols}
        self.update_time = {symbol: 0 for symbol in symbols}

    def clean(self):
        self._init_buf(self.symbols)

    def update(self, msg):
        symbol = msg["data"]["s"]  # 注意msg结构中的data键
        kline = msg["data"]["k"]
        if not self.time:
            self.time = msg["data"]["k"]["t"]
        if symbol in self.symbols:
            if self._data[symbol] is None or (
                    self.update_time[symbol] < msg["data"]["E"] and not self._data[symbol]["x"]):
                self._data[symbol] = kline
                self.update_time[symbol] = msg["data"]["E"]

    @property
    def data(self):
        return self._data

    def to_dataframe(self) -> pd.DataFrame:
        df = pd.DataFrame(list(self._data.values()))
        df = df.rename(columns=rename_dict)
        df = df.drop(columns=['T', 'x', 'f', 'L', 'B'])
        for col in columns_to_convert:
            df[col] = df[col].astype(float)
        return df

    @property
    def is_finished(self):
        for val in self._data.values():
            if (val is None) or (not val["x"]):
                return False
        return True

    def __repr__(self):
        repr_str = []
        repr_str.append(f"Bar time: {self.time}\n")
        repr_str.append(f"Bar symbols: {self.symbols}\n")
        for key, val in self._data.items():
            repr_str.append(f"{key}: {val}\n")
        return ''.join(repr_str)

    # @property
    # def time(self):
    #     return self.time


class KlineBuffer(object):
    def __init__(self, max_size: int, symbols: list):
        self.max_size = max_size
        self.symbols = symbols
        self.buffers: OrderedDict[int, BarBuffer] = OrderedDict()
        self.lock = Lock()
        self.current_bar_time: int = 0
        # self.BarBufferQuery = namedtuple(typename="BarBufferQuery", field_names=symbols)

    def update(self, msg):
        timestamp = msg["data"]["k"]["t"]
        with self.lock:

            if timestamp > self.current_bar_time:
                if len(self.buffers) >= self.max_size:
                    self.buffers.popitem(last=False)  # 移除最旧的数据
                self.buffers[timestamp] = BarBuffer(self.symbols)
                self.current_bar_time = timestamp
            self.buffers[timestamp].update(msg)

    def get_data(self, timestamp: int):
        if timestamp in self.buffers:
            return self.buffers.get(timestamp)
        return None

    def __repr__(self):
        repr_str = []
        for key, val in self.buffers.items():
            repr_str.append(f"Bar time: {key}\n{val}")
        return ''.join(repr_str)


class WebsocketManager(object):
    kline_intervals = ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"]
    _instance = None
    _lock = Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(WebsocketManager, cls).__new__(cls)
        return cls._instance

    def __init__(self, api_key: Optional[str], api_secret: Optional[str],
                 event_engine: Optional[EventTypeDrivenEngine],
                 testnet: bool = False):
        if not hasattr(self, '_initialized'):  # 防止__init__方法的重复调用
            self.twm = ThreadedWebsocketManager(api_key=api_key, api_secret=api_secret,
                                                testnet=testnet)
            self.twm.start()
            self._kline_buffers: Dict[str: KlineBuffer] = {}
            self._lock = Lock()
            self.event_engine: EventTypeDrivenEngine = event_engine
            self.logger = logging.getLogger(self.__class__.__name__)
            self.logger.setLevel(logging.DEBUG)
            self.logger.addHandler(logging.StreamHandler())

    def start_future_kline(self, symbols: list, interval: str):
        if interval not in WebsocketManager.kline_intervals:
            raise ValueError(f"Interval should be in {WebsocketManager.kline_intervals}, but got {interval}.")
        self._kline_buffers[interval] = KlineBuffer(max_size=5, symbols=symbols)
        streams = [f"{symbol.lower()}@kline_{interval}" for symbol in symbols]
        self.twm.start_futures_multiplex_socket(callback=self._update_future_kline_buffer, streams=streams)
        self.logger.debug(f"Start {len(symbols)} futures {interval} kline websocket streams.")

    def _update_future_kline_buffer(self, msg):
        # print(msg)
        # 解析消息并更新缓存
        self.logger.debug(f"Get {msg['data']['s']}'s {msg['data']['k']['i']} msg.")
        with self._lock:  # 确保线程安全
            # symbol = msg["data"]["s"]
            # kline = msg["data"]["k"]
            timestamp = msg["data"]["k"]["t"]
            interval = msg["data"]["k"]["i"]
            # 更新数据
            self._kline_buffers[interval].update(msg)

            # 检查bar是否完成，完成后发送BarFinishedEvent
            if self._kline_buffers[interval].get_data(timestamp=timestamp).is_finished:
                self.logger.info(f"{interval} bar for {timestamp_to_time(timestamp)} finished.")
                self.logger.debug(self._kline_buffers[interval].get_data(timestamp=timestamp).to_dataframe())
                if self.event_engine:
                    self.event_engine.put(BarFinishedEvent(timestamp=timestamp, interval=interval,
                                                           data=self._kline_buffers[interval].get_data(timestamp).to_dataframe()))

    @property
    def running(self):
        return self.twm._running

    def get_data(self, timestamp: int, interval: str):
        if interval in self._kline_buffers:
            return self._kline_buffers[interval].get_data(timestamp)
        return None

    def stop(self):
        self.twm.stop()


if __name__ == "__main__":

    from load_accout import load_account
    import time
    import json

    with open("futures_info.json", "r") as f:
        futures_info = json.load(f)
    api_key, api_secret = load_account("account.json")

    # symbols = ["BTCUSDT", "ETHUSDT", "DOGEUSDT", "SOLUSDT", "LINKUSDT"]
    symbols = list(futures_info.keys())


    def get_ws_names(symbols: list, interval: str):
        res = []
        for symbol in symbols:
            res.append(f"{symbol.lower()}@kline_{interval}")
        return res

    wbm = WebsocketManager(api_key=api_key, api_secret=api_secret, event_engine=None)
    wbm.start_future_kline(symbols=symbols[:10], interval="1m")
    wbm.start_future_kline(symbols=symbols[:10], interval="3m")
    wbm.start_future_kline(symbols=symbols[:10], interval="5m")
    time.sleep(301)
    wbm.stop()

