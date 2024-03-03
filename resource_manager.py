from threading import Lock
from typing import Callable
import logging
import binance

from btlib.data_downloader import DataDownloader
from btlib.event import EventTypeDrivenEngine
from btlib.backtest import BacktestAbstract, DefaultBacktest
from btlib.strategy import BaseStrategy
from btlib.time_utils import adjust_time_period_to_frequency, now_ts, get_interval
from btlib.websocket_manager import WebsocketManager
from btlib.position_manager import PositionManager
from btlib.storage import ParquetDatabase

import pathlib


class ResourceManager:
    _instance = None
    _lock = Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(ResourceManager, cls).__new__(cls)
        return cls._instance

    def __init__(self, api_key: str = None, api_secret: str = None, db_path: str = None, testnet: bool = True):
        if not hasattr(self, '_initialized'):  # 防止__init__方法的重复调用
            self.logger = logging.getLogger(self.__class__.__name__)
            self.api_key: str = api_key
            self.api_secret: str = api_secret
            self.db_path: str = db_path
            self.testnet: bool = testnet

            self.client = None
            self.async_client = None

            self.request_rate_limiter = None
            self.order_rate_limiter = None

            self.database = None
            self.event_engine = None
            self.data_downloader = None
            self.websocket_manager = None

            self.strategy = None
            self.running = False

            # Run time data
            self.exchange_info: dict = None
            self.futures_info: dict = None


            self.position_manager: PositionManager = None
            self.backtest_impl = None
            self.__init: bool = False

    def init(self):
        if self.db_path:
            self.database = ParquetDatabase(db_path=self.db_path)
        else:
            self.logger.warning("No database path provided.")
        self.client = binance.Client(api_key=self.api_key, api_secret=self.api_secret)
        self.async_client = binance.AsyncClient(api_key=self.api_key, api_secret=self.api_secret)
        self.event_engine = EventTypeDrivenEngine()
        self.data_downloader = DataDownloader(api_key=self.api_key, api_secret=self.api_secret, database=self.database)
        self.futures_info, self.exchange_info = self.data_downloader.get_futures_exchange_info()
        self.backtest_impl = DefaultBacktest(strategy=None,
                                             database=self.database,
                                             futures_info=self.futures_info)
        self.websocket_manager = WebsocketManager(api_key=self.api_key, api_secret=self.api_secret,
                                                event_engine=self.event_engine, testnet=self.testnet)
        self.running = False
        self.__init: bool = True

    def register_strategy(self, new_strategy: BaseStrategy):
        if self.strategy:
            print(UserWarning("Warning: Only one strategy can be run at a time. "
                              "Running a new strategy will replace the current one."))
        self.strategy = new_strategy(self.event_engine)
        self.position_manager = PositionManager(api_key=self.api_key, api_secret=self.api_secret,
                                                futures_info=self.futures_info, event_manager=self.event_engine,
                                                websocket_manager=self.websocket_manager,
                                                symbols=self.strategy.data_symbols)

    def register_event_handler(self, event_type: type, handler: Callable):
        self.event_engine.register(event_type=event_type, handler=handler)

    def unregister_event_handler(self, event_type: type, handler: Callable):
        self.event_engine.unregister(event_type=event_type, handler=handler)

    def init_strategy_data(self):
        _, end_time = adjust_time_period_to_frequency(start_time=None, end_time=now_ts(),
                                                      interval_size=get_interval(self.strategy.data_interval))
        start_time = end_time - self.strategy.data_length * get_interval(self.strategy.data_interval)
        data = self.data_downloader.download_history_kline(symbols=self.strategy.data_symbols,
                                                           interval=self.strategy.data_interval
                                                           , start_time=start_time, end_time=end_time)
        self.strategy.update_data(data=data)

    def start_trading(self):
        if self.websocket_manager and self.event_engine and self.strategy and self.position_manager:
            self.event_engine.start()
            self.position_manager.start()

            self.init_strategy_data()
            self.strategy.start()

            self.websocket_manager.start_future_kline(self.strategy.data_symbols, self.strategy.data_interval)
            self.running = True
        else:
            raise RuntimeError(...)

    def stop_trading(self):
        self.event_engine.stop()
        if self.position_manager:
            self.position_manager.stop()
        if self.strategy:
            self.strategy.stop()
        self.websocket_manager.stop()
        self.running = False

    def backtest(self,
                 strategy: BaseStrategy=None,
                 start_time: int = None,
                 end_time: int = None,
                 chunk_size: int = None,
                 initial_capital: float = 1e5,  # USDT
                 fee_rate: float = 5e-4,  # TAKER Fee Rate
                 ):
        strategy = strategy if strategy else self.strategy
        if not strategy:
            raise ValueError("No strategy implement.")
        self.backtest_impl(strategy=strategy, start_time=start_time, end_time=end_time, chunk_size=chunk_size,
                           initial_capital=initial_capital, fee_rate=fee_rate)
        self.backtest_impl.plot()
        self.backtest_impl.info()
        return self.backtest_impl
